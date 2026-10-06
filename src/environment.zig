//! Owns the explicit child environment shared by workstation-tool execution.
//!
//! There is one recipe: start from the embedding process environment, normalize
//! it once through a bounded login-shell snapshot, and pass that exact result to
//! command, shell, or Walker job launch. No second environment owner
//! participates in execution.

const builtin = @import("builtin");
const std = @import("std");
const process = @import("process.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Environ = std.process.Environ;

const query_timeout = Io.Duration.fromSeconds(2);

/// Optional environment marker used only while resolving the login-shell environment.
pub const Marker = struct { name: []const u8, value: []const u8 };
var process_environment_mutex: Io.Mutex = .init;

/// Closed failures while acquiring or applying the child environment.
pub const Error = process.Error || error{
    OutOfMemory,
    SessionEnvironmentUnavailable,
    InvalidSessionEnvironment,
    CommandNotFound,
};

/// Builds one owned child environment from the embedding process environment,
/// then normalizes it through one bounded login-shell snapshot.
pub fn current(
    init: std.process.Init,
    allocator: Allocator,
    operator_marker: ?Marker,
) Error!Environ.Map {
    process_environment_mutex.lockUncancelable(init.io);
    var env = init.environ_map.clone(allocator) catch {
        process_environment_mutex.unlock(init.io);
        return error.OutOfMemory;
    };
    process_environment_mutex.unlock(init.io);
    errdefer env.deinit();

    if (env.get("HOME") == null or env.get("PATH") == null)
        return error.InvalidSessionEnvironment;

    try env.put("HISTFILE", "/dev/null");
    if (operator_marker) |marker| try env.put(marker.name, marker.value);

    var login = try process.run(
        init.gpa,
        init.io,
        &.{
            "/bin/bash",
            "-lc",
            "export HISTFILE=/dev/null; readonly HISTFILE; set +o history; exec /usr/bin/env -0",
        },
        env.get("HOME").?,
        null,
        query_timeout,
        &env,
    );
    defer login.deinit(init.gpa);
    if (login.timed_out or login.term == null or
        !termSucceeded(login.term.?) or login.truncated)
    {
        return error.SessionEnvironmentUnavailable;
    }

    var shell_env = Environ.Map.init(allocator);
    errdefer shell_env.deinit();
    try parseNulEnvironment(&shell_env, login.stdout);
    if (shell_env.get("HOME") == null or shell_env.get("PATH") == null)
        return error.InvalidSessionEnvironment;
    try shell_env.put("HISTFILE", "/dev/null");
    if (operator_marker) |marker| _ = shell_env.swapRemove(marker.name);

    env.deinit();
    return shell_env;
}

fn parseNulEnvironment(env: *Environ.Map, bytes: []const u8) Error!void {
    var entries = std.mem.splitScalar(u8, bytes, 0);
    while (entries.next()) |entry| {
        if (entry.len == 0) continue;
        const split = std.mem.indexOfScalar(u8, entry, '=') orelse
            return error.InvalidSessionEnvironment;
        const key = entry[0..split];
        const value = entry[split + 1 ..];
        if (!Environ.Map.validateKeyForPut(key))
            return error.InvalidSessionEnvironment;
        try env.put(key, value);
    }
}

/// Resolves argv[0] without a slash against the explicit child PATH.
pub fn resolveExecutable(
    io: Io,
    allocator: Allocator,
    env: *const Environ.Map,
    cwd: []const u8,
    name: []const u8,
) Error![]const u8 {
    if (std.mem.indexOfScalar(u8, name, '/') != null)
        return allocator.dupe(u8, name) catch error.OutOfMemory;
    const path = env.get("PATH") orelse return error.InvalidSessionEnvironment;
    var entries = std.mem.splitScalar(u8, path, ':');
    while (entries.next()) |entry| {
        const base = if (entry.len == 0) cwd else entry;
        const candidate = if (std.fs.path.isAbsolute(base))
            std.fs.path.join(allocator, &.{ base, name }) catch
                return error.OutOfMemory
        else
            std.fs.path.join(allocator, &.{ cwd, base, name }) catch
                return error.OutOfMemory;
        const stat = Io.Dir.cwd().statFile(io, candidate, .{}) catch {
            allocator.free(candidate);
            continue;
        };
        if (stat.kind == .file) {
            if (Io.Dir.accessAbsolute(io, candidate, .{ .execute = true })) |_|
                return candidate
            else |_| {}
        }
        allocator.free(candidate);
    }
    return error.CommandNotFound;
}

fn termSucceeded(term: std.process.Child.Term) bool {
    return switch (term) {
        .exited => |code| code == 0,
        else => false,
    };
}

test "NUL environment parser preserves embedded newlines" {
    var env = Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try parseNulEnvironment(
        &env,
        "PATH=/usr/bin\x00VALUE=line one\nline two\x00",
    );
    try std.testing.expectEqualStrings("/usr/bin", env.get("PATH").?);
    try std.testing.expectEqualStrings(
        "line one\nline two",
        env.get("VALUE").?,
    );
}

test "child PATH resolution prefers the supplied environment" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(
        std.testing.io,
        ".",
        std.testing.allocator,
    );
    defer std.testing.allocator.free(root);
    const local = try std.fs.path.join(
        std.testing.allocator,
        &.{ root, "local" },
    );
    defer std.testing.allocator.free(local);
    try Io.Dir.cwd().createDir(std.testing.io, local, .default_dir);
    const executable = try std.fs.path.join(
        std.testing.allocator,
        &.{ local, "session-command" },
    );
    defer std.testing.allocator.free(executable);
    const file = try Io.Dir.cwd().createFile(
        std.testing.io,
        executable,
        .{ .permissions = .fromMode(0o700) },
    );
    file.close(std.testing.io);

    var env = Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("PATH", local);
    const resolved = try resolveExecutable(
        std.testing.io,
        std.testing.allocator,
        &env,
        root,
        "session-command",
    );
    defer std.testing.allocator.free(resolved);
    try std.testing.expectEqualStrings(executable, resolved);
    try std.testing.expectError(
        error.CommandNotFound,
        resolveExecutable(
            std.testing.io,
            std.testing.allocator,
            &env,
            root,
            "missing-command",
        ),
    );
}

test "child environment starts from explicit embedding process state" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var map = Environ.Map.init(std.testing.allocator);
    defer map.deinit();
    try map.put("HOME", "/tmp");
    try map.put("PATH", "/usr/bin:/bin");
    try map.put("WORKSTATION_SOURCE_CANARY", "process-owned");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const init: std.process.Init = .{
        .minimal = undefined,
        .arena = &arena,
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .environ_map = &map,
        .preopens = undefined,
    };
    var env = try current(init, std.testing.allocator, null);
    defer env.deinit();
    try std.testing.expectEqualStrings(
        "process-owned",
        env.get("WORKSTATION_SOURCE_CANARY").?,
    );
    try std.testing.expectEqualStrings("/dev/null", env.get("HISTFILE").?);
}

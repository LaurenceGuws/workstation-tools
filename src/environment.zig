//! Owns the fresh logged-in user environment inherited by workstation-tool child processes.
//!
//! Long-lived agent services cannot treat their startup environment as the current desktop/session environment. The systemd
//! user manager is the Linux session handoff point; each payload takes one bounded fresh snapshot immediately before spawn.

const std = @import("std");
const process = @import("process.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Environ = std.process.Environ;

const systemctl = "/usr/bin/systemctl";
const query_timeout = Io.Duration.fromSeconds(2);

/// Optional environment marker used only while resolving the login-shell environment.
pub const Marker = struct { name: []const u8, value: []const u8 };
var process_environment_mutex: Io.Mutex = .init;

/// Closed failures while acquiring or applying the current user-session environment.
pub const Error = process.Error || error{
    OutOfMemory,
    SessionEnvironmentUnavailable,
    InvalidSessionEnvironment,
    CommandNotFound,
};

/// Builds one owned child environment from the current systemd user-manager environment.
///
/// Stable identity fields fall back to the host process only when the manager does not publish them. Session-scoped graphical
/// fields intentionally do not fall back: absence in the manager means the current session does not own them.
pub fn current(init: std.process.Init, allocator: Allocator, operator_marker: ?Marker) Error!Environ.Map {
    var observed = try process.run(
        init.gpa,
        init.io,
        &.{ systemctl, "--user", "show-environment" },
        "/",
        null,
        query_timeout,
        null,
    );
    defer observed.deinit(init.gpa);
    if (observed.timed_out or observed.term == null or !termSucceeded(observed.term.?) or observed.truncated) {
        return error.SessionEnvironmentUnavailable;
    }

    var env = Environ.Map.init(allocator);
    errdefer env.deinit();
    try parseManagerEnvironment(&env, observed.stdout);

    // These are stable process/user facts, not graphical-session authority. Keep command execution useful on a minimal
    // headless user manager while still allowing a published manager value to win.
    const fallback_keys = [_][]const u8{
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "PATH",
        "DBUS_SESSION_BUS_ADDRESS",
        "SSH_AUTH_SOCK",
        "XDG_RUNTIME_DIR",
    };
    {
        process_environment_mutex.lockUncancelable(init.io);
        defer process_environment_mutex.unlock(init.io);
        for (fallback_keys) |key| {
            if (env.get(key) == null) {
                if (init.environ_map.get(key)) |value| try env.put(key, value);
            }
        }
    }
    if (env.get("HOME") == null or env.get("PATH") == null) return error.InvalidSessionEnvironment;

    // Keep the login-shell environment probe completely outside the user's interactive history from process start.
    try env.put("HISTFILE", "/dev/null");

    // A consumer may select a richer login profile for the snapshot without leaking that selector into the child payload.
    if (operator_marker) |marker| try env.put(marker.name, marker.value);

    // The manager owns live session facts; the consumer-selected Bash profile owns its normal model-facing work environment.
    // Compose them without executing the requested payload through Bash: one bounded env snapshot is then inherited by the
    // exact argv child. `env -0` preserves arbitrary non-NUL environment bytes including embedded newlines.
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
    if (login.timed_out or login.term == null or !termSucceeded(login.term.?) or login.truncated) {
        return error.SessionEnvironmentUnavailable;
    }

    var shell_env = Environ.Map.init(allocator);
    errdefer shell_env.deinit();
    try parseNulEnvironment(&shell_env, login.stdout);
    if (shell_env.get("HOME") == null or shell_env.get("PATH") == null) return error.InvalidSessionEnvironment;
    try shell_env.put("HISTFILE", "/dev/null");
    if (operator_marker) |marker| {
        if (shell_env.swapRemove(marker.name)) {
            // The profile selector is internal probe state, never payload environment.
        }
    }
    env.deinit();
    return shell_env;
}

fn parseManagerEnvironment(env: *Environ.Map, bytes: []const u8) Error!void {
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const split = std.mem.indexOfScalar(u8, line, '=') orelse return error.InvalidSessionEnvironment;
        const key = line[0..split];
        const value = line[split + 1 ..];
        if (!Environ.Map.validateKeyForPut(key)) return error.InvalidSessionEnvironment;
        try env.put(key, value);
    }
}

fn parseNulEnvironment(env: *Environ.Map, bytes: []const u8) Error!void {
    var entries = std.mem.splitScalar(u8, bytes, 0);
    while (entries.next()) |entry| {
        if (entry.len == 0) continue;
        const split = std.mem.indexOfScalar(u8, entry, '=') orelse return error.InvalidSessionEnvironment;
        const key = entry[0..split];
        const value = entry[split + 1 ..];
        if (!Environ.Map.validateKeyForPut(key)) return error.InvalidSessionEnvironment;
        try env.put(key, value);
    }
}

/// Resolves an argv[0] without a slash against the fresh child environment PATH, not the host service startup PATH.
/// The returned path is owned by `allocator`.
pub fn resolveExecutable(
    io: Io,
    allocator: Allocator,
    env: *const Environ.Map,
    cwd: []const u8,
    name: []const u8,
) Error![]const u8 {
    if (std.mem.indexOfScalar(u8, name, '/') != null) return allocator.dupe(u8, name) catch error.OutOfMemory;
    const path = env.get("PATH") orelse return error.InvalidSessionEnvironment;
    var entries = std.mem.splitScalar(u8, path, ':');
    while (entries.next()) |entry| {
        const base = if (entry.len == 0) cwd else entry;
        const candidate = if (std.fs.path.isAbsolute(base))
            std.fs.path.join(allocator, &.{ base, name }) catch return error.OutOfMemory
        else
            std.fs.path.join(allocator, &.{ cwd, base, name }) catch return error.OutOfMemory;
        const stat = Io.Dir.cwd().statFile(io, candidate, .{}) catch {
            allocator.free(candidate);
            continue;
        };
        if (stat.kind == .file) {
            if (Io.Dir.accessAbsolute(io, candidate, .{ .execute = true })) |_| {
                return candidate;
            } else |_| {}
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

test "manager environment parser preserves values and history isolation overrides later" {
    var env = Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try parseManagerEnvironment(
        &env,
        "PATH=/usr/bin:/home/test/.local/bin\nWAYLAND_DISPLAY=wayland-9\nXDG_CURRENT_DESKTOP=Hyprland\nVALUE=two words\n",
    );
    try std.testing.expectEqualStrings("/usr/bin:/home/test/.local/bin", env.get("PATH").?);
    try std.testing.expectEqualStrings("wayland-9", env.get("WAYLAND_DISPLAY").?);
    try std.testing.expectEqualStrings("Hyprland", env.get("XDG_CURRENT_DESKTOP").?);
    try std.testing.expectEqualStrings("two words", env.get("VALUE").?);
}

test "NUL environment parser preserves embedded newlines" {
    var env = Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try parseNulEnvironment(&env, "PATH=/usr/bin\x00VALUE=line one\nline two\x00");
    try std.testing.expectEqualStrings("/usr/bin", env.get("PATH").?);
    try std.testing.expectEqualStrings("line one\nline two", env.get("VALUE").?);
}

test "manager environment parser rejects malformed entries" {
    var env = Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try std.testing.expectError(error.InvalidSessionEnvironment, parseManagerEnvironment(&env, "NOT-AN-ASSIGNMENT\n"));
}

test "child PATH resolution prefers the supplied session environment" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const local = try std.fs.path.join(std.testing.allocator, &.{ root, "local" });
    defer std.testing.allocator.free(local);
    try Io.Dir.cwd().createDir(std.testing.io, local, .default_dir);
    const executable = try std.fs.path.join(std.testing.allocator, &.{ local, "session-command" });
    defer std.testing.allocator.free(executable);
    const file = try Io.Dir.cwd().createFile(std.testing.io, executable, .{ .permissions = .fromMode(0o700) });
    file.close(std.testing.io);

    var env = Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("PATH", local);
    const resolved = try resolveExecutable(std.testing.io, std.testing.allocator, &env, root, "session-command");
    defer std.testing.allocator.free(resolved);
    try std.testing.expectEqualStrings(executable, resolved);
    try std.testing.expectError(
        error.CommandNotFound,
        resolveExecutable(std.testing.io, std.testing.allocator, &env, root, "missing-command"),
    );
}

//! Owns bounded local short-process execution and process-group cleanup.
//!
//! Short commands borrow their invocation and return owned bounded streams. systemd owns durable-job process lifetime;
//! durable job receipts live in `jobs.zig`, and MCP/tool naming lives above this module.

const builtin = @import("builtin");
const std = @import("std");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Maximum stdin bytes admitted to one short or durable child invocation.
///
/// The Go short-command lane historically admitted 128 KiB while durable jobs used 32 KiB. The Zig rewrite initially
/// unified both on the smaller value. Since stdin now progresses concurrently with stdout/stderr under the same absolute
/// deadline, restore the useful 128 KiB text-input capacity without changing the independent 256 KiB encoded MCP request
/// envelope or the 32 KiB argv bound.
pub const max_stdin_bytes: usize = 128 * 1024;
/// Maximum stdout or stderr bytes retained from one short child invocation.
pub const max_stream_bytes: usize = 256 * 1024;
/// Maximum argv entries admitted to one exact short child invocation.
pub const max_arguments: usize = 256;
/// Maximum combined argv bytes admitted to one local child invocation.
pub const max_argv_bytes: usize = 32 * 1024;

/// Closed failures from bounded child execution and process-group cleanup.
pub const Error = error{
    OutOfMemory,
    UnsupportedPlatform,
    InvalidInvocation,
    SpawnFailed,
    StdinWriteFailed,
    StreamFailed,
    WaitFailed,
    SignalFailed,
};

/// One bounded completed short-process observation. The caller owns both streams.
pub const Result = struct {
    stdout: []u8,
    stderr: []u8,
    term: ?std.process.Child.Term,
    timed_out: bool,
    truncated: bool,

    /// Releases both retained streams and invalidates the result.
    pub fn deinit(self: *Result, allocator: Allocator) void {
        allocator.free(self.stderr);
        allocator.free(self.stdout);
        self.* = undefined;
    }
};

/// Runs one local process group to completion with one absolute timeout and bounded retained streams.
pub fn run(
    allocator: Allocator,
    io: Io,
    argv: []const []const u8,
    cwd: []const u8,
    stdin: ?[]const u8,
    timeout: Io.Duration,
    environ_map: ?*const std.process.Environ.Map,
) Error!Result {
    if (comptime builtin.os.tag != .linux) return error.UnsupportedPlatform;
    if (argv.len == 0 or argv.len > max_arguments or cwd.len == 0) return error.InvalidInvocation;
    var argv_bytes: usize = 0;
    for (argv) |argument| {
        if (argument.len == 0) return error.InvalidInvocation;
        argv_bytes = std.math.add(usize, argv_bytes, argument.len) catch return error.InvalidInvocation;
        if (argv_bytes > max_argv_bytes) return error.InvalidInvocation;
    }
    if (stdin) |bytes| if (bytes.len > max_stdin_bytes) return error.InvalidInvocation;

    var child = std.process.spawn(io, .{
        .argv = argv,
        .cwd = .{ .path = cwd },
        .stdin = if (stdin == null) .ignore else .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
        .pgid = 0,
        .environ_map = environ_map,
    }) catch return error.SpawnFailed;
    defer if (child.id != null) terminateGroup(&child, io);
    std.debug.assert(child.id != null);
    std.debug.assert(child.stdout != null);
    std.debug.assert(child.stderr != null);

    // Domain invariant: the absolute runtime bound covers every child-facing byte, including stdin. Writing input before
    // draining output can deadlock on finite pipes and would leave that phase outside the timeout entirely.
    const deadline = Io.Clock.Timestamp.fromNow(io, .{ .raw = timeout, .clock = .awake });
    var input_task = if (stdin) |bytes| task: {
        std.debug.assert(child.stdin != null);
        const input = child.stdin.?;
        child.stdin = null;
        break :task io.concurrent(writeInput, .{ io, input, bytes }) catch return error.StdinWriteFailed;
    } else null;
    defer {
        if (input_task) |*task| task.cancel(io) catch {};
    }

    var storage: Io.File.MultiReader.Buffer(2) = undefined;
    var reader: Io.File.MultiReader = undefined;
    reader.init(allocator, io, storage.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer reader.deinit();
    const stdout = reader.reader(0);
    const stderr = reader.reader(1);
    var stdout_retained = Io.Writer.Allocating.initCapacity(allocator, max_stream_bytes) catch return error.OutOfMemory;
    defer stdout_retained.deinit();
    var stderr_retained = Io.Writer.Allocating.initCapacity(allocator, max_stream_bytes) catch return error.OutOfMemory;
    defer stderr_retained.deinit();
    var timed_out = false;
    var truncated = false;

    while (true) {
        reader.fill(64, if (timed_out) .none else .{ .deadline = deadline }) catch |failure| switch (failure) {
            error.EndOfStream => {
                try retainBounded(&stdout_retained, stdout, &truncated);
                try retainBounded(&stderr_retained, stderr, &truncated);
                break;
            },
            error.Timeout => {
                timed_out = true;
                signalGroup(@intCast(child.id.?), .KILL) catch |signal_failure| switch (signal_failure) {
                    error.ProcessNotFound => {},
                    else => return error.SignalFailed,
                };
                continue;
            },
            else => return error.StreamFailed,
        };
        try retainBounded(&stdout_retained, stdout, &truncated);
        try retainBounded(&stderr_retained, stderr, &truncated);
        if (!timed_out and deadline.untilNow(io).raw.nanoseconds >= 0) {
            timed_out = true;
            signalGroup(@intCast(child.id.?), .KILL) catch |signal_failure| switch (signal_failure) {
                error.ProcessNotFound => {},
                else => return error.SignalFailed,
            };
        }
    }
    reader.checkAnyError() catch return error.StreamFailed;
    const term = child.wait(io) catch return error.WaitFailed;
    if (input_task) |*task| {
        task.await(io) catch {
            if (!timed_out) return error.StdinWriteFailed;
        };
    }
    const stdout_copy = stdout_retained.toOwnedSlice() catch return error.OutOfMemory;
    errdefer allocator.free(stdout_copy);
    const stderr_copy = stderr_retained.toOwnedSlice() catch return error.OutOfMemory;
    return .{
        .stdout = stdout_copy,
        .stderr = stderr_copy,
        .term = if (timed_out) null else term,
        .timed_out = timed_out,
        .truncated = truncated,
    };
}

fn writeInput(io: Io, file: Io.File, bytes: []const u8) Io.File.Writer.Error!void {
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
}

fn retainBounded(output: *Io.Writer.Allocating, input: *Io.Reader, truncated: *bool) Error!void {
    const bytes = input.buffered();
    const remaining = max_stream_bytes - output.writer.buffered().len;
    const retained = @min(bytes.len, remaining);
    output.writer.writeAll(bytes[0..retained]) catch return error.OutOfMemory;
    if (retained != bytes.len) truncated.* = true;
    input.tossBuffered();
}

fn signalGroup(pid: std.posix.pid_t, signal: std.posix.SIG) std.posix.KillError!void {
    if (comptime builtin.os.tag != .linux) return error.Unexpected;
    return std.posix.kill(-pid, signal);
}

fn terminateGroup(child: *std.process.Child, io: Io) void {
    const pid: std.posix.pid_t = @intCast(child.id orelse return);
    signalGroup(pid, .KILL) catch {};
    child.kill(io);
}

test "bounded process keeps stdout and stderr separate" {
    var result = try run(
        std.testing.allocator,
        std.testing.io,
        &.{ "sh", "-c", "printf out; printf err >&2" },
        "/tmp",
        null,
        .fromSeconds(2),
        null,
    );
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("out", result.stdout);
    try std.testing.expectEqualStrings("err", result.stderr);
    try std.testing.expectEqual(@as(u8, 0), result.term.?.exited);
    try std.testing.expect(!result.timed_out);
    try std.testing.expect(!result.truncated);
}

test "excess process output is retained to the exact bound" {
    var result = try run(
        std.testing.allocator,
        std.testing.io,
        &.{ "sh", "-c", "yes o | head -c 262145; yes e | head -c 262146 >&2" },
        "/tmp",
        null,
        .fromSeconds(2),
        null,
    );
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(max_stream_bytes, result.stdout.len);
    try std.testing.expectEqual(max_stream_bytes, result.stderr.len);
    try std.testing.expectEqualStrings("o\n", result.stdout[0..2]);
    try std.testing.expectEqualStrings("e\n", result.stderr[0..2]);
    try std.testing.expectEqual(@as(u8, 0), result.term.?.exited);
    try std.testing.expect(!result.timed_out);
    try std.testing.expect(result.truncated);
}

test "timeout terminates descendants in the child process group" {
    if (builtin.os.tag != .linux) return;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    var result = try run(
        std.testing.allocator,
        std.testing.io,
        &.{ "sh", "-c", "yes x & echo $! > child.pid; wait" },
        root,
        null,
        .fromMilliseconds(100),
        null,
    );
    defer result.deinit(std.testing.allocator);
    try std.testing.expect(result.timed_out);
    try std.testing.expect(result.truncated);
    try std.testing.expectEqual(max_stream_bytes, result.stdout.len);

    const pid_bytes = try temporary.dir.readFileAlloc(
        std.testing.io,
        "child.pid",
        std.testing.allocator,
        .limited(32),
    );
    defer std.testing.allocator.free(pid_bytes);
    const pid = try std.fmt.parseInt(std.posix.pid_t, std.mem.trim(u8, pid_bytes, "\r\n "), 10);
    var path_buffer: [64]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, "/proc/{d}", .{pid});
    for (0..200) |_| {
        var directory = Io.Dir.openDirAbsolute(std.testing.io, path, .{}) catch |failure| switch (failure) {
            error.FileNotFound => return,
            else => return failure,
        };
        directory.close(std.testing.io);
        try Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    return error.TestUnexpectedResult;
}

test "timeout includes stdin rejected before the child exits" {
    var input: [max_stdin_bytes]u8 = @splat('x');
    var result = try run(
        std.testing.allocator,
        std.testing.io,
        &.{ "sh", "-c", "exec 0<&-; sleep 2" },
        "/tmp",
        &input,
        .fromMilliseconds(100),
        null,
    );
    defer result.deinit(std.testing.allocator);
    try std.testing.expect(result.timed_out);
    try std.testing.expect(result.term == null);
}

test "stdin above the admitted bound is rejected before spawn" {
    var input: [max_stdin_bytes + 1]u8 = @splat('x');
    try std.testing.expectError(
        error.InvalidInvocation,
        run(
            std.testing.allocator,
            std.testing.io,
            &.{"true"},
            "/tmp",
            &input,
            .fromSeconds(1),
            null,
        ),
    );
}

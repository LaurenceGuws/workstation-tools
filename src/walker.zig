//! Adapts the versioned Walker CLI. No socket protocol, process supervision, or backend fallback lives here.
const std = @import("std");
const process = @import("process.zig");
const state = @import("state.zig");
const Io = std.Io;
const A = std.mem.Allocator;

pub const Config = struct { executable: []const u8, home: []const u8 };
pub const Ref = struct { config: Config, run_id: []const u8, name: []const u8 };
pub const Error = process.Error || state.Error || error{
    WalkerUnavailable,
    WalkerInvalidResponse,
    WalkerRejected,
    WalkerSubmissionUncertain,
    WalkerOwnershipUnavailable,
    WalkerHistoryFull,
    OffsetOutOfRange,
    JobNotFound,
};
pub const State = enum { starting, running, stopping, exited, stopped, timed_out, failed, indeterminate };
pub const Meta = struct {
    schema: []const u8,
    run_id: []const u8,
    name: []const u8,
    state: State,
    timeout_ms: ?u32,
    output_limit_bytes: u32,
    exit_code: ?i32 = null,
    ended_at_ms: ?i64 = null,
    stdout_discarded_bytes: u64 = 0,
    stderr_discarded_bytes: u64 = 0,
};
pub const Slice = struct { encoding: []const u8, data: []const u8, next_offset: u64, eof: bool };
pub const Logs = struct {
    schema: []const u8,
    ok: bool,
    run_id: []const u8,
    name: []const u8,
    stdout: Slice,
    stderr: Slice,
};

pub fn validConfig(config: Config) bool {
    for ([_][]const u8{ config.executable, config.home }) |path| {
        if (!std.fs.path.isAbsolute(path) or path.len > state.max_path_bytes or
            std.mem.indexOfScalar(u8, path, 0) != null) return false;
    }
    return true;
}

/// Admission checks use only the explicit executable. Missing Walker never selects another process owner.
pub fn check(io: Io, config: Config) Error!void {
    if (!validConfig(config)) return error.WalkerUnavailable;
    Io.Dir.accessAbsolute(io, config.executable, .{ .execute = true }) catch return error.WalkerUnavailable;
}

pub fn launch(
    io: Io,
    a: A,
    ref: Ref,
    argv: []const []const u8,
    cwd: []const u8,
    stdin_path: ?[]const u8,
    timeout_seconds: u32,
    output_limit: usize,
    environment: *const std.process.Environ.Map,
) Error!void {
    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(a);
    try args.appendSlice(a, &.{
        "run",
        "--name",
        ref.name,
        "--run-id",
        ref.run_id,
        "--cwd",
        cwd,
        "--timeout-ms",
        try std.fmt.allocPrint(a, "{d}", .{@as(u64, timeout_seconds) * 1000}),
        "--output-limit-bytes",
        try std.fmt.allocPrint(a, "{d}", .{output_limit}),
    });
    if (stdin_path) |path| try args.appendSlice(a, &.{ "--stdin-file", path });
    try args.append(a, "--");
    try args.appendSlice(a, argv);
    const bytes = try exchange(io, a, ref.config, args.items, environment, true);
    const reply = std.json.parseFromSliceLeaky(struct {
        schema: []const u8,
        ok: bool,
        run_id: []const u8,
        name: []const u8,
    }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.WalkerSubmissionUncertain;
    if (!reply.ok or !eql(reply.schema, "walker/v2") or !eql(reply.run_id, ref.run_id) or !eql(reply.name, ref.name))
        return error.WalkerSubmissionUncertain;
}

pub fn inspect(io: Io, a: A, ref: Ref) Error!Meta {
    const bytes = try exchange(io, a, ref.config, &.{ "inspect", ref.run_id }, null, false);
    const reply = std.json.parseFromSliceLeaky(
        struct { schema: []const u8, ok: bool, animal: Meta },
        a,
        bytes,
        .{ .ignore_unknown_fields = true },
    ) catch return error.WalkerInvalidResponse;
    if (!reply.ok or !eql(reply.schema, "walker/v2")) return error.WalkerInvalidResponse;
    try validateMeta(reply.animal, ref);
    return reply.animal;
}

pub fn logs(io: Io, a: A, ref: Ref, stdout_offset: usize, stderr_offset: usize, max_bytes: usize) Error!Logs {
    const bytes = try exchange(io, a, ref.config, &.{
        "logs",
        ref.run_id,
        "--stdout-offset",
        try std.fmt.allocPrint(a, "{d}", .{stdout_offset}),
        "--stderr-offset",
        try std.fmt.allocPrint(a, "{d}", .{stderr_offset}),
        "--max-bytes",
        try std.fmt.allocPrint(a, "{d}", .{max_bytes}),
    }, null, false);
    var reply = std.json.parseFromSliceLeaky(Logs, a, bytes, .{ .ignore_unknown_fields = true }) catch
        return error.WalkerInvalidResponse;
    if (!reply.ok or !eql(reply.schema, "walker/v2") or !eql(reply.run_id, ref.run_id) or !eql(reply.name, ref.name))
        return error.WalkerInvalidResponse;
    reply.stdout.data = try decode(a, reply.stdout, stdout_offset, max_bytes);
    reply.stderr.data = try decode(a, reply.stderr, stderr_offset, max_bytes - reply.stdout.data.len);
    return reply;
}

pub fn stop(io: Io, a: A, ref: Ref) Error!bool {
    const bytes = try exchange(io, a, ref.config, &.{ "stop", ref.run_id }, null, true);
    const reply = std.json.parseFromSliceLeaky(struct {
        schema: []const u8,
        ok: bool,
        run_id: []const u8,
        stop_requested: bool,
        completed: bool,
    }, a, bytes, .{}) catch return error.WalkerSubmissionUncertain;
    if (!reply.ok or !eql(reply.schema, "walker/v2") or !eql(reply.run_id, ref.run_id))
        return error.WalkerSubmissionUncertain;
    return reply.stop_requested;
}

fn validateMeta(meta: Meta, ref: Ref) Error!void {
    if (!eql(meta.schema, "walker.run/v2") or !eql(meta.run_id, ref.run_id) or !eql(meta.name, ref.name) or
        meta.output_limit_bytes < 4096 or meta.output_limit_bytes > 512 * 1024 * 1024)
        return error.WalkerInvalidResponse;
    const timeout = meta.timeout_ms orelse return error.WalkerInvalidResponse;
    if (timeout == 0 or timeout > 86400000) return error.WalkerInvalidResponse;
}
fn decode(a: A, stream: Slice, offset: usize, limit: usize) Error![]const u8 {
    const data = if (eql(stream.encoding, "utf8")) stream.data else if (eql(stream.encoding, "base64")) blk: {
        const n = std.base64.standard.Decoder.calcSizeForSlice(stream.data) catch return error.WalkerInvalidResponse;
        if (n > limit) return error.WalkerInvalidResponse;
        const bytes = try a.alloc(u8, n);
        std.base64.standard.Decoder.decode(bytes, stream.data) catch return error.WalkerInvalidResponse;
        break :blk bytes;
    } else return error.WalkerInvalidResponse;
    if (data.len > limit or offset > std.math.maxInt(u64) - data.len or stream.next_offset != offset + data.len)
        return error.WalkerInvalidResponse;
    return data;
}

fn exchange(
    io: Io,
    a: A,
    config: Config,
    args: []const []const u8,
    source_env: ?*const std.process.Environ.Map,
    mutation: bool,
) Error![]const u8 {
    try check(io, config);
    var env = if (source_env) |value| try value.clone(a) else std.process.Environ.Map.init(a);
    defer env.deinit();
    try env.put("WALKER_HOME", config.home);
    const argv = try a.alloc([]const u8, args.len + 1);
    defer a.free(argv);
    argv[0] = config.executable;
    @memcpy(argv[1..], args);
    // Wrapper options do not consume the already-validated payload's argv budget.
    var result = process.runWithBudget(a, io, argv, "/", null, .fromSeconds(15), &env, .{
        .arguments = process.max_arguments + 20,
        .bytes = process.max_argv_bytes + 4 * state.max_path_bytes + 1024,
    }) catch |failure| return if (mutation and failure != error.SpawnFailed)
        error.WalkerSubmissionUncertain
    else
        error.WalkerUnavailable;
    defer result.deinit(a);
    if (result.timed_out or result.truncated or result.term == null)
        return if (mutation) error.WalkerSubmissionUncertain else error.WalkerUnavailable;
    if (!result.term.?.success()) {
        const failed = std.json.parseFromSliceLeaky(
            struct { ok: bool, @"error": []const u8 },
            a,
            result.stderr,
            .{ .ignore_unknown_fields = true },
        ) catch
            return if (mutation) error.WalkerSubmissionUncertain else error.WalkerInvalidResponse;
        return failureCode(failed.@"error");
    }
    if (result.stderr.len != 0) return if (mutation) error.WalkerSubmissionUncertain else error.WalkerInvalidResponse;
    return a.dupe(u8, result.stdout);
}
fn failureCode(code: []const u8) Error {
    if (eql(code, "SubmissionUncertain")) return error.WalkerSubmissionUncertain;
    if (eql(code, "AnimalNotFound") or eql(code, "FileNotFound")) return error.JobNotFound;
    if (eql(code, "OffsetOutOfRange")) return error.OffsetOutOfRange;
    if (eql(code, "OwnershipUnavailable")) return error.WalkerOwnershipUnavailable;
    if (eql(code, "HistoryFull")) return error.WalkerHistoryFull;
    if (eql(code, "WalkerUnavailable") or eql(code, "WalkerStartFailed")) return error.WalkerUnavailable;
    return error.WalkerRejected;
}
fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

test "binary slices validate original byte offsets and combined budgets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const binary = Slice{ .encoding = "base64", .data = "/wA=", .next_offset = 6, .eof = true };
    try std.testing.expectEqualSlices(u8, &.{ 255, 0 }, try decode(arena.allocator(), binary, 4, 2));
    try std.testing.expectError(error.WalkerInvalidResponse, decode(arena.allocator(), binary, 3, 2));
    try std.testing.expectError(error.WalkerInvalidResponse, decode(arena.allocator(), binary, 4, 1));
}

test "configuration requires exact absolute executable and state directory" {
    try std.testing.expect(!validConfig(.{ .executable = "walker", .home = "/state" }));
    try std.testing.expect(!validConfig(.{ .executable = "/bin/walker", .home = "state" }));
    try std.testing.expect(validConfig(.{ .executable = "/bin/walker", .home = "/state" }));
}

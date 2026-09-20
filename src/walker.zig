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
    WalkerDurabilityUnavailable,
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
    terminalized_at_ms: ?i64 = null,
    reconciled_at_ms: ?i64 = null,
    stdout_discarded_bytes: ?u64 = 0,
    stderr_discarded_bytes: ?u64 = 0,
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

fn checkExecutable(io: Io, config: Config) Error!void {
    if (!validConfig(config)) return error.WalkerUnavailable;
    Io.Dir.accessAbsolute(io, config.executable, .{ .execute = true }) catch return error.WalkerUnavailable;
}

/// Durable-job admission proves the explicitly selected Walker is the current v5
/// durable workload owner before any workstation-tools job state is created.
pub fn check(io: Io, a: A, config: Config) Error!void {
    try checkExecutable(io, config);
    const bytes = try exchange(io, a, config, &.{"ping"}, null, false);
    const reply = std.json.parseFromSliceLeaky(struct {
        schema: []const u8,
        ok: bool,
        version: u32,
        delegated_cgroup_v2: bool = false,
        delegated_cgroup_v2_admission: []const u8 = "",
        restart_reconciliation_v1: bool = false,
        restart_owner: []const u8 = "",
        durable_workloads_v1: bool = false,
    }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.WalkerInvalidResponse;
    if (!reply.ok) return error.WalkerInvalidResponse;
    if (!eql(reply.schema, "walker/v5") or reply.version != 5)
        return error.WalkerDurabilityUnavailable;
    if (!reply.durable_workloads_v1 or !reply.delegated_cgroup_v2 or
        !eql(reply.delegated_cgroup_v2_admission, "ready") or
        !reply.restart_reconciliation_v1 or !eql(reply.restart_owner, "platform"))
        return error.WalkerDurabilityUnavailable;
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
        "--containment",
        "delegated_cgroup_v2",
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
    if (!reply.ok or !eql(reply.schema, "walker/v5") or !eql(reply.run_id, ref.run_id) or !eql(reply.name, ref.name))
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
    if (!reply.ok or !eql(reply.schema, "walker/v5")) return error.WalkerInvalidResponse;
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
    if (!reply.ok or !eql(reply.schema, "walker/v5") or !eql(reply.run_id, ref.run_id) or !eql(reply.name, ref.name))
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
    if (!reply.ok or !eql(reply.schema, "walker/v5") or !eql(reply.run_id, ref.run_id))
        return error.WalkerSubmissionUncertain;
    return reply.stop_requested;
}

fn validateMeta(meta: Meta, ref: Ref) Error!void {
    if (!eql(meta.schema, "walker.run/v5") or !eql(meta.run_id, ref.run_id) or !eql(meta.name, ref.name) or
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
    try checkExecutable(io, config);
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
            struct { schema: []const u8, ok: bool, @"error": []const u8 },
            a,
            result.stderr,
            .{ .ignore_unknown_fields = true },
        ) catch
            return if (mutation) error.WalkerSubmissionUncertain else error.WalkerInvalidResponse;
        if (failed.ok or !eql(failed.schema, "walker/v5"))
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
    if (eql(code, "PlatformWalkerUnavailable") or eql(code, "DelegatedContainmentUnavailable"))
        return error.WalkerDurabilityUnavailable;
    if (eql(code, "WalkerUnavailable") or eql(code, "WalkerStartFailed")) return error.WalkerUnavailable;
    if (eql(code, "ProtocolVersionMismatch")) return error.WalkerInvalidResponse;
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

/// Host inventory is independent of agent admission history. All returned slices
/// belong to the caller allocator, normally a request arena.
pub const max_workloads = 512;
pub const Retention = enum { prefix, recent };
pub const WorkloadSummary = struct {
    name: []const u8,
    run_id: []const u8,
    state: State,
    pid: ?i32,
    command: []const u8,
    cwd: []const u8,
    created_at_ms: i64,
    log_retention: Retention,
};
pub const Workload = struct {
    schema: []const u8,
    name: []const u8,
    run_id: []const u8,
    state: State,
    pid: ?i32,
    walker_pid: i32,
    argv: []const []const u8,
    cwd: []const u8,
    created_at_ms: i64,
    started_at_ms: ?i64,
    terminalized_at_ms: ?i64,
    reconciled_at_ms: ?i64,
    timeout_ms: ?u32,
    log_retention: Retention,
    output_limit_bytes: u32,
    exit_code: ?u8,
    signal: ?u8,
    failure: ?[]const u8,
    streams_complete: bool,
};
pub const WindowSlice = struct {
    encoding: []const u8,
    data: []const u8,
    requested_offset: ?u64,
    oldest_offset: u64,
    end_offset: u64,
    start_offset: u64,
    next_offset: u64,
    gap_bytes: u64,
    eof: bool,
};
pub const WorkloadLogs = struct {
    schema: []const u8,
    ok: bool,
    run_id: []const u8,
    name: []const u8,
    state: State,
    stdout: WindowSlice,
    stderr: WindowSlice,
    log_retention: Retention,
    streams_complete: bool,
};
pub const Resources = struct {
    scope: []const u8,
    cumulative_for_run: bool,
    atomic_snapshot: bool,
    partial: bool,
    processes: u32,
    threads: u64,
    cpu: struct { running_ns: ?u64, runnable_wait_ns: ?u64 },
    memory: struct { rss_bytes: u64, virtual_bytes: u64, shared_pages_may_be_counted_twice: bool },
};
pub const WorkloadStats = struct {
    run_id: []const u8,
    name: []const u8,
    state: State,
    sampling: enum { observed, unavailable, budget_exhausted },
    resources: ?Resources,
};

/// One bounded CLI invocation, not one invocation per row. An unavailable Walker
/// is an error, never an empty successful inventory or a different backend.
pub fn inventory(io: Io, a: A, config: Config) Error![]WorkloadSummary {
    const bytes = try exchange(io, a, config, &.{"ps"}, null, false);
    const reply = std.json.parseFromSliceLeaky(struct {
        schema: []const u8,
        ok: bool,
        animals: []WorkloadSummary,
    }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.WalkerInvalidResponse;
    if (!reply.ok or !eql(reply.schema, "walker/v5") or reply.animals.len > max_workloads)
        return error.WalkerInvalidResponse;
    for (reply.animals, 0..) |row, i| {
        if (!validRunId(row.run_id) or !validName(row.name) or !validPath(row.cwd) or
            row.command.len == 0 or row.command.len > process.max_argv_bytes) return error.WalkerInvalidResponse;
        if (row.pid) |pid| if (pid <= 1) return error.WalkerInvalidResponse;
        for (reply.animals[0..i]) |previous| {
            if (eql(previous.run_id, row.run_id) or eql(previous.name, row.name)) return error.WalkerInvalidResponse;
        }
    }
    return reply.animals;
}

pub fn inspectWorkload(io: Io, a: A, config: Config, id: []const u8) Error!Workload {
    if (!validRunId(id)) return error.JobNotFound;
    const bytes = try exchange(io, a, config, &.{ "inspect", id }, null, false);
    const reply = std.json.parseFromSliceLeaky(struct {
        schema: []const u8,
        ok: bool,
        animal: Workload,
    }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.WalkerInvalidResponse;
    const row = reply.animal;
    if (!reply.ok or !eql(reply.schema, "walker/v5") or !eql(row.schema, "walker.run/v5") or
        !eql(row.run_id, id) or !validName(row.name) or !validPath(row.cwd) or row.walker_pid <= 1 or
        row.argv.len == 0 or row.argv.len > process.max_arguments or row.argv[0].len == 0 or
        row.output_limit_bytes < 4096 or row.output_limit_bytes > 512 * 1024 * 1024)
        return error.WalkerInvalidResponse;
    if (row.pid) |pid| if (pid <= 1) return error.WalkerInvalidResponse;
    var count: usize = 0;
    for (row.argv) |argument| {
        count = std.math.add(usize, count, argument.len) catch return error.WalkerInvalidResponse;
        if (count > process.max_argv_bytes or std.mem.indexOfScalar(u8, argument, 0) != null)
            return error.WalkerInvalidResponse;
    }
    return row;
}

/// Byte windows are validated before callers convert binary data to presentation
/// text. Display replacement characters must never change original byte cursors.
pub fn workloadLogs(io: Io, a: A, ref: Ref, out: u64, err: u64, max_bytes: u32, tail: ?u32) Error!WorkloadLogs {
    if (!validRunId(ref.run_id) or !validName(ref.name) or max_bytes < 2 or max_bytes > 32768)
        return error.WalkerInvalidResponse;
    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(a);
    try args.appendSlice(a, &.{ "logs", ref.run_id, "--max-bytes", try std.fmt.allocPrint(a, "{d}", .{max_bytes}) });
    if (tail) |n| {
        if (n == 0 or n > 32768 or out != 0 or err != 0) return error.WalkerInvalidResponse;
        try args.appendSlice(a, &.{ "--tail-bytes", try std.fmt.allocPrint(a, "{d}", .{n}) });
    } else {
        try args.appendSlice(a, &.{
            "--stdout-offset", try std.fmt.allocPrint(a, "{d}", .{out}),
            "--stderr-offset", try std.fmt.allocPrint(a, "{d}", .{err}),
        });
    }
    const bytes = try exchange(io, a, ref.config, args.items, null, false);
    var reply = std.json.parseFromSliceLeaky(WorkloadLogs, a, bytes, .{ .ignore_unknown_fields = true }) catch
        return error.WalkerInvalidResponse;
    if (!reply.ok or !eql(reply.schema, "walker/v5") or !eql(reply.run_id, ref.run_id) or !eql(reply.name, ref.name))
        return error.WalkerInvalidResponse;
    reply.stdout.data = try decodeWindow(a, reply.stdout, if (tail == null) out else null, max_bytes);
    reply.stderr.data = try decodeWindow(a, reply.stderr, if (tail == null) err else null, max_bytes - reply.stdout.data.len);
    return reply;
}

pub fn workloadStats(io: Io, a: A, ref: Ref) Error!WorkloadStats {
    if (!validRunId(ref.run_id) or !validName(ref.name)) return error.JobNotFound;
    const bytes = try exchange(io, a, ref.config, &.{ "stats", ref.run_id }, null, false);
    const reply = std.json.parseFromSliceLeaky(struct {
        schema: []const u8,
        ok: bool,
        animals: []WorkloadStats,
    }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.WalkerInvalidResponse;
    if (!reply.ok or !eql(reply.schema, "walker/v5") or reply.animals.len != 1) return error.WalkerInvalidResponse;
    const row = reply.animals[0];
    if (!eql(row.run_id, ref.run_id) or !eql(row.name, ref.name)) return error.WalkerInvalidResponse;
    return row;
}

fn decodeWindow(a: A, slice: WindowSlice, requested: ?u64, limit: usize) Error![]const u8 {
    if (slice.requested_offset != requested or slice.oldest_offset > slice.start_offset or
        slice.start_offset > slice.next_offset or slice.next_offset > slice.end_offset)
        return error.WalkerInvalidResponse;
    if (requested) |offset| {
        if (slice.start_offset != @max(offset, slice.oldest_offset) or
            slice.gap_bytes != slice.oldest_offset -| offset) return error.WalkerInvalidResponse;
    } else if (slice.gap_bytes != 0 or slice.next_offset != slice.end_offset) return error.WalkerInvalidResponse;
    return decode(a, .{
        .encoding = slice.encoding,
        .data = slice.data,
        .next_offset = slice.next_offset,
        .eof = slice.eof,
    }, std.math.cast(usize, slice.start_offset) orelse return error.WalkerInvalidResponse, limit);
}

pub fn validRunId(id: []const u8) bool {
    if (id.len != 32) return false;
    for (id) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return false;
    return true;
}
fn validName(name: []const u8) bool {
    if (name.len == 0 or name.len > 64 or validRunId(name) or eql(name, ".") or eql(name, "..")) return false;
    for (name) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_' and byte != '.') return false;
    return true;
}
fn validPath(path: []const u8) bool {
    return std.fs.path.isAbsolute(path) and path.len <= state.max_path_bytes and std.mem.indexOfScalar(u8, path, 0) == null;
}

test "recent stream gaps are exact and cannot silently renumber data" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var slice = WindowSlice{
        .encoding = "utf8",
        .data = "cat",
        .requested_offset = 2,
        .oldest_offset = 9,
        .start_offset = 9,
        .end_offset = 12,
        .next_offset = 12,
        .gap_bytes = 7,
        .eof = true,
    };
    try std.testing.expectEqualStrings("cat", try decodeWindow(a, slice, 2, 3));
    slice.gap_bytes = 0;
    try std.testing.expectError(error.WalkerInvalidResponse, decodeWindow(a, slice, 2, 3));
    slice.requested_offset = null;
    try std.testing.expectEqualStrings("cat", try decodeWindow(a, slice, null, 3));
    try std.testing.expectError(error.WalkerInvalidResponse, decodeWindow(a, slice, null, 2));
    try std.testing.expect(!validRunId("--help"));
}

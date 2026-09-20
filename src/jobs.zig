//! Owns durable background jobs through one explicitly bound Walker.
//!
//! New work is always delegated-cgroup Walker work. Historical systemd/process
//! request files remain readable as inert evidence only; they never regain
//! launch, signal, adoption, or service-manager authority.

const builtin = @import("builtin");
const std = @import("std");
const environment = @import("environment.zig");
const process = @import("process.zig");
const resources = @import("resources.zig");
const state = @import("state.zig");
const walker = @import("walker.zig");
const host_policy = @import("policy.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const default_timeout_seconds: u32 = 6 * 60 * 60;
pub const max_timeout_seconds: u32 = 24 * 60 * 60;
pub const default_output_limit_bytes: usize = 1024 * 1024;
pub const min_output_limit_bytes: usize = 4096;
pub const max_output_limit_bytes: usize = 512 * 1024 * 1024;
pub const max_read_bytes: usize = 32 * 1024;
pub const min_read_bytes: usize = 2;
pub const default_read_bytes: usize = 24 * 1024;

const max_legacy_systemd_properties: usize = 16;
const max_legacy_systemd_property_bytes: usize = 256;
const legacy_systemd_resource_property_names = [_][]const u8{
    "MemoryHigh", "MemoryMax", "MemorySwapMax", "TasksMax", "CPUQuota", "CPUWeight", "IOWeight",
};

pub const Error = state.Error || environment.Error || walker.Error || error{
    InvalidPolicy,
    InvalidJob,
    JobNotFound,
    JobCreateFailed,
    JobOutputFailed,
    JobControlFailed,
    OffsetOutOfRange,
    WalkerBindingMismatch,
};

pub const StartRequest = struct {
    argv: []const []const u8,
    cwd: []const u8,
    stdin: ?[]const u8 = null,
    timeout_seconds: u32 = default_timeout_seconds,
    output_limit_bytes: usize = default_output_limit_bytes,
    resources: resources.Values = .{},
};

pub const JobState = enum {
    starting,
    running,
    stopping,
    exited,
    timed_out,
    cancelled,
    failed,
    indeterminate,
};

pub const Meta = struct {
    job_id: []const u8,
    state: JobState,
    argv: []const []const u8,
    cwd: []const u8,
    created_at: i64,
    timeout_seconds: u32,
    output_limit_bytes: usize,
    resources: resources.Values = .{},
    stdout_truncated: ?bool,
    stderr_truncated: ?bool,
    exit_code: ?i32 = null,
    ended_at: ?i64 = null,
};

pub const ReadRequest = struct {
    job_id: []const u8,
    stdout_offset: usize = 0,
    stderr_offset: usize = 0,
    max_bytes: usize = default_read_bytes,
};

pub const ReadResult = struct {
    meta: Meta,
    stdout: []const u8,
    stderr: []const u8,
    stdout_offset: usize,
    stderr_offset: usize,
    next_stdout_offset: usize,
    next_stderr_offset: usize,
    stdout_eof: bool,
    stderr_eof: bool,
};

pub const CancelReason = enum { already_finished, stop_requested };
pub const CancelResult = struct { meta: Meta, cancelled: bool, reason: CancelReason };

const StoredBackend = enum { systemd_user, process, walker };

// This is a compatibility decoder, not a backend selector. The defaults match
// pre-backend-tag systemd receipts so old local evidence remains inspectable.
const Request = struct {
    backend: StoredBackend = .systemd_user,
    walker_ref: ?walker.Ref = null,
    job_id: []const u8,
    unit: ?[]const u8 = null,
    argv: []const []const u8,
    cwd: []const u8,
    has_stdin: bool,
    timeout_seconds: u32,
    output_limit_bytes: usize,
    resources: resources.Values = .{},
    systemd_properties: []const []const u8 = &.{},
    created_at: i64,
};

const TerminalState = enum {
    exited,
    timed_out,
    cancelled,
    failed,

    fn jobState(value: TerminalState) JobState {
        return switch (value) {
            .exited => .exited,
            .timed_out => .timed_out,
            .cancelled => .cancelled,
            .failed => .failed,
        };
    }
};

const Terminal = struct {
    state: TerminalState,
    service_result: []const u8 = "",
    exit_kind: []const u8,
    exit_status: []const u8,
    ended_at: i64,
};

const LegacyObservation = struct { meta: Meta, terminal_receipt: bool };
const StreamSlice = struct { bytes: []const u8, next: usize, size: usize };

pub fn start(
    init: std.process.Init,
    allocator: Allocator,
    policy: host_policy.Policy,
    state_dir: []const u8,
    request: StartRequest,
) Error!Meta {
    try policy.validate();
    try validateStart(init.io, request);
    try walker.check(init.io, allocator, policy.walker, request.resources);

    var id: [32]u8 = undefined;
    state.randomHex(init.io, &id);
    const job_id = allocator.dupe(u8, &id) catch return error.OutOfMemory;
    var jobs_dir_buffer: [state.max_path_bytes]u8 = undefined;
    const jobs_dir = std.fmt.bufPrint(&jobs_dir_buffer, "{s}/jobs", .{state_dir}) catch return error.PathTooLong;
    var job_dir_buffer: [state.max_path_bytes]u8 = undefined;
    const job_dir = std.fmt.bufPrint(&job_dir_buffer, "{s}/{s}", .{ jobs_dir, id }) catch return error.PathTooLong;
    state.createDirectoryExclusive(init.io, jobs_dir, job_dir) catch return error.JobCreateFailed;
    var launched = false;
    errdefer if (!launched) Io.Dir.cwd().deleteTree(init.io, job_dir) catch {};

    const stored = Request{
        .backend = .walker,
        .walker_ref = .{
            .config = policy.walker,
            .run_id = job_id,
            .name = try std.fmt.allocPrint(allocator, "{s}{s}", .{ policy.job_name_prefix, job_id }),
        },
        .job_id = job_id,
        .argv = request.argv,
        .cwd = request.cwd,
        .has_stdin = request.stdin != null,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .resources = request.resources,
        .created_at = state.timestamp(init.io),
    };
    var request_path_buffer: [state.max_path_bytes]u8 = undefined;
    try state.writeJsonAtomic(init.io, try jobPath(&request_path_buffer, job_dir, "request.json"), stored);

    var input_path_buffer: [state.max_path_bytes]u8 = undefined;
    const input_path = if (request.stdin) |bytes| blk: {
        const path = try jobPath(&input_path_buffer, job_dir, "stdin");
        try state.writeBytesAtomic(init.io, path, bytes, .fromMode(0o600));
        break :blk path;
    } else null;
    defer if (input_path) |path| Io.Dir.cwd().deleteFile(init.io, path) catch {};

    var env = try environment.current(init, allocator, policy.environment_source, policy.operator_marker);
    defer env.deinit();
    if (policy.agent_marker) |marker| try env.put(marker.name, marker.value);
    walker.launch(
        init.io,
        allocator,
        stored.walker_ref.?,
        request.argv,
        request.cwd,
        input_path,
        request.timeout_seconds,
        request.output_limit_bytes,
        request.resources,
        &env,
    ) catch |failure| {
        if (failure == error.WalkerSubmissionUncertain) {
            launched = true;
            var meta = startingMeta(stored);
            meta.state = .indeterminate;
            return meta;
        }
        return failure;
    };
    launched = true;
    return startingMeta(stored);
}

pub fn read(
    io: Io,
    allocator: Allocator,
    policy: host_policy.Policy,
    state_dir: []const u8,
    request: ReadRequest,
) Error!ReadResult {
    try policy.validate();
    if (!validId(request.job_id) or request.max_bytes < min_read_bytes or request.max_bytes > max_read_bytes)
        return error.InvalidJob;
    var job_dir_buffer: [state.max_path_bytes]u8 = undefined;
    const job_dir = std.fmt.bufPrint(&job_dir_buffer, "{s}/jobs/{s}", .{ state_dir, request.job_id }) catch
        return error.PathTooLong;
    const stored = try readRequest(io, allocator, policy, job_dir, request.job_id);

    if (stored.backend == .walker) {
        const ref = stored.walker_ref.?;
        const meta = try walkerMeta(stored, try walker.inspect(io, allocator, ref));
        const output = try walker.logs(io, allocator, ref, request.stdout_offset, request.stderr_offset, request.max_bytes);
        return .{
            .meta = meta,
            .stdout = output.stdout.data,
            .stderr = output.stderr.data,
            .stdout_offset = request.stdout_offset,
            .stderr_offset = request.stderr_offset,
            .next_stdout_offset = @intCast(output.stdout.next_offset),
            .next_stderr_offset = @intCast(output.stderr.next_offset),
            .stdout_eof = output.stdout.eof,
            .stderr_eof = output.stderr.eof,
        };
    }

    const observation = try observeLegacy(io, allocator, job_dir, stored);
    var stdout_path_buffer: [state.max_path_bytes]u8 = undefined;
    var stderr_path_buffer: [state.max_path_bytes]u8 = undefined;
    const stdout = try readSlice(
        io,
        allocator,
        try jobPath(&stdout_path_buffer, job_dir, "stdout"),
        request.stdout_offset,
        (request.max_bytes + 1) / 2,
    );
    const stderr = try readSlice(
        io,
        allocator,
        try jobPath(&stderr_path_buffer, job_dir, "stderr"),
        request.stderr_offset,
        request.max_bytes - stdout.bytes.len,
    );
    return .{
        .meta = observation.meta,
        .stdout = stdout.bytes,
        .stderr = stderr.bytes,
        .stdout_offset = request.stdout_offset,
        .stderr_offset = request.stderr_offset,
        .next_stdout_offset = stdout.next,
        .next_stderr_offset = stderr.next,
        .stdout_eof = observation.terminal_receipt and stdout.next >= stdout.size,
        .stderr_eof = observation.terminal_receipt and stderr.next >= stderr.size,
    };
}

pub fn cancel(
    io: Io,
    allocator: Allocator,
    policy: host_policy.Policy,
    state_dir: []const u8,
    job_id: []const u8,
) Error!CancelResult {
    try policy.validate();
    if (!validId(job_id)) return error.InvalidJob;
    var job_dir_buffer: [state.max_path_bytes]u8 = undefined;
    const job_dir = std.fmt.bufPrint(&job_dir_buffer, "{s}/jobs/{s}", .{ state_dir, job_id }) catch
        return error.PathTooLong;
    const stored = try readRequest(io, allocator, policy, job_dir, job_id);
    if (stored.backend != .walker) {
        const observation = try observeLegacy(io, allocator, job_dir, stored);
        if (!observation.terminal_receipt) return error.JobControlFailed;
        return .{
            .meta = observation.meta,
            .cancelled = observation.meta.state == .cancelled,
            .reason = .already_finished,
        };
    }

    const requested = try walker.stop(io, allocator, stored.walker_ref.?);
    const meta = try walkerMeta(stored, try walker.inspect(io, allocator, stored.walker_ref.?));
    return .{
        .meta = meta,
        .cancelled = requested or meta.state == .cancelled,
        .reason = if (requested) .stop_requested else .already_finished,
    };
}

fn validateStart(io: Io, request: StartRequest) Error!void {
    try validateArgv(request.argv);
    if (request.cwd.len == 0 or request.cwd.len > state.max_path_bytes) return error.InvalidJob;
    if (request.stdin) |bytes| if (bytes.len > process.max_stdin_bytes) return error.InvalidJob;
    if (request.timeout_seconds == 0 or request.timeout_seconds > max_timeout_seconds) return error.InvalidJob;
    if (request.output_limit_bytes < min_output_limit_bytes or request.output_limit_bytes > max_output_limit_bytes)
        return error.InvalidJob;
    request.resources.validate() catch return error.InvalidJob;
    var directory = Io.Dir.cwd().openDir(io, request.cwd, .{}) catch return error.InvalidJob;
    directory.close(io);
}

fn validateStored(policy: host_policy.Policy, request: Request, expected_job_id: []const u8) Error!void {
    if (!validId(expected_job_id) or !std.mem.eql(u8, request.job_id, expected_job_id)) return error.InvalidJob;
    try validateArgv(request.argv);
    if (request.cwd.len == 0 or request.cwd.len > state.max_path_bytes) return error.InvalidJob;
    if (request.timeout_seconds == 0 or request.timeout_seconds > max_timeout_seconds) return error.InvalidJob;
    if (request.output_limit_bytes < min_output_limit_bytes or request.output_limit_bytes > max_output_limit_bytes)
        return error.InvalidJob;

    switch (request.backend) {
        .walker => {
            const ref = request.walker_ref orelse return error.InvalidJob;
            if (!std.mem.eql(u8, policy.walker.executable, ref.config.executable) or
                !std.mem.eql(u8, policy.walker.home, ref.config.home)) return error.WalkerBindingMismatch;
            if (!walker.validConfig(ref.config) or !std.mem.eql(u8, ref.run_id, expected_job_id) or
                !std.mem.startsWith(u8, ref.name, policy.job_name_prefix) or
                !std.mem.eql(u8, ref.name[policy.job_name_prefix.len..], expected_job_id)) return error.InvalidJob;
            request.resources.validate() catch return error.InvalidJob;
            if (request.unit != null or request.systemd_properties.len != 0) return error.InvalidJob;
        },
        .systemd_user => {
            const unit = request.unit orelse return error.InvalidJob;
            var unit_buffer: [host_policy.max_job_name_prefix_bytes + 32 + ".service".len]u8 = undefined;
            const expected = std.fmt.bufPrint(&unit_buffer, "{s}{s}.service", .{ policy.job_name_prefix, expected_job_id }) catch
                return error.InvalidJob;
            if (!std.mem.eql(u8, unit, expected) or request.walker_ref != null or !request.resources.isEmpty())
                return error.InvalidJob;
            try validateLegacySystemdProperties(request.systemd_properties);
        },
        .process => {
            if (request.unit != null or request.walker_ref != null or !request.resources.isEmpty() or
                request.systemd_properties.len != 0) return error.InvalidJob;
        },
    }
}

fn validateArgv(argv: []const []const u8) Error!void {
    if (argv.len == 0 or argv.len > process.max_arguments) return error.InvalidJob;
    var total: usize = 0;
    for (argv) |argument| {
        if (argument.len == 0) return error.InvalidJob;
        total = std.math.add(usize, total, argument.len) catch return error.InvalidJob;
        if (total > process.max_argv_bytes) return error.InvalidJob;
    }
}

fn validateLegacySystemdProperties(properties: []const []const u8) Error!void {
    if (properties.len > max_legacy_systemd_properties) return error.InvalidJob;
    for (properties, 0..) |property, index| {
        if (property.len == 0 or property.len > max_legacy_systemd_property_bytes) return error.InvalidJob;
        const separator = std.mem.indexOfScalar(u8, property, '=') orelse return error.InvalidJob;
        if (separator == 0 or separator + 1 == property.len) return error.InvalidJob;
        const name = property[0..separator];
        const value = property[separator + 1 ..];
        var allowed = false;
        for (legacy_systemd_resource_property_names) |candidate| if (std.mem.eql(u8, name, candidate)) {
            allowed = true;
            break;
        };
        if (!allowed) return error.InvalidJob;
        for (value) |byte| if (byte < 0x20 or byte == 0x7f) return error.InvalidJob;
        for (properties[0..index]) |prior| {
            const prior_separator = std.mem.indexOfScalar(u8, prior, '=') orelse unreachable;
            if (std.mem.eql(u8, name, prior[0..prior_separator])) return error.InvalidJob;
        }
    }
}

fn observeLegacy(io: Io, allocator: Allocator, job_dir: []const u8, request: Request) Error!LegacyObservation {
    const result = try optionalJson(Terminal, io, allocator, job_dir, "result.json");
    return .{ .meta = .{
        .job_id = request.job_id,
        .state = if (result) |terminal_result| terminal_result.state.jobState() else .indeterminate,
        .argv = request.argv,
        .cwd = request.cwd,
        .created_at = request.created_at,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .resources = .{},
        .stdout_truncated = exists(io, job_dir, "stdout.truncated"),
        .stderr_truncated = exists(io, job_dir, "stderr.truncated"),
        .exit_code = terminalExitCode(result),
        .ended_at = if (result) |terminal_result| terminal_result.ended_at else null,
    }, .terminal_receipt = result != null };
}

fn walkerMeta(request: Request, observed: walker.Meta) Error!Meta {
    if (observed.timeout_ms.? != @as(u64, request.timeout_seconds) * 1000 or
        observed.output_limit_bytes != request.output_limit_bytes or
        !std.meta.eql(observed.resources_requested, request.resources)) return error.WalkerInvalidResponse;
    var meta = startingMeta(request);
    meta.state = switch (observed.state) {
        .starting => .starting,
        .running => .running,
        .stopping => .stopping,
        .exited => .exited,
        .stopped => .cancelled,
        .timed_out => .timed_out,
        .failed => .failed,
        .indeterminate => .indeterminate,
    };
    meta.exit_code = observed.exit_code;
    meta.ended_at = if (observed.terminalized_at_ms) |ms| @divFloor(ms, 1000) else null;
    meta.stdout_truncated = if (observed.stdout_discarded_bytes) |bytes| bytes != 0 else null;
    meta.stderr_truncated = if (observed.stderr_discarded_bytes) |bytes| bytes != 0 else null;
    return meta;
}

fn startingMeta(request: Request) Meta {
    return .{
        .job_id = request.job_id,
        .state = .starting,
        .argv = request.argv,
        .cwd = request.cwd,
        .created_at = request.created_at,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .resources = request.resources,
        .stdout_truncated = false,
        .stderr_truncated = false,
    };
}

fn readRequest(
    io: Io,
    allocator: Allocator,
    policy: host_policy.Policy,
    job_dir: []const u8,
    expected_job_id: []const u8,
) Error!Request {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = try jobPath(&path_buffer, job_dir, "request.json");
    if (Io.Dir.cwd().statFile(io, path, .{})) |_| {} else |failure| switch (failure) {
        error.FileNotFound => return error.JobNotFound,
        else => return error.StateReadFailed,
    }
    const request = state.readJson(Request, io, allocator, path) catch |failure| switch (failure) {
        error.StateJsonInvalid => return error.InvalidJob,
        else => return failure,
    };
    try validateStored(policy, request, expected_job_id);
    return request;
}

fn optionalJson(comptime T: type, io: Io, allocator: Allocator, job_dir: []const u8, name: []const u8) Error!?T {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = try jobPath(&path_buffer, job_dir, name);
    if (Io.Dir.cwd().statFile(io, path, .{})) |_| return try state.readJson(T, io, allocator, path) else |failure| switch (failure) {
        error.FileNotFound => return null,
        else => return error.StateReadFailed,
    }
}

fn terminalExitCode(result: ?Terminal) ?i32 {
    const terminal_result = result orelse return null;
    if (!std.mem.eql(u8, terminal_result.exit_kind, "exited")) return null;
    return std.fmt.parseInt(i32, terminal_result.exit_status, 10) catch null;
}

fn readSlice(io: Io, allocator: Allocator, file_path: []const u8, offset: usize, amount: usize) Error!StreamSlice {
    const file = Io.Dir.cwd().openFile(io, file_path, .{}) catch return error.JobNotFound;
    defer file.close(io);
    const stat = file.stat(io) catch return error.JobOutputFailed;
    const size: usize = std.math.cast(usize, stat.size) orelse return error.JobOutputFailed;
    if (offset > size) return error.OffsetOutOfRange;
    const count = @min(amount, size - offset);
    const bytes = allocator.alloc(u8, count) catch return error.OutOfMemory;
    if (count != 0) {
        const read_count = file.readPositionalAll(io, bytes, offset) catch return error.JobOutputFailed;
        if (read_count != count) return error.JobOutputFailed;
    }
    return .{ .bytes = bytes, .next = offset + count, .size = size };
}

fn exists(io: Io, job_dir: []const u8, name: []const u8) bool {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = jobPath(&path_buffer, job_dir, name) catch return false;
    if (Io.Dir.cwd().statFile(io, path, .{})) |_| return true else |_| return false;
}

fn jobPath(buffer: []u8, job_dir: []const u8, name: []const u8) Error![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}/{s}", .{ job_dir, name }) catch error.PathTooLong;
}

fn validId(id: []const u8) bool {
    if (id.len != 32) return false;
    for (id) |byte| if (!std.ascii.isHex(byte) or std.ascii.isUpper(byte)) return false;
    return true;
}

const test_policy = host_policy.Policy{
    .walker = .{ .executable = "/selected/walker", .home = "/selected/state" },
    .agent_marker = .{ .name = "AGENT_CHILD", .value = "1" },
    .operator_marker = .{ .name = "OPERATOR_PROFILE", .value = "1" },
    .shell_prelude = "unset OPERATOR_PROFILE;HISTFILE=/dev/null;set +o history;",
    .job_name_prefix = "workstation-job-",
};

test "job id validation accepts only canonical lowercase hex" {
    try std.testing.expect(validId("0123456789abcdef0123456789abcdef"));
    try std.testing.expect(!validId("0123456789ABCDEF0123456789ABCDEF"));
    try std.testing.expect(!validId("short"));
}

test "durable job stdin admission matches the shared process bound" {
    var accepted: [process.max_stdin_bytes]u8 = @splat('x');
    try validateStart(std.testing.io, .{
        .argv = &.{"true"},
        .cwd = ".",
        .stdin = &accepted,
        .timeout_seconds = 1,
        .output_limit_bytes = min_output_limit_bytes,
    });
    var rejected: [process.max_stdin_bytes + 1]u8 = @splat('x');
    try std.testing.expectError(error.InvalidJob, validateStart(std.testing.io, .{
        .argv = &.{"true"},
        .cwd = ".",
        .stdin = &rejected,
        .timeout_seconds = 1,
        .output_limit_bytes = min_output_limit_bytes,
    }));
}

test "legacy systemd terminal evidence remains readable without control authority" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const id = "0123456789abcdef0123456789abcdef";
    const job_dir = try std.fs.path.join(std.testing.allocator, &.{ root, "jobs", id });
    defer std.testing.allocator.free(job_dir);
    try Io.Dir.cwd().createDirPath(std.testing.io, job_dir);
    var path: [state.max_path_bytes]u8 = undefined;
    try state.writeJsonAtomic(std.testing.io, try jobPath(&path, job_dir, "request.json"), Request{
        .job_id = id,
        .unit = "workstation-job-" ++ id ++ ".service",
        .argv = &.{"true"},
        .cwd = "/tmp",
        .has_stdin = false,
        .timeout_seconds = 10,
        .output_limit_bytes = 4096,
        .systemd_properties = &.{"CPUWeight=77"},
        .created_at = 1,
    });
    try state.writeJsonAtomic(std.testing.io, try jobPath(&path, job_dir, "result.json"), Terminal{
        .state = .exited,
        .exit_kind = "exited",
        .exit_status = "7",
        .ended_at = 2,
    });
    try state.writeBytesAtomic(std.testing.io, try jobPath(&path, job_dir, "stdout"), "AB", .fromMode(0o600));
    try state.writeBytesAtomic(std.testing.io, try jobPath(&path, job_dir, "stderr"), "C", .fromMode(0o600));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const observed = try read(std.testing.io, arena.allocator(), test_policy, root, .{ .job_id = id, .max_bytes = 3 });
    try std.testing.expectEqual(JobState.exited, observed.meta.state);
    try std.testing.expectEqual(@as(?i32, 7), observed.meta.exit_code);
    try std.testing.expect(observed.meta.resources.isEmpty());
    try std.testing.expectEqualStrings("AB", observed.stdout);
    try std.testing.expectEqualStrings("C", observed.stderr);
    try std.testing.expect(observed.stdout_eof and observed.stderr_eof);
    const cancelled = try cancel(std.testing.io, arena.allocator(), test_policy, root, id);
    try std.testing.expectEqual(CancelReason.already_finished, cancelled.reason);
    try std.testing.expect(!cancelled.cancelled);
}

test "unterminated legacy evidence stays indeterminate and cannot be controlled" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const id = "0123456789abcdef0123456789abcdef";
    const job_dir = try std.fs.path.join(std.testing.allocator, &.{ root, "jobs", id });
    defer std.testing.allocator.free(job_dir);
    try Io.Dir.cwd().createDirPath(std.testing.io, job_dir);
    var path: [state.max_path_bytes]u8 = undefined;
    try state.writeJsonAtomic(std.testing.io, try jobPath(&path, job_dir, "request.json"), Request{
        .backend = .process,
        .job_id = id,
        .argv = &.{"true"},
        .cwd = "/tmp",
        .has_stdin = false,
        .timeout_seconds = 10,
        .output_limit_bytes = 4096,
        .created_at = 1,
    });
    try state.writeBytesAtomic(std.testing.io, try jobPath(&path, job_dir, "stdout"), "A", .fromMode(0o600));
    try state.writeBytesAtomic(std.testing.io, try jobPath(&path, job_dir, "stderr"), "", .fromMode(0o600));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const observed = try read(std.testing.io, arena.allocator(), test_policy, root, .{ .job_id = id, .max_bytes = 2 });
    try std.testing.expectEqual(JobState.indeterminate, observed.meta.state);
    try std.testing.expect(!observed.stdout_eof and !observed.stderr_eof);
    try std.testing.expectError(error.JobControlFailed, cancel(std.testing.io, arena.allocator(), test_policy, root, id));
}

test "Walker receipt cannot override current host executable or namespace" {
    const id = "0123456789abcdef0123456789abcdef";
    var request = Request{
        .backend = .walker,
        .walker_ref = .{ .config = test_policy.walker, .run_id = id, .name = "workstation-job-" ++ id },
        .job_id = id,
        .argv = &.{"true"},
        .cwd = "/",
        .has_stdin = false,
        .timeout_seconds = 1,
        .output_limit_bytes = 4096,
        .created_at = 1,
    };
    try validateStored(test_policy, request, id);
    request.walker_ref.?.config.executable = "/unselected/program";
    try std.testing.expectError(error.WalkerBindingMismatch, validateStored(test_policy, request, id));
}

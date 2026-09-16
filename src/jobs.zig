//! Owns durable background jobs behind one explicit host-selected backend.
//!
//! Current job execution is Linux-only. The systemd backend delegates lifetime and cgroup control to transient user services;
//! the process backend is the smaller `/proc` + process-group supervisor used when systemd is not the selected host facility.
//! Both retain bounded streams and durable receipts. Future OS work should preserve this start/read/cancel surface rather than
//! pretending the current Linux mechanics are already portable.

const std = @import("std");
const environment = @import("environment.zig");
const process = @import("process.zig");
const state = @import("state.zig");
const walker = @import("walker.zig");
const host_policy = @import("policy.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const systemd_run = "/usr/bin/systemd-run";
const systemctl = "/usr/bin/systemctl";

/// Default lifetime granted to one durable job.
pub const default_timeout_seconds: u32 = 6 * 60 * 60;
/// Maximum lifetime admitted for one durable job.
pub const max_timeout_seconds: u32 = 24 * 60 * 60;
/// Default maximum retained bytes for each job output stream.
pub const default_output_limit_bytes: usize = 1024 * 1024;
/// Minimum retained-byte ceiling admitted for each job output stream.
pub const min_output_limit_bytes: usize = 4096;
/// Maximum retained bytes admitted for each job output stream.
pub const max_output_limit_bytes: usize = 512 * 1024 * 1024;
/// Maximum native systemd resource-control properties admitted to one durable job.
pub const max_systemd_properties: usize = 16;
/// Maximum bytes admitted for one native systemd `NAME=VALUE` resource-control property.
pub const max_systemd_property_bytes: usize = 256;
const systemd_resource_property_names = [_][]const u8{
    "MemoryHigh",
    "MemoryMax",
    "MemorySwapMax",
    "TasksMax",
    "CPUQuota",
    "CPUWeight",
    "IOWeight",
};
/// Maximum combined stdout/stderr bytes returned by one job read.
pub const max_read_bytes: usize = 32 * 1024;
/// Minimum combined read budget that can advance both retained streams.
pub const min_read_bytes: usize = 2;
/// Default combined stdout/stderr bytes returned by one job read.
pub const default_read_bytes: usize = 24 * 1024;

/// Closed failures from durable job admission, execution, observation, and cancellation.
pub const Error = state.Error || process.Error || environment.Error || walker.Error || error{
    InvalidPolicy,
    InvalidJob,
    JobNotFound,
    JobCreateFailed,
    JobOutputFailed,
    JobLaunchFailed,
    JobControlFailed,
    OffsetOutOfRange,
    ProcessIdentityUnavailable,
    WalkerBindingMismatch,
};

/// Arguments admitted when creating one durable job.
pub const StartRequest = struct {
    argv: []const []const u8,
    cwd: []const u8,
    stdin: ?[]const u8 = null,
    timeout_seconds: u32 = default_timeout_seconds,
    output_limit_bytes: usize = default_output_limit_bytes,
    systemd_properties: []const []const u8 = &.{},
};

/// Closed externally observable durable-job state.
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

/// Durable metadata returned for one background job.
pub const Meta = struct {
    job_id: []const u8,
    state: JobState,
    unit: ?[]const u8 = null,
    argv: []const []const u8,
    cwd: []const u8,
    created_at: i64,
    timeout_seconds: u32,
    output_limit_bytes: usize,
    systemd_properties: []const []const u8 = &.{},
    stdout_truncated: bool,
    stderr_truncated: bool,
    exit_code: ?i32 = null,
    ended_at: ?i64 = null,
};

/// Bounded incremental read request for one durable job.
pub const ReadRequest = struct {
    job_id: []const u8,
    stdout_offset: usize = 0,
    stderr_offset: usize = 0,
    max_bytes: usize = default_read_bytes,
};

/// One request-lifetime job observation plus bounded stream slices.
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

/// Closed explanation for the result of one cancellation request.
pub const CancelReason = enum {
    already_finished,
    stop_requested,
};

/// Result of one job cancellation request.
pub const CancelResult = struct {
    meta: Meta,
    cancelled: bool,
    reason: CancelReason,
};

const Request = struct {
    backend: host_policy.JobBackend = .systemd_user,
    walker_ref: ?walker.Ref = null,
    job_id: []const u8,
    unit: ?[]const u8 = null,
    argv: []const []const u8,
    cwd: []const u8,
    has_stdin: bool,
    timeout_seconds: u32,
    output_limit_bytes: usize,
    systemd_properties: []const []const u8 = &.{},
    created_at: i64,
};

const Terminal = struct {
    state: TerminalState,
    service_result: []const u8 = "",
    exit_kind: []const u8,
    exit_status: []const u8,
    ended_at: i64,
};

const ProcessRuntime = struct {
    supervisor_pid: i32,
    supervisor_start_time: u64,
    child_pid: ?i32 = null,
    child_start_time: ?u64 = null,
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

const Observation = struct {
    meta: Meta,
    /// Only a validated durable terminal receipt proves that neither stream can grow again.
    terminal_receipt: bool,
};

const StreamSlice = struct {
    bytes: []const u8,
    next: usize,
    size: usize,
};

/// Creates one private job directory and submits it to the selected durable-job backend.
pub fn start(
    init: std.process.Init,
    allocator: Allocator,
    policy: host_policy.Policy,
    state_dir: []const u8,
    executable: []const u8,
    request: StartRequest,
) Error!Meta {
    try policy.validate();
    try validateStart(init.io, policy, request);
    if (policy.job_backend == .walker) try walker.check(init.io, policy.walker.?);
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

    const unit: ?[]const u8 = switch (policy.job_backend) {
        .systemd_user => try std.fmt.allocPrint(allocator, "{s}{s}.service", .{ policy.job_unit_prefix, id }),
        .process, .walker => null,
    };
    const stored = Request{
        .backend = policy.job_backend,
        .walker_ref = if (policy.job_backend == .walker) .{
            .config = policy.walker.?,
            .run_id = job_id,
            .name = try std.fmt.allocPrint(allocator, "{s}{s}", .{ policy.job_unit_prefix, job_id }),
        } else null,
        .job_id = job_id,
        .unit = unit,
        .argv = request.argv,
        .cwd = request.cwd,
        .has_stdin = request.stdin != null,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .systemd_properties = request.systemd_properties,
        .created_at = state.timestamp(init.io),
    };
    var request_path_buffer: [state.max_path_bytes]u8 = undefined;
    const request_path = try jobPath(&request_path_buffer, job_dir, "request.json");
    try state.writeJsonAtomic(init.io, request_path, stored);
    if (request.stdin) |bytes| {
        var stdin_path_buffer: [state.max_path_bytes]u8 = undefined;
        const stdin_path = try jobPath(&stdin_path_buffer, job_dir, "stdin");
        try state.writeBytesAtomic(init.io, stdin_path, bytes, .fromMode(0o600));
    }
    if (stored.backend == .walker) {
        var input_path_buffer: [state.max_path_bytes]u8 = undefined;
        const input_path = if (request.stdin != null) try jobPath(&input_path_buffer, job_dir, "stdin") else null;
        defer if (input_path) |path| Io.Dir.cwd().deleteFile(init.io, path) catch {};
        var env = try environment.current(init, allocator, policy.environment_source, policy.operator_marker);
        defer env.deinit();
        if (policy.agent_marker) |marker| try env.put(marker.name, marker.value);
        // Once submission can occur, keep the binding even on lost acknowledgement. Never replay or erase its ID.
        launched = true;
        walker.launch(
            init.io,
            allocator,
            stored.walker_ref.?,
            request.argv,
            request.cwd,
            input_path,
            request.timeout_seconds,
            request.output_limit_bytes,
            &env,
        ) catch |failure| {
            if (failure == error.WalkerSubmissionUncertain) {
                var meta = startingMeta(stored);
                meta.state = .indeterminate;
                return meta;
            }
            return failure;
        };
        return startingMeta(stored);
    }
    try createEmpty(init.io, job_dir, "stdout");
    try createEmpty(init.io, job_dir, "stderr");

    switch (policy.job_backend) {
        .systemd_user => try launchSystemd(init, allocator, policy, executable, job_dir, stored),
        .process => try launchProcess(init, policy, executable, job_dir),
        .walker => unreachable,
    }
    launched = true;
    return startingMeta(stored);
}

fn launchSystemd(
    init: std.process.Init,
    allocator: Allocator,
    policy: host_policy.Policy,
    executable: []const u8,
    job_dir: []const u8,
    stored: Request,
) Error!void {
    const runtime = try std.fmt.allocPrint(allocator, "RuntimeMaxSec={d}s", .{stored.timeout_seconds});
    const stop_post = try execStopPost(allocator, policy.job_finish_argument, executable, job_dir);
    const unit_arg = try std.fmt.allocPrint(allocator, "--unit={s}", .{stored.unit.?});
    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(allocator);
    argv.appendSlice(allocator, &.{
        systemd_run,
        "--user",
        "--quiet",
        "--collect",
        unit_arg,
        "--property=Type=exec",
        "--property=KillMode=control-group",
        "--property=TimeoutStopSec=5s",
        try std.fmt.allocPrint(allocator, "--property={s}", .{runtime}),
        try std.fmt.allocPrint(allocator, "--property={s}", .{stop_post}),
    }) catch return error.OutOfMemory;
    for (stored.systemd_properties) |property| {
        argv.append(allocator, "--property") catch return error.OutOfMemory;
        argv.append(allocator, property) catch return error.OutOfMemory;
    }
    argv.appendSlice(allocator, &.{ "--", executable, policy.job_run_argument, job_dir }) catch return error.OutOfMemory;
    var result = try process.run(init.gpa, init.io, argv.items, "/", null, .fromSeconds(10), null);
    defer result.deinit(init.gpa);
    if (result.timed_out or result.term == null or exitCode(result.term.?) != 0) return error.JobLaunchFailed;
}

fn launchProcess(
    init: std.process.Init,
    policy: host_policy.Policy,
    executable: []const u8,
    job_dir: []const u8,
) Error!void {
    const launch_argument = policy.job_launch_argument orelse return error.InvalidPolicy;
    var result = try process.run(
        init.gpa,
        init.io,
        &.{ executable, launch_argument, job_dir },
        "/",
        null,
        .fromSeconds(10),
        init.environ_map,
    );
    defer result.deinit(init.gpa);
    if (result.timed_out or result.term == null or exitCode(result.term.?) != 0) return error.JobLaunchFailed;
}

/// Reads one job receipt and bounded positional stream slices without retaining a job registry.
pub fn read(
    io: Io,
    allocator: Allocator,
    policy: host_policy.Policy,
    state_dir: []const u8,
    request: ReadRequest,
) Error!ReadResult {
    try policy.validate();
    if (!validId(request.job_id) or request.max_bytes < min_read_bytes or request.max_bytes > max_read_bytes) {
        return error.InvalidJob;
    }
    var job_dir_buffer: [state.max_path_bytes]u8 = undefined;
    const job_dir = std.fmt.bufPrint(&job_dir_buffer, "{s}/jobs/{s}", .{ state_dir, request.job_id }) catch
        return error.PathTooLong;
    const stored = try readRequest(io, allocator, policy, job_dir, request.job_id);
    if (stored.backend == .walker) {
        const ref = stored.walker_ref.?;
        const output = try walker.logs(io, allocator, ref, request.stdout_offset, request.stderr_offset, request.max_bytes);
        return .{
            .meta = try walkerMeta(stored, try walker.inspect(io, allocator, ref)),
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
    const observation = try observe(io, allocator, job_dir, stored);
    var stdout_path_buffer: [state.max_path_bytes]u8 = undefined;
    var stderr_path_buffer: [state.max_path_bytes]u8 = undefined;
    const stdout_path = try jobPath(&stdout_path_buffer, job_dir, "stdout");
    const stderr_path = try jobPath(&stderr_path_buffer, job_dir, "stderr");
    const stdout_amount = (request.max_bytes + 1) / 2;
    const stdout = try readSlice(io, allocator, stdout_path, request.stdout_offset, stdout_amount);
    const stderr = try readSlice(
        io,
        allocator,
        stderr_path,
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

/// Cancels one running durable job through its selected backend.
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
    if (stored.backend == .walker) {
        const requested = try walker.stop(io, allocator, stored.walker_ref.?);
        const meta = try walkerMeta(stored, try walker.inspect(io, allocator, stored.walker_ref.?));
        return .{
            .meta = meta,
            .cancelled = requested or meta.state == .cancelled,
            .reason = if (requested) .stop_requested else .already_finished,
        };
    }
    const before = try observe(io, allocator, job_dir, stored);
    if (before.terminal_receipt) return .{
        .meta = before.meta,
        .cancelled = before.meta.state == .cancelled,
        .reason = .already_finished,
    };

    switch (stored.backend) {
        .systemd_user => try requestStop(io, allocator, job_dir, &.{ systemctl, "--user", "stop", stored.unit.? }),
        .process => try requestProcessStop(io, allocator, job_dir),
        .walker => unreachable,
    }
    const current = try observe(io, allocator, job_dir, stored);
    return .{ .meta = current.meta, .cancelled = true, .reason = .stop_requested };
}

/// Starts one detached process-backend supervisor from the short-lived launcher role.
pub fn launch(init: std.process.Init, policy: host_policy.Policy, job_dir: []const u8) Error!void {
    try policy.validate();
    if (policy.job_backend != .process) return error.InvalidPolicy;
    const allocator = init.arena.allocator();
    const stored = try readRequest(init.io, allocator, policy, job_dir, std.fs.path.basename(job_dir));
    try validateExecution(init.io, stored);
    const executable = std.process.executablePathAlloc(init.io, allocator) catch return error.JobLaunchFailed;
    var supervisor = std.process.spawn(init.io, .{
        .argv = &.{ executable, policy.job_run_argument, job_dir },
        .cwd = .{ .path = "/" },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
        .pgid = 0,
        .environ_map = init.environ_map,
    }) catch return error.JobLaunchFailed;
    const pid: i32 = @intCast(supervisor.id orelse return error.JobLaunchFailed);
    var published = false;
    defer if (!published) {
        signalProcessGroup(pid, .KILL) catch {};
        _ = supervisor.wait(init.io) catch null;
    };
    const start_time = try processStartTime(init.io, pid);
    try writeProcessRuntime(init.io, job_dir, .{
        .supervisor_pid = pid,
        .supervisor_start_time = start_time,
    });
    published = true;
}

/// Runs one durable job payload under the helper role selected by the backend.
pub fn run(init: std.process.Init, policy: host_policy.Policy, job_dir: []const u8) Error!void {
    try policy.validate();
    const allocator = init.arena.allocator();
    const stored = try readRequest(init.io, allocator, policy, job_dir, std.fs.path.basename(job_dir));
    return switch (stored.backend) {
        .systemd_user => runSystemd(init, policy, job_dir, stored),
        .process => runProcessSupervisor(init, policy, job_dir, stored),
        .walker => error.InvalidJob,
    };
}

fn runSystemd(init: std.process.Init, policy: host_policy.Policy, job_dir: []const u8, stored: Request) Error!void {
    const allocator = init.arena.allocator();
    try validateExecution(init.io, stored);
    var stdout_path_buffer: [state.max_path_bytes]u8 = undefined;
    var stderr_path_buffer: [state.max_path_bytes]u8 = undefined;
    const stdout_path = try jobPath(&stdout_path_buffer, job_dir, "stdout");
    const stderr_path = try jobPath(&stderr_path_buffer, job_dir, "stderr");
    const stdout_file = Io.Dir.cwd().createFile(init.io, stdout_path, .{ .truncate = true, .permissions = .fromMode(0o600) }) catch
        return error.JobOutputFailed;
    defer stdout_file.close(init.io);
    const stderr_file = Io.Dir.cwd().createFile(init.io, stderr_path, .{ .truncate = true, .permissions = .fromMode(0o600) }) catch
        return error.JobOutputFailed;
    defer stderr_file.close(init.io);

    var stdin_bytes: ?[]const u8 = null;
    if (stored.has_stdin) {
        var stdin_path_buffer: [state.max_path_bytes]u8 = undefined;
        const stdin_path = try jobPath(&stdin_path_buffer, job_dir, "stdin");
        stdin_bytes = Io.Dir.cwd().readFileAlloc(
            init.io,
            stdin_path,
            allocator,
            .limited(process.max_stdin_bytes),
        ) catch return error.InvalidJob;
    }
    var child_env = try environment.current(
        init,
        init.gpa,
        policy.environment_source,
        policy.operator_marker,
    );
    defer child_env.deinit();
    if (policy.agent_marker) |marker| try child_env.put(marker.name, marker.value);
    var child_argv = stored.argv;
    var argv_storage: ?[][]const u8 = null;
    defer if (argv_storage) |items| init.gpa.free(items);
    if (std.mem.indexOfScalar(u8, stored.argv[0], '/') == null) {
        const resolved = try environment.resolveExecutable(init.io, allocator, &child_env, stored.cwd, stored.argv[0]);
        const items = init.gpa.alloc([]const u8, stored.argv.len) catch return error.OutOfMemory;
        @memcpy(items, stored.argv);
        items[0] = resolved;
        argv_storage = items;
        child_argv = items;
    }
    var child = std.process.spawn(init.io, .{
        .argv = child_argv,
        .cwd = .{ .path = stored.cwd },
        .stdin = if (stdin_bytes == null) .ignore else .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
        .environ_map = &child_env,
    }) catch return error.SpawnFailed;
    defer if (child.id != null) child.kill(init.io);
    std.debug.assert(child.id != null);
    std.debug.assert(child.stdout != null);
    std.debug.assert(child.stderr != null);
    // The systemd deadline bounds the whole helper, but stdin must still progress concurrently with both output streams;
    // otherwise a finite stdin pipe can deadlock against a child that fills stdout before reading input.
    var input_task = if (stdin_bytes) |bytes| task: {
        std.debug.assert(child.stdin != null);
        const input = child.stdin.?;
        child.stdin = null;
        break :task init.io.concurrent(writeInput, .{ init.io, input, bytes }) catch return error.StdinWriteFailed;
    } else null;
    defer {
        if (input_task) |*task| task.cancel(init.io) catch {};
    }

    var stream_storage: Io.File.MultiReader.Buffer(2) = undefined;
    var streams: Io.File.MultiReader = undefined;
    streams.init(init.gpa, init.io, stream_storage.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer streams.deinit();
    const stdout_reader = streams.reader(0);
    const stderr_reader = streams.reader(1);
    var stdout_written: usize = 0;
    var stderr_written: usize = 0;
    while (true) {
        streams.fill(64 * 1024, .none) catch |failure| switch (failure) {
            error.EndOfStream => break,
            else => return error.JobOutputFailed,
        };
        try drain(init.io, job_dir, "stdout", stdout_reader, stdout_file, stored.output_limit_bytes, &stdout_written);
        try drain(init.io, job_dir, "stderr", stderr_reader, stderr_file, stored.output_limit_bytes, &stderr_written);
    }
    try drain(init.io, job_dir, "stdout", stdout_reader, stdout_file, stored.output_limit_bytes, &stdout_written);
    try drain(init.io, job_dir, "stderr", stderr_reader, stderr_file, stored.output_limit_bytes, &stderr_written);
    streams.checkAnyError() catch return error.JobOutputFailed;
    stdout_file.sync(init.io) catch return error.JobOutputFailed;
    stderr_file.sync(init.io) catch return error.JobOutputFailed;
    const term = child.wait(init.io) catch return error.WaitFailed;
    if (input_task) |*task| task.await(init.io) catch return error.StdinWriteFailed;
    std.process.exit(serviceExitCode(term));
}

fn runProcessSupervisor(
    init: std.process.Init,
    policy: host_policy.Policy,
    job_dir: []const u8,
    stored: Request,
) Error!void {
    const allocator = init.arena.allocator();
    try validateExecution(init.io, stored);
    const supervisor = try awaitProcessRuntime(init.io, allocator, job_dir);
    const self_pid: i32 = @intCast(std.os.linux.getpid());
    if (supervisor.supervisor_pid != self_pid or
        !try processIdentityAlive(init.io, self_pid, supervisor.supervisor_start_time)) return error.InvalidJob;

    var stdout_path_buffer: [state.max_path_bytes]u8 = undefined;
    var stderr_path_buffer: [state.max_path_bytes]u8 = undefined;
    const stdout_path = try jobPath(&stdout_path_buffer, job_dir, "stdout");
    const stderr_path = try jobPath(&stderr_path_buffer, job_dir, "stderr");
    const stdout_file = Io.Dir.cwd().createFile(init.io, stdout_path, .{ .truncate = true, .permissions = .fromMode(0o600) }) catch
        return error.JobOutputFailed;
    defer stdout_file.close(init.io);
    const stderr_file = Io.Dir.cwd().createFile(init.io, stderr_path, .{ .truncate = true, .permissions = .fromMode(0o600) }) catch
        return error.JobOutputFailed;
    defer stderr_file.close(init.io);

    var stdin_bytes: ?[]const u8 = null;
    if (stored.has_stdin) {
        var stdin_path_buffer: [state.max_path_bytes]u8 = undefined;
        const stdin_path = try jobPath(&stdin_path_buffer, job_dir, "stdin");
        stdin_bytes = Io.Dir.cwd().readFileAlloc(
            init.io,
            stdin_path,
            allocator,
            .limited(process.max_stdin_bytes),
        ) catch return error.InvalidJob;
    }
    var child_env = try environment.current(
        init,
        init.gpa,
        policy.environment_source,
        policy.operator_marker,
    );
    defer child_env.deinit();
    if (policy.agent_marker) |marker| try child_env.put(marker.name, marker.value);
    var child_argv = stored.argv;
    var argv_storage: ?[][]const u8 = null;
    defer if (argv_storage) |items| init.gpa.free(items);
    if (std.mem.indexOfScalar(u8, stored.argv[0], '/') == null) {
        const resolved = try environment.resolveExecutable(init.io, allocator, &child_env, stored.cwd, stored.argv[0]);
        const items = init.gpa.alloc([]const u8, stored.argv.len) catch return error.OutOfMemory;
        @memcpy(items, stored.argv);
        items[0] = resolved;
        argv_storage = items;
        child_argv = items;
    }
    var child = std.process.spawn(init.io, .{
        .argv = child_argv,
        .cwd = .{ .path = stored.cwd },
        .stdin = if (stdin_bytes == null) .ignore else .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
        .pgid = 0,
        .environ_map = &child_env,
    }) catch return error.SpawnFailed;
    defer if (child.id != null) child.kill(init.io);
    const child_pid: i32 = @intCast(child.id orelse return error.SpawnFailed);
    const child_start_time = try processStartTime(init.io, child_pid);
    try writeProcessRuntime(init.io, job_dir, .{
        .supervisor_pid = self_pid,
        .supervisor_start_time = supervisor.supervisor_start_time,
        .child_pid = child_pid,
        .child_start_time = child_start_time,
    });
    std.debug.assert(child.stdout != null);
    std.debug.assert(child.stderr != null);
    if (exists(init.io, job_dir, "cancelled")) {
        signalProcessGroup(child_pid, .TERM) catch |failure| switch (failure) {
            error.ProcessNotFound => {},
            else => return error.JobControlFailed,
        };
    }

    var input_task = if (stdin_bytes) |bytes| task: {
        std.debug.assert(child.stdin != null);
        const input = child.stdin.?;
        child.stdin = null;
        break :task init.io.concurrent(writeInput, .{ init.io, input, bytes }) catch return error.StdinWriteFailed;
    } else null;
    defer if (input_task) |*task| task.cancel(init.io) catch {};

    var stream_storage: Io.File.MultiReader.Buffer(2) = undefined;
    var streams: Io.File.MultiReader = undefined;
    streams.init(init.gpa, init.io, stream_storage.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer streams.deinit();
    const stdout_reader = streams.reader(0);
    const stderr_reader = streams.reader(1);
    const run_deadline = Io.Clock.Timestamp.fromNow(init.io, .{
        .raw = .fromSeconds(stored.timeout_seconds),
        .clock = .awake,
    });
    var stop_deadline: ?Io.Clock.Timestamp = null;
    var timed_out = false;
    var killed = false;
    var gave_up_streams = false;
    var stdout_written: usize = 0;
    var stderr_written: usize = 0;
    while (true) {
        const timeout: Io.Timeout = if (!timed_out)
            .{ .deadline = run_deadline }
        else if (!killed)
            .{ .deadline = stop_deadline.? }
        else
            .{ .deadline = stop_deadline.? };
        streams.fill(64 * 1024, timeout) catch |failure| switch (failure) {
            error.EndOfStream => break,
            error.Timeout => {
                if (!timed_out) {
                    timed_out = true;
                    signalProcessGroup(child_pid, .TERM) catch |signal_failure| switch (signal_failure) {
                        error.ProcessNotFound => {},
                        else => return error.JobControlFailed,
                    };
                    stop_deadline = Io.Clock.Timestamp.fromNow(init.io, .{ .raw = .fromSeconds(5), .clock = .awake });
                } else if (!killed) {
                    killed = true;
                    signalProcessGroup(child_pid, .KILL) catch |signal_failure| switch (signal_failure) {
                        error.ProcessNotFound => {},
                        else => return error.JobControlFailed,
                    };
                    stop_deadline = Io.Clock.Timestamp.fromNow(init.io, .{ .raw = .fromSeconds(1), .clock = .awake });
                } else {
                    gave_up_streams = true;
                    break;
                }
                continue;
            },
            else => return error.JobOutputFailed,
        };
        try drain(init.io, job_dir, "stdout", stdout_reader, stdout_file, stored.output_limit_bytes, &stdout_written);
        try drain(init.io, job_dir, "stderr", stderr_reader, stderr_file, stored.output_limit_bytes, &stderr_written);
    }
    try drain(init.io, job_dir, "stdout", stdout_reader, stdout_file, stored.output_limit_bytes, &stdout_written);
    try drain(init.io, job_dir, "stderr", stderr_reader, stderr_file, stored.output_limit_bytes, &stderr_written);
    if (!gave_up_streams) streams.checkAnyError() catch return error.JobOutputFailed;
    stdout_file.sync(init.io) catch return error.JobOutputFailed;
    stderr_file.sync(init.io) catch return error.JobOutputFailed;
    const term = child.wait(init.io) catch return error.WaitFailed;
    if (input_task) |*task| task.await(init.io) catch {
        if (!timed_out and !exists(init.io, job_dir, "cancelled")) return error.StdinWriteFailed;
    };
    try writeProcessTerminal(init.io, allocator, job_dir, term, timed_out);
}

fn writeInput(io: Io, file: Io.File, bytes: []const u8) Io.File.Writer.Error!void {
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
}

/// Writes the terminal receipt from systemd's ExecStopPost environment after synchronizing retained streams.
pub fn finish(init: std.process.Init, policy: host_policy.Policy, job_dir: []const u8) Error!void {
    try policy.validate();
    const stored = try readRequest(init.io, init.arena.allocator(), policy, job_dir, std.fs.path.basename(job_dir));
    if (stored.backend != .systemd_user) return error.InvalidJob;
    std.debug.assert(std.mem.eql(u8, stored.job_id, std.fs.path.basename(job_dir)));
    const service_result = init.environ_map.get("SERVICE_RESULT") orelse return error.InvalidJob;
    const exit_kind = init.environ_map.get("EXIT_CODE") orelse "";
    const exit_status = init.environ_map.get("EXIT_STATUS") orelse "";
    try syncOutput(init.io, job_dir, "stdout");
    try syncOutput(init.io, job_dir, "stderr");
    const terminal_state = terminalState(exists(init.io, job_dir, "cancelled"), service_result, exit_kind, exit_status);
    var result_path_buffer: [state.max_path_bytes]u8 = undefined;
    const result_path = try jobPath(&result_path_buffer, job_dir, "result.json");
    try state.writeJsonAtomic(init.io, result_path, Terminal{
        .state = terminal_state,
        .service_result = service_result,
        .exit_kind = exit_kind,
        .exit_status = exit_status,
        .ended_at = state.timestamp(init.io),
    });
}

fn validateStart(io: Io, policy: host_policy.Policy, request: StartRequest) Error!void {
    try validateArgv(request.argv);
    if (request.cwd.len == 0 or request.cwd.len > state.max_path_bytes) return error.InvalidJob;
    if (request.stdin) |bytes| if (bytes.len > process.max_stdin_bytes) return error.InvalidJob;
    if (request.timeout_seconds == 0 or request.timeout_seconds > max_timeout_seconds) return error.InvalidJob;
    if (request.output_limit_bytes < min_output_limit_bytes or request.output_limit_bytes > max_output_limit_bytes) {
        return error.InvalidJob;
    }
    if (policy.job_backend == .systemd_user) {
        try validateSystemdProperties(request.systemd_properties);
    } else if (request.systemd_properties.len != 0) {
        return error.InvalidJob;
    }
    var directory = Io.Dir.cwd().openDir(io, request.cwd, .{}) catch return error.InvalidJob;
    directory.close(io);
}

fn validateStored(policy: host_policy.Policy, request: Request, expected_job_id: []const u8) Error!void {
    // Domain invariant: durable metadata is redundant evidence, never authority for process control. The caller-selected
    // directory and stored job ID must agree before any observation or signal is admitted; each backend adds its own identity.
    if (!validId(expected_job_id) or !std.mem.eql(u8, request.job_id, expected_job_id)) return error.InvalidJob;
    switch (request.backend) {
        .systemd_user => {
            const unit = request.unit orelse return error.InvalidJob;
            var unit_buffer: [host_policy.max_job_unit_prefix_bytes + 32 + ".service".len]u8 = undefined;
            const expected = std.fmt.bufPrint(&unit_buffer, "{s}{s}.service", .{ policy.job_unit_prefix, expected_job_id }) catch
                return error.InvalidJob;
            if (!std.mem.eql(u8, unit, expected)) return error.InvalidJob;
            try validateSystemdProperties(request.systemd_properties);
        },
        .walker => {
            const ref = request.walker_ref orelse return error.InvalidJob;
            // Receipts bind historical identity; only current host policy selects executable authority.
            const configured = policy.walker orelse return error.WalkerBindingMismatch;
            if (policy.job_backend != .walker or
                !std.mem.eql(u8, configured.executable, ref.config.executable) or
                !std.mem.eql(u8, configured.home, ref.config.home)) return error.WalkerBindingMismatch;
            if (!walker.validConfig(ref.config) or !std.mem.eql(u8, ref.run_id, expected_job_id) or
                !std.mem.startsWith(u8, ref.name, policy.job_unit_prefix) or
                !std.mem.eql(u8, ref.name[policy.job_unit_prefix.len..], expected_job_id)) return error.InvalidJob;
            if (request.unit != null or request.systemd_properties.len != 0) return error.InvalidJob;
        },
        .process => {
            if (request.unit != null or request.systemd_properties.len != 0) return error.InvalidJob;
        },
    }
    try validateArgv(request.argv);
    if (request.cwd.len == 0 or request.cwd.len > state.max_path_bytes) return error.InvalidJob;
    if (request.timeout_seconds == 0 or request.timeout_seconds > max_timeout_seconds) return error.InvalidJob;
    if (request.output_limit_bytes < min_output_limit_bytes or request.output_limit_bytes > max_output_limit_bytes) {
        return error.InvalidJob;
    }
}

fn validateExecution(io: Io, request: Request) Error!void {
    var directory = Io.Dir.cwd().openDir(io, request.cwd, .{}) catch return error.InvalidJob;
    directory.close(io);
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

/// Validates the narrow native systemd resource-control surface without interpreting property values.
pub fn validateSystemdProperties(properties: []const []const u8) Error!void {
    if (properties.len > max_systemd_properties) return error.InvalidJob;
    for (properties, 0..) |property, index| {
        if (property.len == 0 or property.len > max_systemd_property_bytes) return error.InvalidJob;
        const separator = std.mem.indexOfScalar(u8, property, '=') orelse return error.InvalidJob;
        if (separator == 0 or separator + 1 == property.len) return error.InvalidJob;
        const name = property[0..separator];
        const value = property[separator + 1 ..];
        var allowed = false;
        for (systemd_resource_property_names) |candidate| {
            if (std.mem.eql(u8, name, candidate)) {
                allowed = true;
                break;
            }
        }
        if (!allowed) return error.InvalidJob;
        for (value) |byte| if (byte < 0x20 or byte == 0x7f) return error.InvalidJob;
        for (properties[0..index]) |prior| {
            const prior_separator = std.mem.indexOfScalar(u8, prior, '=') orelse unreachable;
            if (std.mem.eql(u8, name, prior[0..prior_separator])) return error.InvalidJob;
        }
    }
}

const test_policy = host_policy.Policy{
    .agent_marker = .{ .name = "AGENT_CHILD", .value = "1" },
    .operator_marker = .{ .name = "OPERATOR_PROFILE", .value = "1" },
    .shell_prelude = "unset OPERATOR_PROFILE;HISTFILE=/dev/null;set +o history;",
    .job_unit_prefix = "workstation-job-",
    .job_launch_argument = "--job-launch",
    .job_run_argument = "--job-run",
    .job_finish_argument = "--job-finish",
};

test "process backend rejects systemd-only resource properties" {
    var policy = test_policy;
    policy.job_backend = .process;
    try std.testing.expectError(error.InvalidJob, validateStart(std.testing.io, policy, .{
        .argv = &.{"true"},
        .cwd = "/tmp",
        .timeout_seconds = 1,
        .output_limit_bytes = min_output_limit_bytes,
        .systemd_properties = &.{"MemoryMax=1G"},
    }));
}

test "linux process identity binds pid to proc start time" {
    const pid: i32 = @intCast(std.os.linux.getpid());
    const start_time = try processStartTime(std.testing.io, pid);
    try std.testing.expect(try processIdentityAlive(std.testing.io, pid, start_time));
    try std.testing.expect(!try processIdentityAlive(std.testing.io, pid, start_time + 1));
}

test "durable jobs admit only unique resource-control systemd properties" {
    try validateSystemdProperties(&.{
        "MemoryHigh=6G",
        "MemoryMax=8G",
        "MemorySwapMax=2G",
        "TasksMax=512",
        "CPUQuota=400%",
        "CPUWeight=50",
        "IOWeight=50",
    });
    try std.testing.expectError(error.InvalidJob, validateSystemdProperties(&.{"Restart=always"}));
    try std.testing.expectError(error.InvalidJob, validateSystemdProperties(&.{"KillMode=process"}));
    try std.testing.expectError(error.InvalidJob, validateSystemdProperties(&.{ "MemoryMax=1G", "MemoryMax=2G" }));
    try std.testing.expectError(error.InvalidJob, validateSystemdProperties(&.{"MemoryMax="}));
    try std.testing.expectError(error.InvalidJob, validateSystemdProperties(&.{"MemoryMax=1G\nRestart=always"}));
}

test "legacy durable job metadata defaults to no systemd resource properties" {
    const legacy =
        "{\"job_id\":\"0123456789abcdef0123456789abcdef\"," ++
        "\"unit\":\"workstation-job-0123456789abcdef0123456789abcdef.service\"," ++
        "\"argv\":[\"true\"],\"cwd\":\"/tmp\",\"has_stdin\":false," ++
        "\"timeout_seconds\":10,\"output_limit_bytes\":4096,\"created_at\":1}";
    const parsed = try std.json.parseFromSlice(Request, std.testing.allocator, legacy, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.systemd_properties.len);
}

test "durable job stdin admission matches the shared process bound" {
    var accepted: [process.max_stdin_bytes]u8 = @splat('x');
    try validateStart(std.testing.io, test_policy, .{
        .argv = &.{"true"},
        .cwd = "/tmp",
        .stdin = &accepted,
        .timeout_seconds = 1,
        .output_limit_bytes = min_output_limit_bytes,
    });

    var rejected: [process.max_stdin_bytes + 1]u8 = @splat('x');
    try std.testing.expectError(error.InvalidJob, validateStart(std.testing.io, test_policy, .{
        .argv = &.{"true"},
        .cwd = "/tmp",
        .stdin = &rejected,
        .timeout_seconds = 1,
        .output_limit_bytes = min_output_limit_bytes,
    }));
}

fn observe(io: Io, allocator: Allocator, job_dir: []const u8, request: Request) Error!Observation {
    const result = try optionalJson(Terminal, io, allocator, job_dir, "result.json");
    const current_state = if (result) |terminal_result|
        terminal_result.state.jobState()
    else switch (request.backend) {
        .systemd_user => try liveSystemdState(io, allocator, request.unit.?),
        .process => try liveProcessState(io, allocator, job_dir),
        .walker => return error.InvalidJob,
    };
    return .{ .meta = .{
        .job_id = request.job_id,
        .state = current_state,
        .unit = request.unit,
        .argv = request.argv,
        .cwd = request.cwd,
        .created_at = request.created_at,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .systemd_properties = request.systemd_properties,
        .stdout_truncated = exists(io, job_dir, "stdout.truncated"),
        .stderr_truncated = exists(io, job_dir, "stderr.truncated"),
        .exit_code = terminalExitCode(result),
        .ended_at = if (result) |terminal_result| terminal_result.ended_at else null,
    }, .terminal_receipt = result != null };
}

fn walkerMeta(request: Request, observed: walker.Meta) Error!Meta {
    if (observed.timeout_ms.? != @as(u64, request.timeout_seconds) * 1000 or
        observed.output_limit_bytes != request.output_limit_bytes) return error.WalkerInvalidResponse;
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
    meta.ended_at = if (observed.ended_at_ms) |ms| @divFloor(ms, 1000) else null;
    meta.stdout_truncated = observed.stdout_discarded_bytes != 0;
    meta.stderr_truncated = observed.stderr_discarded_bytes != 0;
    return meta;
}

fn startingMeta(request: Request) Meta {
    return .{
        .job_id = request.job_id,
        .state = .starting,
        .unit = request.unit,
        .argv = request.argv,
        .cwd = request.cwd,
        .created_at = request.created_at,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .systemd_properties = request.systemd_properties,
        .stdout_truncated = false,
        .stderr_truncated = false,
    };
}

fn liveSystemdState(io: Io, allocator: Allocator, unit: []const u8) Error!JobState {
    var observation = try process.run(
        allocator,
        io,
        &.{ systemctl, "--user", "show", unit, "--property=LoadState", "--property=ActiveState" },
        "/",
        null,
        .fromSeconds(5),
        null,
    );
    defer observation.deinit(allocator);
    if (observation.timed_out or observation.term == null or exitCode(observation.term.?) != 0) return .indeterminate;
    var load: []const u8 = "";
    var active: []const u8 = "";
    var lines = std.mem.splitScalar(u8, observation.stdout, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "LoadState=")) load = line["LoadState=".len..];
        if (std.mem.startsWith(u8, line, "ActiveState=")) active = line["ActiveState=".len..];
    }
    if (std.mem.eql(u8, load, "not-found")) return .indeterminate;
    if (std.mem.eql(u8, active, "activating")) return .starting;
    if (std.mem.eql(u8, active, "active")) return .running;
    if (std.mem.eql(u8, active, "deactivating")) return .stopping;
    return .indeterminate;
}

fn liveProcessState(io: Io, allocator: Allocator, job_dir: []const u8) Error!JobState {
    const runtime = (try optionalJson(ProcessRuntime, io, allocator, job_dir, "runtime.json")) orelse
        return .indeterminate;
    if (!try processIdentityAlive(io, runtime.supervisor_pid, runtime.supervisor_start_time)) return .indeterminate;
    const child_pid = runtime.child_pid orelse return .starting;
    const child_start_time = runtime.child_start_time orelse return .indeterminate;
    if (try processIdentityAlive(io, child_pid, child_start_time)) {
        return if (exists(io, job_dir, "cancelled")) .stopping else .running;
    }
    return .stopping;
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
    if (Io.Dir.cwd().statFile(io, path, .{})) |_| {
        return try state.readJson(T, io, allocator, path);
    } else |failure| switch (failure) {
        error.FileNotFound => return null,
        else => return error.StateReadFailed,
    }
}

fn terminalExitCode(result: ?Terminal) ?i32 {
    const terminal_result = result orelse return null;
    if (!std.mem.eql(u8, terminal_result.exit_kind, "exited")) return null;
    return std.fmt.parseInt(i32, terminal_result.exit_status, 10) catch null;
}

fn writeProcessRuntime(io: Io, job_dir: []const u8, runtime: ProcessRuntime) Error!void {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = try jobPath(&path_buffer, job_dir, "runtime.json");
    try state.writeJsonAtomic(io, path, runtime);
}

fn awaitProcessRuntime(io: Io, allocator: Allocator, job_dir: []const u8) Error!ProcessRuntime {
    for (0..100) |_| {
        if (try optionalJson(ProcessRuntime, io, allocator, job_dir, "runtime.json")) |runtime| return runtime;
        Io.sleep(io, .fromMilliseconds(10), .awake) catch return error.ProcessIdentityUnavailable;
    }
    return error.ProcessIdentityUnavailable;
}

fn processStartTime(io: Io, pid: i32) Error!u64 {
    const identity = (try readProcessIdentity(io, pid)) orelse return error.ProcessIdentityUnavailable;
    return identity.start_time;
}

fn processIdentityAlive(io: Io, pid: i32, start_time: u64) Error!bool {
    const identity = (try readProcessIdentity(io, pid)) orelse return false;
    return identity.state != 'Z' and identity.start_time == start_time;
}

const ProcessIdentity = struct {
    state: u8,
    start_time: u64,
};

fn readProcessIdentity(io: Io, pid: i32) Error!?ProcessIdentity {
    if (pid <= 1) return error.InvalidJob;
    var path_buffer: [64]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buffer, "/proc/{d}/stat", .{pid}) catch return error.InvalidJob;
    if (Io.Dir.cwd().statFile(io, path, .{})) |_| {} else |failure| switch (failure) {
        error.FileNotFound => return null,
        else => return error.ProcessIdentityUnavailable,
    }
    var bytes_buffer: [16 * 1024]u8 = undefined;
    const bytes = Io.Dir.cwd().readFile(io, path, &bytes_buffer) catch return error.ProcessIdentityUnavailable;
    const close = std.mem.lastIndexOfScalar(u8, bytes, ')') orelse return error.ProcessIdentityUnavailable;
    if (close + 2 >= bytes.len or bytes[close + 1] != ' ') return error.ProcessIdentityUnavailable;
    var fields = std.mem.splitScalar(u8, bytes[close + 2 ..], ' ');
    var index: usize = 0;
    var process_state: ?u8 = null;
    while (fields.next()) |field| : (index += 1) {
        if (field.len == 0) continue;
        if (process_state == null) {
            if (field.len != 1) return error.ProcessIdentityUnavailable;
            process_state = field[0];
        }
        if (index == 19) {
            return .{
                .state = process_state orelse return error.ProcessIdentityUnavailable,
                .start_time = std.fmt.parseInt(u64, field, 10) catch return error.ProcessIdentityUnavailable,
            };
        }
    }
    return error.ProcessIdentityUnavailable;
}

fn signalProcessGroup(pid: i32, signal: std.posix.SIG) std.posix.KillError!void {
    return std.posix.kill(-@as(std.posix.pid_t, @intCast(pid)), signal);
}

fn requestProcessStop(io: Io, allocator: Allocator, job_dir: []const u8) Error!void {
    var marker_path_buffer: [state.max_path_bytes]u8 = undefined;
    const marker = try jobPath(&marker_path_buffer, job_dir, "cancelled");
    try state.writeBytesAtomic(io, marker, "", .fromMode(0o600));
    errdefer Io.Dir.cwd().deleteFile(io, marker) catch {};
    const runtime = (try optionalJson(ProcessRuntime, io, allocator, job_dir, "runtime.json")) orelse
        return error.JobControlFailed;
    if (!try processIdentityAlive(io, runtime.supervisor_pid, runtime.supervisor_start_time)) {
        return error.JobControlFailed;
    }
    const child_pid = runtime.child_pid orelse return;
    const child_start_time = runtime.child_start_time orelse return error.JobControlFailed;
    if (!try processIdentityAlive(io, child_pid, child_start_time)) return;
    signalProcessGroup(child_pid, .TERM) catch |failure| switch (failure) {
        error.ProcessNotFound => return,
        else => return error.JobControlFailed,
    };
    for (0..50) |_| {
        if (!try processIdentityAlive(io, child_pid, child_start_time)) return;
        Io.sleep(io, .fromMilliseconds(100), .awake) catch return error.JobControlFailed;
    }
    signalProcessGroup(child_pid, .KILL) catch |failure| switch (failure) {
        error.ProcessNotFound => return,
        else => return error.JobControlFailed,
    };
}

fn writeProcessTerminal(
    io: Io,
    allocator: Allocator,
    job_dir: []const u8,
    term: std.process.Child.Term,
    timed_out: bool,
) Error!void {
    const cancelled = exists(io, job_dir, "cancelled");
    const terminal_state: TerminalState = if (timed_out)
        .timed_out
    else if (cancelled)
        .cancelled
    else switch (term) {
        .exited => .exited,
        .signal, .stopped, .unknown => .failed,
    };
    const exit_kind = switch (term) {
        .exited => "exited",
        .signal => "signal",
        .stopped => "stopped",
        .unknown => "unknown",
    };
    const exit_status = switch (term) {
        .exited => |value| try std.fmt.allocPrint(allocator, "{d}", .{value}),
        .signal => |value| try std.fmt.allocPrint(allocator, "{d}", .{@backingInt(value)}),
        .stopped => |value| try std.fmt.allocPrint(allocator, "{d}", .{@backingInt(value)}),
        .unknown => |value| try std.fmt.allocPrint(allocator, "{d}", .{value}),
    };
    var result_path_buffer: [state.max_path_bytes]u8 = undefined;
    const result_path = try jobPath(&result_path_buffer, job_dir, "result.json");
    try state.writeJsonAtomic(io, result_path, Terminal{
        .state = terminal_state,
        .exit_kind = exit_kind,
        .exit_status = exit_status,
        .ended_at = state.timestamp(io),
    });
}

fn execStopPost(allocator: Allocator, finish_argument: []const u8, executable: []const u8, job_dir: []const u8) Error![]const u8 {
    const quoted_executable = try quoteExecArgument(allocator, executable);
    const quoted_job_dir = try quoteExecArgument(allocator, job_dir);
    return std.fmt.allocPrint(
        allocator,
        "ExecStopPost={s} {s} {s}",
        .{ quoted_executable, finish_argument, quoted_job_dir },
    ) catch error.OutOfMemory;
}

fn quoteExecArgument(allocator: Allocator, value: []const u8) Error![]const u8 {
    var output = Io.Writer.Allocating.init(allocator);
    output.writer.writeByte('"') catch return error.OutOfMemory;
    for (value) |byte| switch (byte) {
        '\\' => output.writer.writeAll("\\\\") catch return error.OutOfMemory,
        '"' => output.writer.writeAll("\\\"") catch return error.OutOfMemory,
        '$' => output.writer.writeAll("$$") catch return error.OutOfMemory,
        '%' => output.writer.writeAll("%%") catch return error.OutOfMemory,
        '\n' => output.writer.writeAll("\\n") catch return error.OutOfMemory,
        '\r' => output.writer.writeAll("\\r") catch return error.OutOfMemory,
        '\t' => output.writer.writeAll("\\t") catch return error.OutOfMemory,
        else => output.writer.writeByte(byte) catch return error.OutOfMemory,
    };
    output.writer.writeByte('"') catch return error.OutOfMemory;
    return output.writer.buffered();
}

fn systemctlCommand(allocator: Allocator, io: Io, argv: []const []const u8) Error!bool {
    var result = try process.run(allocator, io, argv, "/", null, .fromSeconds(10), null);
    defer result.deinit(allocator);
    if (result.timed_out or result.term == null) return false;
    return exitCode(result.term.?) == 0;
}

fn requestStop(io: Io, allocator: Allocator, job_dir: []const u8, argv: []const []const u8) Error!void {
    var marker_path_buffer: [state.max_path_bytes]u8 = undefined;
    const marker = try jobPath(&marker_path_buffer, job_dir, "cancelled");
    try state.writeBytesAtomic(io, marker, "", .fromMode(0o600));
    if (systemctlCommand(allocator, io, argv) catch false) return;
    Io.Dir.cwd().deleteFile(io, marker) catch {};
    return error.JobControlFailed;
}

fn terminalState(marker: bool, service_result: []const u8, exit_kind: []const u8, exit_status: []const u8) TerminalState {
    if (std.mem.eql(u8, service_result, "timeout")) return .timed_out;
    if (marker and std.mem.eql(u8, service_result, "success") and
        std.mem.eql(u8, exit_kind, "killed") and std.mem.eql(u8, exit_status, "TERM"))
    {
        return .cancelled;
    }
    if (std.mem.eql(u8, service_result, "success") or std.mem.eql(u8, service_result, "exit-code")) return .exited;
    return .failed;
}

fn createEmpty(io: Io, job_dir: []const u8, name: []const u8) Error!void {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = try jobPath(&path_buffer, job_dir, name);
    state.writeBytesAtomic(io, path, "", .fromMode(0o600)) catch return error.JobCreateFailed;
}

fn syncOutput(io: Io, job_dir: []const u8, name: []const u8) Error!void {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = try jobPath(&path_buffer, job_dir, name);
    const file = Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write }) catch return error.JobOutputFailed;
    defer file.close(io);
    file.sync(io) catch return error.JobOutputFailed;
}

fn drain(
    io: Io,
    job_dir: []const u8,
    name: []const u8,
    reader: *Io.Reader,
    file: Io.File,
    limit: usize,
    written: *usize,
) Error!void {
    const bytes = reader.buffered();
    const remaining = limit -| written.*;
    const retained = @min(bytes.len, remaining);
    if (retained != 0) file.writeStreamingAll(io, bytes[0..retained]) catch return error.JobOutputFailed;
    written.* += retained;
    if (retained != bytes.len) try markTruncated(io, job_dir, name);
    reader.tossBuffered();
}

fn markTruncated(io: Io, job_dir: []const u8, name: []const u8) Error!void {
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buffer, "{s}/{s}.truncated", .{ job_dir, name }) catch return error.PathTooLong;
    if (Io.Dir.cwd().statFile(io, path, .{})) |_| return else |failure| switch (failure) {
        error.FileNotFound => {},
        else => return error.JobOutputFailed,
    }
    try state.writeBytesAtomic(io, path, "", .fromMode(0o600));
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

fn exitCode(term: std.process.Child.Term) i32 {
    return switch (term) {
        .exited => |code| code,
        .signal => |signal| -@as(i32, @intCast(@backingInt(signal))),
        .stopped => |signal| -@as(i32, @intCast(@backingInt(signal))),
        .unknown => |code| @intCast(code),
    };
}

fn serviceExitCode(term: std.process.Child.Term) u8 {
    return switch (term) {
        .exited => |code| code,
        .signal => |signal| @intCast(@min(128 + @backingInt(signal), 255)),
        .stopped => |signal| @intCast(@min(128 + @backingInt(signal), 255)),
        .unknown => 1,
    };
}

test "job id validation accepts only canonical lowercase hex" {
    try std.testing.expect(validId("0123456789abcdef0123456789abcdef"));
    try std.testing.expect(!validId("0123456789ABCDEF0123456789ABCDEF"));
    try std.testing.expect(!validId("short"));
}

test "two-byte job reads advance both streams and reuse unused budget" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const job_id = "0123456789abcdef0123456789abcdef";
    const job_dir = try std.fs.path.join(std.testing.allocator, &.{ root, "jobs", job_id });
    defer std.testing.allocator.free(job_dir);
    try Io.Dir.cwd().createDirPath(std.testing.io, job_dir);
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    try state.writeJsonAtomic(std.testing.io, try jobPath(&path_buffer, job_dir, "request.json"), Request{
        .job_id = job_id,
        .unit = "workstation-job-0123456789abcdef0123456789abcdef.service",
        .argv = &.{"true"},
        .cwd = "/tmp",
        .has_stdin = false,
        .timeout_seconds = 10,
        .output_limit_bytes = 4096,
        .created_at = 1,
    });
    try state.writeJsonAtomic(std.testing.io, try jobPath(&path_buffer, job_dir, "result.json"), Terminal{
        .state = .exited,
        .service_result = "success",
        .exit_kind = "exited",
        .exit_status = "0",
        .ended_at = 2,
    });
    try state.writeBytesAtomic(
        std.testing.io,
        try jobPath(&path_buffer, job_dir, "stdout"),
        "AB",
        .fromMode(0o600),
    );
    try state.writeBytesAtomic(
        std.testing.io,
        try jobPath(&path_buffer, job_dir, "stderr"),
        "CD",
        .fromMode(0o600),
    );
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    try std.testing.expectError(error.InvalidJob, read(std.testing.io, allocator, test_policy, root, .{
        .job_id = job_id,
        .max_bytes = 1,
    }));
    const first = try read(std.testing.io, allocator, test_policy, root, .{ .job_id = job_id, .max_bytes = 2 });
    try std.testing.expectEqualStrings("A", first.stdout);
    try std.testing.expectEqualStrings("C", first.stderr);
    try std.testing.expectEqual(@as(usize, 2), first.stdout.len + first.stderr.len);
    const second = try read(std.testing.io, allocator, test_policy, root, .{
        .job_id = job_id,
        .stdout_offset = first.next_stdout_offset,
        .stderr_offset = first.next_stderr_offset,
        .max_bytes = 2,
    });
    try std.testing.expectEqualStrings("B", second.stdout);
    try std.testing.expectEqualStrings("D", second.stderr);
    const stderr_only = try read(std.testing.io, allocator, test_policy, root, .{
        .job_id = job_id,
        .stdout_offset = 2,
        .max_bytes = 2,
    });
    try std.testing.expectEqualStrings("", stderr_only.stdout);
    try std.testing.expectEqualStrings("CD", stderr_only.stderr);
}

test "job control rejects swapped identity and arbitrary unit before writing a marker" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const requested_id = "0123456789abcdef0123456789abcdef";
    const stored_id = "fedcba9876543210fedcba9876543210";
    const job_dir = try std.fs.path.join(std.testing.allocator, &.{ root, "jobs", requested_id });
    defer std.testing.allocator.free(job_dir);
    try Io.Dir.cwd().createDirPath(std.testing.io, job_dir);
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    const request_path = try jobPath(&path_buffer, job_dir, "request.json");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    try state.writeJsonAtomic(std.testing.io, request_path, testRequest(stored_id));
    try std.testing.expectError(error.InvalidJob, cancel(std.testing.io, allocator, test_policy, root, requested_id));
    try std.testing.expect(!exists(std.testing.io, job_dir, "cancelled"));

    var arbitrary_unit = testRequest(requested_id);
    arbitrary_unit.unit = "graphical-session.target";
    try state.writeJsonAtomic(std.testing.io, request_path, arbitrary_unit);
    try std.testing.expectError(error.InvalidJob, cancel(std.testing.io, allocator, test_policy, root, requested_id));
    try std.testing.expect(!exists(std.testing.io, job_dir, "cancelled"));
}

test "indeterminate observation is not EOF or an already-finished cancellation" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const job_id = "0123456789abcdef0123456789abcdef";
    const job_dir = try std.fs.path.join(std.testing.allocator, &.{ root, "jobs", job_id });
    defer std.testing.allocator.free(job_dir);
    try Io.Dir.cwd().createDirPath(std.testing.io, job_dir);
    var path_buffer: [state.max_path_bytes]u8 = undefined;
    try state.writeJsonAtomic(std.testing.io, try jobPath(&path_buffer, job_dir, "request.json"), testRequest(job_id));
    try state.writeBytesAtomic(std.testing.io, try jobPath(&path_buffer, job_dir, "stdout"), "A", .fromMode(0o600));
    try state.writeBytesAtomic(std.testing.io, try jobPath(&path_buffer, job_dir, "stderr"), "", .fromMode(0o600));

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const first = try read(std.testing.io, arena.allocator(), test_policy, root, .{ .job_id = job_id, .max_bytes = 2 });
    try std.testing.expectEqual(JobState.indeterminate, first.meta.state);
    try std.testing.expectEqualStrings("A", first.stdout);
    try std.testing.expect(!first.stdout_eof);
    try std.testing.expect(!first.stderr_eof);

    try state.writeBytesAtomic(std.testing.io, try jobPath(&path_buffer, job_dir, "stdout"), "AB", .fromMode(0o600));
    const second = try read(std.testing.io, arena.allocator(), test_policy, root, .{
        .job_id = job_id,
        .stdout_offset = first.next_stdout_offset,
        .max_bytes = 2,
    });
    try std.testing.expectEqualStrings("B", second.stdout);
    try std.testing.expect(!second.stdout_eof);
    try std.testing.expectError(error.JobControlFailed, cancel(std.testing.io, arena.allocator(), test_policy, root, job_id));
    try std.testing.expect(!exists(std.testing.io, job_dir, "cancelled"));
}

fn testRequest(job_id: []const u8) Request {
    return .{
        .job_id = job_id,
        .unit = "workstation-job-" ++ "0123456789abcdef0123456789abcdef" ++ ".service",
        .argv = &.{"true"},
        .cwd = "/path/need/not/exist/for/observation",
        .has_stdin = false,
        .timeout_seconds = 10,
        .output_limit_bytes = 4096,
        .created_at = 1,
    };
}

test "failed stop removes its cancellation marker" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    try std.testing.expectError(
        error.JobControlFailed,
        requestStop(std.testing.io, std.testing.allocator, root, &.{"/usr/bin/false"}),
    );
    try std.testing.expect(!exists(std.testing.io, root, "cancelled"));
    try std.testing.expectError(
        error.JobControlFailed,
        requestStop(std.testing.io, std.testing.allocator, root, &.{"/does/not/exist"}),
    );
    try std.testing.expect(!exists(std.testing.io, root, "cancelled"));
}

test "terminal evidence outranks a stale cancellation marker" {
    try std.testing.expectEqual(TerminalState.cancelled, terminalState(true, "success", "killed", "TERM"));
    try std.testing.expectEqual(TerminalState.timed_out, terminalState(true, "timeout", "killed", "TERM"));
    try std.testing.expectEqual(TerminalState.exited, terminalState(true, "success", "exited", "0"));
    try std.testing.expectEqual(TerminalState.exited, terminalState(true, "exit-code", "exited", "1"));
    try std.testing.expectEqual(TerminalState.failed, terminalState(true, "resources", "killed", "KILL"));
    try std.testing.expectEqual(TerminalState.exited, terminalState(false, "success", "killed", "TERM"));
}

test "Walker receipt cannot override current host executable or namespace" {
    var policy = test_policy;
    policy.job_backend = .walker;
    policy.walker = .{ .executable = "/selected/walker", .home = "/selected/state" };
    const id = "0123456789abcdef0123456789abcdef";
    var request = Request{
        .backend = .walker,
        .walker_ref = .{ .config = policy.walker.?, .run_id = id, .name = "workstation-job-" ++ id },
        .job_id = id,
        .argv = &.{"true"},
        .cwd = "/",
        .has_stdin = false,
        .timeout_seconds = 1,
        .output_limit_bytes = 4096,
        .created_at = 1,
    };
    try validateStored(policy, request, id);
    request.walker_ref.?.config.executable = "/unselected/program";
    try std.testing.expectError(error.WalkerBindingMismatch, validateStored(policy, request, id));
    request.walker_ref.?.config = policy.walker.?;
    request.walker_ref.?.config.home = "/another/state";
    try std.testing.expectError(error.WalkerBindingMismatch, validateStored(policy, request, id));
    request.walker_ref.?.config = policy.walker.?;
    policy.job_backend = .process;
    try std.testing.expectError(error.WalkerBindingMismatch, validateStored(policy, request, id));
}

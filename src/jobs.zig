//! Adapts durable workstation jobs to one explicitly selected Walker.
//!
//! Walker is the only durable job owner. The tool job ID is the Walker run ID;
//! this module keeps no shadow job registry, process receipt, or service-manager
//! compatibility support.

const std = @import("std");
const environment = @import("environment.zig");
const process = @import("process.zig");
const resources = @import("resources.zig");
const support = @import("support.zig");
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

pub const Error = support.Error || environment.Error || walker.Error || error{
    InvalidPolicy,
    InvalidJob,
    JobCreateFailed,
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
pub const CancelResult = struct {
    meta: Meta,
    cancelled: bool,
    reason: CancelReason,
};

/// Starts one Walker-owned durable workload.
///
/// scratch_dir is consumer-owned transient storage used only for finite stdin
/// handoff. It never contains job identity, status, logs, or control authority.
pub fn start(
    init: std.process.Init,
    allocator: Allocator,
    policy: host_policy.Policy,
    scratch_dir: []const u8,
    request: StartRequest,
) Error!Meta {
    try policy.validate();
    try validateStart(init.io, request);
    try walker.check(init.io, allocator, policy.walker, request.resources);

    var id: [32]u8 = undefined;
    support.randomHex(init.io, &id);
    const job_id = allocator.dupe(u8, &id) catch return error.OutOfMemory;
    const ref = try walkerRef(allocator, policy, job_id);
    const created_at = support.timestamp(init.io);

    var input_dir_buffer: [support.max_path_bytes]u8 = undefined;
    var input_path_buffer: [support.max_path_bytes]u8 = undefined;
    const input_path = if (request.stdin) |bytes| blk: {
        if (scratch_dir.len == 0 or scratch_dir.len > support.max_path_bytes)
            return error.InvalidJob;
        var parent_buffer: [support.max_path_bytes]u8 = undefined;
        const parent = std.fmt.bufPrint(
            &parent_buffer,
            "{s}/workstation-inputs",
            .{scratch_dir},
        ) catch return error.PathTooLong;
        const directory = std.fmt.bufPrint(
            &input_dir_buffer,
            "{s}/{s}",
            .{ parent, job_id },
        ) catch return error.PathTooLong;
        support.createScratchDirectory(
            init.io,
            parent,
            directory,
        ) catch return error.JobCreateFailed;
        errdefer Io.Dir.cwd().deleteTree(init.io, directory) catch {};
        const path = std.fmt.bufPrint(
            &input_path_buffer,
            "{s}/stdin",
            .{directory},
        ) catch return error.PathTooLong;
        try support.writeScratchFile(init.io, path, bytes);
        break :blk path;
    } else null;
    defer if (request.stdin != null)
        Io.Dir.cwd().deleteTree(
            init.io,
            std.fs.path.dirname(input_path.?) orelse unreachable,
        ) catch {};

    var env = try environment.current(
        init,
        allocator,
        policy.operator_marker,
    );
    defer env.deinit();
    if (policy.agent_marker) |marker|
        try env.put(marker.name, marker.value);

    walker.launch(
        init.io,
        allocator,
        ref,
        request.argv,
        request.cwd,
        input_path,
        request.timeout_seconds,
        request.output_limit_bytes,
        request.resources,
        &env,
    ) catch |failure| {
        if (failure == error.WalkerSubmissionUncertain) {
            var meta = startingMeta(job_id, request, created_at);
            meta.state = .indeterminate;
            return meta;
        }
        return failure;
    };

    return startingMeta(job_id, request, created_at);
}

/// Reads one exact Walker workload and bounded log slices.
pub fn read(
    io: Io,
    allocator: Allocator,
    policy: host_policy.Policy,
    request: ReadRequest,
) Error!ReadResult {
    try policy.validate();
    if (!validId(request.job_id) or
        request.max_bytes < min_read_bytes or
        request.max_bytes > max_read_bytes)
    {
        return error.InvalidJob;
    }

    const ref = try walkerRef(allocator, policy, request.job_id);
    const meta = try walkerMeta(try walker.inspect(io, allocator, ref));
    const output = try walker.logs(
        io,
        allocator,
        ref,
        request.stdout_offset,
        request.stderr_offset,
        request.max_bytes,
    );
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

/// Requests stop of one exact Walker workload.
pub fn cancel(
    io: Io,
    allocator: Allocator,
    policy: host_policy.Policy,
    job_id: []const u8,
) Error!CancelResult {
    try policy.validate();
    if (!validId(job_id)) return error.InvalidJob;

    const ref = try walkerRef(allocator, policy, job_id);
    const requested = try walker.stop(io, allocator, ref);
    const meta = try walkerMeta(try walker.inspect(io, allocator, ref));
    return .{
        .meta = meta,
        .cancelled = requested or meta.state == .cancelled,
        .reason = if (requested)
            .stop_requested
        else
            .already_finished,
    };
}

fn validateStart(io: Io, request: StartRequest) Error!void {
    try validateArgv(request.argv);
    if (request.cwd.len == 0 or
        request.cwd.len > support.max_path_bytes)
    {
        return error.InvalidJob;
    }
    if (request.stdin) |bytes|
        if (bytes.len > process.max_stdin_bytes) return error.InvalidJob;
    if (request.timeout_seconds == 0 or
        request.timeout_seconds > max_timeout_seconds)
    {
        return error.InvalidJob;
    }
    if (request.output_limit_bytes < min_output_limit_bytes or
        request.output_limit_bytes > max_output_limit_bytes)
    {
        return error.InvalidJob;
    }
    request.resources.validate() catch return error.InvalidJob;
    var directory = Io.Dir.cwd().openDir(io, request.cwd, .{}) catch
        return error.InvalidJob;
    directory.close(io);
}

fn validateArgv(argv: []const []const u8) Error!void {
    if (argv.len == 0 or argv.len > process.max_arguments)
        return error.InvalidJob;
    var total: usize = 0;
    for (argv) |argument| {
        if (argument.len == 0) return error.InvalidJob;
        total = std.math.add(usize, total, argument.len) catch
            return error.InvalidJob;
        if (total > process.max_argv_bytes) return error.InvalidJob;
    }
}

fn walkerRef(
    allocator: Allocator,
    policy: host_policy.Policy,
    job_id: []const u8,
) Error!walker.Ref {
    if (!validId(job_id)) return error.InvalidJob;
    return .{
        .config = policy.walker,
        .run_id = job_id,
        .name = std.fmt.allocPrint(
            allocator,
            "{s}{s}",
            .{ policy.job_name_prefix, job_id },
        ) catch return error.OutOfMemory,
    };
}

fn walkerMeta(observed: walker.Meta) Error!Meta {
    const timeout_ms = observed.timeout_ms orelse
        return error.WalkerInvalidResponse;
    if (timeout_ms % 1000 != 0)
        return error.WalkerInvalidResponse;
    const timeout_seconds = std.math.cast(
        u32,
        timeout_ms / 1000,
    ) orelse return error.WalkerInvalidResponse;
    return .{
        .job_id = observed.run_id,
        .state = switch (observed.state) {
            .starting => .starting,
            .running => .running,
            .stopping => .stopping,
            .exited => .exited,
            .stopped => .cancelled,
            .timed_out => .timed_out,
            .failed => .failed,
            .indeterminate => .indeterminate,
        },
        .argv = observed.argv,
        .cwd = observed.cwd,
        .created_at = @divFloor(observed.created_at_ms, 1000),
        .timeout_seconds = timeout_seconds,
        .output_limit_bytes = observed.output_limit_bytes,
        .resources = observed.resources_requested,
        .stdout_truncated = if (observed.stdout_discarded_bytes) |bytes|
            bytes != 0
        else
            null,
        .stderr_truncated = if (observed.stderr_discarded_bytes) |bytes|
            bytes != 0
        else
            null,
        .exit_code = observed.exit_code,
        .ended_at = if (observed.terminalized_at_ms) |ms|
            @divFloor(ms, 1000)
        else
            null,
    };
}

fn startingMeta(
    job_id: []const u8,
    request: StartRequest,
    created_at: i64,
) Meta {
    return .{
        .job_id = job_id,
        .state = .starting,
        .argv = request.argv,
        .cwd = request.cwd,
        .created_at = created_at,
        .timeout_seconds = request.timeout_seconds,
        .output_limit_bytes = request.output_limit_bytes,
        .resources = request.resources,
        .stdout_truncated = false,
        .stderr_truncated = false,
    };
}

fn validId(id: []const u8) bool {
    if (id.len != 32) return false;
    for (id) |byte|
        if (!std.ascii.isHex(byte) or
            std.ascii.isUpper(byte))
        {
            return false;
        };
    return true;
}

const test_policy = host_policy.Policy{
    .walker = .{
        .executable = "/selected/walker",
        .home = "/selected/state",
    },
    .agent_marker = .{ .name = "AGENT_CHILD", .value = "1" },
    .operator_marker = .{ .name = "OPERATOR_PROFILE", .value = "1" },
    .shell_prelude = "unset OPERATOR_PROFILE;HISTFILE=/dev/null;set +o history;",
    .job_name_prefix = "workstation-job-",
};

test "job id validation accepts only canonical lowercase hex" {
    try std.testing.expect(validId(
        "0123456789abcdef0123456789abcdef",
    ));
    try std.testing.expect(!validId(
        "0123456789ABCDEF0123456789ABCDEF",
    ));
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
    try std.testing.expectError(
        error.InvalidJob,
        validateStart(std.testing.io, .{
            .argv = &.{"true"},
            .cwd = ".",
            .stdin = &rejected,
            .timeout_seconds = 1,
            .output_limit_bytes = min_output_limit_bytes,
        }),
    );
}

test "Walker reference is derived only from current policy and job id" {
    const id = "0123456789abcdef0123456789abcdef";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const ref = try walkerRef(arena.allocator(), test_policy, id);
    try std.testing.expectEqualStrings(
        "/selected/walker",
        ref.config.executable,
    );
    try std.testing.expectEqualStrings(
        "/selected/state",
        ref.config.home,
    );
    try std.testing.expectEqualStrings(id, ref.run_id);
    try std.testing.expectEqualStrings(
        "workstation-job-" ++ id,
        ref.name,
    );
}

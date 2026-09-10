//! Owns bounded workstation operations that can be embedded independently of transport, session identity, and UI.
//!
//! Calls use request-lifetime allocation, direct process execution, durable job files, and bounded image I/O.

const std = @import("std");
const jobs = @import("jobs.zig");
const environment = @import("environment.zig");
const process = @import("process.zig");
const state = @import("state.zig");
const host_policy = @import("policy.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Maximum stdin bytes admitted to one process or durable job.
pub const max_stdin_bytes = process.max_stdin_bytes;
/// Maximum retained stdout or stderr bytes for one synchronous process.
pub const max_stream_bytes = process.max_stream_bytes;
/// Maximum argv entries admitted to one process.
pub const max_arguments = process.max_arguments;
/// Maximum combined argv bytes admitted to one process.
pub const max_argv_bytes = process.max_argv_bytes;
/// Maximum durable-job lifetime.
pub const max_job_timeout_seconds = jobs.max_timeout_seconds;
/// Minimum durable-job output retention ceiling.
pub const min_job_output_limit_bytes = jobs.min_output_limit_bytes;
/// Maximum durable-job output retention ceiling.
pub const max_job_output_limit_bytes = jobs.max_output_limit_bytes;
/// Maximum number of native systemd resource-control properties on one durable job.
pub const max_job_systemd_properties = jobs.max_systemd_properties;
/// Maximum bytes in one native systemd resource-control property.
pub const max_job_systemd_property_bytes = jobs.max_systemd_property_bytes;
/// Minimum combined durable-job read size.
pub const min_job_read_bytes = jobs.min_read_bytes;
/// Maximum combined durable-job read size.
pub const max_job_read_bytes = jobs.max_read_bytes;
/// Durable job state shared with consumers that describe tool output schemas.
pub const JobState = jobs.JobState;
/// Durable job cancellation reason shared with consumers that describe tool output schemas.
pub const JobCancelReason = jobs.CancelReason;

/// Maximum PNG/JPEG bytes admitted to one native MCP image result.
pub const max_image_bytes: usize = 8 * 1024 * 1024;
/// Default lifetime for one synchronous command or shell call.
pub const default_process_timeout_seconds: u32 = 20;
/// Maximum lifetime admitted for one synchronous command or shell call.
pub const max_process_timeout_seconds: u32 = 300;
/// Deterministic login-shell executable used by the `shell` tool.
pub const shell_executable = "/bin/bash";
/// Login-shell option paired with `shell_executable`.
pub const shell_option = "-lc";
/// Reserved consumer-owned shell prelude budget; policy may use fewer bytes but never more.
pub const shell_guard_budget_bytes: usize = host_policy.max_shell_prelude_bytes;
/// Maximum shell text admitted by the stable public schema.
pub const max_shell_command_bytes: usize =
    process.max_argv_bytes - shell_executable.len - shell_option.len - shell_guard_budget_bytes;

/// Host execution policy supplied by the embedding application or transport.
pub const Policy = host_policy.Policy;
/// Optional environment marker supplied by host policy.
pub const Marker = environment.Marker;

/// Closed workstation tool surface independent of transport, session identity, and activity logging.
pub const Tool = enum {
    command,
    shell,
    image_read,
    job_start,
    job_read,
    job_cancel,

    /// Returns the exact public MCP name of this tool.
    pub fn name(tool: Tool) []const u8 {
        return @tagName(tool);
    }
};

/// Every tool in stable catalogue order.
pub const all = std.meta.tags(Tool);

/// Parses one exact public MCP tool name.
pub fn parse(name: []const u8) ?Tool {
    return std.meta.stringToEnum(Tool, name);
}

/// Closed failures from workstation tool validation and execution.
pub const Error = state.Error || process.Error || jobs.Error || environment.Error || error{
    InvalidArguments,
    WorkingDirectoryUnavailable,
    FileNotFound,
    FileReadFailed,
    UnsupportedImage,
    InvalidImageData,
    ImageTooLarge,
};

/// Request-scoped execution dependencies borrowed by every tool call.
pub const Context = struct {
    init: std.process.Init,
    policy: Policy,
    allocator: Allocator,
    state_dir: []const u8,
    root: []const u8,
    executable: []const u8,
};

/// Executes one already validated tool and returns request-lifetime structured content.
pub fn call(context: Context, tool: Tool, arguments: std.json.ObjectMap) Error!std.json.Value {
    try context.policy.validate();
    return switch (tool) {
        .command => command(context, arguments),
        .shell => shell(context, arguments),
        .image_read => imageRead(context, arguments),
        .job_start => jobStart(context, arguments),
        .job_read => jobRead(context, arguments),
        .job_cancel => jobCancel(context, arguments),
    };
}

/// Returns one short human-facing tool title independent of any transport.
pub fn title(tool: Tool) []const u8 {
    return switch (tool) {
        .command => "Run Exact Command",
        .shell => "Run Shell Command",
        .image_read => "Read Image",
        .job_start => "Start Durable Job",
        .job_read => "Read Durable Job",
        .job_cancel => "Cancel Durable Job",
    };
}

/// Returns one transport-neutral model description of the workstation operation.
pub fn description(tool: Tool) []const u8 {
    return switch (tool) {
        .command => "Execute exact argv in the current logged-in user environment with bounded stdin, stdout, stderr, " ++
            "and runtime.",
        .shell => "Execute text through the current history-isolated Bash login environment with bounded stdin, stdout, " ++
            "stderr, and runtime.",
        .image_read => "Read one bounded, byte-validated PNG or JPEG as native image content.",
        .job_start => "Start one bounded long-running argv in the current logged-in user environment and return a durable " ++
            "job ID. Optional systemd_properties pass bounded native resource controls.",
        .job_read => "Read durable job status and bounded stdout/stderr slices.",
        .job_cancel => "Request cancellation of one running durable job through systemd.",
    };
}

/// Rejects malformed known-tool arguments before workstation activity admission.
pub fn validateArguments(tool: Tool, arguments: std.json.ObjectMap) Error!void {
    switch (tool) {
        .command => {
            try onlyArguments(arguments, &.{ "argv", "cwd", "stdin", "timeout_seconds" });
            try requiredArgvBounded(arguments, "argv");
            try validateProcessOptions(arguments);
        },
        .shell => {
            try onlyArguments(arguments, &.{ "command", "cwd", "stdin", "timeout_seconds" });
            const command_text = try requiredString(arguments, "command");
            if (command_text.len > max_shell_command_bytes) return error.InvalidArguments;
            try validateProcessOptions(arguments);
        },
        .image_read => {
            try onlyArguments(arguments, &.{"path"});
            try validatePath(arguments, "path");
        },
        .job_start => {
            try onlyArguments(arguments, &.{
                "argv",
                "cwd",
                "stdin",
                "timeout_seconds",
                "output_limit_bytes",
                "systemd_properties",
            });
            try requiredArgvBounded(arguments, "argv");
            try validatePath(arguments, "cwd");
            try validateOptionalStdin(arguments);
            const timeout = try optionalPositiveInt(arguments, "timeout_seconds", jobs.default_timeout_seconds);
            if (timeout > jobs.max_timeout_seconds) return error.InvalidArguments;
            const output_limit = try optionalPositiveInt(arguments, "output_limit_bytes", jobs.default_output_limit_bytes);
            if (output_limit < jobs.min_output_limit_bytes or output_limit > jobs.max_output_limit_bytes) {
                return error.InvalidArguments;
            }
            try validateOptionalSystemdProperties(arguments);
        },
        .job_read => {
            try onlyArguments(arguments, &.{ "job_id", "stdout_offset", "stderr_offset", "max_bytes" });
            try validateJobId(arguments);
            try validateOptionalNonNegativeInt(arguments, "stdout_offset");
            try validateOptionalNonNegativeInt(arguments, "stderr_offset");
            const max_bytes = try optionalPositiveInt(arguments, "max_bytes", jobs.default_read_bytes);
            if (max_bytes < jobs.min_read_bytes or max_bytes > jobs.max_read_bytes) return error.InvalidArguments;
        },
        .job_cancel => {
            try onlyArguments(arguments, &.{"job_id"});
            try validateJobId(arguments);
        },
    }
}

fn command(context: Context, arguments: std.json.ObjectMap) Error!std.json.Value {
    return runProcess(context, arguments, try requiredArgv(context.allocator, arguments, "argv"), false);
}

fn shell(context: Context, arguments: std.json.ObjectMap) Error!std.json.Value {
    // Fleet provisions Bash as the login shell on every accepted node. Profiles run normally, then the command starts by
    // disabling Bash history in a readonly /dev/null lane. Exact non-login or POSIX argv remains `command`'s domain.
    const guarded = try guardedShellCommand(context.allocator, context.policy, try requiredString(arguments, "command"));
    return runProcess(context, arguments, &.{ shell_executable, shell_option, guarded }, true);
}

fn guardedShellCommand(allocator: Allocator, policy: Policy, command_text: []const u8) Error![]const u8 {
    return std.mem.concat(allocator, u8, &.{ policy.shell_prelude, command_text }) catch error.OutOfMemory;
}

fn runProcess(
    context: Context,
    arguments: std.json.ObjectMap,
    argv: []const []const u8,
    operator_login: bool,
) Error!std.json.Value {
    const cwd = try state.resolvePath(context.allocator, context.root, try requiredString(arguments, "cwd"));
    try requireWorkingDirectory(context.init.io, cwd);
    const timeout_seconds = try optionalPositiveInt(arguments, "timeout_seconds", default_process_timeout_seconds);
    if (timeout_seconds > max_process_timeout_seconds) return error.InvalidArguments;
    const stdin = try optionalString(arguments, "stdin");

    var child_env = try environment.current(context.init, context.init.gpa, context.policy.operator_marker);
    defer child_env.deinit();
    if (context.policy.agent_marker) |marker| try child_env.put(marker.name, marker.value);
    if (operator_login) if (context.policy.operator_marker) |marker| try child_env.put(marker.name, marker.value);

    var spawn_argv = argv;
    var owned_argv: ?[][]const u8 = null;
    var resolved_executable: ?[]const u8 = null;
    defer if (resolved_executable) |path| context.init.gpa.free(path);
    defer if (owned_argv) |items| context.init.gpa.free(items);
    if (std.mem.indexOfScalar(u8, argv[0], '/') == null) {
        const resolved = try environment.resolveExecutable(
            context.init.io,
            context.init.gpa,
            &child_env,
            cwd,
            argv[0],
        );
        resolved_executable = resolved;
        const items = context.init.gpa.alloc([]const u8, argv.len) catch return error.OutOfMemory;
        @memcpy(items, argv);
        items[0] = resolved;
        owned_argv = items;
        spawn_argv = items;
    }

    var result = try process.run(
        context.init.gpa,
        context.init.io,
        spawn_argv,
        cwd,
        stdin,
        .fromSeconds(@intCast(timeout_seconds)),
        &child_env,
    );
    defer result.deinit(context.init.gpa);
    var output = object();
    try put(context.allocator, &output, "exit_code", if (result.term) |term| .{ .integer = exitCode(term) } else .null);
    try put(context.allocator, &output, "stdout", .{ .string = try dupe(context.allocator, result.stdout) });
    try put(context.allocator, &output, "stderr", .{ .string = try dupe(context.allocator, result.stderr) });
    try put(context.allocator, &output, "truncated", .{ .bool = result.truncated });
    try put(context.allocator, &output, "timed_out", .{ .bool = result.timed_out });
    return .{ .object = output };
}

fn imageRead(context: Context, arguments: std.json.ObjectMap) Error!std.json.Value {
    const path = try state.resolvePath(context.allocator, context.root, try requiredString(arguments, "path"));
    const mime = if (std.mem.endsWith(u8, path, ".png"))
        "image/png"
    else if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg"))
        "image/jpeg"
    else
        return error.UnsupportedImage;
    const bytes = Io.Dir.cwd().readFileAlloc(context.init.io, path, context.allocator, .limited(max_image_bytes)) catch |failure|
        switch (failure) {
            error.FileNotFound => return error.FileNotFound,
            error.StreamTooLong => return error.ImageTooLarge,
            else => return error.FileReadFailed,
        };
    if (!validImageBytes(mime, bytes)) return error.InvalidImageData;
    const encoded = context.allocator.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len)) catch
        return error.OutOfMemory;
    _ = std.base64.standard.Encoder.encode(encoded, bytes);
    var image = object();
    try put(context.allocator, &image, "type", .{ .string = "image" });
    try put(context.allocator, &image, "data", .{ .string = encoded });
    try put(context.allocator, &image, "mimeType", .{ .string = mime });
    var native: std.json.Array = .init(context.allocator);
    native.append(.{ .object = image }) catch return error.OutOfMemory;
    var hash_buffer: [64]u8 = undefined;
    var output = object();
    try put(context.allocator, &output, "_native", .{ .array = native });
    try put(context.allocator, &output, "size", .{ .integer = @intCast(bytes.len) });
    try put(
        context.allocator,
        &output,
        "sha256",
        .{ .string = try dupe(context.allocator, state.hashHex(bytes, &hash_buffer)) },
    );
    return .{ .object = output };
}

fn jobStart(context: Context, arguments: std.json.ObjectMap) Error!std.json.Value {
    const cwd = try state.resolvePath(context.allocator, context.root, try requiredString(arguments, "cwd"));
    try requireWorkingDirectory(context.init.io, cwd);
    const timeout = try optionalPositiveInt(arguments, "timeout_seconds", jobs.default_timeout_seconds);
    const output_limit = try optionalPositiveInt(arguments, "output_limit_bytes", jobs.default_output_limit_bytes);
    const meta = try jobs.start(context.init, context.allocator, context.policy, context.state_dir, context.executable, .{
        .argv = try requiredArgv(context.allocator, arguments, "argv"),
        .cwd = cwd,
        .stdin = try optionalString(arguments, "stdin"),
        .timeout_seconds = std.math.cast(u32, timeout) orelse return error.InvalidArguments,
        .output_limit_bytes = output_limit,
        .systemd_properties = try optionalSystemdProperties(context.allocator, arguments),
    });
    return metaValue(context.allocator, meta);
}

fn jobRead(context: Context, arguments: std.json.ObjectMap) Error!std.json.Value {
    const result = try jobs.read(context.init.io, context.allocator, context.policy, context.state_dir, .{
        .job_id = try requiredString(arguments, "job_id"),
        .stdout_offset = try optionalNonNegativeInt(arguments, "stdout_offset", 0),
        .stderr_offset = try optionalNonNegativeInt(arguments, "stderr_offset", 0),
        .max_bytes = try optionalPositiveInt(arguments, "max_bytes", jobs.default_read_bytes),
    });
    var output = (try metaValue(context.allocator, result.meta)).object;
    try put(context.allocator, &output, "stdout", .{ .string = result.stdout });
    try put(context.allocator, &output, "stderr", .{ .string = result.stderr });
    try put(context.allocator, &output, "stdout_offset", .{ .integer = @intCast(result.stdout_offset) });
    try put(context.allocator, &output, "stderr_offset", .{ .integer = @intCast(result.stderr_offset) });
    try put(context.allocator, &output, "next_stdout_offset", .{ .integer = @intCast(result.next_stdout_offset) });
    try put(context.allocator, &output, "next_stderr_offset", .{ .integer = @intCast(result.next_stderr_offset) });
    try put(context.allocator, &output, "stdout_eof", .{ .bool = result.stdout_eof });
    try put(context.allocator, &output, "stderr_eof", .{ .bool = result.stderr_eof });
    return .{ .object = output };
}

fn jobCancel(context: Context, arguments: std.json.ObjectMap) Error!std.json.Value {
    const result = try jobs.cancel(
        context.init.io,
        context.allocator,
        context.policy,
        context.state_dir,
        try requiredString(arguments, "job_id"),
    );
    var output = (try metaValue(context.allocator, result.meta)).object;
    try put(context.allocator, &output, "cancelled", .{ .bool = result.cancelled });
    try put(context.allocator, &output, "reason", .{ .string = @tagName(result.reason) });
    return .{ .object = output };
}

fn metaValue(allocator: Allocator, meta: jobs.Meta) Error!std.json.Value {
    var output = object();
    try put(allocator, &output, "job_id", .{ .string = meta.job_id });
    try put(allocator, &output, "state", .{ .string = @tagName(meta.state) });
    try put(allocator, &output, "unit", .{ .string = meta.unit });
    try put(allocator, &output, "argv", try argvValue(allocator, meta.argv));
    try put(allocator, &output, "created_at", .{ .integer = meta.created_at });
    try put(allocator, &output, "cwd", .{ .string = meta.cwd });
    try put(allocator, &output, "timeout_seconds", .{ .integer = meta.timeout_seconds });
    try put(allocator, &output, "output_limit_bytes", .{ .integer = @intCast(meta.output_limit_bytes) });
    try put(allocator, &output, "systemd_properties", try argvValue(allocator, meta.systemd_properties));
    try put(allocator, &output, "stdout_truncated", .{ .bool = meta.stdout_truncated });
    try put(allocator, &output, "stderr_truncated", .{ .bool = meta.stderr_truncated });
    if (meta.exit_code) |value| try put(allocator, &output, "exit_code", .{ .integer = value });
    if (meta.ended_at) |value| try put(allocator, &output, "ended_at", .{ .integer = value });
    return .{ .object = output };
}

fn requiredArgv(allocator: Allocator, arguments: std.json.ObjectMap, name: []const u8) Error![]const []const u8 {
    const value = arguments.get(name) orelse return error.InvalidArguments;
    if (value != .array or value.array.items.len == 0 or value.array.items.len > process.max_arguments) {
        return error.InvalidArguments;
    }
    const result = allocator.alloc([]const u8, value.array.items.len) catch return error.OutOfMemory;
    for (value.array.items, result) |item, *slot| {
        if (item != .string or item.string.len == 0) return error.InvalidArguments;
        slot.* = item.string;
    }
    return result;
}

fn requiredArgvBounded(arguments: std.json.ObjectMap, name: []const u8) Error!void {
    const value = arguments.get(name) orelse return error.InvalidArguments;
    if (value != .array or value.array.items.len == 0 or value.array.items.len > process.max_arguments) {
        return error.InvalidArguments;
    }
    var total: usize = 0;
    for (value.array.items) |item| {
        if (item != .string or item.string.len == 0) return error.InvalidArguments;
        total = std.math.add(usize, total, item.string.len) catch return error.InvalidArguments;
        if (total > process.max_argv_bytes) return error.InvalidArguments;
    }
}

fn validateOptionalSystemdProperties(arguments: std.json.ObjectMap) Error!void {
    const value = arguments.get("systemd_properties") orelse return;
    if (value != .array or value.array.items.len > jobs.max_systemd_properties) return error.InvalidArguments;
    var properties: [jobs.max_systemd_properties][]const u8 = undefined;
    for (value.array.items, 0..) |item, index| {
        if (item != .string) return error.InvalidArguments;
        properties[index] = item.string;
    }
    jobs.validateSystemdProperties(properties[0..value.array.items.len]) catch return error.InvalidArguments;
}

fn optionalSystemdProperties(allocator: Allocator, arguments: std.json.ObjectMap) Error![]const []const u8 {
    const value = arguments.get("systemd_properties") orelse return &.{};
    if (value != .array or value.array.items.len > jobs.max_systemd_properties) return error.InvalidArguments;
    const result = allocator.alloc([]const u8, value.array.items.len) catch return error.OutOfMemory;
    for (value.array.items, result) |item, *slot| {
        if (item != .string) return error.InvalidArguments;
        slot.* = item.string;
    }
    jobs.validateSystemdProperties(result) catch return error.InvalidArguments;
    return result;
}

fn validateProcessOptions(arguments: std.json.ObjectMap) Error!void {
    try validatePath(arguments, "cwd");
    try validateOptionalStdin(arguments);
    const timeout = try optionalPositiveInt(arguments, "timeout_seconds", default_process_timeout_seconds);
    if (timeout > max_process_timeout_seconds) return error.InvalidArguments;
}

fn validateOptionalStdin(arguments: std.json.ObjectMap) Error!void {
    const value = arguments.get("stdin") orelse return;
    if (value == .null) return;
    if (value != .string or value.string.len > process.max_stdin_bytes) return error.InvalidArguments;
}

fn validatePath(arguments: std.json.ObjectMap, name: []const u8) Error!void {
    const value = try requiredString(arguments, name);
    if (value.len > state.max_path_bytes) return error.InvalidArguments;
}

fn validateJobId(arguments: std.json.ObjectMap) Error!void {
    if (!validHex(try requiredString(arguments, "job_id"), 32, true)) return error.InvalidArguments;
}

fn validateOptionalNonNegativeInt(arguments: std.json.ObjectMap, name: []const u8) Error!void {
    const value = arguments.get(name) orelse return;
    if (value != .integer or value.integer < 0) return error.InvalidArguments;
}

fn validHex(value: []const u8, expected_len: usize, lowercase_only: bool) bool {
    if (value.len != expected_len) return false;
    for (value) |byte| {
        if (!std.ascii.isHex(byte) or (lowercase_only and std.ascii.isUpper(byte))) return false;
    }
    return true;
}

/// Validates the bounded PNG/JPEG byte shapes admitted by the public `image_read` operation.
pub fn validImageBytes(mime: []const u8, bytes: []const u8) bool {
    if (std.mem.eql(u8, mime, "image/png")) {
        const signature = "\x89PNG\r\n\x1a\n";
        if (bytes.len < 33 or !std.mem.eql(u8, bytes[0..signature.len], signature)) return false;
        if (!std.mem.eql(u8, bytes[8..16], "\x00\x00\x00\x0dIHDR")) return false;
        if (std.mem.allEqual(u8, bytes[16..20], 0) or std.mem.allEqual(u8, bytes[20..24], 0)) return false;
        return bytes[26] == 0 and bytes[27] == 0 and bytes[28] <= 1;
    }
    if (std.mem.eql(u8, mime, "image/jpeg")) {
        return bytes.len >= 5 and std.mem.eql(u8, bytes[0..3], "\xff\xd8\xff") and
            std.mem.eql(u8, bytes[bytes.len - 2 ..], "\xff\xd9");
    }
    return false;
}

fn onlyArguments(arguments: std.json.ObjectMap, allowed: []const []const u8) Error!void {
    var iterator = arguments.iterator();
    while (iterator.next()) |entry| {
        var accepted = false;
        for (allowed) |name| {
            if (std.mem.eql(u8, entry.key_ptr.*, name)) {
                accepted = true;
                break;
            }
        }
        if (!accepted) return error.InvalidArguments;
    }
}

fn argvValue(allocator: Allocator, argv: []const []const u8) Error!std.json.Value {
    var output: std.json.Array = .init(allocator);
    for (argv) |argument| output.append(.{ .string = argument }) catch return error.OutOfMemory;
    return .{ .array = output };
}

fn requireWorkingDirectory(io: Io, path: []const u8) Error!void {
    var directory = Io.Dir.cwd().openDir(io, path, .{}) catch return error.WorkingDirectoryUnavailable;
    directory.close(io);
}

fn requiredString(arguments: std.json.ObjectMap, name: []const u8) Error![]const u8 {
    const value = arguments.get(name) orelse return error.InvalidArguments;
    if (value != .string or value.string.len == 0) return error.InvalidArguments;
    return value.string;
}

fn optionalString(arguments: std.json.ObjectMap, name: []const u8) Error!?[]const u8 {
    const value = arguments.get(name) orelse return null;
    if (value == .null) return null;
    if (value != .string) return error.InvalidArguments;
    return value.string;
}

fn optionalPositiveInt(arguments: std.json.ObjectMap, name: []const u8, fallback: usize) Error!usize {
    const value = arguments.get(name) orelse return fallback;
    if (value != .integer or value.integer < 1) return error.InvalidArguments;
    return std.math.cast(usize, value.integer) orelse return error.InvalidArguments;
}

fn optionalNonNegativeInt(arguments: std.json.ObjectMap, name: []const u8, fallback: usize) Error!usize {
    const value = arguments.get(name) orelse return fallback;
    if (value != .integer or value.integer < 0) return error.InvalidArguments;
    return std.math.cast(usize, value.integer) orelse return error.InvalidArguments;
}

fn exitCode(term: std.process.Child.Term) i32 {
    return switch (term) {
        .exited => |code| code,
        .signal => |signal| -@as(i32, @intCast(@backingInt(signal))),
        .stopped => |signal| -@as(i32, @intCast(@backingInt(signal))),
        .unknown => |code| @intCast(code),
    };
}

fn object() std.json.ObjectMap {
    return .empty;
}

fn put(allocator: Allocator, output: *std.json.ObjectMap, key: []const u8, value: std.json.Value) Error!void {
    output.put(allocator, key, value) catch return error.OutOfMemory;
}

fn dupe(allocator: Allocator, bytes: []const u8) Error![]u8 {
    return allocator.dupe(u8, bytes) catch error.OutOfMemory;
}

/// Runs one durable job helper role using the embedding host policy.
pub fn runJob(init: std.process.Init, policy: Policy, job_dir: []const u8) Error!void {
    return jobs.run(init, policy, job_dir);
}

/// Writes one durable job terminal receipt using the embedding host policy.
pub fn finishJob(init: std.process.Init, policy: Policy, job_dir: []const u8) Error!void {
    return jobs.finish(init, policy, job_dir);
}

/// Returns the transport-neutral JSON input schema owned by one workstation tool.
pub fn inputSchemaJson(tool: Tool) []const u8 {
    return switch (tool) {
        .command =>
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "argv": {
        \\      "type": "array", "minItems": 1, "maxItems": 256,
        \\      "items": {"type": "string", "minLength": 1, "maxLength": 32768},
        \\      "description": "Combined argv bytes must not exceed 32768."
        \\    },
        \\    "cwd": {
        \\      "type": "string", "minLength": 1, "maxLength": 4096,
        \\      "description": "Existing target directory. Start with '.'; do not infer /home/<node> from the node label."
        \\    },
        \\    "stdin": {"type": ["string", "null"], "maxLength": 131072},
        \\    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300}
        \\  },
        \\  "required": ["argv", "cwd"],
        \\  "additionalProperties": false
        \\}
        ,
        .shell =>
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "command": {"type": "string", "minLength": 1, "maxLength": 32694},
        \\    "cwd": {
        \\      "type": "string", "minLength": 1, "maxLength": 4096,
        \\      "description": "Existing target directory. Start with '.'; do not infer /home/<node> from the node label."
        \\    },
        \\    "stdin": {"type": ["string", "null"], "maxLength": 131072},
        \\    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300}
        \\  },
        \\  "required": ["command", "cwd"],
        \\  "additionalProperties": false
        \\}
        ,
        .image_read =>
        \\{
        \\  "type": "object",
        \\  "properties": {"path": {
        \\    "type": "string", "minLength": 1, "maxLength": 4096,
        \\    "pattern": "\\.(png|jpg|jpeg)$"
        \\  }},
        \\  "required": ["path"],
        \\  "additionalProperties": false
        \\}
        ,
        .job_start =>
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "argv": {
        \\      "type": "array", "minItems": 1, "maxItems": 256,
        \\      "items": {"type": "string", "minLength": 1, "maxLength": 32768},
        \\      "description": "Combined argv bytes must not exceed 32768."
        \\    },
        \\    "cwd": {
        \\      "type": "string", "minLength": 1, "maxLength": 4096,
        \\      "description": "Existing target directory. Start with '.'; do not infer /home/<node> from the node label."
        \\    },
        \\    "stdin": {"type": ["string", "null"], "maxLength": 131072},
        \\    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
        \\    "output_limit_bytes": {"type": "integer", "minimum": 4096, "maximum": 536870912},
        \\    "systemd_properties": {
        \\      "type": "array", "maxItems": 16,
        \\      "items": {
        \\        "type": "string", "minLength": 1, "maxLength": 256,
        \\        "pattern": "^(MemoryHigh|MemoryMax|MemorySwapMax|TasksMax|CPUQuota|CPUWeight|IOWeight)=.+$"
        \\      },
        \\      "description": "Optional native systemd resource-control NAME=VALUE properties; lifecycle properties are refused."
        \\    }
        \\  },
        \\  "required": ["argv", "cwd"],
        \\  "additionalProperties": false
        \\}
        ,
        .job_read =>
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "job_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
        \\    "stdout_offset": {"type": "integer", "minimum": 0},
        \\    "stderr_offset": {"type": "integer", "minimum": 0},
        \\    "max_bytes": {"type": "integer", "minimum": 2, "maximum": 32768}
        \\  },
        \\  "required": ["job_id"],
        \\  "additionalProperties": false
        \\}
        ,
        .job_cancel =>
        \\{
        \\  "type": "object",
        \\  "properties": {"job_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"}},
        \\  "required": ["job_id"],
        \\  "additionalProperties": false
        \\}
        ,
    };
}

/// Returns the transport-neutral JSON success schema owned by one workstation tool.
pub fn outputSchemaJson(tool: Tool) []const u8 {
    return switch (tool) {
        .command, .shell =>
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "exit_code": {"type": ["integer", "null"]},
        \\    "stdout": {"type": "string", "maxLength": 262144},
        \\    "stderr": {"type": "string", "maxLength": 262144},
        \\    "truncated": {"type": "boolean"},
        \\    "timed_out": {"type": "boolean"}
        \\  },
        \\  "required": ["exit_code", "stdout", "stderr", "truncated", "timed_out"],
        \\  "additionalProperties": false
        \\}
        ,
        .image_read =>
        \\{
        \\  "type": "object",
        \\  "properties": {
        \\    "size": {"type": "integer", "minimum": 0, "maximum": 8388608},
        \\    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"}
        \\  },
        \\  "required": ["size", "sha256"],
        \\  "additionalProperties": false
        \\}
        ,
        .job_start => jobMetaSchemaJson(),
        .job_read => jobReadSchemaJson(),
        .job_cancel => jobCancelSchemaJson(),
    };
}

fn jobMetaSchemaJson() []const u8 {
    return
    \\{
    \\  "type": "object",
    \\  "properties": {
    \\    "job_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    \\    "state": {"enum": ["starting", "running", "stopping", "exited", "timed_out", "cancelled", "failed", "indeterminate"]},
    \\    "unit": {"type": "string", "minLength": 41, "maxLength": 72},
    \\    "argv": {"type": "array", "minItems": 1, "maxItems": 256, "items": {"type": "string", "minLength": 1, "maxLength": 32768}},
    \\    "cwd": {"type": "string", "minLength": 1, "maxLength": 4096},
    \\    "created_at": {"type": "integer"},
    \\    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
    \\    "output_limit_bytes": {"type": "integer", "minimum": 4096, "maximum": 536870912},
    \\    "systemd_properties": {
    \\      "type": "array", "maxItems": 16,
    \\      "items": {"type": "string", "minLength": 1, "maxLength": 256}
    \\    },
    \\    "stdout_truncated": {"type": "boolean"},
    \\    "stderr_truncated": {"type": "boolean"},
    \\    "exit_code": {"type": "integer"},
    \\    "ended_at": {"type": "integer"}
    \\  },
    \\  "required": ["job_id", "state", "unit", "argv", "cwd", "created_at", "timeout_seconds",
    \\    "output_limit_bytes", "systemd_properties", "stdout_truncated", "stderr_truncated"],
    \\  "additionalProperties": false
    \\}
    ;
}

fn jobReadSchemaJson() []const u8 {
    return
    \\{
    \\  "type": "object",
    \\  "properties": {
    \\    "job_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    \\    "state": {"enum": ["starting", "running", "stopping", "exited", "timed_out", "cancelled", "failed", "indeterminate"]},
    \\    "unit": {"type": "string", "minLength": 41, "maxLength": 72},
    \\    "argv": {"type": "array", "minItems": 1, "maxItems": 256, "items": {"type": "string", "minLength": 1, "maxLength": 32768}},
    \\    "cwd": {"type": "string", "minLength": 1, "maxLength": 4096},
    \\    "created_at": {"type": "integer"},
    \\    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
    \\    "output_limit_bytes": {"type": "integer", "minimum": 4096, "maximum": 536870912},
    \\    "systemd_properties": {
    \\      "type": "array", "maxItems": 16,
    \\      "items": {"type": "string", "minLength": 1, "maxLength": 256}
    \\    },
    \\    "stdout_truncated": {"type": "boolean"}, "stderr_truncated": {"type": "boolean"},
    \\    "exit_code": {"type": "integer"}, "ended_at": {"type": "integer"},
    \\    "stdout": {"type": "string", "maxLength": 32768},
    \\    "stderr": {"type": "string", "maxLength": 32768},
    \\    "stdout_offset": {"type": "integer", "minimum": 0},
    \\    "stderr_offset": {"type": "integer", "minimum": 0},
    \\    "next_stdout_offset": {"type": "integer", "minimum": 0},
    \\    "next_stderr_offset": {"type": "integer", "minimum": 0},
    \\    "stdout_eof": {"type": "boolean"}, "stderr_eof": {"type": "boolean"}
    \\  },
    \\  "required": ["job_id", "state", "unit", "argv", "cwd", "created_at", "timeout_seconds",
    \\    "output_limit_bytes", "systemd_properties", "stdout_truncated", "stderr_truncated", "stdout", "stderr", "stdout_offset",
    \\    "stderr_offset", "next_stdout_offset", "next_stderr_offset", "stdout_eof", "stderr_eof"],
    \\  "additionalProperties": false
    \\}
    ;
}

fn jobCancelSchemaJson() []const u8 {
    return
    \\{
    \\  "type": "object",
    \\  "properties": {
    \\    "job_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    \\    "state": {"enum": ["starting", "running", "stopping", "exited", "timed_out", "cancelled", "failed", "indeterminate"]},
    \\    "unit": {"type": "string", "minLength": 41, "maxLength": 72},
    \\    "argv": {"type": "array", "minItems": 1, "maxItems": 256, "items": {"type": "string", "minLength": 1, "maxLength": 32768}},
    \\    "cwd": {"type": "string", "minLength": 1, "maxLength": 4096},
    \\    "created_at": {"type": "integer"},
    \\    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
    \\    "output_limit_bytes": {"type": "integer", "minimum": 4096, "maximum": 536870912},
    \\    "systemd_properties": {
    \\      "type": "array", "maxItems": 16,
    \\      "items": {"type": "string", "minLength": 1, "maxLength": 256}
    \\    },
    \\    "stdout_truncated": {"type": "boolean"}, "stderr_truncated": {"type": "boolean"},
    \\    "exit_code": {"type": "integer"}, "ended_at": {"type": "integer"},
    \\    "cancelled": {"type": "boolean"},
    \\    "reason": {"enum": ["already_finished", "stop_requested"]}
    \\  },
    \\  "required": ["job_id", "state", "unit", "argv", "cwd", "created_at", "timeout_seconds",
    \\    "output_limit_bytes", "systemd_properties", "stdout_truncated", "stderr_truncated", "cancelled", "reason"],
    \\  "additionalProperties": false
    \\}
    ;
}

const test_policy = Policy{
    .agent_marker = .{ .name = "AGENT_CHILD", .value = "1" },
    .operator_marker = .{ .name = "OPERATOR_PROFILE", .value = "1" },
    .shell_prelude = "unset OPERATOR_PROFILE;HISTFILE=/dev/null;set +o history;",
    .job_unit_prefix = "workstation-job-",
    .job_run_argument = "--job-run",
    .job_finish_argument = "--job-finish",
};

test "job start validates native systemd resource properties before dispatch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var accepted = object();
    var accepted_argv: std.json.Array = .init(allocator);
    try accepted_argv.append(.{ .string = "/usr/bin/true" });
    try put(allocator, &accepted, "argv", .{ .array = accepted_argv });
    try put(allocator, &accepted, "cwd", .{ .string = "/tmp" });
    var properties: std.json.Array = .init(allocator);
    try properties.append(.{ .string = "MemoryMax=1G" });
    try properties.append(.{ .string = "CPUWeight=50" });
    try put(allocator, &accepted, "systemd_properties", .{ .array = properties });
    try validateArguments(.job_start, accepted);

    var rejected = object();
    var rejected_argv: std.json.Array = .init(allocator);
    try rejected_argv.append(.{ .string = "/usr/bin/true" });
    try put(allocator, &rejected, "argv", .{ .array = rejected_argv });
    try put(allocator, &rejected, "cwd", .{ .string = "/tmp" });
    var lifecycle: std.json.Array = .init(allocator);
    try lifecycle.append(.{ .string = "Restart=always" });
    try put(allocator, &rejected, "systemd_properties", .{ .array = lifecycle });
    try std.testing.expectError(error.InvalidArguments, validateArguments(.job_start, rejected));
}

test "process tools reject an unavailable working directory before spawn" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const context: Context = .{
        .init = .{
            .minimal = undefined,
            .arena = &arena,
            .gpa = std.testing.allocator,
            .io = std.testing.io,
            .environ_map = undefined,
            .preopens = undefined,
        },
        .policy = test_policy,
        .allocator = allocator,
        .state_dir = root,
        .root = root,
        .executable = "/unused",
    };

    var command_arguments = object();
    var argv: std.json.Array = .init(allocator);
    try argv.append(.{ .string = "/usr/bin/true" });
    try put(allocator, &command_arguments, "argv", .{ .array = argv });
    try put(allocator, &command_arguments, "cwd", .{ .string = "missing-user-home" });
    try std.testing.expectError(error.WorkingDirectoryUnavailable, command(context, command_arguments));

    var shell_arguments = object();
    try put(allocator, &shell_arguments, "command", .{ .string = "true" });
    try put(allocator, &shell_arguments, "cwd", .{ .string = "missing-user-home" });
    try std.testing.expectError(error.WorkingDirectoryUnavailable, shell(context, shell_arguments));

    var job_arguments = object();
    var job_argv: std.json.Array = .init(allocator);
    try job_argv.append(.{ .string = "/usr/bin/true" });
    try put(allocator, &job_arguments, "argv", .{ .array = job_argv });
    try put(allocator, &job_arguments, "cwd", .{ .string = "missing-user-home" });
    try std.testing.expectError(error.WorkingDirectoryUnavailable, jobStart(context, job_arguments));
}

test "image read distinguishes missing read and size failures" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const context: Context = .{
        .init = .{
            .minimal = undefined,
            .arena = &arena,
            .gpa = std.testing.allocator,
            .io = std.testing.io,
            .environ_map = undefined,
            .preopens = undefined,
        },
        .policy = test_policy,
        .allocator = allocator,
        .state_dir = root,
        .root = root,
        .executable = "/unused",
    };
    var arguments = object();
    try put(allocator, &arguments, "path", .{ .string = "missing.png" });
    try std.testing.expectError(error.FileNotFound, imageRead(context, arguments));

    try temporary.dir.createDir(std.testing.io, "unreadable.png", .default_dir);
    try put(allocator, &arguments, "path", .{ .string = "unreadable.png" });
    try std.testing.expectError(error.FileReadFailed, imageRead(context, arguments));

    const oversized = try temporary.dir.createFile(std.testing.io, "oversized.png", .{});
    defer oversized.close(std.testing.io);
    try oversized.setLength(std.testing.io, max_image_bytes + 1);
    try put(allocator, &arguments, "path", .{ .string = "oversized.png" });
    try std.testing.expectError(error.ImageTooLarge, imageRead(context, arguments));

    const fake = try temporary.dir.createFile(std.testing.io, "fake.png", .{});
    try fake.writeStreamingAll(std.testing.io, "this is not png");
    fake.close(std.testing.io);
    try put(allocator, &arguments, "path", .{ .string = "fake.png" });
    try std.testing.expectError(error.InvalidImageData, imageRead(context, arguments));
}

test "image byte admission rejects suffix-only and mismatched formats" {
    try std.testing.expect(!validImageBytes("image/png", "this is not png"));
    try std.testing.expect(!validImageBytes("image/jpeg", "this is not jpeg"));
    try std.testing.expect(!validImageBytes("image/png", "\xff\xd8\xff\xd9"));
    try std.testing.expect(validImageBytes("image/jpeg", "\xff\xd8\xff\xe0\xff\xd9"));

    var png: [33]u8 = @splat(0);
    @memcpy(png[0..16], "\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR");
    png[19] = 1;
    png[23] = 1;
    png[24] = 8;
    png[25] = 2;
    try std.testing.expect(validImageBytes("image/png", &png));
    png[27] = 1;
    try std.testing.expect(!validImageBytes("image/png", &png));
}

test "workstation shell guard cannot persist Bash history" {
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root = try temporary.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const history_path = try std.fs.path.join(std.testing.allocator, &.{ root, ".bash_history" });
    defer std.testing.allocator.free(history_path);
    const history = try Io.Dir.cwd().createFile(std.testing.io, history_path, .{ .permissions = .fromMode(0o600) });
    try history.writeStreamingAll(std.testing.io, "human-history\n");
    history.close(std.testing.io);

    const guarded = try guardedShellCommand(
        std.testing.allocator,
        test_policy,
        "set -o history; history -s workstation-history-probe; history -w",
    );
    defer std.testing.allocator.free(guarded);
    var env = std.process.Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("HOME", root);
    try env.put("PATH", "/usr/bin");
    try env.put("HISTFILE", history_path);
    var result = try process.run(
        std.testing.allocator,
        std.testing.io,
        &.{ shell_executable, "-c", guarded },
        root,
        null,
        .fromSeconds(2),
        &env,
    );
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u8, 0), result.term.?.exited);
    const after = try Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        history_path,
        std.testing.allocator,
        .limited(128),
    );
    defer std.testing.allocator.free(after);
    try std.testing.expectEqualStrings("human-history\n", after);
}

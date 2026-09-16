//! Test-only caller of the public workstation tool contract; no model or transport shortcut is installed.
const std = @import("std");
const tools = @import("workstation_tools");
pub fn main(init: std.process.Init) void {
    run(init) catch |failure| {
        std.debug.print("{{\"error\":\"{s}\"}}\n", .{@errorName(failure)});
        std.process.exit(1);
    };
}
fn run(init: std.process.Init) !void {
    const a = init.arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    if (args.len != 6) return error.InvalidArguments;
    const policy = tools.Policy{
        .job_backend = .walker,
        .environment_source = .process,
        .walker = .{ .executable = args[1], .home = args[2] },
        .job_unit_prefix = "canary-",
        .job_run_argument = "--unused-run",
        .job_finish_argument = "--unused-finish",
    };
    const tool = tools.parse(args[4]) orelse return error.InvalidArguments;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, args[5], a, .limited(256 * 1024 + 1));
    const request = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
    if (request != .object) return error.InvalidArguments;
    try tools.validateArgumentsForPolicy(policy, tool, request.object);
    const result = try tools.call(.{ .init = init, .policy = policy, .allocator = a, .state_dir = args[3], .root = "/", .executable = args[0] }, tool, request.object);
    var buf: [4096]u8 = undefined;
    var out = std.Io.File.stdout().writer(init.io, &buf);
    try std.json.Stringify.value(result, .{}, &out.interface);
    try out.interface.writeByte('\n');
    try out.interface.flush();
}

//! Test-only direct caller for the typed Walker observation adapter.
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
    if (args.len < 4 or args.len > 5) return error.InvalidArguments;
    const config = tools.WalkerConfig{ .executable = args[1], .home = args[2] };
    const op = args[3];

    if (std.mem.eql(u8, op, "inventory")) {
        return emit(init.io, try tools.Walker.inventory(init.io, a, config));
    }
    if (args.len != 5) return error.InvalidArguments;
    const detail = try tools.Walker.inspectWorkload(init.io, a, config, args[4]);
    if (std.mem.eql(u8, op, "inspect")) return emit(init.io, detail);
    const ref = tools.Walker.Ref{ .config = config, .run_id = detail.run_id, .name = detail.name };
    if (std.mem.eql(u8, op, "logs")) {
        return emit(init.io, try tools.Walker.workloadLogs(init.io, a, ref, 0, 0, 32768, 4096));
    }
    if (std.mem.eql(u8, op, "stats")) {
        return emit(init.io, try tools.Walker.workloadStats(init.io, a, ref));
    }
    return error.InvalidArguments;
}

fn emit(io: std.Io, value: anytype) !void {
    var buffer: [4096]u8 = undefined;
    var out = std.Io.File.stdout().writer(io, &buffer);
    try std.json.Stringify.value(value, .{}, &out.interface);
    try out.interface.writeByte('\n');
    try out.interface.flush();
}

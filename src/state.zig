//! Owns small generic durable helpers for workstation-tool job state.
//!
//! Session identity, activity history, transport state, and application configuration belong to consumers.

const std = @import("std");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Maximum path bytes materialized by one workstation-tool state operation.
pub const max_path_bytes: usize = 4096;
/// Maximum encoded bytes accepted for one atomic JSON job-state file.
pub const max_json_bytes: usize = 64 * 1024;

/// Closed failures from generic path and durable-file operations.
pub const Error = error{
    OutOfMemory,
    PathTooLong,
    StateCreateFailed,
    StateOpenFailed,
    StateReadFailed,
    StateWriteFailed,
    StateSyncFailed,
    StateReplaceFailed,
    StateJsonInvalid,
};

/// Returns the current real clock in whole Unix seconds for human-operable records.
pub fn timestamp(io: Io) i64 {
    return Io.Clock.real.now(io).toSeconds();
}

/// Writes a lowercase SHA-256 digest into caller-owned fixed storage.
pub fn hashHex(bytes: []const u8, out: *[64]u8) []const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bufPrint(out, "{x}", .{digest}) catch unreachable;
}

/// Fills an even-length destination with cryptographically random lowercase hexadecimal bytes.
pub fn randomHex(io: Io, out: []u8) void {
    std.debug.assert(out.len % 2 == 0 and out.len <= 64);
    var bytes: [32]u8 = undefined;
    const count = out.len / 2;
    io.random(bytes[0..count]);
    _ = std.fmt.bufPrint(out, "{x}", .{bytes[0..count]}) catch unreachable;
}

/// Resolves a relative workstation path against `root`, allocating only for the current request.
pub fn resolvePath(allocator: Allocator, root: []const u8, path: []const u8) Error![]const u8 {
    if (path.len == 0 or path.len > max_path_bytes) return error.PathTooLong;
    if (std.fs.path.isAbsolute(path)) return path;
    return std.fs.path.join(allocator, &.{ root, path }) catch return error.OutOfMemory;
}

/// Atomically replaces one private JSON file after synchronizing its complete temporary contents.
pub fn writeJsonAtomic(io: Io, path: []const u8, value: anytype) Error!void {
    if (path.len == 0 or path.len > max_path_bytes) return error.PathTooLong;
    var encoded: [max_json_bytes]u8 = undefined;
    var writer = Io.Writer.fixed(&encoded);
    std.json.Stringify.value(value, .{}, &writer) catch return error.StateJsonInvalid;
    writer.writeByte('\n') catch return error.StateJsonInvalid;
    try writeBytesAtomic(io, path, writer.buffered(), .fromMode(0o600));
}

/// Atomically replaces one file and synchronizes both its contents and containing directory entry.
pub fn writeBytesAtomic(io: Io, path: []const u8, bytes: []const u8, permissions: Io.File.Permissions) Error!void {
    if (path.len == 0 or path.len > max_path_bytes) return error.PathTooLong;
    var suffix: [16]u8 = undefined;
    randomHex(io, &suffix);
    var temporary_buffer: [max_path_bytes]u8 = undefined;
    const temporary = std.fmt.bufPrint(&temporary_buffer, "{s}.tmp.{s}", .{ path, suffix }) catch
        return error.PathTooLong;
    const dir = Io.Dir.cwd();
    const file = dir.createFile(io, temporary, .{
        .truncate = true,
        .exclusive = true,
        .permissions = permissions,
    }) catch return error.StateCreateFailed;
    var committed = false;
    defer if (!committed) dir.deleteFile(io, temporary) catch {};
    var open = true;
    defer if (open) file.close(io);
    file.writeStreamingAll(io, bytes) catch return error.StateWriteFailed;
    file.sync(io) catch return error.StateSyncFailed;
    file.close(io);
    open = false;
    dir.rename(temporary, dir, path, io) catch return error.StateReplaceFailed;
    const parent = std.fs.path.dirname(path) orelse ".";
    try syncDirectory(io, parent);
    committed = true;
}

/// Ensures one directory path exists and durably publishes every newly created directory entry.
pub fn ensureDirectory(io: Io, path: []const u8) Error!void {
    if (path.len == 0 or path.len > max_path_bytes) return error.PathTooLong;
    if (directoryExists(io, path)) return;
    const parent = std.fs.path.dirname(path) orelse ".";
    if (!std.mem.eql(u8, parent, path)) try ensureDirectory(io, parent);
    Io.Dir.cwd().createDir(io, path, .default_dir) catch |failure| switch (failure) {
        error.PathAlreadyExists => {
            if (!directoryExists(io, path)) return error.StateCreateFailed;
            return;
        },
        else => return error.StateCreateFailed,
    };
    try syncDirectory(io, parent);
}

/// Creates one new directory entry exclusively and synchronizes the parent that makes it reachable.
pub fn createDirectoryExclusive(io: Io, parent: []const u8, path: []const u8) Error!void {
    if (parent.len == 0 or path.len == 0 or parent.len > max_path_bytes or path.len > max_path_bytes) {
        return error.PathTooLong;
    }
    try ensureDirectory(io, parent);
    Io.Dir.cwd().createDir(io, path, .default_dir) catch return error.StateCreateFailed;
    try syncDirectory(io, parent);
}

/// Parses one bounded private JSON file into caller-owned allocation.
pub fn readJson(comptime T: type, io: Io, allocator: Allocator, path: []const u8) Error!T {
    if (path.len == 0 or path.len > max_path_bytes) return error.PathTooLong;
    const bytes = Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_json_bytes)) catch
        return error.StateReadFailed;
    return std.json.parseFromSliceLeaky(T, allocator, bytes, .{ .ignore_unknown_fields = false }) catch
        return error.StateJsonInvalid;
}

fn syncDirectory(io: Io, path: []const u8) Error!void {
    var directory = Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch return error.StateOpenFailed;
    defer directory.close(io);
    while (true) switch (std.posix.errno(std.posix.system.fsync(directory.handle))) {
        .SUCCESS => return,
        .INTR => {},
        else => return error.StateSyncFailed,
    };
}

fn directoryExists(io: Io, path: []const u8) bool {
    const stat = Io.Dir.cwd().statFile(io, path, .{}) catch return false;
    return stat.kind == .directory;
}

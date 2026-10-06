//! Small request-local helpers shared by workstation tool implementations.
//!
//! Durable workload state belongs to Walker. This module owns only generic
//! path/hash/time helpers and finite scratch creation for launch handoff.

const std = @import("std");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Maximum path bytes materialized by one workstation tool operation.
pub const max_path_bytes: usize = 4096;

/// Closed failures from request-local filesystem helpers.
pub const Error = error{
    OutOfMemory,
    PathTooLong,
    DirectoryCreateFailed,
    ScratchCreateFailed,
    ScratchWriteFailed,
};

/// Returns the current real clock in whole Unix seconds for human-readable metadata.
pub fn timestamp(io: Io) i64 {
    return Io.Clock.real.now(io).toSeconds();
}

/// Writes a lowercase SHA-256 digest into caller-owned fixed storage.
pub fn hashHex(bytes: []const u8, out: *[64]u8) []const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bufPrint(out, "{x}", .{digest}) catch unreachable;
}

/// Fills an even-length destination with cryptographically random lowercase hex.
pub fn randomHex(io: Io, out: []u8) void {
    std.debug.assert(out.len % 2 == 0 and out.len <= 64);
    var bytes: [32]u8 = undefined;
    const count = out.len / 2;
    io.random(bytes[0..count]);
    _ = std.fmt.bufPrint(out, "{x}", .{bytes[0..count]}) catch
        unreachable;
}

/// Resolves one relative workstation path against the consumer root.
pub fn resolvePath(
    allocator: Allocator,
    root: []const u8,
    path: []const u8,
) Error![]const u8 {
    if (path.len == 0 or path.len > max_path_bytes)
        return error.PathTooLong;
    if (std.fs.path.isAbsolute(path)) return path;
    return std.fs.path.join(allocator, &.{ root, path }) catch
        return error.OutOfMemory;
}

/// Creates one private scratch directory exclusively below one private parent.
pub fn createScratchDirectory(
    io: Io,
    parent: []const u8,
    path: []const u8,
) Error!void {
    if (parent.len == 0 or path.len == 0 or
        parent.len > max_path_bytes or path.len > max_path_bytes)
    {
        return error.PathTooLong;
    }
    try ensureDirectory(io, parent);
    Io.Dir.cwd().createDir(
        io,
        path,
        .fromMode(0o700),
    ) catch return error.DirectoryCreateFailed;
}

/// Writes one complete private scratch file before any consumer can observe it.
pub fn writeScratchFile(
    io: Io,
    path: []const u8,
    bytes: []const u8,
) Error!void {
    if (path.len == 0 or path.len > max_path_bytes)
        return error.PathTooLong;
    const file = Io.Dir.cwd().createFile(io, path, .{
        .truncate = true,
        .exclusive = true,
        .permissions = .fromMode(0o600),
    }) catch return error.ScratchCreateFailed;
    defer file.close(io);
    file.writeStreamingAll(io, bytes) catch
        return error.ScratchWriteFailed;
}

fn ensureDirectory(io: Io, path: []const u8) Error!void {
    if (path.len == 0 or path.len > max_path_bytes)
        return error.PathTooLong;
    if (directoryExists(io, path)) return;
    const parent = std.fs.path.dirname(path) orelse ".";
    if (!std.mem.eql(u8, parent, path))
        try ensureDirectory(io, parent);
    Io.Dir.cwd().createDir(
        io,
        path,
        .fromMode(0o700),
    ) catch |failure| switch (failure) {
        error.PathAlreadyExists => {
            if (!directoryExists(io, path))
                return error.DirectoryCreateFailed;
        },
        else => return error.DirectoryCreateFailed,
    };
}

fn directoryExists(io: Io, path: []const u8) bool {
    const stat = Io.Dir.cwd().statFile(io, path, .{}) catch return false;
    return stat.kind == .directory;
}

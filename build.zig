//! Builds and tests the reusable workstation-tools Zig module.

const std = @import("std");

/// Exposes the workstation_tools module and runs its source-local tests.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const self_hosted = target.result.cpu.arch == .x86_64;
    const module = b.addModule("workstation_tools", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    _ = module;
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .use_llvm = !self_hosted,
        .use_lld = !self_hosted,
    });
    const audit_run = b.addSystemCommand(&.{ "bash", "tools/audit_source.sh" });
    audit_run.setCwd(b.path("."));
    const check = b.step("check", "Compile and audit workstation tools and tests");
    check.dependOn(&tests.step);
    check.dependOn(&audit_run.step);
    const test_step = b.step("test", "Run workstation tool tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);
}

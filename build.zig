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
    const driver_module = b.createModule(.{
        .root_source_file = b.path("tools/walker_driver.zig"),
        .target = target,
        .optimize = optimize,
    });
    driver_module.addImport("workstation_tools", module);
    const driver = b.addExecutable(.{
        .name = "walker-contract-driver",
        .root_module = driver_module,
        .use_llvm = !self_hosted,
        .use_lld = !self_hosted,
    });
    const install_driver = b.addInstallArtifact(driver, .{});
    const observer_module = b.createModule(.{
        .root_source_file = b.path("tools/walker_observer_driver.zig"),
        .target = target,
        .optimize = optimize,
    });
    observer_module.addImport("workstation_tools", module);
    const observer = b.addExecutable(.{
        .name = "walker-observer-driver",
        .root_module = observer_module,
        .use_llvm = !self_hosted,
        .use_lld = !self_hosted,
    });
    const install_observer = b.addInstallArtifact(observer, .{});
    const walker_driver = b.step("walker-driver", "Build the test-only Walker adapter callers");
    walker_driver.dependOn(&install_driver.step);
    walker_driver.dependOn(&install_observer.step);
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .use_llvm = !self_hosted,
        .use_lld = !self_hosted,
    });
    b.step("check-compile", "Compile workstation tool tests without the host source audit").dependOn(&tests.step);
    const audit_run = b.addSystemCommand(&.{ "bash", "tools/audit_source.sh" });
    audit_run.setCwd(b.path("."));
    const check = b.step("check", "Compile and audit workstation tools and tests");
    check.dependOn(&tests.step);
    check.dependOn(&audit_run.step);
    const test_step = b.step("test", "Run workstation tool tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);
}

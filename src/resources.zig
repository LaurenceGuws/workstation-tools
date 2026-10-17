//! Portable resource requests for Walker-owned durable workloads.
//! Walker must enforce an admitted semantic exactly or reject it.
const std = @import("std");

/// Largest admitted whole-workload memory limit in bytes (1 EiB).
pub const max_memory_bytes: u64 = 1 << 60;
/// Minimum admitted CPU time quota, in microseconds per second.
pub const min_cpu_max_us_per_second: u32 = 10_000;
/// Smallest admitted portable CPU or I/O relative weight.
pub const min_relative_weight: u16 = 1;
/// Largest admitted portable CPU or I/O relative weight.
pub const max_relative_weight: u16 = 10_000;

/// Optional whole-workload limits; null means unconstrained, not an inferred platform default.
pub const Values = struct {
    memory_max_bytes: ?u64 = null,
    memory_pressure_bytes: ?u64 = null,
    swap_max_bytes: ?u64 = null,
    tasks_max: ?u32 = null,
    cpu_max_us_per_second: ?u32 = null,
    cpu_weight: ?u16 = null,
    io_weight: ?u16 = null,

    /// True when the request specifies no resource control, so Walker capability gating is optional.
    pub fn isEmpty(self: Values) bool {
        return self.memory_max_bytes == null and
            self.memory_pressure_bytes == null and
            self.swap_max_bytes == null and
            self.tasks_max == null and
            self.cpu_max_us_per_second == null and
            self.cpu_weight == null and
            self.io_weight == null;
    }

    /// Rejects impossible limits or pressure thresholds above hard memory limits.
    pub fn validate(self: Values) error{InvalidResources}!void {
        if (self.memory_max_bytes) |value| if (value > max_memory_bytes) return error.InvalidResources;
        if (self.memory_pressure_bytes) |value| if (value > max_memory_bytes) return error.InvalidResources;
        if (self.swap_max_bytes) |value| if (value > max_memory_bytes) return error.InvalidResources;
        if (self.memory_pressure_bytes) |pressure| if (self.memory_max_bytes) |hard|
            if (pressure > hard) return error.InvalidResources;
        if (self.tasks_max) |value| if (value == 0) return error.InvalidResources;
        if (self.cpu_max_us_per_second) |value|
            if (value < min_cpu_max_us_per_second) return error.InvalidResources;
        if (self.cpu_weight) |value|
            if (value < min_relative_weight or value > max_relative_weight) return error.InvalidResources;
        if (self.io_weight) |value|
            if (value < min_relative_weight or value > max_relative_weight) return error.InvalidResources;
    }
};

/// Per-control Walker admission evidence; every requested resource must have explicit support.
pub const Capabilities = struct {
    memory_max_bytes: bool = false,
    memory_pressure_bytes: bool = false,
    swap_max_bytes: bool = false,
    tasks_max: bool = false,
    cpu_max_us_per_second: bool = false,
    cpu_weight: bool = false,
    io_weight: bool = false,

    /// Requires a ready capability for every non-null requested whole-workload resource.
    pub fn supports(self: Capabilities, requested: Values) bool {
        return (requested.memory_max_bytes == null or self.memory_max_bytes) and
            (requested.memory_pressure_bytes == null or self.memory_pressure_bytes) and
            (requested.swap_max_bytes == null or self.swap_max_bytes) and
            (requested.tasks_max == null or self.tasks_max) and
            (requested.cpu_max_us_per_second == null or self.cpu_max_us_per_second) and
            (requested.cpu_weight == null or self.cpu_weight) and
            (requested.io_weight == null or self.io_weight);
    }
};

test "portable resources reject impossible relationships and relative weights" {
    try (Values{
        .memory_max_bytes = 8192,
        .memory_pressure_bytes = 4096,
        .swap_max_bytes = 0,
        .tasks_max = 1,
        .cpu_max_us_per_second = min_cpu_max_us_per_second,
        .cpu_weight = min_relative_weight,
        .io_weight = max_relative_weight,
    }).validate();

    try std.testing.expectError(
        error.InvalidResources,
        (Values{ .memory_max_bytes = 4096, .memory_pressure_bytes = 4097 }).validate(),
    );
    try std.testing.expectError(error.InvalidResources, (Values{ .tasks_max = 0 }).validate());
    try std.testing.expectError(error.InvalidResources, (Values{ .cpu_max_us_per_second = min_cpu_max_us_per_second - 1 }).validate());
    try std.testing.expectError(error.InvalidResources, (Values{ .cpu_weight = 0 }).validate());
    try std.testing.expectError(error.InvalidResources, (Values{ .io_weight = max_relative_weight + 1 }).validate());
}

test "resource capabilities are request specific" {
    const caps = Capabilities{ .tasks_max = true, .cpu_weight = true };
    try std.testing.expect(caps.supports(.{ .tasks_max = 12, .cpu_weight = 50 }));
    try std.testing.expect(!caps.supports(.{ .io_weight = 50 }));
}

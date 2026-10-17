//! Defines the narrow host policy needed to embed workstation tools without naming a particular agent application.

const std = @import("std");
const environment = @import("environment.zig");

/// Maximum host-provided Bash prelude bytes reserved inside the bounded argv budget.
pub const max_shell_prelude_bytes: usize = 62;
/// Maximum bytes in a portable Walker workload-name prefix.
pub const max_job_name_prefix_bytes: usize = 32;
/// Exact Walker executable and state namespace; never resolved implicitly from PATH.
pub const WalkerConfig = @import("walker.zig").Config;

/// Host-supplied execution binding; the module neither discovers nor chooses the Walker owner.
pub const Policy = struct {
    walker: WalkerConfig,
    agent_marker: ?environment.Marker = null,
    operator_marker: ?environment.Marker = null,
    shell_prelude: []const u8 = "declare -xr HISTFILE=/dev/null;set +o history;",
    job_name_prefix: []const u8,

    /// Rejects invalid Walker paths, environment marker values, shell budgets and workload names.
    pub fn validate(self: Policy) error{InvalidPolicy}!void {
        if (!@import("walker.zig").validConfig(self.walker)) return error.InvalidPolicy;
        if (self.shell_prelude.len > max_shell_prelude_bytes) return error.InvalidPolicy;
        if (!validJobNamePrefix(self.job_name_prefix)) return error.InvalidPolicy;
        if (self.agent_marker) |marker| try validateMarker(marker);
        if (self.operator_marker) |marker| try validateMarker(marker);
    }
};

fn validateMarker(marker: environment.Marker) error{InvalidPolicy}!void {
    if (!std.process.Environ.Map.validateKeyForPut(marker.name) or marker.value.len == 0 or
        std.mem.indexOfScalar(u8, marker.value, 0) != null) return error.InvalidPolicy;
}

fn validJobNamePrefix(value: []const u8) bool {
    if (value.len == 0 or value.len > max_job_name_prefix_bytes) return false;
    for (value) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_') return false;
    return true;
}

test "host policy requires exact Walker binding and safe names" {
    const valid = Policy{
        .walker = .{ .executable = "/selected/walker", .home = "/selected/state" },
        .agent_marker = .{ .name = "AGENT_CHILD", .value = "1" },
        .operator_marker = .{ .name = "OPERATOR_PROFILE", .value = "1" },
        .job_name_prefix = "agent-job-",
    };
    try valid.validate();
    var invalid = valid;
    invalid.job_name_prefix = "bad/prefix";
    try std.testing.expectError(error.InvalidPolicy, invalid.validate());
    invalid = valid;
    invalid.agent_marker = .{ .name = "BAD=NAME", .value = "1" };
    try std.testing.expectError(error.InvalidPolicy, invalid.validate());
    invalid = valid;
    invalid.walker.executable = "walker";
    try std.testing.expectError(error.InvalidPolicy, invalid.validate());
}

//! Defines the narrow host policy needed to embed workstation tools without naming a particular agent application.

const std = @import("std");
const environment = @import("environment.zig");

/// Maximum consumer-owned shell prelude bytes reserved from the public shell-command budget.
pub const max_shell_prelude_bytes: usize = 62;
/// Maximum systemd unit prefix bytes before the 32-hex job id and `.service` suffix.
pub const max_job_unit_prefix_bytes: usize = 32;
/// Maximum executable role argument bytes used by durable job helpers.
pub const max_job_role_argument_bytes: usize = 64;

/// Host-selected execution policy. Empty optional markers mean no environment marker is injected.
pub const Policy = struct {
    agent_marker: ?environment.Marker = null,
    operator_marker: ?environment.Marker = null,
    shell_prelude: []const u8 = "declare -xr HISTFILE=/dev/null;set +o history;",
    job_unit_prefix: []const u8,
    job_run_argument: []const u8,
    job_finish_argument: []const u8,

    /// Rejects policy bytes that cannot safely participate in environment names, argv, or systemd unit identity.
    pub fn validate(self: Policy) error{InvalidPolicy}!void {
        if (self.shell_prelude.len > max_shell_prelude_bytes) return error.InvalidPolicy;
        if (!validUnitPrefix(self.job_unit_prefix)) return error.InvalidPolicy;
        if (!validRoleArgument(self.job_run_argument) or !validRoleArgument(self.job_finish_argument)) {
            return error.InvalidPolicy;
        }
        if (self.agent_marker) |marker| try validateMarker(marker);
        if (self.operator_marker) |marker| try validateMarker(marker);
    }
};

fn validateMarker(marker: environment.Marker) error{InvalidPolicy}!void {
    if (!std.process.Environ.Map.validateKeyForPut(marker.name) or marker.value.len == 0 or
        std.mem.indexOfScalar(u8, marker.value, 0) != null)
    {
        return error.InvalidPolicy;
    }
}

fn validUnitPrefix(value: []const u8) bool {
    if (value.len == 0 or value.len > max_job_unit_prefix_bytes) return false;
    for (value) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_') return false;
    return true;
}

fn validRoleArgument(value: []const u8) bool {
    if (value.len < 3 or value.len > max_job_role_argument_bytes or !std.mem.startsWith(u8, value, "--")) return false;
    for (value[2..]) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-') return false;
    return true;
}

test "host policy rejects unsafe markers and job identity bytes" {
    const valid = Policy{
        .agent_marker = .{ .name = "AGENT_CHILD", .value = "1" },
        .operator_marker = .{ .name = "OPERATOR_PROFILE", .value = "1" },
        .job_unit_prefix = "agent-job-",
        .job_run_argument = "--job-run",
        .job_finish_argument = "--job-finish",
    };
    try valid.validate();
    var invalid = valid;
    invalid.job_unit_prefix = "bad/prefix";
    try std.testing.expectError(error.InvalidPolicy, invalid.validate());
    invalid = valid;
    invalid.agent_marker = .{ .name = "BAD=NAME", .value = "1" };
    try std.testing.expectError(error.InvalidPolicy, invalid.validate());
}

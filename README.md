# Workstation Tools

Small bounded Zig workstation operations for agent runtimes and transports.

This package owns execution mechanics and their base schemas. It does not own MCP, HTTP, agent/session identity, conversation state, Activity/Chronicle logging, Fleet topology, authentication, or UI presentation.

Embedding hosts explicitly select where child-process environment comes from through `Policy.environment_source`. The default
`user_manager` source snapshots the current systemd user-manager environment before each process or durable job through
the manager's D-Bus `Environment` property, preserving raw values rather than parsing `systemctl`'s shell-rendered output. This suits
long-lived desktop agents. An isolated host such as a container may instead select `process`, which snapshots the embedding
process environment and still runs the same bounded login-shell environment normalization. The package never guesses between
those environments from filesystem or process-manager availability.

Durable jobs likewise use an explicit `Policy.job_backend`. Current server support is Linux. `systemd_user` preserves the
existing transient-user-service backend, including cgroup lifetime, native resource controls and restart-independent
observation. The current `process` backend is the small Linux fallback: a detached package-owned supervisor using `/proc`
PID/start-time identity and process groups, with bounded stream files, timeout/cancel handling and terminal receipts. It does
not claim cgroup-equivalent containment and deliberately does not advertise or accept systemd resource properties. Future OS
support may add different backend mechanics without changing the small start/read/cancel job surface. Backend selection is a
host decision; workstation-tools never guesses from what happens to be reachable.

## Walker backend

`Policy.job_backend = .walker` requires an explicit `Policy.walker` with absolute `executable` and `home` paths.
The package invokes Walker's JSON CLI. It does not import Walker's implementation, speak its socket protocol, configure
cgroups/restart owners, or fall back when that command is unavailable. The host must deliberately choose the process
environment source to avoid user-manager lookup; job backend and environment source are separate policy fields.

Walker-backed jobs require the current `walker/v5` durable workload contract before workstation-tools creates any job
state. Admission performs one bounded `walker ping` against the configured executable/home and requires
`durable_workloads_v1=true`, `restart_owner=platform`, ready delegated admission, and restart reconciliation support.
A missing Walker reports `WalkerUnavailable`; a reachable but weaker/JIT Walker reports
`WalkerDurabilityUnavailable`. Neither condition authorizes systemd/process fallback.

Launches explicitly request `delegated_cgroup_v2`. Platform/bootstrap owns `WALKER_CGROUP_ROOT`,
`WALKER_RESTART_OWNER`, controller delegation, and the finite Walker restart policy. workstation-tools owns none of
those host mechanics.

The tool job ID is exactly the Walker run ID. A stable local binding records that ID, the selected Walker store/executable,
and launch metadata, while Walker alone owns logs, runtime state and stop escalation. The leash name uses the host prefix
plus the job ID. This lets a human inspect the same workload with `WALKER_HOME=... walker inspect JOB_ID`.

The existing text start/read/cancel vocabulary is preserved, including 128 KiB stdin, 32 KiB combined incremental reads,
4 KiB to 512 MiB prefix retention per stream, and finite 1-second to 24-hour workloads. The default retained prefix remains
1 MiB; accepting a larger cap does not preallocate that amount of memory. `systemd_properties` is not advertised or admitted.
A cancellation acknowledgement means stop was requested, not that cleanup has completed. Read the terminal state to confirm.
Lost launch acknowledgements retain the job ID with `indeterminate` state and never trigger replay.
If the platform-owned Walker later crashes after acknowledged launch, its successor owns reconciliation. The adapter
observes the resulting v5 terminal receipt; it never signals a saved PID, adopts a process, or replays argv.

`stdout_truncated` and `stderr_truncated` are required nullable booleans. `false` means exact zero discarded bytes,
`true` means exact nonzero discarded bytes, and `null` means Walker retained bytes after owner loss but the exact
discarded count is unknowable. systemd/process jobs currently return non-null values.

Run the live adapter contract suite against an explicitly built, externally platform-owned v5 Walker. `WALKER_HOME`
must name that already-running Walker's store; the contract itself does not bootstrap systemd or another owner:

```sh
zig build walker-driver -Doptimize=ReleaseSafe
WALKER_BINARY=/absolute/walker WALKER_HOME=/absolute/platform/walker-state \
  WALKER_TEST_ROOT=/absolute/private/fixtures python3 tools/walker_contract.py
WALKER_BINARY=/absolute/walker WALKER_HOME=/absolute/platform/walker-state \
  WALKER_TEST_ROOT=/absolute/private/fixtures python3 tools/walker_review_contract.py
```

The test-only drivers are not part of any model-facing tool surface. The second driver directly exercises Walker
inventory/detail/log/stats decoding used by operator consumers. Existing systemd/process receipts remain routed by their
stored backend, not reinterpreted as Walker jobs when the host changes its selection.


### Walker identity and text boundaries

A saved Walker reference must match the currently selected host-policy executable and state directory before a read or
cancellation can invoke any program. Changed configuration reports `WalkerBindingMismatch`; receipts never select an old
executable or another namespace on the caller's behalf. Rebinding requires an explicit migration, not automatic fallback.

The common tool surface remains text-only: command, shell and job stdout/stderr replace invalid UTF-8 bytes (including partial
code points at chunk boundaries) with U+FFFD. Job offsets and retention limits still count original bytes. Walker keeps the
original logs and its CLI exposes lossless UTF-8/base64 chunks, so binary consumers should use that interface directly.

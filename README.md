# Workstation Tools

Small bounded Zig workstation operations for agent runtimes and transports.

This package owns execution mechanics and their base schemas. It does not own MCP, HTTP, agent/session identity, conversation state, Activity/Chronicle logging, Fleet topology, authentication, or UI presentation.

Embedding hosts explicitly select where child-process environment comes from through `Policy.environment_source`. The default
`user_manager` source snapshots the current systemd user-manager environment before each process or durable job, which suits
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

# Workstation Tools

One bounded Zig workstation tool surface shared by agent runtimes and transports.

The package owns execution mechanics and base JSON schemas. It does not own MCP, HTTP, Agent/session identity, conversation state, provenance logging, authentication, Fleet topology, or UI presentation.

## Tool surface

The complete public tool vocabulary is:

- command
- shell
- image_read
- job_start
- job_read
- job_cancel

Consumers may rename or wrap transport presentation, but execution semantics live here.

## Execution

command executes exact argv. shell is the explicit Bash-language operation. Both use the same child environment recipe:

1. clone the embedding process environment;
2. run one bounded login-shell environment snapshot with human shell history disabled;
3. execute the requested payload with that explicit environment.

There is no environment-source selector. A long-lived embedding application is responsible for starting workstation-tools with the environment it wants its children to inherit.

image_read directly reads one bounded PNG or JPEG, validates its byte signature, and returns native image bytes plus size and SHA-256 metadata. It never launches image helpers or OCR.

## Durable jobs

Walker is the only durable job owner.

job_start first proves the explicitly selected Walker satisfies the current durable-workload and requested resource-control contract, then launches one exact Walker run. The returned tool job ID is exactly the Walker run ID.

workstation-tools keeps no durable job registry, duplicate status record, captured log copy, PID authority, or supervisor state. job_read and job_cancel reconstruct the exact Walker name from current policy plus the job ID and ask that Walker directly. Walker owns containment, lifetime, timeout, output retention, cancellation, terminal evidence, and crash reconciliation.

Finite stdin uses consumer-private scratch only during launch. The handoff file is removed as soon as the Walker CLI returns and is never job authority.

A lost launch acknowledgement is never replayed. The preselected run ID is returned as indeterminate evidence so the caller can observe that exact Walker identity.

## Policy

Embedding hosts provide one small Policy:

- exact Walker executable and Walker state root;
- optional child environment markers;
- optional shell prelude;
- deterministic Walker job-name prefix.

Walker paths are absolute and never discovered from PATH. Changing the selected Walker namespace changes which jobs are observable; a job ID never selects an executable or state root by itself.

Portable resource requests cover whole-workload memory, task, CPU and I/O controls. Admission fails unless the selected Walker reports every requested semantic ready. Unsupported controls are rejected rather than approximated.

## Bounds

The stable tool contracts retain:

- process stdin up to 128 KiB;
- exact argv up to 256 entries / 32768 combined bytes;
- synchronous command/shell timeouts from 1 to 300 seconds;
- durable job lifetime up to 24 hours;
- Walker output retention from 4 KiB to 512 MiB per stream;
- incremental job reads up to 32 KiB combined.

Text outputs replace invalid UTF-8 with U+FFFD while Walker retains original log bytes for lossless operator inspection.

## Build and test

Use the exact Zig revision declared by .zigversion and build.zig.zon.

    zig build check
    zig build test

The live Walker contract uses an explicitly installed platform-owned Walker:

    zig build walker-driver -Doptimize=ReleaseSafe
    WALKER_BINARY=/absolute/walker
    WALKER_HOME=/absolute/walker-state
    WALKER_TEST_ROOT=/absolute/private/fixtures
    python3 tools/walker_contract.py

walker-observer-driver exercises the same Walker inventory/detail/log/stats decoding used by operator consumers. The test drivers are not part of the model-facing tool surface.

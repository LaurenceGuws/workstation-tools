# Workstation Tools source contract

This repository owns the small reusable Zig workstation execution package shared by agent applications and transports.
Keep the package transport-neutral and application-neutral. Consumers own agent/session identity, provenance logging,
authentication, network transport, and presentation.

Use the exact Zig version in `.zigversion` and `build.zig.zon`. Keep execution bounded, cleanup explicit, public error sets
closed, and durable job state finite and inspectable. A host-specific behavior must enter through an explicit host policy
rather than a hidden dependency on one consumer.

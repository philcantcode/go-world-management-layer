# Quarantine

Quarantine isolates uncertain state while preserving evidence. It is not a
successful teardown or a substitute for a terminal run result.

## Target quarantine

1. Reject new exec, file-transfer, ADB, reset, and capture mutations for the
   target generation. Keep only bounded host-owned evidence collection.
2. Open/link an incident with the target/run IDs, generation, last known state,
   reason, cursors, collector coverage, and operator.
3. Revoke scoped endpoints and close transports. Preserve the target container
   or device only while policy, capacity, and host safety permit; otherwise
   capture minimum evidence and stop it explicitly.
4. Seal the run with gaps and incidents. Do not reuse the quarantined writable
   state. Recovery creates a new target generation linked to the sealed
   incident while the healthy agent generation may continue.
5. Destroy the quarantined resource only after evidence custody and an explicit
   release decision are recorded.

## Node quarantine

1. Close admission and remove the node from external scheduling. Keep the
   control endpoint loopback/local and read-only where practical.
2. Revoke node credentials and external target gateways if compromise is
   suspected. Preserve clocks, logs, ledgers, database, mounts, cgroups, runtime
   metadata, and volatile evidence under the incident policy.
3. Do not migrate or adopt live generations. Finalize them failed/lost and use
   fresh generations on a qualified node.
4. Release quarantine only after root cause, integrity checks, cleanup, runtime
   canaries, credential rotation where applicable, and explicit approval.

With the Docker Linux-target composition enabled, quarantine calls the runtime
containment boundary first and accepts the logical transition only after the
driver proves execution stopped, networking unreachable, writable state
preserved, and the exact runtime identity. It also revokes active target
transports and run deadlines. If the runtime cannot provide that evidence, the
operation fails closed and logical state is not presented as contained.
The managed Android composition applies the same fail-closed rule: after
durable exact host-process ownership it first force-stops the AVD and proves
the serial unreachable, then boundedly revokes/drains the run-scoped ADB and
file endpoints while preserving private state and allocation for evidence.
The durable stopped authority is adopted as the expected quarantined
generation after restart and is not destroyed by orphan cleanup. Launch intent without either
committed ownership or a live candidate that passes the exact image/PID-file
binding is unresolved, not proof of containment; quarantine, destruction, and
new admission remain closed until the operator provides exact containment or
reboots the host. Physical-device containment and node scheduling remain
unavailable and deployment-owned.

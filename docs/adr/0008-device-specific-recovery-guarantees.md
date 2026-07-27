# ADR 0008: Keep virtual- and physical-device recovery guarantees distinct

- Status: proposed
- Date: 2026-07-24

## Context

Android emulators and physical phones share ADB and many observation tools, but
their lifecycle and isolation guarantees differ. An emulator can have an
isolated data directory and version-bound snapshots. A physical phone has
battery, thermal, authorization, USB, OEM, root/debuggability, and persistent
state constraints and generally cannot provide a complete snapshot rollback.

## Decision

Implement one capability-aware `TargetDriver` contract. V1 implements the
Android virtual-device adapter; a physical-device adapter is future scope and
must not weaken the contract when introduced.

Both device kinds use ADR 0010's scoped ADB contract: an ordinary ADB-compatible
endpoint exposes exactly one assigned serial and forwards arbitrary device-
scoped services while rejecting host-server control and all other transports.
This interaction contract is common; reset and cleanliness guarantees are not.

Virtual-device recovery may load a validated immutable baseline snapshot,
powerwash a Cuttlefish device, or cold boot isolated state. Each action starts a
new target generation after incident evidence capture without changing a
healthy agent generation. Snapshot validity is bound to runtime version, system
image, device configuration, and features.

Physical devices are exclusively reserved and reached through a scoped ADB
gateway that exposes only the assigned serial. Raw USB and the host ADB server
are not exposed to the agent. Recovery uses only measured capabilities:
reconnect, gateway restart, reboot, app force-stop/clear/reinstall, approved
fixture application, or human reconditioning. Serious failure or uncertain
cleanup quarantines the device; it is never labeled clean based on a partial
reset.

## Consequences

- Shared scheduling and observation concepts do not erase meaningful device
  differences.
- Physical-device pools require operator workflows, inventory history, battery/
  thermal controls, and quarantine capacity.
- A future physical-device implementation requires fake-device contract tests
  plus dedicated hardware-lab scenarios; virtual-device tests are not evidence
  for phone recovery.

## Rejected alternatives

- Treat a reboot or `pm clear` as a phone snapshot restore: it does not reset the
  whole device.
- Give the agent workspace raw USB: exposes all devices and a large host attack
  surface.
- Use one host ADB server directly: ordinary device listing/selection can reveal
  or target phones outside the lease.

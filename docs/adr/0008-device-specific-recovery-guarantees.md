# ADR 0008: Keep emulator and physical-device recovery guarantees distinct

- Status: proposed
- Date: 2026-07-24

## Context

Android emulators and physical phones share ADB and many observation tools, but
their lifecycle and isolation guarantees differ. An emulator can have an
isolated data directory and version-bound snapshots. A physical phone has
battery, thermal, authorization, USB, OEM, root/debuggability, and persistent
state constraints and generally cannot provide a complete snapshot rollback.

## Decision

Implement one capability-aware `TargetDriver` contract with distinct emulator
and physical-device adapters.

Emulator recovery may load a validated immutable baseline snapshot or cold boot
an isolated AVD clone. Either action starts a new environment generation after
incident evidence capture. Snapshot validity is bound to emulator version,
system image, AVD configuration, and features.

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
- Tests need fake devices plus dedicated hardware-lab scenarios; emulator tests
  are not evidence for phone recovery.

## Rejected alternatives

- Treat a reboot or `pm clear` as a phone snapshot restore: it does not reset the
  whole device.
- Give the container raw USB: exposes all devices and a large host attack
  surface.
- Use one host ADB server directly: ordinary device listing/selection can reveal
  or target phones outside the lease.

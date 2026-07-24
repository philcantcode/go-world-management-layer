# ADR 0005: Make failures and recovery explicit through incidents and generations

- Status: proposed
- Date: 2026-07-24

## Context

Containers and emulators can crash, OOM, hang, lose ADB, or be evicted under
host pressure. Emulators can often restore a snapshot. Automatically restoring
and continuing an agent as if the same environment survived would hide the
failure, destroy causality, and mislead provider/session retry logic.

## Decision

Every realization of an environment has a monotonically increasing generation.
A failure seals the active exec and generation, records a typed incident,
captures bounded minimum evidence, and surfaces the incident before recovery.

Container recreate, emulator snapshot load, cold boot, physical-device reboot,
or reconditioning starts a new generation linked to the incident and previous
generation. The old process is never presented as resumed. A new agent
invocation may receive a factual recovery summary and surviving artifact refs.

Incidents separate proven cause, inferred correlation with confidence, and
unknown cause. They include high-water resource values, gaps, last actions,
outputs known to be sealed, and recovery actions. A bounded incident ID appears
on shim stderr; full detail is out of band.

## Consequences

- Provider retry, world recovery, and analytical follow-up are distinguishable.
- Snapshot restore can be automatic by policy only after visible failure and
  generation rollover.
- Consumers must handle generation changes and cannot assume a lease points to
  mutable continuous state forever.
- Incident evidence capture needs an emergency resource budget so it does not
  worsen host failure.

## Rejected alternatives

- Transparent auto-restart: violates the explicit-crash requirement and can
  make destructive actions appear not to have happened.
- Reuse the same generation after restore: makes before/after state and clocks
  ambiguous.
- Attribute the last observed action as the cause: timing alone is not causal
  proof.

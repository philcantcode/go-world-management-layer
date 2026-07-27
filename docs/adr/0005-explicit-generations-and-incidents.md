# ADR 0005: Make failures and recovery explicit through incidents and generations

- Status: proposed
- Date: 2026-07-24

## Context

Agent workspaces and targets can crash, OOM, hang, lose ADB, or be evicted under
host pressure. Targets can often be reset or restored without discarding the
agent's investigation state. Automatically restoring either resource as if the
same realization survived would hide the failure and destroy causality.

## Decision

Agent workspaces and targets have independent monotonically increasing
generation counters. A failure seals the affected exec or target run and
generation, records a typed incident, captures bounded minimum evidence, and
surfaces the incident before recovery.

Agent-container recreate starts a new agent generation. Linux-target recreate,
Android snapshot load, powerwash, or cold boot starts a new target generation
linked to the incident and previous target generation. Target recovery does not
roll the healthy agent generation. No old process is presented as resumed; the
continuing or new agent invocation receives the sealed observation bundle and a
factual recovery summary.

Incidents separate proven cause, inferred correlation with confidence, and
unknown cause. They include high-water resource values, gaps, last actions,
outputs known to be sealed, and recovery actions. The runner execution error is
correlated with a bounded incident ID; full detail is out of band.

## Consequences

- Provider retry, world recovery, and analytical follow-up are distinguishable.
- Snapshot restore can be automatic by policy only after visible failure and
  generation rollover.
- Consumers must track the resource kind with every generation and cannot assume
  target and agent generation numbers advance together.
- Incident evidence capture needs an emergency resource budget so it does not
  worsen host failure.

## Rejected alternatives

- Transparent auto-restart: violates the explicit-crash requirement and can
  make destructive actions appear not to have happened.
- Reuse the same generation after restore: makes before/after state and clocks
  ambiguous.
- Attribute the last observed action as the cause: timing alone is not causal
  proof.

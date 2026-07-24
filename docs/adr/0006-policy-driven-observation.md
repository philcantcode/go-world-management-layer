# ADR 0006: Use low-overhead baseline and policy-triggered invasive observation

- Status: proposed
- Date: 2026-07-24

## Context

Rich vulnerability-research evidence may require system calls, packets,
decrypted HTTP, Android scheduling traces, runtime hooks, screenshots, or heap
profiles. Running every collector continuously would consume excessive CPU,
memory, disk, and bandwidth; alter target timing; collect sensitive payloads;
and sometimes require privileges incompatible with stronger container
isolation.

## Decision

Use three observation levels:

1. Always-on baseline: lifecycle, Docker events/stats, cgroup/PSI metrics,
   process lifecycle, workspace mutations, network-flow metadata, ADB state,
   permitted Android logs, and collector health.
2. Policy triggers: temporarily increase metric resolution or start bounded
   captures when a defined event/threshold occurs.
3. Authorized on-demand: a host or agent requests a named collector within the
   immutable effective policy's maximum powers.

`strace`, packet payloads, mitmproxy, Perfetto, Frida, screen recording, and
profiles are never invisible defaults. Each activation records an injection
manifest, version/configuration digest, privileges, expected and measured
overhead, sensitivity, outputs, and verified teardown.

Collector failure and ring-buffer loss are observable. If a collector is marked
required, its failure fails or quarantines the generation according to policy;
otherwise the generation records an explicit downgrade/gap.

## Consequences

- Normal runs remain observable without drowning in data.
- Invasive observation is reproducible and its observer effect is visible.
- Policies need strict cross-field capability and sensitivity validation.
- Tests must measure collector overhead and prove cleanup after cancellation,
  target crash, and daemon restart.

## Rejected alternatives

- Capture everything always: creates noise, cost, sensitivity, and severe
  observer effects.
- Leave observation entirely to the agent: a crashed agent cannot preserve its
  own final evidence, and it could disable inconvenient records.
- Silently skip unsupported collectors: makes absence indistinguishable from
  an observed non-event.

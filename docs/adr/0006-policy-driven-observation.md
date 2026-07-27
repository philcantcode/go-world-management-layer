# ADR 0006: Use low-overhead baseline and policy-triggered invasive observation

- Status: Accepted
- Date: 2026-07-24

## Context

Rich vulnerability-research evidence may require system calls, packets,
decrypted HTTP, Android scheduling traces, runtime hooks, screenshots, or heap
profiles. Running every collector continuously would consume excessive CPU,
memory, disk, and bandwidth; alter target timing; collect sensitive payloads;
and sometimes require privileges incompatible with stronger container
isolation.

## Decision

Target visibility is preferred over selecting a stronger but opaque isolation
runtime. Use three cumulative observation levels:

1. Required metadata baseline: lifecycle, Docker events/stats, cgroup/PSI,
   separately attributed agent/target/emulator/observer metrics, process and
   syscall results, file-open/read/write metadata, authoritative target changes,
   network-flow metadata, ADB state, permitted Android logs, Android health, and
   collector coverage.
2. Deep observation: policy triggers or named requests start bounded broader
   syscall arguments, packet rings, Perfetto/ftrace, Frida hooks, or state
   snapshots.
3. Payload observation: explicitly filtered file/socket buffers, decrypted
   traffic, memory, or screens within strict sensitivity, duration, and byte
   limits.

`strace`, packet payloads, mitmproxy, Perfetto, Frida, screen recording, and
profiles are never invisible defaults. Each activation records an injection
manifest, version/configuration digest, privileges, expected and measured
overhead, sensitivity, outputs, and verified teardown.

These rules govern host-managed collectors and coverage claims. ADR 0010 also
allows the agent to install arbitrary instrumentation inside its assigned
target. That tooling is recorded as an agent intervention and never becomes
coverage authority merely because it produced output.

Collector placement is recorded as host, runtime, guest, or injected app.
Collector failure and ring-buffer loss are observable. If required coverage
cannot start or is lost, admission or the target run fails according to policy;
otherwise the run records an explicit downgrade/gap. Every finalized target run
produces an observation bundle containing raw references, normalized events,
coverage, changes, and a derived summary.

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

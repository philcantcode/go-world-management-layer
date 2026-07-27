# ADR 0007: Admit and shed work using hard limits and measured pressure

- Status: proposed
- Date: 2026-07-24

## Context

Containers, emulators, snapshots, instrumentation, and captures compete for CPU,
memory, I/O, pids, disk, KVM, and devices. Static concurrency alone cannot see
that two nominally equal jobs have different costs or that the host is
thrashing. Waiting for OOM can lose evidence and the control plane itself.

## Decision

Each lease declares requests, hard limits, priority, preemptibility, TTL, and
observer/capture cost. The agent workspace, Linux targets, Android host
processes, and observers have separate cgroup leaves under an aggregate lease
parent. Admission uses allocatable resources, current and trending
host/per-cgroup PSI, disk bytes/inodes, devices, snapshot headroom, warm-pool
cost, and a reserved control-plane margin.

Pressure actions escalate in a fixed, logged order: raise observation, stop
admission, expire unused reservations, shrink unleased warm pools, quiesce idle
preemptible targets, then revoke active target runs before agent workspaces when
priority permits. Active work is not silently paused. Revocation fails and
finalizes the affected run or exec and creates a `resource_eviction` incident.

Hard cgroup CPU/memory/I/O/pids limits remain the containment boundary. OOM,
throttling, `memory.events`, pids events, and PSI are preserved so a job-limit
failure can be distinguished from host contention.

## Consequences

- Concurrency adapts to actual productivity loss and resource shape.
- Admission and eviction decisions are explainable and replayable.
- Policies must prevent starvation and define priority/preemption fairness.
- Pressure tests require controlled cgroup/VM environments; developer-machine
  results are not sufficient evidence.

## Rejected alternatives

- Fixed maximum container count: ignores emulators, observers, and workload
  shape.
- CPU/memory percentages only: can miss I/O stalls and reclaim thrashing.
- `docker pause` active agents: appears as unexplained protocol stall and hides
  scheduler action.
- Kill the largest process first: may destroy high-priority or non-preemptible
  evidence without considering policy.

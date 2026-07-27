# ADR 0001: Target Linux nodes and split control from node execution

- Status: proposed
- Date: 2026-07-24

## Context

The required guarantees depend on Linux-specific facilities: OverlayFS,
cgroup v2 and PSI, mount/PID/network/user namespaces, eBPF/fanotify, and KVM.
Docker Engine access, mounts, ADB/USB, emulator processes, and privileged
observers are high-authority operations. Combining them with public scheduling
and subscription APIs would make the control-plane attack surface unnecessarily
powerful.

## Decision

Version 1 runs on dedicated Linux nodes. Remote clients may be cross-platform,
but Docker Desktop is not the reference isolation or performance environment.

Split the implementation into:

- `worldd`, an unprivileged logical control plane that owns policy, admission,
  leases, agent/target generations, target runs, durable state, incidents,
  observation bundles, and subscriptions; and
- `world-node`, a local trusted authority that owns Docker Engine, mounts,
  cgroups, sibling agent/target containers, Android virtual devices, scoped ADB
  access, and privileged observers.

The node accepts only typed, resolved plans under configured roots and
allowlists. It does not expose shell, Docker passthrough, arbitrary host paths,
or raw USB operations. It independently validates lease identity and policy
constraints so a confused control-plane request cannot become arbitrary host
execution.

ADR 0010 permits arbitrary shell and ADB commands only after a typed envelope
has bound them to one active target run. This does not add a node or host shell:
the node selects the already-assigned target transport before forwarding opaque
guest command bytes.

## Consequences

- Linux behavior can be tested honestly instead of hidden behind weak portable
  abstractions.
- Privileged code and API surface are narrow and separately fuzzable.
- Deployment has two cooperating daemons and an authenticated local protocol.
- Windows/macOS native node support is a separate future design, not a v1
  compatibility shim.

## Rejected alternatives

- One privileged daemon: simpler initially, but greatly enlarges the exposed
  authority and makes least-privilege testing harder.
- A platform-neutral v1: would either omit required facilities or falsely claim
  equivalent guarantees.
- Run Docker/ADB inside the agent workspace: exposes management authority to an
  untrusted workload and prevents sibling isolation between agent and target.

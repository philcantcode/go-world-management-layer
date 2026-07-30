# ADR 0001: Target Linux nodes and split control from node execution

- Status: **Superseded** (as product surface)
- Date: 2026-07-24
- Superseded-by: [library-only Manager](../designs/library-only-manager.md)
  (2026-07 cutover)

## Context

The required guarantees depend on Linux-specific facilities: OverlayFS,
cgroup v2 and PSI, mount/PID/network/user namespaces, eBPF/fanotify, and KVM.
Docker Engine access, mounts, ADB/USB, emulator processes, and privileged
observers are high-authority operations. Combining them with public scheduling
and subscription APIs would make the control-plane attack surface unnecessarily
powerful.

## Original decision (historical)

Version 1 was planned as two cooperating daemons on dedicated Linux nodes:

- `worldd`, an unprivileged logical control plane that owns policy, admission,
  leases, agent/target generations, target runs, durable state, incidents,
  observation bundles, and subscriptions; and
- `world-node`, a local trusted authority that owns Docker Engine, mounts,
  cgroups, sibling agent/target containers, Android virtual devices, scoped ADB
  access, and privileged observers.

Remote clients would Dial an authenticated WorldService endpoint.

## Current product decision

The dual-daemon and remote Dial product is **deleted**. The control plane is an
imported library:

- Hosts call `world.Open(Config)` and receive `*world.Manager`.
- Exclusive process ownership of one control-state tree is enforced by
  processlock (same fail-closed rules as the former daemons).
- A fixed local `Config.Subject` replaces bearer tokens and mTLS client identity.
- Physical composition (Docker agent/target, managed Android, observers,
  capture) remains opt-in and deployment-profile gated inside the host process.
- Multi-tenant isolation is separate host processes and state trees, not
  multi-subject network sessions on one Manager.

Logical vs privileged boundary intent from this ADR remains: policy and
lifecycle authority stay explicit; physical drivers stay under typed plans and
configured roots. They are no longer separate remote services.

## Consequences

- No `worldd` / `world-node` binaries, no `world.Dial`, no host-facing gRPC
  Serve path.
- Operator CLIs and adapters embed Manager only.
- Linux node qualification and physical-driver fail-closed rules still apply
  inside the embedding host process.

## Rejected alternatives (still rejected)

- One privileged daemon with a large remote API surface.
- A platform-neutral claim of equivalent isolation without Linux facilities.
- Reintroducing a “compatible remote mode” alongside the library.

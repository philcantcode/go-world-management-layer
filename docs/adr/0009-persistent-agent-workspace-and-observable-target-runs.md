# ADR 0009: Separate the persistent agent workspace from observable target runs

- Status: accepted
- Date: 2026-07-27

## Context

The provider agent needs a durable place for research tools, notes, scripts, and
policy-selected evidence. The Linux program or Android app being investigated
needs a disposable environment that can be reset repeatedly and observed more
aggressively. Treating both as one container or generation couples unrelated
failures, makes clean reruns expensive, and risks exposing provider credentials
and workspace material to a hostile target.

Strong application-kernel or VM isolation can also hide target syscalls and
other guest behavior from host eBPF collectors. For this product, useful and
honest target visibility is more important than selecting the strongest
available isolation boundary by default.

## Decision

Model two untrusted execution tiers beneath the trusted control and observation
plane:

1. one persistent agent workspace per active lease, containing the provider,
   research tools, and an exact policy-projected filesystem view; and
2. disposable target sandboxes created by add-on drivers, beginning with a
   Linux OCI-container driver and an Android virtual-device driver.

The resources are siblings owned by `world-node`. The agent never receives a
Docker/containerd socket, raw ADB server, privileged collector authority, or a
shared writable mount into a target. It requests frozen target templates
through a lease-scoped API, then receives arbitrary command and file-transfer
authority inside only the assigned target. Linux uses direct exec/shell and
push/pull streams; Android uses a one-device ADB gateway compatible with normal
ADB clients. Command semantics are recorded, not allowlisted. Host/runtime
management and cross-target authority remain unavailable.

Agent workspaces and targets have independent generation counters. A
`TargetRun` is one bounded workload execution and observation window. Resetting,
restoring, or replacing a target creates a new target generation without
changing the agent generation.

Every target run finalizes into an `ObservationBundle` containing raw capture
references, normalized events and metrics, an authoritative target change
manifest, collector coverage and gaps, incidents, and a derived agent-facing
summary.

The default Linux target uses a hardened standard OCI/runc runtime so host-owned
eBPF, namespace, cgroup, network, and filesystem collectors retain high-fidelity
visibility. gVisor, Kata, or another stronger boundary is an optional capability
profile. Admission fails if that runtime cannot satisfy policy-required
visibility; the manager never silently trades away observation completeness.

## Consequences

- The agent can run the same specimen repeatedly in clean targets without
  losing its investigation state.
- The agent can install Frida or other tooling and adapt its investigation
  without waiting for a new control-plane operation type.
- A target compromise does not directly expose provider credentials, agent
  notes, or runtime-management authority.
- Target drivers and observer drivers can evolve independently behind common
  run and bundle contracts.
- Linux visibility can reuse Tracee or Inspektor Gadget; Android visibility can
  reuse Perfetto, logcat, Frida, and MobSF adapters.
- Standard OCI targets share the host kernel. This is an explicit accepted risk
  for the default visibility-first profile, mitigated by hardened container
  configuration, dedicated nodes, least privilege, and optional stronger tiers.
- Observation coverage must describe collector placement and distinguish
  host-visible, guest-visible, app-hook, and framework-level signals.

## Rejected alternatives

- Run investigated programs directly in the agent workspace: target state,
  compromise, credentials, and research state would share one failure domain.
- Give the agent a Docker socket and let it create nested targets: the socket is
  host authority and defeats the trusted control boundary.
- Use one generation for the workspace and all targets: a target reset would
  unnecessarily invalidate the provider session and research state.
- Default every target to gVisor or a microVM: stronger isolation is useful, but
  it cannot silently replace the required syscall and filesystem visibility.
- Return only an LLM summary: summaries are derived; raw evidence, coverage, and
  normalized records must remain independently reviewable.

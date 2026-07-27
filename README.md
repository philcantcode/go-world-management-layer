# Go world management layer

Status: architecture and implementation plan

`go-world-management-layer` will provide isolated, observable research sessions
for autonomous vulnerability-research agents. Each session contains a persistent
agent workspace and disposable target sandboxes. V1 will run Linux programs in
separate OCI containers and APKs in instrumented Android virtual devices while
host-owned collectors preserve their behavior as explicit evidence.

The central promise is:

> Transparent while healthy, explicit when the environment changes or fails.

An agent can use its normal CLI and tools inside its policy-projected workspace.
Investigated programs and apps run in sibling target sandboxes, never inside the
agent workspace. The management layer does not parse or approve each ordinary
agent tool call, and it never exposes the Docker socket, host filesystem,
artifact repository, raw ADB authority, privileged collectors, or another
target to the agent. Target resets do not discard the agent's notes or tools.
Within its assigned disposable target the agent can run arbitrary commands,
push or pull files, install tools such as Frida, and use ordinary device-scoped
ADB operations. Scope and infrastructure authority are constrained; command
semantics are observed rather than allowlisted.

## Ecosystem position

```text
campaign manager / host application
  |-- go-vr-research-framework       analytical truth
  |-- go-agent-runner                provider CLI invocation and protocol
  |       `-- world ExecutionEnvironment adapter
  |               `-- worldd
  |                    `-- world-node
  |                         |-- agent workspace container + copy-on-write view
  |                         |-- Linux target container(s)
  |                         |-- instrumented Android virtual device(s)
  |                         `-- host-owned observers and bundle builder
  `-- go-forensic-artifacts          immutable input/output/trace authority
```

This repository owns operational environment truth. It does not own research
conclusions, provider-agent protocol semantics, or immutable forensic bytes.

## Planned v1

- A Linux node service using Docker Engine/Moby, cgroup v2, OverlayFS, eBPF, and
  KVM-backed Android virtual devices.
- Separate lease, agent-workspace, target-generation, and target-run state
  machines with idempotent lifecycle operations.
- A lease-bound `go-agent-runner` execution environment that runs every
  provider probe and attempt in the agent workspace while preserving the
  runner's nil-as-local-host default for other callers.
- A persistent agent workspace that can survive many clean, failed, or reset
  target runs without receiving runtime-management authority.
- Add-on target drivers beginning with a visibility-first OCI/runc Linux target
  and an instrumented AOSP Android target.
- Transparent target data planes: arbitrary Linux exec/shell/file transfer and
  a one-device ADB gateway, with MCP reserved for lifecycle and evidence-query
  ergonomics rather than used as a command bottleneck.
- Selection-specific, read-only artifact views backed by a node-local
  content-addressed cache and shared copy-on-write extents, with per-lease
  OverlayFS upper layers and safe, explicit output capture.
- Live snapshots and resumable streams for agent and target lifecycle,
  process/syscall/file/network/Android activity, visual device state, collector
  coverage, and separately attributed host/workspace/target/app/process metrics.
- Reuse of open-source collectors such as Tracee or Inspektor Gadget, Perfetto,
  logcat, Frida, packet capture, Zeek, mitmproxy, and MobSF adapters rather than
  implementing new tracing stacks.
- One sealed observation bundle per target run containing native captures,
  normalized events, target changes, collector coverage/gaps, incidents, and an
  agent-facing derived summary.
- Pressure-aware admission and shedding. Active work is never silently paused,
  killed, restored, or moved to a new workspace or target generation.
- Crash evidence capture and explicit target reset/restore. Agent and target
  generations advance independently.
- A durable causal ledger with source-local ordering, explicit causal links,
  clock-domain metadata, and gap records whenever a collector drops data.
- Fault injection, model-based state-machine tests, security escape tests,
  resource-pressure tests, and long-running chaos/soak verification.

## Documents

- [Architecture](docs/design.md)
- [Implementation plan](docs/implementation-plan.md)
- [Architecture decisions](docs/adr/README.md)
- [Example policy](docs/examples/environment-policy.yaml)

The architecture is intentionally implementation-ready, but no runtime code is
claimed yet.

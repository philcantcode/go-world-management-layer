# Go world management layer

Status: architecture and implementation plan

`go-world-management-layer` will provide isolated, observable execution
environments for autonomous vulnerability-research agents. It will own the
lifecycle of Docker containers, Android emulators, and leased physical Android
devices; create copy-on-write workspaces; stream live activity and performance
signals; react to host pressure; and preserve crashes as explicit incidents.

The central promise is:

> Transparent while healthy, explicit when the environment changes or fails.

An agent can use its normal CLI and tools inside its container. The management
layer does not parse or approve each tool call, and it never exposes the Docker
socket, the host filesystem, the artifact repository, or an unscoped physical
device to the agent. Provider stdin, stdout, stderr, signals, and exit status are
bridged without changing the provider protocol. Environment faults and recovery
are reported out of band and as a bounded terminal diagnostic; they are never
made to look like successful tool execution.

## Ecosystem position

```text
campaign manager / host application
  |-- go-vr-research-framework       analytical truth
  |-- go-agent-runner                provider CLI invocation and protocol
  |       `-- worldexec              transparent execution bridge
  |               `-- worldd
  |                    `-- world-node
  |                         |-- container + copy-on-write workspace
  |                         |-- Android emulator or scoped phone lease
  |                         `-- observers and resource controllers
  `-- go-forensic-artifacts          immutable input/output/trace authority
```

This repository owns operational environment truth. It does not own research
conclusions, provider-agent protocol semantics, or immutable forensic bytes.

## Planned v1

- A Linux node service using Docker Engine, cgroup v2, OverlayFS, and KVM-backed
  Android emulators.
- A lease and generation state machine with idempotent lifecycle operations.
- A transparent `worldexec` bridge compatible with the existing
  `go-agent-runner` executable boundary.
- Selection-specific, read-only artifact views backed by a node-local
  content-addressed cache and shared copy-on-write extents, with per-lease
  OverlayFS upper layers and safe, explicit output capture.
- Live, resumable streams for lifecycle events, process/file/network activity,
  Android logs, and host/container/emulator/process resource metrics.
- Low-overhead baseline observation plus policy-triggered `strace`, eBPF,
  Perfetto, packet capture, mitmproxy, and Frida collectors.
- Pressure-aware admission and shedding. Active work is never silently paused,
  killed, restored, or moved to a new environment generation.
- Crash evidence capture and explicit emulator snapshot restore. Physical
  devices use quarantine and reconditioning because they cannot promise a true
  snapshot rollback.
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

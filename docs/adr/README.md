# Architecture decision records

These decisions define the accepted v1 architecture. Acceptance records the
chosen direction; implementation evidence is tracked by the implementation
plan and its phase exit gates.

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](0001-linux-node-control-and-execution-split.md) | Accepted | Target Linux nodes and split logical control from privileged node execution |
| [0002](0002-host-default-runner-execution-environment.md) | Accepted | Run provider CLIs through a lease-bound, host-default runner execution environment |
| [0003](0003-dedicated-overlay-workspaces.md) | Accepted | Use shared immutable input views and dedicated OverlayFS workspaces |
| [0004](0004-causal-ledger-and-live-streams.md) | Accepted | Keep a durable causal ledger behind resumable live streams |
| [0005](0005-explicit-generations-and-incidents.md) | Accepted | Make failures and recovery explicit through incidents and generations |
| [0006](0006-policy-driven-observation.md) | Accepted | Use low-overhead baseline and policy-triggered invasive observation |
| [0007](0007-pressure-aware-admission-and-shedding.md) | Accepted | Admit and shed work using hard limits and measured pressure |
| [0008](0008-device-specific-recovery-guarantees.md) | Accepted | Keep virtual- and physical-device recovery guarantees distinct |
| [0009](0009-persistent-agent-workspace-and-observable-target-runs.md) | Accepted | Separate the persistent agent workspace from observable target runs |
| [0010](0010-arbitrary-guest-control-through-scoped-target-transports.md) | Accepted | Permit arbitrary guest control through scoped target transports |

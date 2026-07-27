# Architecture decision records

| ADR | Decision |
| --- | --- |
| [0001](0001-linux-node-control-and-execution-split.md) | Target Linux nodes and split logical control from privileged node execution |
| [0002](0002-host-default-runner-execution-environment.md) | Run provider CLIs through a lease-bound, host-default runner execution environment |
| [0003](0003-dedicated-overlay-workspaces.md) | Use shared immutable input views and dedicated OverlayFS workspaces |
| [0004](0004-causal-ledger-and-live-streams.md) | Keep a durable causal ledger behind resumable live streams |
| [0005](0005-explicit-generations-and-incidents.md) | Make failures and recovery explicit through incidents and generations |
| [0006](0006-policy-driven-observation.md) | Use low-overhead baseline and policy-triggered invasive observation |
| [0007](0007-pressure-aware-admission-and-shedding.md) | Admit and shed work using hard limits and measured pressure |
| [0008](0008-device-specific-recovery-guarantees.md) | Keep virtual- and physical-device recovery guarantees distinct |
| [0009](0009-persistent-agent-workspace-and-observable-target-runs.md) | Separate the persistent agent workspace from observable target runs |
| [0010](0010-arbitrary-guest-control-through-scoped-target-transports.md) | Permit arbitrary guest control through scoped target transports |

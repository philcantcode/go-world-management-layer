# ADR 0002: Run provider CLIs through a lease-bound execution environment

- Status: accepted
- Date: 2026-07-27

## Context

`go-agent-runner` owns provider capability probes, direct command construction,
machine-readable stream parsing, retries, session recovery, schema validation,
and terminal success. The provider and all of its tools must execute inside the
agent workspace, not on the host. An executable shim is insufficient because
provider adapters also create temporary prompt or schema files whose host paths
are meaningless inside a container.

The runner now provides a provider-neutral `ExecutionEnvironment` composition
seam. Its nil value deliberately preserves direct local-host execution for all
existing and unrelated callers.

## Decision

The host application sets `agentrunner.Request.ExecutionEnvironment` to a
world-owned, lease-bound adapter. The adapter binds one lease and agent
generation and implements three operations:

1. resolve a working directory visible inside the agent workspace;
2. resolve the provider executable and return a generation-sensitive identity;
   and
3. run a direct command through `world-guest`, preserving argv, stdin, separate
   ordered stdout/stderr, callback backpressure, limits, cancellation, exit
   status, and confirmed cleanup.

Temporary inputs cross the interface as bytes with an argument index and safe
name hint. The adapter materializes them privately in the workspace, replaces
that argument slot, and removes them before reporting cleanup. It never parses
provider flags or protocols.

The environment ID includes the lease, `AgentGeneration`, workspace image/tool
fingerprint, and protocol version. A changed realization therefore cannot reuse
the runner's cached executable capabilities. An unconfirmed remote process-tree
kill escalates to agent-container destruction and a world incident.

Provider events remain byte-transparent and free of management records.
Structured world incidents travel out of band and are correlated with the
runner's environment-aware error.

## Consequences

- Existing runner callers continue to execute on the host when the environment
  field is nil.
- Provider adapters, probes, schema handling, retry logic, and normalization
  remain solely in `go-agent-runner`.
- Prompt-file providers work without same-path host mounts or provider-aware
  transport logic.
- The world adapter and `world-guest` must pass the runner's execution-
  environment contract suite, including cancellation, limits, temporary-input
  cleanup, executable-generation invalidation, and stream fault cases.
- The agent workspace container remains the final kill domain for provider and
  tool descendants.

## Rejected alternatives

- Teach a shim about provider file flags: duplicates private adapter knowledge
  and will drift as CLIs change.
- Mount host temporary directories into the workspace: exposes unrelated host
  state and fails for remote nodes.
- Parse provider JSON in the world layer: duplicates the runner and risks
  protocol corruption.
- Run the provider on the host and only its tools in Docker: an agent or plugin
  could still execute host-side code.

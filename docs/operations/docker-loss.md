# Docker loss

Use when the Docker daemon is unavailable, its API becomes inconsistent, or
Docker restarts while resources exist in the shipped physical Linux
composition. A host that Opened Docker agent and
Linux-target drivers through a trusted deployment profile.

## Immediate containment

1. Stop new lease, workspace, target, and run admission. Do not repeatedly
   retry mutating Docker calls.
2. Keep the host process and local evidence services running if their health does not
   depend on Docker. Reject new exec and target transports.
3. Open a `control_plane_failure` incident. Record the first failed operation,
   daemon/service state, Docker client/server versions, host pressure, and the
   affected lease, exec, target, generation, and run IDs.
4. Preserve version-6 observer markers, collector output transactions/objects,
   bundle reservations/stages/files/indexes/completions, local seals, and ledger
   segments. Record coverage gaps from the first interval whose collection or
   attribution is uncertain.

## Diagnose and recover

1. Have the host service owner restore Docker. Do not expose the socket to an
   agent or target and do not replace it with an unauthenticated TCP endpoint.
2. Probe `docker version` and `docker info`; require Linux, cgroup-v2, and the
   deployment's recorded security options before mutation resumes.
3. Restart the world daemon only after Docker is stable. Before opening its
   listener, startup reconstructs every nonterminal exact plan, re-applies
   policy admission, inventories only structurally owned containers, and
   compares full labels and physical configuration with durable state.
4. A matching, healthy container is adopted. Missing, foreign, conflicting,
   uncertain, or mismatched ownership fails startup; never create a replacement
   under the old generation. A uniquely owned terminal orphan is removed only
   through its driver and a second inventory must prove it absent.
5. Stop/finalize affected execs and runs. For an interrupted process observer,
   require the supported Linux direct-child or Windows per-collector Job death
   proof and exact output/object reconciliation; retained or crash-finalized
   artifacts do not restore continuity.
   Complete the reservation-to-stage-to-Core-to-index-to-observer-to-completion
   bundle saga, and seal explicit Docker/collector gaps. If policy authorizes
   recovery, create only the failed resource's next generation.
6. Remove proven orphan containers through the scoped driver after evidence is
   safe. Send uncertain ownership to [quarantine](quarantine.md).
7. Reopen admission after repeated probes are stable and a fresh canary
   create/start/inspect/stop/remove cycle passes on the qualified node.

The daemon performs startup inventory/adoption and terminal-orphan cleanup, but
it does not manage the Docker service or automatically reopen an external
scheduler. Docker repair, host quarantine, monitoring, and canary authorization
remain operator responsibilities.

# ADR 0011: Stimulus classification and per-action evidence bundles

- Status: Accepted
- Date: 2026-07-28
- Revised: 2026-07-28 (class-aware collectors)

## Context

Vulnerability-research agents need more than terminal stdout. Meaningful
interpretation requires joining stimulus identity, raw streams, host causality,
network semantics, state differences, and (when available) target oracle and
replay evidence. ADR 0004 defines the causal ledger and sealed observation
bundles for **target runs**. ADR 0006 defines cumulative observation levels.
Neither defined a durable per-**action**/exec evidence backbone that classifies
tools and records explicit coverage gaps when collectors are absent.

Agents must not scrape transcripts for tool names. Classification must use
executable basename and argv[0] only. Capture authority must remain in the
world management layer; optional MCP tools are a query facade only.

## Decision

1. **Stimulus classification** (`internal/research`) maps instrumented commands
   to classes: `http_client`, `port_scanner`, `web_scanner`, `browser`,
   `binary_exec`, `generic`. Classification uses executable path basename and
   argv[0] basename only. Production wiring passes Executable as argv[0]
   (WML launch model); dual-candidate classification remains available for
   wrappers and tests.

2. **Observation levels** reuse ADR 0006 semantics: `baseline`, `deep`,
   `payload`. Only allowlisted named profiles (`deep`, `escalate`, `payload`,
   `invasive`) raise the level; unknown profiles stay **baseline** (fail
   closed). Payload requires an explicit invasive allowance. Intended
   companions are recorded as metadata **and** drive class-aware collector
   selection via `BuildCollectors`.

   **Production wiring is still baseline-only for policy escalate**:
   `OpenExec`, `OpenTargetExec`, and `adapters/agentrunner` call
   `ResolveObservationLevel(false, "", false)`. Baseline class-driven
   companions (e.g. network capture for `http_client`) **do** run collectors.
   Policy/named-profile escalate through public exec RPCs remains a follow-up;
   deep/payload companions activate when the action's observation level is set.

3. **Action evidence bundles** are written under
   `{stateRoot}/action-evidence/actions/<action_id>/` with:
   - `action.json` — join IDs (lease, exec/operation, run, generation, PID,
     times, correlation), stimulus class, level, exit, stream bounds, coverage
     and **explicit gaps**
   - `summary.json` — bounded agent summary with `confidence_floor` and
     evidence-role checklist
   - `stdout.bounded` / `stderr.bounded` — **high-sensitivity forensic class**;
     may retain secrets. Not served by the MCP facade.
   - `host/`, `network/`, `state/`, `static/`, `target/`, `replay/` —
     companion artifacts or gap-bearing placeholders

   Action IDs join to existing ledger identities: agent exec ID or target
   operation ID. Concurrent Begin for the same action_id is rejected.

4. **Class-aware collector registry** (`BuildCollectors`) selects companions
   from stimulus class + observation level + `IntendedCompanions`:

   | Companion | Behavior |
   |-----------|----------|
   | `host_process` | Ambient identity at Begin; PID-attributed process/sockets at Seal |
   | `network_capture` | Startable: dumpcap/tcpdump when present; else OS conn table by PID; ambient inventory is never claimed as pcap |
   | `network_decode` | tshark fields when pcap exists; else structured flow/endpoint records |
   | `host_syscall` | Linux strace brief attach when tool+PID available; explicit gap otherwise |
   | `state_diff` | Working-directory before/after + diff (deep+ or when intended) |
   | `static_context` | File type, SHA-256, PE/ELF headers, optional `file` tool |
   | `target_oracle` | Configured log paths only; gap when unconfigured |
   | `replay` | Minimal package: argv, cwd, env **keys**, capture refs (no env values) |

   **Lifecycle**: Startable collectors start at Begin and stop at Seal.
   CaptureAfter finalizes host/network/syscall with ProcessID. Collector
   failure is always fail-open (gaps, never block the command).

   **Attribution**: `Available`+`Attributed` only when evidence is action-linked
   (PID and/or action-window pcap). Ambient interface inventory alone is not
   semantic network.

   **Capability probe**: external tools resolved via `exec.LookPath` (injectable
   for tests). Missing tools produce stable gap reason codes, not invented data.
   Capture sizes are bounded; pcap files use mode `0600` under the action dir.

5. **Confidence floor** is a derived analytical ladder, not a full findings
   system. Machine tokens (JSON) are snake_case:

   `reported` → `observed` → `attributed` → `validated` → `demonstrated` →
   `reproduced` → `root_caused`

   Prose may say “root-caused”; the wire value is always `root_caused`.
   Missing roles never inflate confidence. Roles use `present` / `gap` /
   `unsupported` (unsupported when the role was never intended).

6. **Wiring**: `OpenExec` and `OpenTargetExec` begin/seal action sessions.
   `adapters/agentrunner` optionally accepts the same store and records
   process lifecycle PID. Collector failure or absence never blocks the
   command; it records gaps. Pure transport failures leave `exit_code` null
   rather than zero. Failed Begin writes a durable
   `*.begin-failed.json` marker without blocking exec. Default store creation
   failure disables evidence rather than aborting node construction.

7. **MCP facade** (`adapters/researchmcp`) exposes a small tool surface
   (`get_action`, `list_actions`, `query_host`, `query_network`, `state_diff`,
   `escalate_observation`, `assess`, `evidence_graph`) over the store. The
   store also exposes `QuerySyscall`, `QueryStatic`, `QueryOracle`,
   `QueryReplay` for richer joins; `evidence_graph` surfaces optional
   companion sidecars without adding MCP tools beyond the existing set.

### Capability matrix (best-effort)

| Evidence | Linux | Windows | Offline unit tests |
|----------|-------|---------|--------------------|
| host_process ambient | yes | yes | yes |
| host_process PID sockets | `/proc` inodes | `netstat -ano` | fakes / live PID |
| network pcap | dumpcap/tcpdump if present | dumpcap if present | disabled via options |
| network conn table | PID-attributed | PID-attributed | fakes |
| network_decode | tshark or flow table | flow table | fakes / flow table |
| host_syscall | strace attach if present | explicit tool-missing gap | fakes / gap |
| state_diff | working dir walk | working dir walk | yes |
| static_context | ELF + optional `file` | PE + optional `file` | yes (test binary) |
| target_oracle | configured paths | configured paths | yes |
| replay package | yes | yes | yes |

## Consequences

- Every instrumented exec produces a durable, queryable action bundle with
  class-appropriate collectors (or explicit gaps).
- Concurrent execs get distinct action IDs (their exec/operation IDs).
- Failed commands still seal bundles; exit codes are present only when known.
- Offline unit and fake-collector tests cover classification, sealing, gaps,
  registry composition, start/stop lifecycle, concurrency, authz, redaction,
  and the MCP library API without Docker/root/pcap hardware.
- Full Zeek/Suricata/Frida production stacks remain optional enhancements;
  capability probes and gaps keep absence observable.

## Rejected alternatives

- Scraping model text or shell transcripts for tool names.
- Hundreds of Kali-tool MCP wrappers as the evidence system.
- Putting capture authority in MCP or go-agent-runner.
- Silent skip of missing collectors.
- Requiring the full VR findings framework for confidence language.
- Escalating unknown named profiles to deep (fail-open overhead).
- One ambient collector set for every stimulus class.

## How to run tests

```text
go test ./internal/research/... ./adapters/researchmcp/... ./adapters/agentrunner/... ./internal/orchestration/ -count=1
go test ./internal/research/... -race -count=1
gofmt -w internal/research
```

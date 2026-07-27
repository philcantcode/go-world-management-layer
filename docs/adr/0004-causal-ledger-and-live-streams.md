# ADR 0004: Keep a durable causal ledger behind resumable live streams

- Status: Accepted
- Date: 2026-07-24

## Context

Consumers need live activity and performance while a run is happening, but
full-speed process, file, network, Android, and metric signals can exceed a
subscriber's capacity. Docker, kernel, host, emulator, and Android clocks do not
share one exact total order. A metrics backend or OpenTelemetry collector may
sample, aggregate, or become unavailable and therefore cannot be the sole audit
authority.

## Decision

Persist control/incident truth and high-rate observations before or alongside
live fan-out:

- SQLite WAL stores state, revisions, idempotency, incidents, policy snapshots,
  export transactions, and segment indexes.
- Append-only checksummed protobuf segments store high-rate observations and
  raw metric samples.
- Finalized segments and large captures are committed to the forensic artifact
  authority.
- OTLP and Prometheus are optional operational exports, not authoritative
  storage.

Every event carries source-local sequence, cursor, wall/monotonic times, clock
domain and sync epoch, session/lease, agent-workspace/generation/exec, and
target/generation/run/operation identities where applicable, collector
placement and coverage, process identity including start time,
policy/capability digests, evidence-backed origin classification, and explicit
causal or correlation fields. Target operations distinguish agent control,
agent-installed instrumentation, declared specimen, system behavior, and
mixed/unknown origin without discarding the raw observation.

Only defined parentage, trace context, or state/action relationships populate
`causation_id`. Timestamp proximity is recorded as correlation with method and
confidence. Collector overflow, restart, compaction, or loss emits a typed gap.

Subscribers use durable cursors. Slow consumers cannot block the agent or
collectors; they resume from local or artifact-backed segments.

Each finalized target-run interval produces an `ObservationBundle` that links
native/raw captures, normalized records, the target change manifest, coverage
and gaps, incidents, and a derived summary. The bundle cites ledger ranges; it
does not replace or duplicate the ledger's operational truth.

A deterministic reducer maintains a current `LiveSnapshot` of subject topology,
latest values, sample age, incidents, pressure, and collector coverage. Clients
fetch snapshot-at-cursor and then follow the streams. Missing, unsupported,
stale, and gap states are distinct from numeric zero. Agent and operator views
are authorization projections over the same ledger, not duplicated telemetry.

## Consequences

- Live UI/agents and later forensic reconstruction use one semantic event
  model.
- `worldctl top`, dashboards, and agent views share the same snapshot reducer
  and staleness semantics rather than independently interpreting metrics.
- Perfect cross-system total ordering is deliberately not claimed.
- Segment recovery, hash chaining, cursor stability, clock jumps, duplicate
  delivery, and gap behavior require focused tests.
- Storage budgets and sensitivity policy are part of admission, not an
  afterthought.

## Rejected alternatives

- Stream only: loses evidence on disconnect and cannot support replay.
- Put every sample in SQLite: row-level write amplification would couple
  control latency to telemetry volume.
- Treat OTLP as the audit log: exporters and backends are designed to transform
  and sample signals.
- Order everything by host receipt time: creates false causal claims and hides
  clock uncertainty.

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Version 0.x is pre-v1: minor releases may contain breaking API, CLI, policy,
schema, or on-disk format changes. Prefer the newest 0.x tag for consumers.

## [Unreleased]

### Added

- Opt-in managed Android SDK Emulator daemon composition with exact full-tree
  system-image identity, durable AVD/port ownership, scoped ADB/file transport,
  one-run replacement reset, startup reconciliation, and real signed-APK E2E.
- Native-Linux Docker cgroup-v2 identity discovery from the exact engine PID's
  `/proc` membership, with explicit unavailable capability reporting on other
  hosts instead of a synthesized identifier.
- Managed Android resource enforcement on Windows: atomic named-Job CPU and
  whole-process-tree memory limits that survive daemon restart, independent
  exact guest-RAM configuration, and a post-boot proof of exact guest `/data`
  block capacity.
- Exact process-observer configuration fingerprints and typed runtime bindings
  in `world-capabilities`, plus per-generation managed-Android logcat selection
  whose retained immutable bytes are verified by the combined daemon
  qualification.

### Changed

- Run finalization now arms process observers before target teardown and
  commits their stop afterward, so target-owned stream closure is an explicit
  boundary rather than a false collector crash; failed target stops cancel the
  preparation.

- Docker Linux target generations now grant mutable authority to exactly one
  run. Stop/finalization proves the exact container stopped, including detached
  or session-escaped processes, and another run requires replacement reset.
- Interrupted Docker and Android runs remain physically stopped while failed
  evidence is reconstructed; startup never resumes a tainted specimen runtime.
- Startup preserves current quarantined targets as expected stopped evidence,
  resumes only exact generation-bound destroy intents, and proves absence with
  a second authoritative inventory before committing destruction.
- Real target drivers persist exact reset-transition receipts, so same-request
  replay survives a daemon crash after physical replacement while any changed
  key or payload remains a conflict.
- Docker agent provisioning is authoritative and restart-convergent: exact
  stopped containers are started, exact running containers receive a fresh
  framed guest-readiness proof, and foreign or ambiguous state fails closed.
- Startup closes initial provisioning crash windows from immutable acquisition
  and creation keys, preserves both halves of an interrupted target reset for
  exact client replay, and orders quarantine as admission closure, run/bundle
  finalization, physical containment, then logical commit.
- Startup terminates interrupted agent exec boundaries before ordinary
  provisioning or workspace mutation, restores fresh framed guest readiness,
  and records the interrupted exec as lost only after the physical proof.
- Startup inventories complete historical physical plans through a distinct
  cleanup-only channel. Terminal Docker/Android targets can retire exact local
  residue even when their runtime is already absent, terminal Docker agents
  retire exact persisted workspaces, and follow-up inventory must prove cleanup
  complete; references and labels alone never authorize deletion or adoption.
- Resolved physical recoveries are revalidated from their complete
  predecessor/successor plans and require the exact trusted, strategy-derived
  physical completion action before startup accepts them.
- Windows process observers are atomically assigned to per-collector
  kill-on-close Jobs, so controller death removes the whole collector tree and
  restart can authoritatively reconcile an interrupted Android logcat run.
- Start-committed observer stdout/stderr partials survive controller loss:
  recovery fsyncs and bounds the exact pair, publishes the same canonical
  immutable artifacts as normal finalization, and still records continuity as
  lost with explicit gaps.
- The deployment-profile schema is now version 3 and the durable run-observer
  marker is now version 6. Older profile/marker formats are rejected rather
  than interpreted without the typed runtime-binding authority.

## [0.1.0] - 2026-07-28

Initial public release of the world management layer.

### Added

- Authenticated `world.v1` gRPC control plane served by independent `worldd`
  and `world-node` daemons (no controller/worker link between them).
- Stable public Go client package `world` over the versioned contract.
- Revisioned, idempotent logical lifecycle: research sessions/leases, agent
  workspaces and generations, targets and target runs, execs, incidents,
  recovery, and optimistic concurrency.
- SQLite WAL control store with forward migrations, hash-chain verification,
  and startup replay.
- Durable observation ledger with hash-chained segments, explicit gaps,
  resumable live projections, process-backed observer supervision, ledger
  capture, and crash-resumable sealed observation-bundle finalization.
- Opt-in physical Linux composition from an immutable version-2 deployment
  profile: digest-pinned Docker agent workspaces, directory-copy workspaces,
  Docker Linux targets, deployment-authorized local material, optional process
  observers, and ledger capture. Physical startup probes drivers, compiles
  every strict policy against the complete capability fingerprint, preflights
  plans, and fails closed before listening.
- Strict YAML policy compiler and `world-capabilities` probe/compile CLI.
- Operator and agent-facing commands: `worldctl`, `world-target`,
  `world-observe`, `world-capture`, `world-export`, `world-guest`,
  `world-idle`.
- First-party adapters: lease-bound `ExecutionEnvironment` for
  `go-agent-runner`, and scoped `MaterialAuthority` for
  `go-forensic-artifacts`.
- Cuttlefish-family driver contracts and opt-in AttachedEmulator APK/scoped-ADB
  qualification (not selectable as a daemon Android composition).
- Deterministic verify gate (`make verify` / `go run ./cmd/verify`) covering
  module drift, format, schema, fuzz seeds, contracts, security boundaries,
  integration tests, full suite, race, vet, and Linux/amd64 cross-build.
- Windows Docker Desktop end-to-end harness under `testdata/e2e/`.
- Architecture, implementation plan, accepted ADRs, example policy, and
  operator runbooks under `docs/`.
- CI across Ubuntu, Windows, and macOS; tag-driven GitHub release workflow;
  Dependabot for Go modules and Actions; MIT license; changelog, releasing,
  and security docs.

### Fixed

- Canonicalize agent workspace mount paths through `EvalSymlinks` so host
  path aliases (for example Windows TEMP junctions) do not fail closed when
  the leaf tree is clean.
- Build input-cache views with writable staging directories, then seal the
  finished root to `0o500` (Unix hosts previously failed while publishing
  entries into a read-only staging root).
- Collect sealed input views through the same chmod-before-remove path used
  for rebuild cleanup so Unix GC no longer fails on read-only trees.
- Make unit tests portable on non-root Linux and CRLF Windows checkouts:
  guest ownership fixtures use the current process identity when root handoff
  is unavailable; policy fixture replacements normalize newlines; directory-copy
  composition tests skip off Windows where `node.os.windows` is required;
  sealed managed trees are re-opened for write before intentional test
  corruption. CI exercises Ubuntu and Windows (safe-path namespaces are
  implemented for those platforms only).

### Notes

- Status remains pre-v1. Managed Cuttlefish daemon composition, production
  collector suites, remote forensic repository integration, packaging, and a
  supported host/version matrix remain follow-on release work.
- Ordinary `go test ./...` does not start Docker, an emulator, OverlayFS, eBPF,
  or a remote artifact service. Real-node qualification is separate evidence.

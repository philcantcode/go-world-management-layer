# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Version 0.x is pre-v1: minor releases may contain breaking API, CLI, policy,
schema, or on-disk format changes. Prefer the newest 0.x tag for consumers.

## [Unreleased]

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

# Design: library-only Manager (breaking cutover)

- Status: **implemented** (accepted product direction; cutover complete)
- Date: 2026-07-30
- Revised: 2026-07-30
- Supersedes (as product surface): dual `worldd` / `world-node` remote gRPC
  control plane, `world.Dial` / `world.Client`, and ADR 0001 dual-daemon product
  topology (see [Key Decisions](#10-key-decisions--risks); ADR 0001 is
  superseded as current product, not deleted from history)
- Related: [docs/design.md](../design.md), [README.md](../../README.md)

## 0. Product decision

Replace the external control-plane system with a **single imported application
library**. Hosts embed `world.Open` and hold a `*world.Manager`.

**Do not support both models.** Delete remote Dial, bearer/mTLS listen
products, and dual `worldd` + `world-node` as independent servers. A thin local
CLI may embed `Manager`; it must not reintroduce socket Dial as a product path.

This is a breaking 0.x cutover on one branch. No dual-mode compatibility shim.

---

## 1. Public package API

Package path remains `github.com/philcantcode/go-world-management-layer/world`.

### 1.1 Construction

```go
// Config is the host-owned Open configuration. Paths, drivers, and the local
// subject replace former daemon flags and network credentials.
type Config struct {
    // LocalPaths holds exclusive control-state and material roots.
    Paths LocalPaths

    // Subject is the fixed in-process policy subject for all Manager calls.
    // Required. No bearer token or mTLS client identity is accepted.
    Subject Subject

    // DeploymentProfile is the absolute path to an immutable version-3
    // deployment profile, or empty for logical-only composition.
    DeploymentProfile string

    // Drivers selects physical composition (none | docker/directory/process/…).
    Drivers DriverConfig

    // Bounds and timeouts previously exposed as WORLD_* daemon flags.
    ControlTimeout         time.Duration
    ReconciliationInterval time.Duration
    ReconciliationTimeout  time.Duration
    ShutdownTimeout        time.Duration
    MaxTransferBytes       int64
    MaxExecBytes           int64
    MaxADBBytes            int64
    MaxBundleBytes         int64
    MaxCaptureRecords      int
    AllowRemoteADB         bool
    ProbeTimeout           time.Duration
}

type LocalPaths struct {
    StatePath              string // SQLite control DB; processlock.Acquire target
    LedgerDirectory        string
    OrchestrationStateRoot string
    BundleRoot             string
    MaterialRoot           string
}

type Subject struct {
    // Name is the policy owner subject (formerly WORLD_BEARER_SUBJECT /
    // mTLS CN). Default conceptual value for operator CLIs: "local-operator".
    Name string
    // Role optionally restricts which transitions the Manager may invoke
    // (operator vs internal). See §3.
    Role SubjectRole
}

type SubjectRole string

const (
    RoleOperator SubjectRole = "operator" // default: lease-scoped host ops
    RoleInternal SubjectRole = "internal" // node-only transitions if still needed
)

type DriverConfig struct {
    AgentDriver      string // none | docker
    LinuxTarget      string // none | docker
    AndroidTarget    string // none | android-emulator
    WorkspaceDriver  string // none | directory (directory-copy-non-production mode)
    MaterialDriver   string // none | local (default local)
    ObserverDriver   string // none | process
    CaptureDriver    string // none | ledger
    // Absolute roots and image/binary fields mirror former daemon composition
    // config (agent workspace root, target root, android SDK paths, …).
    // Exact field set is production HostConfig minus listen/auth fields.
}

// Open acquires exclusive process ownership of Paths.StatePath, composes
// production Core/Service/drivers, runs startup reconciliation, and returns a
// ready Manager. It fails closed on lock contention, policy preflight failure,
// or incomplete physical recovery — the same fail-closed rules as today's
// daemon before it opened a listener.
func Open(ctx context.Context, cfg Config) (*Manager, error)

// Manager is the sole public control handle for one control-state tree.
// It is not safe to Open the same StatePath from two processes; the second
// Open fails with a processlock error.
type Manager struct { /* unexported production wiring */ }

func (m *Manager) Close() error
```

`Open` is the only supported constructor. There is no `Dial`, `DialContext`,
`DialOptions`, or `NewClient`.

### 1.2 Lifecycle and mutation methods

Unary methods mirror today's `world.Client` surface but invoke the in-process
`orchestration.Service` / `Controller` directly (no network hop).

| Manager method | Purpose |
| --- | --- |
| `AcquireResearchSession` | Acquire lease + agent workspace generation |
| `GetResearchSession` | Read session view |
| `WaitResearchSession` | Block until terminal session state |
| `RenewLease` | Extend lease TTL |
| `ReleaseResearchSession` | Drain and release |
| `CreateTarget` / `GetTarget` | Target generation lifecycle |
| `StartTargetRun` / `WaitTargetRun` / `StopTargetRun` | Mutable run |
| `ResetTarget` / `DestroyTarget` / `QuarantineTarget` | Target ops |
| `RequestRecovery` | Explicit recovery generation |
| `GetIncident` / `CreateIncident` / `TransitionIncident` | Incidents |
| `GetExec` / `CreateExec` / `TransitionExec` / `FinalizeExec` | Logical exec records |
| `GetLiveSnapshot` | Observation projection |
| `GetObservationBundle` | Sealed bundle read |
| `StartCapture` / `RequestCapture` / `StopCapture` | Capture control |
| `DeclareExport` / `PreviewChangeSet` / `CommitExport` | Export |
| `TransitionAgentGeneration` / `TransitionTargetGeneration` / `TransitionTargetRun` | Generation transitions |
| `CreateTargetOperation` / `TransitionTargetOperation` | Target operations |
| `Reconcile` | Optional host-triggered reconciliation tick |

Helpers for host composition (adapters):

| Accessor | Purpose |
| --- | --- |
| `Core()` | `*application.Core` for advanced/adapters (stable internal hand-off) |
| `AgentDriver()` | `ports.AgentWorkspaceDriver` for agentrunner |
| `ActionEvidence()` | `*research.Store` when composed |
| `Material()` | `ports.MaterialAuthority` for forensic adapters |

Exact Go signatures take `context.Context` plus the same request DTO types as
today's client (see §2). Each method applies `Config.Subject` before calling
Service authorization — callers do not pass transport identity.

### 1.3 Streaming sessions (product shape)

gRPC bidirectional streams are **not** the product. Manager returns Go session
handles that wrap existing Service/transport pumps:

```go
type ExecSession interface {
    // Start sends the open/start request (lease, plan, argv, …).
    Start(ctx context.Context, req *worldv1.OpenExecStart /* or retained DTO */) error
    WriteStdin([]byte) error
    Signal(string) error
    Resize(*worldv1.TerminalSettings) error
    Heartbeat() error
    // Recv blocks for the next stdout/stderr/heartbeat/outcome frame.
    Recv() (*worldv1.ExecFrame, error) // or pure-Go ExecEvent; see §2
    Close() error
}

func (m *Manager) OpenExec(ctx context.Context) (ExecSession, error)
func (m *Manager) OpenTargetExec(ctx context.Context) (TargetExecSession, error)
func (m *Manager) PushTargetFile(ctx context.Context) (FileTransferSession, error)
func (m *Manager) PullTargetFile(ctx context.Context, req *worldv1.PullTargetFileRequest) (FileTransferSession, error)
func (m *Manager) OpenTargetADB(ctx context.Context) (ADBSession, error)

type ObservationSubscription interface {
    Recv() (*worldv1.ObservationRecord, error) // name per retained DTO
    Close() error
}
type MetricSubscription interface {
    Recv() (*worldv1.MetricSample, error)
    Close() error
}

func (m *Manager) SubscribeObservations(ctx context.Context, req *worldv1.SubscribeObservationsRequest) (ObservationSubscription, error)
func (m *Manager) SubscribeMetrics(ctx context.Context, req *worldv1.SubscribeMetricsRequest) (MetricSubscription, error)
```

Session implementations adapt Service's existing `execWire` / stream handlers
to channels or `io`-style methods **without** depending on
`google.golang.org/grpc` stream server interfaces at the public boundary.
Internal pumps in `internal/transport` remain the byte-pump authority.

### 1.4 Mutation metadata

Keep `NewMutation` / `MutationMetadata` in package `world` (wrapping or
aliasing `worldv1.MutationMetadata`) so idempotency keys, correlation IDs, and
deadlines stay host-visible without a separate RPC envelope.

---

## 2. Request/response types: retain worldv1 DTOs short-term

**Decision: reuse `api/world/v1` protobuf message types as plain Go structs for
Manager request/response DTOs in the first cutover.**

### Justification

1. **Shrinks the diff.** Mapping already exists between Service, Core, and
   protobuf views. Replacing every type with pure-Go structs doubles the cut
   without changing runtime semantics.
2. **Manager methods require no network hop.** Types may live in `worldv1` and
   still be passed in-process; protobuf wire encoding is not used on the
   Manager path.
3. **Stable field names.** Existing tests, golden fixtures, and adapters already
   speak these shapes.
4. **Follow-up allowed.** A later 0.x may introduce pure-Go types in `package
   world` and drop public dependence on generated stubs; that is out of scope
   for this cutover.

### Constraints

- Package `world` **must not** export `WorldServiceClient`, gRPC dial helpers, or
  require hosts to import `google.golang.org/grpc` for control.
- Generated gRPC **server/client stubs** cease to be product surface. The `.proto`
  file and message types may remain for DTO generation and optional internal
  tooling; the `WorldService` RPC product is deleted.
- Manager returns deep-copied DTOs (same defensive-copy rule as today's Client)
  so callers cannot mutate Service-owned state.
- Prefer type aliases in `world` for the most-used views
  (`ResearchSessionView`, `Lease`, `Target`, `TargetRun`, `Incident`,
  `ObservationBundle`) to keep host import paths short.

### Explicit non-goal for this cut

Do not keep dual `Client` + `Manager` exports. After cutover, only `Manager`
(and `Open` / `Config` / sessions) remain public in `package world`.

---

## 3. Subject / auth model (in-process)

### 3.1 Model

| Former product | Library-only replacement |
| --- | --- |
| Bearer token → subject map | `Config.Subject.Name` fixed at `Open` |
| mTLS client cert identity | Not used for host control |
| `trusted-node-subjects` | `Config.Subject.Role` / explicit internal role if node-only transitions remain |
| `rpc.IdentityFromContext` | Open installs a `SubjectResolver` that always returns `Config.Subject.Name` |
| Listen auth (loopback bearer rules) | Deleted with listen path |

No bearer tokens are required for local embed. Host process identity and OS
permissions on state directories are the trust boundary; the library assumes
the importing process is already trusted.

### 3.2 Authorization rules

Service `authorize()` rules remain. They still scope leases, generations, and
policy digests to the resolved subject string. What changes is **how** the
subject is resolved:

```text
Open(cfg) → SubjectResolver = func(ctx) (cfg.Subject.Name, true)
```

### 3.3 Role capabilities (mitigation for auth collapse)

Today, trusted-node subjects gate certain generation/exec transitions. Collapsing
to one fixed subject must not silently grant every in-process caller node-only
power.

**Rule for cutover:**

- Default `RoleOperator`: Manager methods expose the same capability set as
  today's bearer operator client (lease-scoped host ops).
- `RoleInternal` (or an explicit allow-list on `Config`) is required for any
  transition that previously demanded a trusted-node subject.
- Thin operator CLIs always open with `RoleOperator`.
- Tests that formerly asserted RPC auth move to Manager-level authorization
  tests with configured subjects/roles.

Hosts that need multi-tenant isolation run **separate processes** (or separate
state trees), each with its own `Open` and subject — not multiple subjects on
one Manager in v1 of this design.

---

## 4. Daemon composition → callable Open path

### 4.1 What moves

`internal/orchestration/daemon` today does:

1. parse flags/env → `config`
2. `processlock.Acquire(statePath)`
3. open store, Core, policy registry/authority, ledger
4. `configureHostDrivers` (deployment profile, preflight, capability fingerprint)
5. `orchestration.NewProduction`
6. **startup reconciliation** (`reconcileStartupStateWithin`)
7. start lease-termination ticker
8. `rpc.NewServer` + `rpc.Listen` + `Serve` ← **DELETE**

Steps 2–7 become the body of library `Open` (or an unexported
`daemon.OpenProduction` / `orchestration.OpenHost` helper that `world.Open`
calls). Step 1 becomes `world.Config` population by the host or CLI.

### 4.2 Package layout

As implemented:

```text
world.Open
  → internal/orchestration/daemon.OpenHost
       processlock, store, core, ledger, configureHostDrivers,
       NewProduction, reconcileStartup, background ticker
  → world.Manager holds *daemon.Host + in-process facade
```

Driver wiring (`configureHostDrivers`, profile load/preflight) remains under
`internal/orchestration/daemon`. **Export only what Open needs**; do not
re-export a public multi-mode daemon or listen product.

### 4.3 Optional thin CLI

One local debug/operator binary may call `world.Open` with flags mapped to
`Config` (paths, drivers, deployment profile, subject name). It:

- does **not** listen on unix/TCP for WorldService;
- does **not** accept `-token` / mTLS client flags for remote Dial;
- exits when the subcommand finishes and `Manager.Close` runs.

Collapse choice: **delete `cmd/worldd` and `cmd/world-node` as external
servers**; optionally keep a single `cmd/world` or retarget `cmd/worldctl` as
the only operator entrypoint that embeds Manager (see §6).

### 4.4 Background reconciliation

The lease-termination ticker and any periodic reconcile loops continue for the
lifetime of `Manager`, cancelled in `Close`. Startup reconciliation **always**
completes successfully inside `Open` before `Manager` is returned to the caller
(§8).

---

## 5. Explicit DELETE list

Product surface to remove (no dual-mode retention):

| Item | Action |
| --- | --- |
| `cmd/worldd` | **Delete** (ModeController server entrypoint) |
| `cmd/world-node` | **Delete** (ModeNode server entrypoint) |
| `daemon.Mode` / `ModeController` / `ModeNode` dual defaults | **Delete**; single path prefix family (`world-*` paths via `Config.Paths`) |
| `daemon.Main` / `daemon.Run` listener path (`rpc.NewServer`, `rpc.Listen`, Serve) | **Delete** |
| Daemon flags/env: unix-socket, listen, bearer-token, bearer-subject, trusted-node-subjects, tls-cert/key/client-ca | **Delete** as product auth/listen surface |
| `world.Dial` / `DialContext` / `DialOptions` | **Delete** |
| `world.Client` as gRPC `WorldServiceClient` wrapper | **Delete** |
| `cmd/internal/worldcli.ConnectionConfig` Dial flags | **Delete** remote connection fields; load `world.Config` / `*Manager` instead |
| `worldcli.Dial` / `LoadClientTLS` | **Delete** |
| `internal/rpc` as product control plane (`Listen`, bearer/mTLS host auth) | **Delete** listen/auth product path; package may remain as **in-process facade** used only by Manager |
| `api/world/v1` **WorldService** gRPC service as host talk path | **Drop server/client stubs as product**; keep message DTOs if still useful |
| `google.golang.org/grpc` as **required host integration** dependency for public control | **Remove** from public host integration; may remain for internal/generated code only |
| README/docs dual `worldd`\|`world-node` remote architecture and Dial quickstarts | **Rewrite** to library embed |
| ADR 0001 dual-daemon as **current product** | **Supersede** (history retained) |
| E2E `Start-WorldDaemon` / `worldd.exe` process authority + client Dial | **Rewrite** for Open / in-process CLI (legacy function name may remain as no-op state prep) |
| `world/client_test.go` Dial harness; `internal/rpc/*` transport auth as public contract | **Replace** with Manager ownership/auth tests |
| RELEASING / SECURITY language shipping authenticated daemon sockets as product | **Rewrite** |

### Collapse-or-delete decision (servers)

**Delete both external servers.** Do not ship one daemon that still Serves
WorldService. Operator needs are met by in-process CLIs (`worldctl` et al.)
embedding `Manager`. A future debug-only long-running process is out of scope
and must not restore gRPC WorldService listen.

---

## 6. Operator CLIs: in-process only

| Command | After cutover |
| --- | --- |
| `cmd/worldctl` | Opens `world.Manager` via local flags → `Config`; all subcommands use Manager |
| `cmd/world-target` | In-process Manager target sessions (exec / file / ADB) |
| `cmd/world-observe` | In-process snapshot / watch / metrics / bundle |
| `cmd/world-capture` | `Manager.RequestCapture` (and start/stop) |
| `cmd/world-export` | `Manager.DeclareExport` / `CommitExport` |
| `cmd/internal/worldcli` | Presentation helpers accept `*world.Manager`, not `*world.Client` |
| `cmd/world-guest` | **Keep** — framed supervisor inside containers only (not control plane) |
| `cmd/world-idle`, `world-capabilities`, `world-android-image-digest`, `verify` | Keep as non-control utilities |

**Forbidden:** thin wrappers that Dial an external socket. Any flag named
`-address`, `-unix-socket`, `-token` for WorldService is removed.

CLI flags become Open configuration: `-state`, `-ledger-dir`, driver roots,
`-deployment-profile`, `-subject` (optional override of default
`local-operator`).

### Adapters

| Adapter | Migration |
| --- | --- |
| `adapters/agentrunner` | Resolve Core / AgentDriver (or Manager-bound exec API) from `Open` / Manager accessors rather than host-only external assembly; share Service/Controller paths with `Manager.OpenExec` |
| `adapters/researchmcp` | Bind `research.Store` from `Manager.ActionEvidence()` |
| `adapters/forensicartifacts` | Wire Backend via Open/Manager material composition |

External hosts (e.g. go-agent-runner) currently expected to Dial worldd must
`import world` and call `Open`.

---

## 7. Streaming: in-process sessions, not gRPC product streams

### 7.1 Design

- Public API: Go interfaces (`ExecSession`, `TargetExecSession`,
  `FileTransferSession`, `ADBSession`, subscriptions) — see §1.3.
- Implementation: thin adapters over existing `orchestration` stream methods,
  replacing `worldv1.WorldService_*Server` parameters with channel/callback
  wires that implement the same `execWire` (or successor) contract.
- Byte pumps and framed guest protocol stay in `internal/transport` and
  `internal/framing`.
- Backpressure: session buffers match current Service defaults
  (`defaultStreamBuffer`); context cancel closes the session; `Close` is
  idempotent and waits for pump shutdown.

### 7.2 Tests

- Golden protocol tests that today go through gRPC streams move to in-process
  session tests against Manager (or Service with a non-gRPC wire).
- agentrunner continues to use driver-level `OpenExec` for provider execution;
  both paths must share plan binding and Core transitions so they cannot
  diverge.

### 7.3 Non-goals

- Do not expose gRPC stream clients on Manager.
- Do not keep a "legacy stream" Dial path for one release.

---

## 8. Startup reconciliation inside Open

**Invariant:** `Open` does not return a usable `*Manager` until startup
reconciliation has finished successfully for that control-state tree.

Sequence (preserved from daemon `Run`):

1. `processlock.Acquire(Paths.StatePath)` — fail closed on contention.
2. Open store, Core, policy registry/authority, ledger (apply ledger tail
   repairs; log them).
3. `configureHostDrivers` — deployment profile load, capability probe, strict
   policy compile, plan preflight; fail closed if physical composition cannot
   be enforced.
4. `NewProduction`.
5. **`reconcileStartupStateWithin(ctx, …, ReconciliationTimeout)`** — physical
   inventory/adoption, observer markers, lease termination reconciliation,
   output/finalization proof. Any incomplete identity/cleanup/finalization
   proof fails Open.
6. Start background lease-termination ticker (Manager-scoped).
7. Return `*Manager`.

Callers must treat a successful `Open` as ready to admit new work: evidence and
generations are consistent enough for host operations (same fail-closed bar as
the former daemon before it would have opened a listener).

`Manager.Close` stops tickers, closes observers/drivers, releases the process
lock, and closes store/ledger — same resource ownership as daemon shutdown
without serving.

---

## 9. Implementation status (cutover complete)

Single branch cutover; no dual-mode. Steps below are **done** as of the
library-only product surface:

1. **Done.** `daemon.OpenHost` constructs production wiring without
   listen/Serve: processlock → store/core/ledger → drivers → Production →
   startup reconcile → handle. Lock contention is unit-tested via `world.Open`.

2. **Done.** `world.Config`, `world.Open` / `OpenContext`, and `world.Manager`
   with Close, fixed Subject, and unary methods.

3. **Done.** Remaining unary Manager methods (targets, runs, incidents,
   capture, export, transitions, snapshots, bundles).

4. **Done.** In-process session types for OpenExec, OpenTargetExec, file
   push/pull, ADB, observation/metric subscriptions (Manager stream API).

5. **Done.** Adapters (`agentrunner`, `researchmcp`, `forensicartifacts`) bind
   through Manager / Open accessors.

6. **Done.** CLIs embed Open + Manager only; remote Dial flags removed.

7. **Done (product surface).** `world.Dial` / remote `Client`, `cmd/worldd`,
   `cmd/world-node`, and the listen/Serve path are deleted. `internal/rpc`
   remains as an **in-process facade** used by Manager (not a host-facing
   WorldService product). `google.golang.org/grpc` may still appear in
   go.mod for internal/generated code; hosts must not Dial for control.

8. **Done.** E2E harness prepares shared local state trees and drives
   in-process CLIs (`worldctl` / `world-target` / …) via Open; it does not
   spawn `worldd`. ADR 0001 is superseded; README, design.md, SECURITY,
   RELEASING, and CHANGELOG describe library-only integration.

9. **Gate.** Prefer `go run ./cmd/verify` / `go test ./...` for local
   evidence; Docker/Android opt-in suites remain release evidence.

---

## 10. Key Decisions + risks

### Key Decisions

| ID | Decision |
| --- | --- |
| K1 | **Library-only control plane.** Hosts embed `world.Open`; no remote WorldService product. |
| K2 | **Single process ownership** per control DB via `processlock` at Open; multi-open fails closed. |
| K3 | **Reuse worldv1 message DTOs** short-term; drop gRPC stubs as product. |
| K4 | **Fixed `Config.Subject`** replaces bearer/mTLS; optional Role for former node-only gates. |
| K5 | **Delete both daemons**; collapse operator UX to in-process CLIs embedding Manager. |
| K6 | **Streaming = Go sessions**, reusing internal pumps; not gRPC streams. |
| K7 | **Startup reconciliation inside Open** before Manager is returned. |
| K8 | **One-branch breaking cutover**; no Dial+Open dual export. |
| K9 | **ADR 0001 dual-daemon topology superseded** as product; physical isolation goals of drivers remain. |
| K10 | **world-guest stays** as in-container supervisor only. |

### Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Stream API redesign breaks framing/backpressure | Reuse `internal/transport` pumps and Service logic with thin non-gRPC adapters; golden protocol tests. |
| Auth collapse exposes node-only transitions | Encode Role on Config/Manager; keep authorize() rules; Manager-level auth tests replace RPC tests. |
| Multi-open races on one control DB | Keep `processlock.Acquire` at Open; document single-owner semantics in README/SECURITY. |
| E2E crash tests assume worldd PID/parent identity | Rewrite harness: Open in helper process only when needed; assert lock/state recovery without daemon PID mythology. |
| API type churn (protobuf vs pure Go) | Keep worldv1 DTO field names first; version Config if needed; avoid dual Client+Manager. |
| CLI dual-mode temptation | Delete Dial entirely; only Open with local paths/drivers. |
| Operators keep dual world / world-node state trees | Single Config schema, one lock path family; migration notes for node-prefixed dirs. |
| agentrunner vs Manager.OpenExec divergence | Share Service/Controller paths only. |
| pkg.go.dev / release surface points at Dial | CHANGELOG + RELEASING rewrite; pin guidance for 0.x break. |
| grpc status codes leak from Service | Map to domain errors at Manager boundary over time; stop importing grpc in new public code. |

### Supersession note (ADR 0001)

ADR 0001 correctly motivated Linux-first dedicated nodes and narrow privileged
drivers. Those goals remain. What is rejected as **product** is shipping two
independent full daemons plus an authenticated network control plane as the
primary integration path. Control and physical composition run in one host
process that imports the library; privilege separation is process/OS/deployment
owned, not dual-daemon WorldService.

---

## 11. Keep list (unchanged internals)

Preserved as-is unless a step above requires a thin adapter:

- `internal/application`, `internal/domain`, `internal/store`, `internal/ledger`
- `internal/observation`, `internal/observationbundle`
- `internal/orchestration` Controller/Service/NewProduction/reconciliation/…
- daemon **composition** pieces without RPC Serve
- `internal/orchestration/policyauthority`, `internal/policyregistry`, `policy`
- `internal/ports`, drivers (agent docker, linuxcontainer, cuttlefish,
  workspace directory, observer process, command, deviceproxy, dockercli)
- `internal/guest` + `cmd/world-guest`, `cmd/world-idle`
- `internal/transport`, `internal/framing`, `internal/processlock`
- `internal/localmaterial`, `internal/research`, `internal/inputcache`,
  `internal/admission`, safepath/atomicfile/workspace, androidcontract
- adapters (migrated wiring only), `cmd/world-capabilities`,
  `cmd/world-android-image-digest`, `cmd/verify`, `internal/testkit`

---

## 12. Documentation touchpoints

This design updates the top-level architecture story in:

- [README.md](../../README.md) — library embed diagram and Open quickstart
  direction
- [docs/design.md](../design.md) §1.1 — implementation boundary: Manager library,
  not dual daemons

Ops runbooks under `docs/operations/` and RELEASING/SECURITY are rewritten in
implementation step 8; they are not fully edited in this design commit beyond
architecture pointers.

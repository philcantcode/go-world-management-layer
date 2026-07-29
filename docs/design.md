# World management layer design

- Status: v1 target architecture with current implementation boundary
- Date: 2026-07-24
- Revised: 2026-07-27
- Initial deployment: dedicated Linux hosts

## 1. Summary

The world management layer is the operational boundary between an autonomous
agent and the programs or apps it investigates. Each leased research session
contains two deliberately separate execution tiers:

1. a persistent **agent workspace** containing the provider agent, its research
   tools, and only the files projected by policy; and
2. zero or more disposable **target sandboxes** in which Linux programs or
   Android apps are executed under host-owned observation.

Both tiers are untrusted and sit beneath a trusted control and observation
plane. A target is never a child of the agent container. Resetting or replacing
a target does not discard the agent's notes, scripts, or other workspace state.
The manager retains exclusive control over provisioning, containment, resource
budgets, evidence movement, collectors, recovery, and teardown.

The system is a single-host control plane first. A trusted host application
acquires a research-session lease, gives `go-agent-runner` a lease-bound
`ExecutionEnvironment`, and releases the lease when the investigation ends.
That adapter resolves and runs all provider probes and attempts inside the
agent workspace. From there the agent uses ordinary tools, a transparent
`world-target` command channel, a one-device ADB endpoint, and an optional MCP
facade to control its assigned targets, inspect live evidence, and consume
sealed observation bundles.

Version 1 targets dedicated Linux nodes. OverlayFS, cgroup v2 pressure signals,
Linux namespaces, eBPF, and KVM are part of the actual design, not portable
implementation details. A Windows or macOS client may call a remote Linux node,
but Docker Desktop is not the initial security or performance reference.

### 1.1 Current implementation boundary

The target architecture below intentionally reaches beyond the currently
shipped executable topology. Today `worldd` and `world-node` are independent
full daemons with different default state paths/endpoints; there is no
controller-to-node protocol and `world-node` does not register with `worldd`.
Either binary can run logical-only or activate the same fail-closed physical
Linux and managed-Android composition from a trusted version-3 deployment profile:

- a directory-copy workspace and digest-pinned Docker agent container;
- digest-pinned Docker Linux targets with scoped exec/file transports, exact
  container stop proof, and one mutable run per replacement generation;
- full-tree-digest-pinned Android SDK Emulator AVDs with scoped ADB/file
  transports and replacement-generation clean reset;
- deployment-authorized local input/output/bundle material;
- optional profile-defined process observers and ledger capture; and
- startup policy compilation, plan preflight, exact physical-plan binding,
  resource admission, inventory/adoption, lease drain, and crash recovery.

`directory-copy-non-production` is the only daemon-composed workspace mode;
the OverlayFS, cgroup/pressure actuation, eBPF suites, split controller/node
topology, and fleet scheduler described later remain target architecture.
Directory copy bounds bytes and inodes during prepare/scan/seal/export, but it
does not enforce either as a live filesystem quota while the agent runs. Those
two physical support facts are reported as `unsupported` and are the only
resource-support checks omitted for this explicit non-production mode. Runtime
identity/isolation and CPU, memory, swap, PID, and capture enforcement remain
mandatory; an OverlayFS policy fails closed without live byte and inode support.

Android SDK Emulator targets are daemon-composed with durable AVD/port
allocation, exact installed-image and runtime identity, headless accelerated
clean boot, exact-serial ADB authorization, one mutable run per generation,
replacement reset, quarantine, destruction, and startup reconciliation. The
AttachedEmulator backend remains an opt-in boundary test that never assumes
ownership of its externally started emulator. A daemon-selected Cuttlefish or
physical-device backend is not shipped.

## 2. Ecosystem boundaries

| System | Owns | Does not own |
| --- | --- | --- |
| `go-agent-runner` | provider selection, CLI protocol normalization, provider session recovery, schema validation, provider process result | agent/target lifecycle, host containment, workspace custody, system telemetry |
| this repository | research-session leases, agent/target generations, target runs, workspace/target lifecycle, containment, observation bundles, incidents, pressure decisions | analytical conclusions, provider protocol interpretation, immutable forensic custody |
| `go-forensic-artifacts` | immutable bytes, occurrence identity, selections, projections, capture, fixity, provenance | hostile-code sandboxing, agent/target lifecycle, live scheduling |
| `go-vr-research-framework` | observations, hypotheses, claims, findings, research activities, review, analytical history | processes, agents, raw bytes, environment scheduling |
| campaign manager / host | authentication, authorization, policy selection, correlations across systems, translating accepted results into domain commands | bypassing any subsystem's invariants |

The dependencies point inward through narrow host-owned interfaces. The world
manager does not import the VR research core. A first-party artifact adapter may
import `go-forensic-artifacts`, but the domain and driver packages depend on a
world-owned `MaterialAuthority` interface.

## 3. Goals

- Run arbitrary agent-selected tools inside a leased environment without a
  per-command allowlist or host execution path.
- Ensure no provider or tool process executes on the host through the runner
  integration.
- Keep the agent workspace alive across independent target creation, execution,
  reset, failure, and destruction.
- Run each investigated program or app in a disposable target sandbox rather
  than in the agent workspace.
- Materialize immutable inputs without exposing the artifact repository.
- Give each agent workspace a frozen artifact view without repeatedly
  copying shared bytes or leaking entries from another view.
- Preserve an authoritative change set and capture only safe, explicitly
  selected outputs back into the artifact authority.
- Provide capability-aware add-on target drivers, beginning with observable
  Linux containers and instrumented Android virtual devices.
- Stream live health, lifecycle, activity, and performance data from the host,
  agent workspace, Linux target, emulator, Android guest, and important process
  scopes.
- Prefer high-fidelity, independently collected target visibility over a
  stronger runtime whose boundary hides the target's behavior. Any resulting
  isolation trade-off is explicit in policy and capability evidence.
- Seal each target run into an observation bundle containing raw captures,
  normalized events, a change manifest, collector coverage and gaps, and a
  derived agent-facing summary.
- Use host and per-lease pressure to control admission and reduce concurrency
  before the host becomes unusable.
- Detect environment, workload, observer, and device failures; record them as
  incidents; capture evidence; and provide actionable feedback.
- Restore virtual-device state or recreate containers only as explicit new
  generations of the affected resource. Never hide a crash behind an apparently
  continuous session.
- Make every mutation idempotent, every long operation cancellable, every
  stream bounded, and every state reconstructable after a daemon crash.
- Support adversarial, crash, race, resource-exhaustion, and observer-effect
  testing from the first vertical slice.

## 4. Non-goals

- Interpreting Claude, Codex, Grok, or another provider's streaming protocol.
- Acting as an analytical knowledge graph or deciding whether a vulnerability
  conclusion is valid.
- Replacing the forensic artifact store with a second byte authority.
- Treating the agent workspace itself as the target or allowing an agent to
  create nested targets through a Docker/containerd socket.
- Claiming that ordinary Docker containers are a perfect boundary against a
  hostile kernel exploit.
- Maximizing target isolation at the expense of required visibility. Stronger
  runtimes remain optional capability profiles, not the v1 default.
- Claiming exact causality from timestamps alone. Explicit parentage and trace
  context are causal; time proximity is correlation with stated confidence.
- Transparently continuing an agent process after its agent workspace failed.
- Managing physical Android devices in v1; they remain a future target driver
  with different cleanup and recovery guarantees.
- Capturing every payload by default. High-volume or invasive observation is
  activated by an authorized policy or request within that policy.

## 5. System invariants

1. Every executing provider and descendant belongs to exactly one lease, agent
   workspace generation, container, and cgroup subtree. Every investigated
   workload belongs to exactly one target, target generation, and target run.
2. The agent never receives the Docker socket, host PID namespace, host root,
   artifact repository path, raw USB bus, control-plane credentials, or a
   general lifecycle/management socket. It may issue arbitrary commands and
   transfer arbitrary bytes inside its assigned disposable targets, but every
   transport is structurally bound to one lease, target generation, and run.
   Lifecycle, resource, network, capture, and export mutations remain validated
   against the frozen policy.
3. Agent workspaces and target sandboxes are sibling resources owned by the
   trusted host-driver composition. A target never mounts the agent workspace
   or receives its provider credentials unless an explicit material-transfer
   policy names the exact paths and direction.
4. A workspace lower layer is immutable. Writes go only to the lease's upper
   layer. A new agent generation never reuses an unsealed upper layer.
5. An input view contains exactly the entries in its canonical manifest. The
   agent cannot enumerate the node cache, another view, or excluded artifact
   metadata. Shared physical extents do not imply shared logical visibility.
6. Output capture accepts logical relative paths, not host destinations. It
   opens paths beneath the workspace without following symlinks and copies bytes
   through a host-owned artifact adapter.
7. Provider stdout is never mixed with management records. Doing so would
   corrupt the provider's machine-readable protocol.
8. A crash, OOM, disconnect, forced eviction, restore, cold boot, or observer
   gap is externally visible and durably recorded.
9. Agent workspace and target generations advance independently. Target reset,
   restore, or recreate never rolls the agent workspace generation. Failed
   generations and runs remain addressable in the ledger and incident record.
10. Collectors are controlled outside the target's compromise domain wherever
    the required signal permits. Guest or app injection is explicit, measured,
    and never the sole source for host-enforceable lifecycle truth.
11. Control and incident events are loss-intolerant. Bulk collectors may be
   sampled or dropped only if a typed gap event states the source and range.
12. File and syscall metadata may be baseline observation, but payload capture
    requires explicit process/path filters and byte, duration, and sensitivity
    limits. Missing payloads are never described as missing operations.
13. The effective policy and capability fingerprint are frozen for each agent
    workspace and target generation. A runtime, image, emulator, snapshot,
    tool, or collector change requires the corresponding new generation.
14. Host pressure decisions are policy decisions with inputs and rationale, not
    invisible scheduler behavior.
15. Target command content is observed and attributed, not semantically
    allowlisted. Installing Frida, replacing packages, starting services,
    changing guest configuration, or rebooting an Android target is permitted
    when the assigned target can do it; none of those actions grants host,
    Docker, raw ADB-server, another-target, or agent-workspace authority.
16. Required observers remain outside the target compromise domain where
    possible. If an agent action disables an in-guest observer, the run records
    the action and a coverage gap rather than blocking the command or claiming
    complete visibility.

## 6. Component architecture

### 6.1 Current executable topology

```text
trusted operator / client
  | world.v1 gRPC (bearer or mTLS)
  v
worldd OR world-node
  |-- logical lifecycle core + SQLite control journal
  |-- durable observation ledger/live projections
  |-- local bundle finalizer/material publication
  `-- optional version-3 deployment-profile composition
        |-- strict policy compile + complete capability fingerprint
        |-- directory workspace + Docker agent + world-guest exec
        |-- Docker Linux target + scoped exec/file transports
        |-- managed Android SDK Emulator + scoped ADB/file transports
        |-- optional process observers
        `-- optional ledger capture
```

Both binaries use this same composition. They are independent services; there
is no RPC, registration, or scheduling relationship between them. The default
driver selection is logical-only. Physical mode requires the Docker agent and
directory workspace together, an absolute immutable deployment profile, exact
locally present image/system-image digests, and non-overlapping managed roots.
Docker Linux targets, managed Android SDK Emulator targets, process observers,
and ledger capture are explicit selections constrained by that profile.
Daemon-selected Cuttlefish and physical-device selections are not shipped.

### 6.2 Target controller/node split

The following is the intended dedicated-host/fleet topology, not a claim about
the currently shipped relationship between the two binaries:

```text
trusted host application
  | acquire / release / incidents / observations / exports
  v
worldd (unprivileged control plane)
  |-- scheduler and admission controller
  |-- session/workspace/target/run state machines
  |-- durable state database and causal ledger
  |-- live subscription fan-out
  |-- policy compiler and capability negotiation
  `-- artifact and ecosystem adapters
             |
             | authenticated local node protocol
             v
world-node (trusted node authority)
  |-- Docker Engine adapter
  |-- cgroup and pressure controller
  |-- scoped immutable content and input-view cache
  |-- OverlayFS/openat2 workspace helper
  |-- target drivers and scoped target gateways
  |-- observer supervisor and observation-bundle builder
  `-- network namespace and traffic-capture controller
             |
             +-- agent workspace: world-guest -> provider CLI -> research tools
             +-- Linux target: disposable OCI container -> investigated program
             `-- Android target: Cuttlefish/emulator -> investigated APK

go-agent-runner -> lease-bound ExecutionEnvironment -> worldd -> world-guest
agent tools -> world-target / scoped ADB / optional MCP -> target gateways
agent tools -> world-observe -> worldd -> observer projections and bundles
```

### 6.3 Target `worldd` responsibility

`worldd` owns logical truth: research sessions, leases, agent workspace and
target generations, target runs, effective policies, idempotency, transitions,
incidents, export intents, observation bundles, cursors, and admission
decisions. It has no host command-execution API and does not accept arbitrary
host paths from clients.

### 6.4 Target `world-node` responsibility

In the target split, `world-node` is the only component that can access Docker Engine, create agent
or target containers, mount workspaces, place processes in cgroups, allocate
virtual-device ports, talk to raw ADB, and attach privileged observers. Its
local API accepts resolved workspace, target, and collector plans plus
target-scoped direct-exec and ADB-service envelopes. Those envelopes may contain
arbitrary guest commands; they can never select a host executable, Docker
operation, host ADB service, mount, namespace, or unassigned target. The node
independently validates image allowlists, mount roots, target assignments,
cgroup roots, observer permissions, and lease ownership.

Access to the Docker socket is root-equivalent and must never be delegated to
the agent workspace or a target. The v1 Linux target uses a hardened standard
OCI runtime because host eBPF, namespace, cgroup, and filesystem observers can
attribute its behavior directly. The shipped policy, plan compiler, and Linux
target driver accept only `runc`; gVisor and Kata are not selectable profiles.
They remain future research candidates whose different observation boundary
would require a new explicit policy and capability contract before shipping.

### 6.5 `world-guest`

`world-guest` is a small, versioned framed exec supervisor running inside the
agent workspace. The current Docker plan uses Docker's `--init`, so host
enforcement does not rely on `world-guest` being container PID 1. It:

- starts the requested pinned provider executable directly with an argument
  vector and explicit working directory;
- creates and kills a process group for each exec;
- forwards raw stdin, stdout, stderr, resize, signal, and exit status;
- reports fork/exec/exit identity needed for causal attribution;
- exposes narrow capabilities used by the read-only observation helper, bounded
  target transport, named capture-request helper, and append-only export-
  declaration helper;
- never has host credentials or artifact-store access; and
- exits if its lease identity or host heartbeat becomes invalid.

The guest protocol is framed and versioned, but the provider byte streams inside
the frames remain opaque.

### 6.4 Runner execution environment

The host application supplies a world-owned implementation of
`agentrunner.ExecutionEnvironment` on each runner request. A nil environment in
`go-agent-runner` still means direct host execution; this integration always
sets the field and never relies on that default. The adapter is bound to one
lease and `AgentGeneration`, resolves the workspace-visible working directory
and provider executable, and transports every capability probe and attempt to
`world-guest`.

The runner sends direct argv, stdin bytes, limits, cleanup grace, and temporary
input bytes through the interface. The adapter materializes temporary prompt or
schema files inside the agent workspace, replaces their designated argv slots,
and confirms removal. It does not parse provider flags or protocols. Provider
stdout and stderr remain separate, ordered, byte-transparent streams with
callback backpressure.

The environment ID includes the lease, agent generation, workspace image/tool
fingerprint, and environment-protocol version. It changes whenever cached CLI
capabilities may no longer be reused. A management failure closes streams,
cancels the remote process group, waits for bounded cleanup, and returns an
execution error linked to an out-of-band world incident. If cleanup cannot be
confirmed, `world-node` destroys the agent container, which is the provider's
final kill domain.

### 6.5 Agent workspace

The agent workspace is a long-lived research workstation within one lease. It
contains the provider CLI, approved research tools, a policy-projected immutable
input view, and a private writable upper layer. It can outlive many disposable
target generations and target runs. It does not contain the Docker daemon,
target runtime authority, raw ADB server, privileged collectors, or artifact
credentials.

An agent can request only target templates frozen into the effective policy.
Within an assigned target, however, command authority is deliberately broad:
the agent may open a shell, run arbitrary argv, push or pull files, install
instrumentation, start services, change guest state, and—on Android—use normal
device-scoped ADB operations. `world-target`, the scoped ADB endpoint, and the
optional MCP facade share lease/target/run authorization. They do not accept
host paths, runtime flags, arbitrary mounts, collector-control commands, raw
ADB-server operations, or another lease or target identity.

### 6.6 Target sandboxes and observation bundles

A target sandbox is an add-on resource owned by a `TargetDriver`. The v1 target
architecture provides shipped Docker Linux-container and managed Android SDK
Emulator drivers. Each `TargetInstance` has an independently versioned runtime
realization, while each `TargetRun` is one bounded execution and observation
window within that realization. Both physical drivers grant mutable authority
to only one run per generation; finalization proves the runtime stopped, and
another run requires a replacement generation.

Before a run, `world-node` stages the declared specimen and fixtures, starts
required collectors, verifies their coverage, and opens the scoped target
transports. During the run the agent may make arbitrary in-target changes and
push additional workspace-produced bytes subject only to transport/resource
bounds and target scope, not command semantics. Every intervention is assigned
an operation ID and records command metadata, transferred-byte hashes, process
identity where available, and resulting observation links.

At finalization the node stops remaining workloads, drains collectors, seals
target filesystem changes and raw captures, records gaps, and builds an
immutable `ObservationBundle`. The bundle distinguishes initial material,
agent-supplied tooling/interventions, and observed specimen behavior. It
contains raw artifact references, normalized events, coverage, an authoritative
change manifest, and a derived summary. Raw facts remain available when
summaries are regenerated or challenged.

## 7. Domain model

### 7.1 Primary identities

- `ResearchSessionID`: logical investigation containing one persistent agent
  workspace and its target history.
- `LeaseID`: exclusive authorization to use one research session.
- `AgentWorkspaceID`: logical provider/tool workspace across realizations.
- `AgentGeneration`: monotonically increasing agent workspace realization.
- `ExecID`: one provider or tool execution inside an agent generation.
- `TargetID`: logical Linux or Android target across realizations.
- `TargetGeneration`: monotonically increasing target realization, independent
  from `AgentGeneration`.
- `TargetRunID`: one bounded workload execution and observation window.
- `TargetOperationID`: one target-scoped exec, shell, file transfer, ADB
  service, or other agent intervention within a run.
- `InputViewID`: domain-separated digest of the canonical selection, layout,
  metadata, and visibility manifest.
- `WorkspaceID`: immutable lower plus one upper/work/merged set.
- `ObservationCursor`: durable position in the observation ledger.
- `IncidentID`: immutable failure or hazardous-state record.
- `CaptureID`: packet, trace, log, profile, or filesystem capture.
- `ObservationBundleID`: sealed raw, normalized, coverage, change, and summary
  outputs for one target run.
- `ExportID`: explicit output-capture transaction.

Resource and operation IDs are opaque UUIDv7 values with human-readable type
prefixes. `InputViewID` is content-addressed so an exact view is reusable.
Docker IDs, PIDs, ADB serials, emulator ports, cgroup paths, and host paths are
attributes, never public identity.

### 7.2 Session, workspace, and target state machines

```text
research session: requested -> admitted -> leased -> releasing -> released

lease: active -> releasing -> released
          `-----------------> expired

agent workspace generation:
provisioning -> booting -> ready -> running -> quiescing -> sealed
                                  |               |
                                  `----failed-----'

target generation:
provisioning -> instrumenting -> ready -> resettable -> destroyed
                    |              |          |
                    `----failed----+----------'

target run:
requested -> preparing -> observing -> running -> finalizing -> completed
                              |          |            |
                              `----------+----------> failed
```

Any non-terminal resource may enter quarantined or lost after a recorded
incident. Recreate or restore advances only that resource's generation. A
failed target run is finalized and remains addressable; a clean reset creates a
new target generation while the agent workspace continues unchanged.

The implementation uses one shared transition guard over separate declarative
tables. Drivers report observations; they do not update state directly.
Replaying accepted transition records must reconstruct the same current state.

Release and expiry use a separate durable termination record so admission
closes before cleanup begins. A requested release persists `releasing`; a due
active lease persists `expiring` while its public lease state remains active.
The trusted drain stops captures, resumes or completes reserved exports,
force-stops and finalizes nonterminal target runs, destroys physical target and
agent ownership, marks remaining execs/target operations terminal, and only
then commits `released` or `expired`. Startup and a bounded periodic ticker
resume the same child idempotency identities after interruption.

### 7.3 Shipped daemon ownership and startup crash reconciliation

Each daemon first takes a nonblocking, process-wide lock on the sibling
`<canonical-control-path>.worldd.lock`. Acquisition resolves the absolute
control path and parent-directory aliases, requires regular single-link
control/lock files whose opened handles still match their paths, and rejects
symlink/reparse, hard-link, and special-file aliases. Linux, Darwin, BSD, and
Solaris lock the canonical parent directory before the sibling file to
stabilize the pathname namespace, then release the file before the directory;
same-directory over-exclusion is intentional. Windows uses `LockFileEx` and
holds the lock file without delete sharing. AIX fails closed as unsupported
because its available sibling-file lock cannot provide replacement-resistant
namespace ownership. The lock precedes credentials, SQLite, the ledger, driver
construction, reconciliation, and listener creation, and is released last
after RPC shutdown and local cleanup. It coordinates conforming daemons; it is
not a defense against arbitrary same-user mutation.

The current daemon then performs physical/run reconciliation before lease
cleanup and before constructing its RPC server or listener. It reconstructs
immutable plans from durable state, re-applies policy admission, inventories
exact physical identities, and adopts only a resource whose labels and
inspected configuration match that plan. Version-6 observer markers bind their
filename and run ID to the persisted run-plan digest and full observer start
signature. They also persist every exact external `CollectorPlan`, a
`start_committed` flag written only after `ObserverDriver.Start` succeeds, and
the intrinsic collector ID/start time when `target.lifecycle` is required.
The marker's stopped/committed phases additionally carry the digest of the
complete persisted target-run result, the digest of the canonical version-2
stop preparation permitted to consume that result, and a bounded evidence-
journal reference. Both stop digests must be present together and must match
the preparation file and its hash-chain anchor; a stopped marker cannot be
reattached to different evidence after a crash.
The complete plan carries the child idempotency key, collector/session/lease,
workspace/agent generation, target/generation/run, runtime attachment,
requirement, adapter/version/configuration digest, resource and byte limits,
and original start time. Recovery validates these bindings against the
authority-selected plan; it never mints a replacement collector identity or
start time. Foreign, malformed, ambiguous, missing-when-required, or mismatched
evidence fails startup.

A nonterminal bound run is an interruption, not a resumable execution. The
Linux reconciler force-stops surviving execution and proves it stopped; it may
restart only an inert target and reconstruct the run as prepared for the normal
failure-finalization path. It never calls `StartRun`, restarts a specimen or
collector, or replaces the original maximum-duration timer. The built-in
Windows starter atomically places each collector in its own anonymous
kill-on-close Job before any child instruction executes and retains the sole
Job handle in the daemon; normal teardown also requires the Job's active-process
count to reach zero. The built-in Linux parent-death `SIGKILL` contract proves
death only for the directly spawned collector process; Go's fork/exec path also
closes the parent-exit race. Adapters that daemonize on Linux or leave surviving
helpers are unsupported unless an external cgroup/process-tree supervisor
supplies an equivalent authoritative proof. Unsupported platforms or custom
cleanup contracts fail closed.

Only after that death proof may the process observer reconcile durable output.
For every exact persisted collector binding it requires one bounded,
non-symlink transaction directory and validates terminal manifests, stream
roles, artifact references, digests, sizes, the shared byte limit, and object
reachability. A valid finalized transaction retains its immutable artifacts.
For a start-committed transaction with both exact regular partial streams,
recovery fsyncs and bounds the retained bytes, then uses the normal publisher to
create canonical immutable stdout/stderr objects and a finalized manifest.
Continuity is still recorded as lost. An uncommitted transaction is closed with
an exact-plan `aborted` state and its partial files are removed. A missing
transaction is tolerated only when `start_committed` is false; a present valid
finalized transaction remains authoritative even when that flag is false
because output publication may have won the marker-update race. Foreign collectors/files/objects, conflicting or
mismatched control files, missing referenced objects, and unsafe path/type
changes fail startup. Complete unreferenced objects are safely removed; a
truncated pending object is removed only when it is a verified prefix tied to
an exact interrupted partial. Complete pending objects referenced by a valid
finalized manifest are promoted. A final scan requires precisely the reachable
digest set.

Successful recovery records planned signals as lost, adds explicit
control-plane-loss gaps, seals a failed observation bundle, links a
`control_plane_failure` incident, commits the final observer marker and
`bundle.completed` read gate, and marks nonterminal target operations `lost`.
Retrying leaves this terminal run and bundle unchanged. Any incomplete
identity, stop, collector-cleanup, or finalization proof aborts startup before
RPC and before lease cleanup.

### 7.4 Capability fingerprint

Before admission, each driver reports tri-state capabilities (`supported`,
`unsupported`, `unknown`) plus constraints and immutable version evidence:

- Docker Engine/API/runtime/storage/cgroup versions;
- kernel, cgroup v2, OverlayFS, user namespace, KVM, eBPF, fanotify, and
  `openat2` support;
- emulator binary, system image digest, AVD configuration digest, snapshot
  fingerprint, acceleration, and Perfetto availability;
- observer versions and required privileges; and
- artifact-adapter instance and protocol versions.

A future physical-device driver adds build fingerprint, API level,
root/debuggable state, battery, authorization, and recovery capabilities through
the same fingerprint contract.

Required unknown capabilities fail admission. Preferred features may downgrade
only through an explicit effective-policy record and client-visible warning.

## 8. Public control contract

The canonical API is versioned protobuf over mutually authenticated gRPC. A
Unix-domain socket is the first deployment. A small Go client and `worldctl`
use the same contract.

### 8.1 Lifecycle operations

- `AcquireResearchSession(request) -> lease`
- `GetResearchSession(research_session) -> view`
- `WaitResearchSession(lease, desired_state) -> view`
- `RenewLease(lease, expected_revision, ttl) -> lease`
- `ReleaseResearchSession(lease, reason) -> outcome`
- `CreateTarget(lease, template) -> target`
- `StartTargetRun(target, run_spec) -> target_run`
- `OpenTargetExec(target_run, command) -> exec_transport`
- `PushTargetFile(target_run, target_path) -> transfer`
- `PullTargetFile(target_run, target_path) -> transfer`
- `OpenTargetADB(target_run) -> scoped_endpoint`
- `WaitTargetRun(target_run, desired_state) -> target_run`
- `StopTargetRun(target_run, reason) -> observation_bundle`
- `ResetTarget(target, reset_mode, snapshot_name?) -> new_target_generation`, where
  `reset_mode` is exactly `baseline`, `recreate`, or `snapshot`; `snapshot_name`
  is required only for `snapshot`
- `DestroyTarget(target, reason) -> outcome`
- `RequestRecovery(incident, mode) -> recovered_resource`
- `QuarantineTarget(target, reason) -> target`

Every mutation requires an idempotency key, expected revision where applicable,
correlation ID, causation ID when explicit, authorized policy reference, and
deadline. An idempotency key is canonical only when it is non-empty, valid
UTF-8, unchanged by trimming, and at most 1024 bytes. Retrying the same key and
payload returns the original result; reusing a key with different input
conflicts. Internal saga steps derive child identities through one
domain-separated, length-framed function. It keeps an unambiguous
`parent/suffix` readable and otherwise appends a SHA-256 commitment while
remaining within 1024 bytes, so nested or maximum-length keys cannot poison
replay or collapse distinct component boundaries.

The acquire request contains an agent-workspace `InputViewSpec`, not a host
path. It identifies a frozen selection or explicit immutable occurrences,
layout/path mapping, allowed sidecars, cache security scope, and whether
zero-copy view construction is required. A target-run request separately names
specimen occurrence refs and policy-defined fixtures. The returned lease freezes
the resolved agent `InputViewID`; each target run freezes its own materialization
manifest so target access never implies access to the whole agent workspace.

### 8.2 Execution transport

`OpenExec` is a bidirectional framed stream into the agent workspace. The start
frame carries a provider executable, the arguments after `argv[0]`, a working
directory, bounded temporary-input declarations, terminal settings, and exec
idempotency key. The executable supplies `argv[0]`; temporary-input indices are
zero-based over the separate argument vector.
Subsequent frames carry raw stream bytes, signals, resize events, heartbeats,
and one terminal outcome. It is the transport behind the runner execution
environment, not a provider-aware protocol. The control plane maintains the
guest heartbeat for the full physical execution lifetime, including after the
client half-closes its input stream; client heartbeat frames are supplementary,
not the sole source of guest liveness.

`StartTargetRun` creates the bounded observation window and initial material
plan. It does not freeze the commands that may run during that window. Target
interaction uses three related scoped data-plane interfaces:

- `OpenTargetExec` carries arbitrary direct argv or explicit shell bytes,
  stdin/stdout/stderr, signals, terminal resize, and exit status for a Linux
  target or supported Android-side helper;
- `PushTargetFile` and `PullTargetFile` stream bytes between the current agent
  workspace and target-relative paths while hashing and attributing the
  transfer; and
- `OpenTargetADB` exposes a lease-scoped endpoint compatible with ordinary ADB
  clients and exactly one assigned Android serial.

There is no per-command semantic allowlist or approval loop. The gateway may
forward arbitrary device-scoped ADB services—including shell, sync, install,
uninstall, forward/reverse, logcat, root/remount where the image permits,
reboot, and commands that install or start Frida. It rejects host ADB-server
control, transport selection outside the assigned serial, access to another
target, and requests outside the active lease/run. Resource, duration, stream,
network, and storage limits still apply.

The optional MCP server is a discoverable control and query facade for target
lifecycle, operation creation, observation queries, and bundle retrieval. It
may expose a convenience exec call, but it is not the only command path and is
not placed in the high-volume or interactive byte stream. `world-target` and
normal ADB clients use the same authorization and operation records directly.

The API never offers `host_shell`, arbitrary host executable paths, Docker API
passthrough, arbitrary mount creation, raw host ADB access, or target selection
outside the lease.

### 8.3 Observation and performance

- `GetLiveSnapshot(lease, filter)` returns the current subject topology, latest
  health and metric values, staleness, and collector coverage at one cursor.
- `SubscribeObservations(filter, after_cursor)` returns ordered ledger records.
- `SubscribeMetrics(filter, resolution, after_cursor)` returns live metric
  samples and pressure transitions.
- `StartCapture(lease, capture_spec)` activates only capabilities allowed by the
  effective policy.
- `RequestCapture(lease, named_profile)` records an agent or host request for a
  pre-authorized bounded profile; it accepts no arbitrary command or privilege.
- `StopCapture(capture)` finalizes and returns capture metadata.
- `GetObservationBundle(target_run)` returns coverage, gaps, normalized
  summaries, change manifests, and raw artifact references for a sealed run.
- `GetIncident(incident)` returns facts, correlations, recovery, and evidence
  references.

Subscriptions are resumable. A slow subscriber never blocks collectors or an
agent process. It receives a typed gap/compaction record and resumes from a
durable cursor or an artifact-backed segment.

Inside the agent workspace, `world-observe snapshot|watch|top|bundle` can read
only the current lease's authorized event, metric, and sealed-bundle views
through a short-lived guest capability. It cannot change collectors, policy,
resources, or other leases. This gives an agent useful live feedback without
making the observation channel part of ordinary tool execution.

If policy permits agent-initiated diagnostics, `world-capture request <profile>`
submits one named request through a separate append-only capability. The control
plane validates bounds and records acceptance or denial; the helper cannot pass
collector arguments, address another target, or stop a required collector.

### 8.4 Output capture

- `DeclareExport(lease, relative_paths, roles)` records an export intent.
- `PreviewChangeSet(lease)` returns added, modified, deleted, renamed, and
  metadata-only changes.
- `CommitExport(export, expected_workspace_revision)` seals selected files,
  copies them to the artifact adapter, and returns qualified occurrence refs.

The agent may declare paths through a narrow `world-export` guest helper, or the
host may translate the agent's structured result into the same API. Declaration
does not itself copy or trust a file.

## 9. Agent workspace, target materialization, and artifact flow

### 9.1 Input preparation

1. The trusted host identifies immutable artifact occurrences or a frozen
   selection plus an explicit logical layout.
2. The `MaterialAuthority` adapter resolves it to a canonical `InputViewManifest`
   containing only selected logical paths, qualified occurrence refs, hashes,
   sizes, modes, and permitted sidecars. Path conflicts fail; there is no
   last-writer-wins projection behavior.
3. `world-node` checks its scoped local content cache. Missing content is
   streamed through the adapter's public object-reader boundary, written to a
   private staging file, hashed, atomically published by digest, and never
   opened through an artifact-store filesystem path.
4. The node constructs or reuses a read-only input-view tree keyed by the
   canonical manifest digest. View files use extent-sharing clones on a
   capability-probed reflink filesystem so each path has independent inode and
   metadata semantics without initially duplicating data blocks.
5. A dedicated OverlayFS is mounted with that view as `lowerdir` and new agent-
   generation-owned `upperdir` and `workdir` directories on the same filesystem.
6. Only the merged workspace is bind-mounted into the agent container. The cache,
   view parent, artifact repository, lower-layer parent, upper-layer parent,
   and mount-control paths are not visible to the agent or any target.

Docker's own storage-driver directories are not a supported application API and
are never inspected for workspace changes. The manager owns a separate
OverlayFS mount whose layout and lifecycle it controls.

### 9.2 Shared content and input-view cache

The cache is an expendable performance layer, never a second artifact
authority. Its initial v1 shape is:

```text
<node-root>/cache/<security-scope>/
  content/sha256/<prefix>/<digest>       verified unique bytes
  views/<input-view-digest>/
    input-view-manifest.json             selected entries only
    root/                                reflinked logical tree
```

The default security scope is a campaign or explicitly configured trust domain,
not the whole node. Node-wide deduplication is opt-in because cache-hit timing
and shared-resource behavior can reveal that another tenant has equal content.
The cache path and content digests are never visible inside the container.

One unique content digest is fetched once per cache scope. Concurrent misses
join one build lease; staged content is atomically published only after hash and
size verification. Different input views contain inexpensive directory entries
and reflinked extents with their own mode, ownership, timestamps, and inode.
This avoids the false aliasing that a hardlink farm would create when equal
bytes occur at multiple logical paths or need different metadata. Exact
matching views reuse the same read-only tree across many OverlayFS mounts;
Linux explicitly permits shared lower layers.

Each active or recoverable agent generation and target materialization pins the
content it uses. Releasing an agent generation removes its upper/work tree after
output and incident finalization, then drops the view pin. Finalizing a target
run seals its changes and captures before its generation may be reset or
destroyed. Unpinned views are retained by TTL/LRU and removed under a high-water
policy. Content entries are removed only after no view, target, or in-flight
builder references them. Reference counts are transactionally recorded but are
reconciled after a crash from leases, mounts, manifests, and the filesystem;
correctness never depends on timely garbage collection.

Cache entries are root/node-owned, non-writable, never mounted directly, and
verified when populated. Phase 0 must determine whether fs-verity, immutable
flags, periodic rehash, or a combination provides the best integrity/clone
compatibility on the chosen backing filesystem. A cache hit with uncertain
integrity is reverified or discarded, never trusted because its filename looks
like a digest.

Reflink support is a required capability when policy says
`construction: require-reflink`; failure must not silently fall back to a full
copy. A separately named `allow-copy` policy supports less capable development
nodes and reports actual physical bytes allocated. A future manifest-backed
read-only filesystem such as EROFS/composefs may replace view trees at very high
view cardinality without changing `InputViewManifest` or lease semantics.

This removes repeated lower-layer byte copies, but it does not make mutations
free. OverlayFS copies a lower file up when it is opened for data modification;
that per-generation changed data and genuinely new outputs consume upper-layer
quota until export/incident finalization and cleanup.

### 9.3 Node layout

```text
<node-root>/leases/<lease-id>/
  agent/<agent-generation>/
    lower -> pinned cached input view
    upper/          agent changes only
    work/           OverlayFS work directory
    merged/         sole workspace mount exposed to the agent container
  targets/<target-id>/<target-generation>/
    material/       exact specimen and fixture projection
    writable/       target-private mutable state where applicable
  runs/<target-run-id>/
    captures/       host-owned raw capture staging
    normalized/     normalized event segment staging
    bundle/         sealed manifest and derived-summary staging
  ledger/           host-owned session observation segment staging
```

Mount propagation is private. The agent merged mount is `nodev` and `nosuid`;
the agent container root filesystem is read-only except for explicit tmpfs
locations and the workspace. Targets receive only their material projection and
target-private writable state. No target mount aliases the agent upper or merged
tree. Writable state is never reused across generations.

### 9.4 Target materialization

A `TargetRunSpec` references immutable occurrences already authorized to the
session. `world-node` builds an exact, target-specific materialization manifest
and transfers only those files through driver-defined paths. Linux targets
receive regular files in a private mount or image layer. Android targets receive
the APK and named fixtures through the scoped ADB gateway. This initial manifest
is the reproducible baseline, not a command allowlist.

During an active run the agent may push arbitrary files it can already read in
its own workspace—such as a newly downloaded debugger, Frida binary, script, or
rebuilt APK—to target-relative paths. The transfer service opens the workspace
source beneath the lease mount, streams bounded bytes without exposing a host
path, hashes them, and records them in an `AgentInterventionManifest`. Pulls use
the inverse scoped stream and become evidence. A target never reads the agent
workspace directly, and pushed bytes do not become immutable forensic inputs
unless the artifact authority captures them.

Target-generated bytes are evidence, not agent workspace output. Policy selects
automatic target captures such as filesystem changes, stdout/stderr, Android
app data permitted by the image, tombstones, traces, and packet rings. These are
sealed into the observation bundle even if the target crashes before the agent
can declare an export.

### 9.5 Authoritative change sets

OverlayFS deletions and opaque directories use whiteout semantics, so a naive
directory diff is insufficient. Finalization combines:

- the immutable lower manifest;
- an overlay-aware upper scan, including whiteouts and opaque directories;
- the stable merged-tree view;
- file-event observations; and
- hashes and metadata obtained from already-open file descriptors.

Incremental events make the diff fast and explain timing; the final sealed scan
is authoritative. Agent workspace and target change sets remain distinct.
Disagreement becomes an incident rather than being silently resolved.

### 9.6 Safe export

Export paths must be normalized relative paths. The node opens each component
under a pre-opened workspace directory using `openat2`-style beneath/no-magic-
link/no-symlink constraints, rejects non-regular files unless a future typed
export supports them, checks file/byte quotas, hashes the open descriptor, and
copies from that descriptor. It never resolves a user path and later reopens it.

Only explicit paths and policy-mandated crash/trace outputs are committed. The
complete change set is still retained as metadata so an analyst can see what
was not exported. Managed lower bytes are reverified after execution. Artifact
capture records the research session, agent generation, exec, target generation
and run where applicable, image digest, policy digest, input refs, output roles,
and observation-bundle refs as provenance.

## 10. Agent workspace and Linux target lifecycle

The agent workspace profile uses:

- a pinned image digest and signed/approved image policy;
- no privileged mode, no Docker socket, no host namespaces, no host devices,
  and no arbitrary bind mounts;
- a non-root user with user-namespace mapping or rootless Docker where
  compatible;
- all Linux capabilities dropped, with narrow additions only in a distinct
  policy profile;
- `no-new-privileges`, a versioned seccomp profile, and AppArmor or SELinux;
- a read-only root filesystem, bounded tmpfs, pids limit, cgroup v2 CPU/memory/
  I/O limits, and grouped OOM behavior;
- a per-lease network namespace and explicit egress profile; and
- one agent container per active lease so container destruction is a valid
  final kill domain for the provider and research tools.

Instrumentation privileges never go into the agent container. A host observer
or separate observer namespace attaches by cgroup, PID namespace, or network
namespace. An invasive profile that needs `ptrace` is separately named,
capability-probed, and recorded.

Container readiness requires Docker start, `world-guest` protocol handshake,
workspace verification, observer readiness, and provider executable probe. A
health check alone is not readiness authority.

The v1 Linux target driver creates a separate hardened OCI container for the
investigated program. It uses the standard `runc` path by default because the
host can then observe its process tree, syscalls, file operations, cgroup, and
network namespace with mature open-source collectors. The target has no Docker
socket, agent workspace mount, provider credential, host namespace, or route to
the control plane. Agent access is structurally limited to the assigned target
and policy-declared target network while `world-target` permits arbitrary
commands and transfers inside that boundary.

The default Linux target observation profile is visibility-first:

- Tracee or Inspektor Gadget, selected by the phase-0 contract spike, provides
  container-scoped eBPF process, syscall, file, and network metadata;
- a host-owned packet ring captures target-namespace traffic, with Zeek-style
  protocol summaries produced after or during the run;
- OverlayFS or a driver-owned writable layer provides the authoritative final
  target change manifest; and
- `strace`, payload capture, mitmproxy, perf, and debugger attachment are
  bounded named profiles, not invisible defaults.

The `world-target exec|shell|push|pull` client gives the agent arbitrary control
inside the assigned Linux target. It may install packages or tooling, replace
the specimen, start several processes, and alter target state. The manager
records these as interventions and enforces the target's cgroup, filesystem,
network, time, and capture budgets; it does not interpret command text or turn
guest root into host authority.

gVisor and Kata are not admitted by the current policy or Linux target driver.
Future research may define a separate in-guest collector contract for such
boundaries; v1 does not claim equivalent host-eBPF syscall visibility or expose
a selectable compatibility path.

## 11. Android target management

### 11.1 Virtual-device driver

The v1 Android target driver supports a hardware-accelerated, instrumented AOSP
virtual device through the Android SDK Emulator. Cuttlefish can be added behind
the same driver contract after separately satisfying the lifecycle and
qualification gates. Each target generation uses isolated writable device
state, fixed console/ADB endpoints, a recorded system-image/runtime fingerprint,
and a dedicated scoped-ADB path. Managed lifecycle execution is currently
Windows-only: the emulator process tree is atomically placed in one
deterministically named Job whose CPU and committed-memory limits, membership,
and restart reopen are verified. Other hosts fail resource-containment
preflight. Readiness requires:

- the virtual-device process is alive;
- its exact host process identity remains in the configured named Job;
- ADB reports the expected serial as `device`;
- boot completion and package manager readiness checks pass;
- Android build fingerprint matches the plan; and
- the guest `/data` block device has the exact planned capacity; and
- all required collectors have started and meet required coverage; optional
  collectors have started or recorded an explicit downgrade.

Host Job memory is distinct from emulator guest RAM: the former caps committed
memory for the whole process tree, while the latter is the exact `-memory`
topology value. Writable-state bytes bind the guest `/data` block capacity,
not host AVD metadata or diagnostic logs. Managed AVDs disable cache, SD-card,
snapshot load/save, and writable-system state. The shipped reset modes create a
separately allocated clean-boot AVD; no snapshot restore is claimed. Before a
reset, the target run and generation are sealed and crash evidence is captured.
Reset always increments `TargetGeneration` without changing `AgentGeneration`.

The visibility-first Android image is rooted/debuggable, contains pinned guest
collector components, and is treated as instrumentation rather than a consumer-
device fidelity profile. Baseline observation includes host-process/cgroup
metrics, runtime stderr, ADB state, permitted logcat buffers, boot properties,
package/process/activity lifecycle, a bounded Perfetto ring, screenshots at
important transitions, and network flow metadata. Frida hooks collect selected
Java/native app behavior, including intent API calls, file APIs, sockets, and
crypto use, while target-namespace packet capture observes traffic independently.

Coverage is stated precisely. App-process hooks such as `startActivity`,
`sendBroadcast`, or `startService` show API calls observed in attached
processes; they are not called complete framework-level intent coverage. A
custom AOSP/Cuttlefish image instrumenting ActivityManager, broadcast dispatch,
and Binder may advertise `android.intent.framework` coverage. The effective
policy and observation bundle distinguish these capability levels.

Perfetto, logcat, dumpsys snapshots, tombstones, ANR traces, bugreports, packet
rings, Frida output, and MobSF-produced static or dynamic results are retained
in their native formats and normalized through adapters. MobSF may be reused as
an external analysis engine or implementation reference, but the world manager
retains target lifecycle, evidence identity, and coverage truth.

The agent connects a normal ADB client to a lease-scoped gateway advertising
only this target. Within that device it has the full authority exposed by the
rooted/debuggable image: arbitrary `adb shell`, sync/push/pull, APK install and
uninstall, port forward/reverse, root/remount, package-manager and activity-
manager commands, reboot, and installation or replacement of Frida server and
other research tools. The gateway filters device selection and host-global ADB
services, not device command semantics. Each request, transfer hash, resulting
process where observable, disconnect, and reboot is linked to the initiating
agent operation and target run.

Host-owned packet, runtime, process-controller, filesystem, and gateway observers remain
outside the Android guest. Agent-installed instrumentation is labeled as an
intervention. If a command disrupts logcat, Perfetto, Frida hooks, or another
in-guest collector, the command proceeds and the observation bundle records the
resulting coverage transition or gap.

### 11.2 Future physical-device driver

Physical phones are outside v1. A future driver must exclusively reserve a
device before an agent can address it. The agent never sees raw USB. It reuses
the same scoped ADB contract: expose only the assigned serial, hide all other
devices, forward arbitrary device-scoped services supported by that build,
reject host-level ADB services, record service requests, and close when the
lease ends. Network policy prevents bypassing the gateway to another host ADB
server.

Capabilities are measured per device and build. Production builds may not
permit root, complete log buffers, tombstones, system tracing, Frida server, or
reliable app-data cleanup. Unsupported access stays unsupported.

A physical phone has no snapshot guarantee. Recovery actions are explicit and
capability-specific: reconnect ADB, restart the scoped gateway, reboot the
device, force-stop/clear/reinstall an app, apply an approved fixture, or
quarantine for human reconditioning. After a serious crash, disconnect,
unexpected build change, low battery, thermal event, or failed cleanup, the
device is quarantined rather than returned to the clean pool.

## 12. Live observation and performance architecture

### 12.1 Two synchronized streams

Every lease exposes session streams, and every target run defines a bounded
observation window within them:

1. an activity stream: lifecycle, input-view/cache, process, file, network,
   device, policy, incident, capture, and export events; and
2. a performance stream: host, aggregate lease, agent workspace, Linux target,
   virtual-device host, Android guest, and selected process metrics.

Both streams carry the same session, agent generation, target generation,
target run, lease, exec, subject, and clock-domain attributes. Metric samples
can link to active spans, execs, or target runs and can be queried around an
incident without pretending the sample caused it.

### 12.2 Subject attribution and baseline matrix

Resource attribution follows a stable subject tree. Agent workspaces, Linux
targets, and Linux observers use separate cgroup leaves. A managed Android
runtime on Windows uses its own named Job and is joined into the same logical
lease aggregate by durable plan identity rather than by pretending that a
Windows Job is a Linux cgroup:

```text
host
`-- lease aggregate cgroup
    |-- agent-workspace cgroup
    |   `-- provider/tool process groups
    |-- Linux-target cgroup(s)
    |   `-- investigated process trees
    |-- Android-runtime controller(s) (Windows named Jobs)
    `-- observer cgroup

Android guest/app subjects map to the emulator target and a separate Android
clock domain; they are not falsely represented as Linux cgroup descendants.
```

Every supported subject has a low-overhead, metadata-first baseline:

| Subject | Live activity | Live performance |
| --- | --- | --- |
| host and lease aggregate | admission, pressure, OOM and shedding decisions | CPU/run queue, memory/swap/PSI, disk bytes/inodes/latency, network, thermal and control-plane reserve |
| agent workspace and exec processes | Docker state, fork/exec/exit/signal/OOM and process tree | CPU/throttling, RSS/cache/swap, I/O, pids/threads, descriptors, sockets and per-process hot spots |
| Linux target and investigated processes | target/run lifecycle; exec, syscall, file-open/read/write/mutation, signal, DNS, socket and connection metadata | target/process CPU, memory, I/O, pids, descriptors, sockets and syscall/event rates |
| emulator host process | boot phase, QEMU exit, snapshot operation and stderr | CPU/RSS/I/O, vCPU/thread use, KVM availability, boot duration and GPU data where available |
| Android guest and selected apps | ADB state, boot properties, foreground package/activity, logcat, crash/ANR/reboot and package/process changes | process CPU/memory, battery, thermal, disk, frame/jank and Binder/scheduler data where supported |
| workspace, input view and network | cache/view lifecycle, file mutations, DNS and connection/flow metadata | logical/physical bytes, inode/quota use, cache reuse, network bytes/packets/drops/retransmits and latency |
| collectors | start/stop/configuration, coverage changes, gaps and teardown | collector CPU/memory/I/O/storage, queue depth, bytes produced and records dropped |

Capabilities are explicit. An unavailable Android, GPU, frame, or per-process
measure is `unsupported`, a late source is `stale`, and a failed or overflowing
collector produces a gap. None of these states is rendered or exported as a
numeric zero.

### 12.3 Agent interventions and specimen behavior

Every target exec, shell, file transfer, and ADB service request creates a
`TargetOperationID` before bytes are forwarded. The ledger records the caller,
exact target/run scope, structured argv or shell-byte digest, bounded/redacted
command display, stdin and transfer digests, start/stop times, exit status, and
process or Android identity where observable. Secrets and unbounded payloads
are referenced by digest rather than copied into routine events.

Normalized observations carry an origin classification:

- `agent-control` for gateway and lifecycle actions;
- `agent-instrumentation` for known pushed tooling, injected libraries, Frida,
  debuggers, and their attributable processes;
- `specimen` for the declared APK/binary and attributable descendants;
- `system` for target OS/runtime behavior; or
- `mixed-or-unknown` when the evidence does not justify a sharper claim.

These labels are evidence-backed, not trusted solely from an agent-provided
role. Binary hashes, process ancestry, package identity, injection records, and
explicit operation edges support the classification. Raw events remain
available, and the bundle never subtracts instrumentation side effects from the
record merely to produce a cleaner specimen story.

### 12.4 Live projections and consumers

The append-only streams drive a `LiveSnapshot` reducer keyed by subject. A
snapshot includes the current topology, latest raw values and derived rates,
sample age, pressure state, active incidents/captures, and collector coverage at
one durable cursor. Reconnecting clients fetch a snapshot, then continue from
that cursor without inventing an unobserved interval.

The trusted host can subscribe across its authorized leases. An in-container
agent receives a policy-filtered projection for only its own lease and allowed
signal families; filtering does not create a second telemetry copy. The
`worldctl watch` command shows the event timeline and `worldctl top` shows the
live subject tree.
OTLP and Prometheus exports support external dashboards but are not the audit
authority.

Visual emulator state is policy-controlled: event-triggered or periodic
screenshots are the default, while a bounded low-frame-rate live stream or full
screen recording is an explicit sensitive capture. Visual state is correlated
with input, process, log, network, and performance records rather than treated
as proof by itself.

When a target run finalizes, its cursor interval is sealed into one
`ObservationBundle` containing:

- immutable references to native/raw collector output;
- normalized events and metrics keyed to the target run and subjects;
- the authoritative target filesystem change manifest;
- collector configuration, placement, coverage, drops, gaps, and measured
  overhead;
- crash and incident evidence; and
- a reproducible derived summary optimized for agent consumption.

The summary is never the sole record. It cites event ranges or artifact refs,
labels inference, and can be regenerated without rerunning the target.

### 12.5 Collector stack and observation profiles

| Collector | Best use | Important constraint |
| --- | --- | --- |
| Tracee or Inspektor Gadget | container-scoped process, syscall, file, and network metadata | standard OCI targets only unless equivalent guest collection exists; report ring-buffer loss |
| `strace` | exact syscall diagnosis for one process tree | high overhead and timing effect; not baseline |
| `dumpcap`/tcpdump plus Zeek | packet evidence and protocol summaries | sensitive and high volume; rotate bounded ring files |
| mitmproxy | authorized HTTP/TLS semantic capture | changes trust/network behavior; pinning may prevent it |
| Perfetto | Android scheduling, CPU, Binder, app/system timelines | buffers and tracing services vary by Android version |
| logcat/dumpsys/tombstones | Android lifecycle, state, crash, and system evidence | permissions and build type bound completeness |
| Frida | Android intent/API/native runtime instrumentation | attached-process coverage is not framework completeness; root, repackaging, gadget, or debugger may be required |
| MobSF | reusable APK static/dynamic analysis and reports | external adapter; GPL and output provenance require review |
| screenshot/screen recording | visible Android state | sensitive, bandwidth-heavy, and not causal alone |
| heap/profile capture | resource root-cause analysis | stop-the-world or sampling overhead varies |

Observation profiles are cumulative:

- `metadata`: process, syscall names/results, file path/flags/byte counts,
  network-flow metadata, target changes, logs, and resource metrics;
- `deep`: broader syscall arguments, higher-rate Perfetto/ftrace, selected Frida
  hooks, packet rings, and periodic state snapshots; and
- `payload`: explicitly filtered file/socket buffers or decrypted traffic under
  strict byte, duration, path/process, and sensitivity bounds.

Visibility-first means the `metadata` profile is required for target runs; it
does not mean unbounded payload capture. Every injected collector has a
manifest: purpose, question, target, placement, version, configuration digest,
required privileges, start/stop times, expected and actual overhead, outputs,
teardown result, coverage level, and observer-effect warning. Instrumentation
teardown is verified. An orphaned capture is an incident.

### 12.6 Metric model

Samples contain raw counters and derived rates separately. Important measures
include:

- CPU time, throttled time, utilization, run queue, and PSI;
- memory current/peak/limit, anonymous/file cache, swap, reclaim, high/max/OOM
  counters, and PSI;
- block/read/write bytes and operations, latency, and I/O PSI;
- pids, threads, file descriptors, sockets, and configured limits;
- network bytes/packets/drops/retransmits and per-flow latency where available;
- scoped cache logical/physical bytes, deduplication ratio, view count, pinned
  entries, build/verification latency, GC/reconciliation state, and errors;
- per-lease lower logical bytes plus upper/workspace/capture physical bytes and
  inode consumption;
- emulator RSS/CPU/GPU where available, boot duration, ADB latency, Android
  process memory, battery, and thermal status; and
- collector CPU/memory/I/O plus lost events.

Sampling resolution is policy-controlled and may change on a trigger. A
`SamplingChanged` record explains every change. Raw Docker and cgroup values are
retained so UI-specific cache subtraction or percentage formulas do not erase
the underlying measurement. Every sample records collection and publication
times, and the live path reports delivery latency independently from target
performance.

## 13. Causal ledger

### 13.1 Event envelope

Every observation has:

```text
schema_version, event_id, kind
research_session_id, lease_id
agent_workspace_id, agent_generation, exec_id
target_id, target_generation, target_run_id, target_operation_id
correlation_id, causation_id, trace_id, span_id
source, source_instance, source_sequence, source_cursor
collector_id, collector_placement, coverage_level
observed_wall_time, observed_monotonic_time
subject_time, subject_clock_domain, clock_sync_epoch
host_boot_id, container_id, cgroup_id
process_id + process_start_time, Android pid/uid/package where known
policy_digest, capability_fingerprint_digest
payload, sensitivity, completeness, confidence
```

`causation_id` is set only for an explicit command, transition, parent process,
trace-context propagation, or collector-derived relationship with defined
semantics. Events aligned only by time use `correlated_with` plus method and
confidence. PID alone is never identity because it can be reused.

### 13.2 Ordering and clocks

There is no honest total causal order across Docker, kernel, host, emulator, and
Android clocks. The ledger provides:

- strict accepted order for world control records;
- monotonic source sequence within each collector instance;
- parent/child edges for process and control actions;
- wall and monotonic time where available;
- repeated clock-sync samples and boot IDs; and
- merged timeline views that retain source order and uncertainty.

Clock jumps or Android/emulator restore start a new sync epoch. Gaps, duplicate
source records, restarts, and uncertainty are first-class records.

### 13.3 Persistence

SQLite in WAL mode stores state, revisions, idempotency, policies, incidents,
segment indexes, and export transactions. High-rate observations use append-only
length-delimited protobuf segments with checksums and a hash chain. Control and
incident records are synced at acceptance; metric/event segments are rotated and
synced on bounded intervals.

Recovery truncates only an incomplete trailing frame, records that repair, and
replays accepted control records to compare reconstructed and materialized
state. Finalized segments and large captures are committed to the forensic
artifact authority. OTLP/Prometheus export is supported for live operations but
is not the authoritative audit ledger.

Target-run finalization spans several durable authorities and therefore uses a
recoverable ordered saga:

1. append the exact per-run reservation and its idempotency/signature binding;
2. stop the target and observers and durably journal their complete result;
3. write the canonical version-2 stop preparation containing the reservation,
   mutation metadata, initial run state/revision, generation identities,
   creation time, required coverage, complete persisted result, and compact
   failure-incident intent;
4. bind the stopped observer marker to both the result digest and the complete
   preparation digest, then anchor that file identity in the hash-chained
   control ledger;
5. create or resume the exact failure incident when the preparation contains
   an incident intent;
6. seal immutable evidence and publish the digest-matching material artifact;
7. write the canonical public bundle/terminal-commit projection, then anchor
   its file identity, size, and digest in the hash-chained control ledger;
8. commit the Core run terminal state from that exact projection;
9. publish and hash-chain-index the public bundle file;
10. commit the stopped observer marker; and
11. append `bundle.completed`, which is the public read gate.

Startup removes only recognized unreachable atomic temporary files. It resumes
an anchored stage without rebuilding evidence, verifies an already-terminal
Core record against that stage, repairs a missing public file/index, promotes
the matching stopped observer marker, and writes the completion gate. An
unanchored pre-terminal public stage is rolled back for exact-reservation retry;
an unanchored terminal stage, anchored stage with no exact file, non-canonical
or tampered bytes, foreign bundle file, or conflicting index fails startup.
Recovery after an earlier stop reservation reuses that reservation's original
namespace, idempotency key, and signature rather than inventing startup
ownership. A marker-bound stop-preparation file without its ledger anchor is
retained and anchored; an unbound unanchored file is removable. Once anchored,
the exact preparation is the only allowed source for incident creation, seal,
publication, and terminal commit. Public bundle reads remain unavailable until
the index, committed observer marker, and `bundle.completed` record agree.

## 14. Pressure-aware admission and shedding

Each lease has resource requests, hard limits, priority, preemptibility, and a
cost estimate. Linux workloads use separate leaves under one lease cgroup
subtree. Managed Android runtime processes use exact per-generation Windows
Jobs, while durable admission accounts their requested resources in the same
logical lease aggregate. Future physical-device helpers also belong to that
logical aggregate, though device-side consumption is reported separately.

Admission evaluates allocatable CPU/memory/storage/pids/devices, warm-pool cost,
current PSI trends, requested observers, snapshot memory spikes, and safety
headroom. It records all inputs and the selected effective budget.

Pressure response is ordered:

1. increase metric resolution and verify the signal;
2. stop admitting new work for the constrained resource;
3. expire unused reservations and reduce unleased warm pools;
4. stop or reset idle targets within policy;
5. ask preemptible active target runs to quiesce before a deadline;
6. capture minimum incident evidence and revoke the lowest-priority target run,
   then a preemptible agent workspace only if the host remains at risk; and
7. protect the control plane and quarantine the node if safety cannot be
   restored.

Active work is not silently `docker pause`d because that can look like an agent
or target stall and corrupt timeout semantics. A forced target eviction fails
and finalizes its target run without necessarily terminating the agent
workspace. Evicting the agent workspace fails its active exec. Both create a
`resource_eviction` incident and preserve the affected generations.

Hard cgroup limits remain the final containment boundary. `memory.events`, OOM
events, pids events, throttling, and PSI distinguish a workload limit from
host-wide contention.

## 15. Failure, incident, and recovery semantics

### 15.1 Classification

- `agent_exec_failure`: provider or research-tool process failed while its agent
  workspace lived.
- `agent_workspace_failure`: agent container died, OOMed, became unhealthy, or
  lost guest supervision.
- `target_workload_exit`: investigated Linux process or Android app exited,
  crashed, or was killed while the target remained healthy.
- `linux_target_failure`: target container died, OOMed, or became unhealthy.
- `emulator_failure`: QEMU/emulator process died, hung, or lost required ADB
  readiness.
- `android_failure`: app crash, native crash, ANR, system_server restart, or
  Android reboot while the emulator process survived.
- `device_disconnect`: future physical device changed authorization/state or
  vanished.
- `host_pressure` / `resource_eviction`: admission or safety action.
- `observer_failure`: required collector failed, overflowed, or could not tear
  down.
- `workspace_integrity`: lower changed, overlay disagreement, unsafe export,
  or quota violation.
- `control_plane_failure`: lost node, Docker daemon, database, or protocol.

### 15.2 Incident record

An incident contains immutable facts, affected agent or target generation,
target run and exec where applicable, trigger, last known state,
exit/signal/OOM evidence, high-water metrics, relevant event ranges, collector
coverage, observation-bundle/artifact refs, recovery actions, and visibility
acknowledgements. It separates:

- proven cause;
- likely correlation with method and confidence; and
- unknown cause.

The generated agent feedback is factual and bounded: what failed, relevant
limits/high-water values, whether the agent's last action is proven or merely
nearby, the incident ID, which outputs survived, and concrete safer retry
options. It never fabricates a successful tool result or blames an agent from
timing alone.

### 15.3 Recovery order

1. Freeze affected mutation and mark the exec or target run failed.
2. Capture the policy-mandated minimum evidence under a strict emergency budget.
3. Seal ledger, workspace, target, and observation-bundle segments, including
   any capture gaps.
4. Publish the incident to the live stream and execution-environment error.
5. Decide quarantine, teardown, snapshot restore, cold recreate, reboot, or
   human action according to policy and capabilities.
6. If recovery is authorized, create a new generation for only the failed
   resource, with an explicit link to the incident and previous generation.
7. A failed target may be replaced while the agent workspace continues and
   receives the sealed observation bundle plus recovery context. A failed agent
   workspace requires a new provider invocation; do not present either old
   process as though it resumed.

## 16. Policy model

Policies are host-owned, immutable-by-digest YAML documents. They define
allowed research-session shapes and maximum powers, not an agent-editable
checklist. The compiler uses strict decoding, rejects unknown fields, validates
cross-field invariants, resolves defaults, probes capabilities, and emits a
canonical effective-policy document for each admitted agent or target
generation.

In the shipped physical composition, a version-3 deployment profile names each
regular policy source by exact `metadata.name@metadata.revision`. Startup probes
the selected agent, target, workspace, and observer facts, constructs one
complete capability fingerprint, compiles every source against it, and binds
the resulting effective-policy and capability digests into the immutable agent
and target plans. Every configured plan passes through the same physical-policy
admission checks before those effective policies are published to durable
control state. No listener is open during load, probe, compile, preflight, or
startup reconciliation.

Immediately before a physical mutation, admission resolves the durable policy
pair again and checks the exact driver-reported plan rather than trusting the
request or the startup report alone. It enforces runtime, image, isolation,
network, workspace, resource, target concurrency, reset/recovery, capture, and
export constraints. Aggregate CPU, memory, and capture admission is calculated
from an authoritative inventory of all persisted sessions with their exact
plans re-resolved; an idempotent candidate is excluded once so a retry does not
double-count itself. Policy denial occurs before logical or physical mutation.

`world-capabilities -policy <path>` performs the non-provisioning probe for the
selected Docker and/or managed Android composition and reports the canonical
policy digest plus the complete capability digest.
The daemon independently repeats compilation and physical-plan preflight; the
operator tool is preparation and evidence, not an authorization bypass.

Policy areas are:

- agent-workspace runtime/image/isolation, input-view selection/layout, cache
  scope/construction, workspace quotas, and export rules;
- allowed target templates, images/system images, concurrency, target reset,
  specimen transfer, interaction transport capabilities, and target resource
  limits; target command text is not a policy dimension;
- network mode and sensitive capture permissions;
- aggregate lease, agent, target, and observer resources;
- Linux target and Android virtual-device requirements;
- required coverage, collector placement, baseline/deep/payload observation,
  triggers, and on-demand captures;
- sampling, buffers, retention, and sensitivity;
- incident evidence minimums and recovery modes; and
- priority, preemptibility, lease TTL, and pressure behavior.

Visibility is a first-class admission requirement. A policy can require, for
example, container-scoped syscall/file/network metadata or framework-level
Android intent coverage. Admission fails when the selected runtime and
collector placement cannot provide it. It never silently selects a stronger but
opaque sandbox.

An on-demand observation request can narrow or activate a permitted collector;
it cannot change the target template's guest privileges, expand network access,
choose another target, increase a hard retention ceiling, or bypass redaction.
Denials are observable policy decisions. Guest root in a rooted target does not
imply any of those infrastructure powers.

See [the example policy](examples/environment-policy.yaml).

## 17. Driver and helper boundaries

Stable world-owned ports keep vendor APIs out of the domain. The following
shows conceptual shapes with names abbreviated for readability; the compiled
interfaces in `internal/ports` are authoritative:

```text
type AgentWorkspaceDriver interface {
    Probe(context.Context) (CapabilitySet, error)
    Provision(context.Context, AgentWorkspacePlan) (AgentWorkspace, error)
    OpenExec(context.Context, ExecPlan) (ExecTransport, error)
    Inspect(context.Context, AgentWorkspaceID) (AgentWorkspaceStatus, error)
    Stop(context.Context, AgentWorkspaceID, StopMode) error
    Destroy(context.Context, AgentWorkspaceID) error
}

type TargetDriver interface {
    Probe(context.Context, TargetTemplate) (CapabilitySet, error)
    Create(context.Context, TargetPlan) (Target, error)
    PrepareRun(context.Context, TargetRunPlan) (PreparedTargetRun, error)
    StartRun(context.Context, TargetRunID) error
    OpenTransport(context.Context, TargetRunID) (TargetTransport, error)
    StopRun(context.Context, TargetRunID, StopMode) (TargetRunResult, error)
    Reset(context.Context, TargetID, ResetPlan) (Target, error)
    Destroy(context.Context, TargetID) error
}

type TargetTransport interface {
    OpenExec(context.Context, TargetExecPlan) (ExecTransport, error)
    PushFile(context.Context, TargetTransferPlan, io.Reader) (TransferResult, error)
    PullFile(context.Context, TargetTransferPlan) (io.ReadCloser, error)
    OpenADB(context.Context) (ScopedADBEndpoint, error)
}

type ObserverDriver interface {
    Probe(context.Context, ObservationRequirement) (CapabilitySet, error)
    Start(context.Context, CollectorPlan) (Collector, error)
    Stop(context.Context, CollectorID) (CollectorResult, error)
    Coverage(context.Context, CollectorID) (Coverage, error)
}

type MaterialAuthority interface {
    ResolveInputView(context.Context, InputPlan) (InputViewManifest, error)
    OpenContent(context.Context, ArtifactOccurrence) (ContentReader, error)
    CaptureOutputs(context.Context, OutputPlan) ([]ArtifactOccurrence, error)
    CaptureObservationBundle(context.Context, ObservationBundlePlan) (ArtifactOccurrence, error)
}
```

Workspace, observation-bundle, ledger, and resource-controller ports follow the
same shape. Shared patterns are extracted once: transition validation,
capability requirements, safe beneath-path opening, bounded stream pumping,
idempotency, clock conversion, collector supervision, target-run finalization,
and fault injection. Target and provider adapters must not each reimplement
these guarantees.

## 18. Cross-repository integration

### 18.1 `go-agent-runner`

The runner now exposes the required host-default execution seam:

- acquire a world lease and ready agent generation first;
- set `Request.ExecutionEnvironment` to the world adapter; nil remains the
  runner's ordinary local-host default for all unrelated callers;
- let the adapter resolve the workspace-visible working directory and provider
  executable, including versioned executable identity;
- carry runner temporary inputs as bytes and materialize them inside the agent
  workspace, never as host paths;
- include lease, agent generation, image/tool fingerprint, and protocol version
  in the environment ID so capability caches cannot cross realizations;
- forward runner `OnEvent` observations into the campaign manager with the same
  correlation/exec IDs and environment ID; and
- combine execution-environment errors with the corresponding world incident.

The adapter implements generic command execution only. Provider adapters,
protocol normalization, retry/session logic, schema validation, event ordering,
and success authority remain entirely in `go-agent-runner`. The world layer
owns workspace containment, transport, resource limits, cleanup confirmation,
and incident evidence.

### 18.2 `go-forensic-artifacts`

The first-party adapter uses existing freeze/projection metadata, `OpenObject`
streaming, activity capture, and verification facilities. The world manager
does not derive or expose forensic CAS paths, and it does not materialize a full
copy for every lease. If the exact canonical view manifest cannot be expressed
through the current artifact API, the narrow addition belongs in the artifact
repository rather than duplicating projection semantics here. World output,
stdout/stderr, target change manifests, observation bundles, ledger segments,
pcaps, Perfetto traces, screenshots, tombstones, and incident reports become
immutable occurrences with agent-execution and target-run activities plus named
input/output roles.

The artifact store's wrapped runner is not used as a sandbox; its own design
correctly states that it is only an execution/capture boundary.

### 18.3 `go-vr-research-framework`

The campaign manager records completed world execs and target runs as VR
`RecordActivity` commands using artifact refs for inputs, outputs, observation
bundles, logs, and attestations. Directly measured incident and telemetry
conclusions become typed `RecordObservation` commands. The world manager does
not create hypotheses, claims, findings, or authoritative VR events.

Correlation and causation IDs flow unchanged through world, artifact activity,
and VR command metadata so the host can join operational evidence to analytical
history without collapsing the systems into one database.

## 19. Security model

### 19.1 Adversaries

- an agent deliberately or accidentally attempting host access;
- hostile APKs, binaries, source trees, build scripts, and dependencies;
- a compromised tool inside the agent workspace;
- a hostile target attempting to cross into the agent workspace or control
  plane;
- malicious paths, symlinks, hardlinks, devices, sockets, whiteouts, archives,
  or output floods;
- a target app attempting anti-instrumentation, traffic evasion, or collector
  exhaustion;
- confused-deputy requests against Docker, ADB, artifact, or export APIs; and
- daemon crashes, host reboots, disk exhaustion, OOM, clock jumps, and partial
  writes.

### 19.2 Trust boundaries

The host application, each independently deployed daemon, its selected host
drivers, policy store, observer supervisor, and material adapter are trusted.
The agent workspace, provider, research tools, Linux targets, inputs, Android
apps, and device responses are untrusted. Agent workspace and targets are
sibling trust domains with no shared writable mount or management authority.
`world-guest` is trusted for orchestration but runs in the agent workspace
compromise domain; host enforcement never depends solely on it.

Physical devices and emulators may attack ADB, USB, network, parsers, or
collectors. Those adapters are separate processes with bounded inputs and least
privilege. Captured content is untrusted evidence, not safe structured data.

### 19.3 Isolation and visibility profiles

- `agent-standard`: an unprivileged numeric container user and hardened Docker
  plan for the persistent agent workspace: read-only root, no network, no new
  privileges, all capabilities dropped, no host namespaces/devices/runtime
  socket, and one managed workspace mount. Running Docker itself rootless is a
  deployment capability to probe and qualify, not an assumption of this name.
- `observable-container`: hardened standard OCI/runc Linux target with required
  host eBPF, namespace, network, and filesystem coverage. This is the v1 target
  default because visibility has priority.
- `sandboxed-kernel`: a future, non-shipped research tier for runtimes such as
  gVisor or Kata. It is not a current policy value or selectable target profile;
  shipping it would require a separate reduced-visibility or in-guest collector
  contract.
- `instrumented-android`: target profile for a rooted/debuggable AOSP virtual
  device with pinned Perfetto, logcat, state, packet, and Frida/framework
  observation capabilities. It remains outside the daemon composition.
- `device-lab`: future dedicated node/USB/network isolation for physical phones.

No tier is named `unrestricted`; permissions are described concretely.

## 20. Research basis

- Docker provides a versioned Engine API, a live event stream, and detailed
  stats; cgroup v1/v2 fields differ and raw counters should be retained:
  <https://docs.docker.com/reference/api/engine/> and
  <https://docs.docker.com/engine/containers/runmetrics/>.
- Docker rootless mode runs both daemon and containers without root:
  <https://docs.docker.com/engine/security/rootless/>.
- Linux OverlayFS whiteouts and opaque directories define deletion semantics:
  <https://docs.kernel.org/filesystems/overlayfs.html>.
- OverlayFS lower layers may be shared by multiple mounts, while a lower file is
  copied up when data is modified; fs-verity can provide read-time integrity for
  supported read-only cache files:
  <https://docs.kernel.org/filesystems/overlayfs.html> and
  <https://docs.kernel.org/filesystems/fsverity.html>.
- cgroup v2 and PSI expose per-cgroup limits, OOM events, and CPU/memory/I/O
  pressure suitable for admission and shedding:
  <https://docs.kernel.org/admin-guide/cgroup-v2.html> and
  <https://docs.kernel.org/accounting/psi.html>.
- Android Emulator supports explicit data images, headless operation, packet
  capture, named snapshots, and snapshot storage:
  <https://developer.android.com/studio/run/emulator-commandline> and
  <https://developer.android.com/studio/run/emulator-snapshots>.
- Perfetto combines Android/Linux scheduling and userspace traces and supports
  programmatic analysis:
  <https://perfetto.dev/docs/getting-started/system-tracing>.
- Cuttlefish is AOSP's configurable local/remote virtual Android device, supports
  parallel devices, and presents normal ADB interaction:
  <https://source.android.com/docs/devices/cuttlefish>.
- Android logcat provides multiple buffers and monotonic/epoch formats, subject
  to device permissions: <https://developer.android.com/tools/logcat>.
- Tracee and Inspektor Gadget provide open-source, container-aware eBPF event
  collection so v1 does not need to create a new Linux tracing stack:
  <https://aquasecurity.github.io/tracee/> and
  <https://inspektor-gadget.io/docs/latest/>.
- MobSF provides reusable open-source APK static and dynamic analysis with API
  integration; it remains an external adapter because its lifecycle and
  evidence model are not the world's authority:
  <https://github.com/MobSF/Mobile-Security-Framework-MobSF>.
- Docker can be configured upstream with gVisor, but World does not select or
  admit it; its distinct filesystem, network, and debugging behavior remains a
  future research subject: <https://gvisor.dev/docs/user_guide/quick_start/docker/>.
- OpenTelemetry correlates traces, metrics, and logs but is an export framework,
  not the durable world ledger: <https://opentelemetry.io/docs/concepts/signals/>.
- Frida and mitmproxy are invasive, capability-dependent collectors rather than
  invisible defaults: <https://frida.re/docs/modes/> and
  <https://docs.mitmproxy.org/stable/concepts/how-mitmproxy-works/>.

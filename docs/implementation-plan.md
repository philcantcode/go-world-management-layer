# Implementation plan

- Status: proposed
- Date: 2026-07-24
- Target module: `github.com/philcantcode/go-world-management-layer`
- Proposed Go floor: 1.25.12

## 1. Delivery strategy

Build one real, hostile-input vertical slice before adding multiple runtimes and
collectors. The first slice must prove the difficult seams together:

1. a host acquires a lease with one frozen forensic input selection;
2. the manager admits it against host capacity and creates a dedicated cgroup;
3. the selection resolves to a canonical input view, whose missing bytes are
   streamed once into the scoped node cache and exposed as a verified OverlayFS
   lower layer;
4. Docker starts a hardened container and `world-guest` becomes ready;
5. `go-agent-runner` invokes a fake provider through lease-specific
   `worldexec`, including its version/help probes;
6. the provider starts ordinary tools, reads input, and writes derived files;
7. a client watches live process/file events and host/container metrics;
8. the provider or host declares one output path;
9. the manager seals the change set and captures the output, logs, ledger, and
   execution provenance in `go-forensic-artifacts`;
10. a forced cgroup OOM in a second run produces an explicit incident, a failed
    runner result, preserved evidence, and a clean new generation; and
11. teardown leaves no process, mount, cgroup, network namespace, temporary
    descriptor, observer, or writable upper layer behind; released input views
    remain only according to their bounded cache policy.

No emulator, physical-device, Frida, mitmproxy, or distributed scheduling work
should precede this slice. They depend on its lease, generation, observation,
incident, artifact, and cleanup contracts.

## 2. Engineering rules

- Define protobuf/API, domain invariants, state transitions, and error taxonomy
  before implementing vendor drivers.
- Use fake drivers and a deterministic clock/random source for every control
  path. A real driver must pass the same contract suite.
- Every mutation is idempotent and revisioned. Every blocking driver operation
  has a context deadline and a distinct cleanup deadline.
- Every goroutine and external process has an owner. Tests assert owner shutdown.
- Keep provider stdout/stderr transport independent from observation transport.
- Treat captured paths, Docker/ADB events, Android output, collector records,
  and artifact metadata as hostile input.
- Preserve raw counters and source facts. Derived rates, diagnoses, and agent
  guidance are separate typed fields.
- When a pattern appears twice, extract the guarantee into a helper. Initial
  shared helpers are transition guarding, capability requirements, safe path
  opening, idempotency, stream pumping, collector supervision, segment framing,
  clock synchronization, and fault injection.
- Do not add compatibility aliases or deprecated APIs during pre-v1 iteration.
  Replace a contract and all consumers together.
- A phase exits only through executable evidence, not completed code review.

## 3. Proposed repository layout

```text
api/world/v1/                    protobuf control and observation contracts
world/                           public Go client and stable views
policy/                          strict public policy DTOs and compiler entrypoint
adapters/forensicartifacts/      first-party MaterialAuthority adapter
cmd/
  worldd/                        logical control-plane daemon
  world-node/                    trusted Linux node authority
  worldexec/                     lease-bound provider execution shim
  world-guest/                   container PID 1 and exec supervisor
  worldctl/                      operator/debug CLI over the public API
internal/
  application/                   commands, queries, transactions, orchestration
  domain/                        lease/generation/incident/resource invariants
  admission/                     capacity, pressure, priority, shedding
  ledger/                        control records, segments, cursors, replay
  transport/                     framed exec and bounded stream helpers
  inputcache/                    scoped CAS, view construction, pins, GC
  workspace/                     OverlayFS plan, diff, seal, export
  drivers/
    runtime/docker/              Docker Engine adapter
    target/emulator/             Android Emulator adapter
    target/androiddevice/        physical-device inventory and lease adapter
    deviceproxy/                 scoped ADB protocol gateway
    observer/                    collector implementations
  linux/                         cgroup, mount, namespace, openat2 helpers
  testkit/                       fake drivers, clocks, fault points, leak checks
docs/                            design, ADRs, policies, operations
testdata/                        hostile paths, event fixtures, policy fixtures
```

Concept packages stay free of Docker SDK, ADB, protobuf-generated, SQLite, and
artifact-provider types. Public views are stable copies, not internal state
pointers.

## 4. Phase 0: feasibility and contract spikes

Purpose: retire high-risk unknowns before production scaffolding.

### 4.1 Linux node matrix

Exercise at least Ubuntu LTS and one other cgroup v2 distribution in disposable
VMs. Record kernel, filesystem, Docker Engine/API, runc, rootless, gVisor, KVM,
SELinux/AppArmor, eBPF, `openat2`, fanotify, and PSI capabilities.

Prove:

- a container can be placed under a manager-created lease cgroup parent;
- container plus emulator processes can be measured in one aggregate subtree;
- PSI poll triggers wake within documented windows;
- grouped cgroup OOM kills only the intended workload and emits distinguishable
  counters/events;
- Docker events and stats survive reconnect with a known replay limitation; and
- the control/node processes retain resource headroom during workload pressure.

### 4.2 Input-view cache and Overlay workspace spike

On each candidate backing filesystem, build two substantially overlapping input
views from a synthetic content-addressed corpus. Require reflink construction,
reuse an exact view, and measure physical allocated blocks rather than logical
file sizes. Prove a content digest is populated once per security scope,
overlapping views share data extents, and their logical entries retain
independent inode metadata and modes. Admission must fail rather than silently
copy when policy requires reflinks and the filesystem lacks the capability.

Mount each immutable view with independent upper, work, and merged directories
under a private mount namespace. Exercise additions, copy-up, truncation, chmod,
hardlinks, renames, whiteouts, opaque directories, case collisions, Unicode,
deep trees, sparse files, sockets/FIFOs/devices, and concurrent mutation during
sealing.

Prove descriptor-based safe export against symlink swaps and path traversal.
Compare overlay-aware diff, merged-tree diff, and event journal. Define the exact
canonical change-manifest schema from results. Kill the manager during content
staging, atomic publication, view construction, pinning, unmount, and GC; then
prove reconciliation neither deletes a live view nor permanently retains an
unreferenced one. Verify an agent cannot enumerate cache paths, excluded entries,
or another selection.

### 4.3 Runner bridge spike

Use the existing `go-agent-runner` deterministic fake CLIs. Run capability
probes, one successful provider protocol, large simultaneous stdout/stderr,
stdin close, cancellation, process descendants, terminal disagreement, and a
shim/network disconnect through `worldexec` into a container.

Prove:

- no byte changes in successful stdout/stderr;
- backpressure does not deadlock either side;
- runner cancellation kills the remote process group;
- an unconfirmed kill tears down the container;
- the same absolute working directory works on host and in container; and
- an incident ID reaches stderr without corrupting stdout.

### 4.4 Emulator spike

On a KVM host, boot one headless AVD with an isolated data directory and fixed
ports. Save/load/list an immutable baseline snapshot, kill the emulator during
boot and during snapshot save/load, corrupt or invalidate snapshot metadata,
and verify cold-boot behavior. Capture emulator stderr, logcat with monotonic
and epoch clocks, network packets, a bounded Perfetto trace, CPU/memory/thermal
state, and ADB readiness.

### 4.5 Scoped ADB spike

Prototype a protocol-aware proxy that exposes one assigned serial. Verify that
device listing, transport selection, server-control services, reverse/forward,
sync, shell, logcat, and reconnect behavior cannot address another attached
device or host ADB control. Fuzz ADB framing before selecting or writing the
production implementation.

### 4.6 Artifact round-trip spike

Against a disposable `go-forensic-artifacts` case:

- freeze a selection and resolve it to a canonical input-view manifest;
- stream missing objects through the public artifact reader, populate the
  scoped cache, verify them, and mount the resulting view as lower;
- capture selected derived files plus stdout/stderr, incident JSON, metric/event
  segment, and packet/trace object under one activity;
- trace every output to the input occurrence and world execution; and
- prove a modified workspace never changes a managed blob or cached input.

### Phase 0 exit gate

Check in spike code or reproducible test harnesses, machine-readable results,
selected capability requirements, benchmark numbers, and revised ADRs. Do not
proceed if safe export, remote cancellation, aggregate accounting, or emulator
generation rollover remains ambiguous. Do not proceed without a verified
reflink-capable storage profile or an explicit decision to accept copy cost for
a named non-production profile.

## 5. Phase 1: deterministic control core

Implement without Docker or Android dependencies:

- typed IDs, revisions, errors, and immutable public views;
- environment, lease, generation, exec, target, input-view, cache-scope,
  cache-entry, workspace, incident, capture, export, and policy models;
- declarative state transition tables and replay;
- strict YAML decoding, canonical policy digest, defaults, cross-field
  validation, capability requirements, and effective-policy output;
- SQLite schema/migrations, transaction boundaries, idempotency, leases, and
  recovery ownership;
- append-only segment framing, checksums/hash chain, cursors, rotation, replay,
  compaction markers, and gap records;
- admission model and deterministic pressure decision engine;
- fake runtime/target/input-cache/workspace/observer/material/node drivers;
- gRPC API, Go client, Unix socket authentication, and `worldctl`; and
- one `make verify` or equivalent command that runs format, unit, fuzz seed,
  race, vet, generated-contract, and migration checks.

### Phase 1 exit gate

- Model-based tests traverse every legal and illegal transition.
- Replaying the ledger reconstructs exactly the materialized state.
- Killing the daemon at every database/segment fault point reopens to a valid,
  explicitly repaired or blocked state.
- One thousand concurrent fake acquire/renew/release/subscribe flows pass under
  the race detector with bounded goroutine and memory growth.
- Unknown policy fields, capability unknowns, conflicting idempotency keys, and
  stale revisions fail deterministically.

## 6. Phase 2: Docker, guest, shim, workspace, and live metrics

Implement:

- authenticated `worldd` to `world-node` protocol;
- Docker Engine probe and version negotiation;
- hardened container plan and inspection reconciliation;
- per-lease cgroup parent, hard limits, raw cgroup metrics, PSI triggers, Docker
  events/stats, and host metrics;
- scoped content cache population, canonical read-only view construction,
  reference pins, bounded TTL/LRU collection, and startup reconciliation;
- OverlayFS prepare/mount/diff/seal/unmount with descriptor-safe exports;
- `world-guest` PID 1, process-group supervision, and guest heartbeat;
- framed exec transport and lease-specific `worldexec` descriptors;
- baseline process/file/network-flow collectors and collector health;
- a policy-bounded, non-blocking raw exec-stream tee plus lease-scoped
  `world-observe` and append-only `world-export` guest helpers; and
- cleanup/reconciliation after node, Docker daemon, guest, shim, or container
  failure.

The vertical slice uses a fake `MaterialAuthority` first, followed immediately
by the real adapter in phase 3.

### Phase 2 exit gate

- The runner-bridge spike becomes a permanent end-to-end test.
- A malicious tool cannot see host paths, Docker socket, host processes,
  management credentials, node cache, excluded artifacts, another input view,
  another lease, or another network namespace.
- Exact views are reused, overlapping views do not duplicate their full physical
  data allocation, and each generation still has an independent writable upper.
- Cancellation and every abnormal shim disconnect eliminate the remote process
  tree within the selected bound or destroy the container and report failure.
- Live metrics include host, lease aggregate, container, and selected process;
  forced CPU throttling, pids exhaustion, memory high, OOM, and I/O pressure are
  correctly distinguished.
- Slow/disconnected observation consumers neither block the provider nor lose
  unreported data; gap records are correct.
- Teardown leak checks find zero remaining processes, containers, mounts,
  cgroups, namespaces, capture files, and lease descriptors.

## 7. Phase 3: forensic artifact authority integration

Implement the first-party adapter at the repository edge. Map:

- qualified input occurrences or a frozen selection to a canonical
  `InputViewManifest`, then stream only cache misses through public object
  readers;
- world generation/exec to one forensic session/activity;
- execution image/tool/policy/capability/host descriptors to allowlisted
  provenance fields;
- declared outputs, stdout, stderr, incidents, change manifests, ledger
  segments, pcaps, traces, profiles, screenshots, and tombstones to named output
  roles; and
- artifact acknowledgements to safe local-retention cleanup.

The adapter must stage outputs, seal workspace revision, import from open file
descriptors or immutable staging copies, and handle the lack of a distributed
transaction through an explicit reconciliation state.

### Phase 3 exit gate

- The complete first vertical slice passes against a real disposable forensic
  repository.
- Each unique selected digest is fetched and verified once per cache scope;
  repeated and overlapping selections reuse it without exposing unselected
  occurrence metadata.
- Every captured occurrence traces to the world activity and original inputs.
- Kill injection before/after every staging, hash, artifact import, ledger
  reference, acknowledgement, and cleanup point leaves either a reconciled
  capture or an explicit incomplete export—never an unreferenced success.
- Managed blobs, cached content, and read-only views remain unchanged after
  arbitrary upper-layer mutation.
- Replaying the same export idempotency key returns the same occurrences;
  changed path/role/hash conflicts.

## 8. Phase 4: Android emulator driver

Implement:

- emulator/system-image/AVD/snapshot capability fingerprints;
- collision-free port/data-directory allocation and lease cgroup placement;
- headless boot, multi-signal readiness, health, graceful stop, forced stop,
  destruction, and reconciliation;
- immutable baseline snapshots, explicit save/load, invalidation detection,
  cold boot, and generation rollover;
- ADB state, build fingerprint, boot properties, logcat, battery/thermal/disk,
  emulator stderr, screenshot, packet ring, and bounded crash evidence;
- Android crash/ANR/system-restart classification distinct from emulator death;
  and
- optional Perfetto capture within policy.

### Phase 4 exit gate

- Parallel emulators never collide on ports, AVD state, snapshots, cgroups, or
  observation attribution.
- Killing at every boot/snapshot/capture transition produces a visible incident
  and a cleanly linked next generation or quarantine.
- Restoring a snapshot cannot reuse the failed clock-sync epoch or exec.
- Deliberate app Java crash, native crash, ANR, Android reboot, ADB offline, QEMU
  kill, disk full, and host OOM are classified correctly.
- Live host/container/emulator/Android performance streams can be viewed on one
  uncertain-but-honest timeline with collector gaps visible.

## 9. Phase 5: physical-device inventory and lease driver

Implement:

- durable device inventory, build/authorization/capability history, operator
  labels, battery/thermal/health, and exclusive reservation;
- scoped ADB gateway with one-serial visibility and policy-recorded services;
- USB/network reconnect, gateway restart, reboot, app reset/reinstall/fixture,
  lease release, and manual reconditioning workflows;
- quarantine triggers and operator acknowledgement; and
- permitted logcat/Perfetto/bugreport/screenshot/Frida capabilities without
  claiming completeness on production builds.

### Phase 5 exit gate

- One lease cannot list, select, forward to, or otherwise affect another phone.
- Unplug/replug, unauthorized, offline, reboot, low battery, thermal critical,
  changed build, failed app cleanup, and proxy crash all have tested outcomes.
- No automated path returns an uncertain device to the clean pool.
- Hardware-lab tests preserve unrelated devices and run with explicit operator
  opt-in for destructive app/device reset operations.

## 10. Phase 6: advanced observers and feedback

Add each observer as an independent driver only after its contract and teardown
test exist:

- eBPF process/syscall/file/network probes with ring-loss counters;
- targeted `strace` process-tree capture;
- bounded tcpdump/tshark packet rings and finalization;
- mitmproxy explicit/transparent capture with CA and sensitive-payload policy;
- Android/Linux Perfetto configuration and query summaries;
- Frida injected/server/gadget modes with capability and modification manifest;
- screenshots/screen recording, bugreports, tombstones, ANR traces, heap dumps,
  and profiles; and
- incident feedback synthesis that cites facts/evidence and labels inference.

### Phase 6 exit gate

- Every collector reports its own CPU/memory/I/O/storage and dropped records.
- Forced cancellation, target crash, daemon restart, and disk quota leave no
  orphan collector and create a gap/incident when appropriate.
- A/B benchmarks quantify observer effects for each supported profile.
- Sensitive payloads cannot enter default logs, metrics, or unrestricted
  artifact roles.
- Agent feedback never claims causality not represented by a defined edge.

## 11. Phase 7: hardening and operations

Implement:

- rootless and gVisor production profiles where phase 0 proves compatibility;
- signed/pinned images and policy distribution;
- node registration, drain, maintenance, quarantine, upgrade, and version skew;
- backup/snapshot/restore and migration for world state and unfinalized segments;
- dashboards and alerts derived from the same public observation schema;
- capacity benchmarks, warm-pool policy, fairness, and starvation prevention;
- scheduled chaos and hardware-lab suites; and
- operator runbooks for leaked mounts, Docker loss, corrupt AVD/snapshot,
  quarantined phone, artifact outage, disk pressure, and state reconciliation.

### Phase 7 exit gate

All v1 acceptance criteria below pass on the documented node matrix. Security
review must include the node protocol, Docker authority, scoped ADB gateway,
mount/export path, observer privileges, policy compiler, and artifact adapter.

## 12. Test architecture

### 12.1 Test classes

| Class | Required evidence |
| --- | --- |
| unit | invariants, transitions, policy, resource math, error mapping, event conversion |
| model/property | state-machine sequences, admission fairness, cache pin/GC safety, change-set algebra, replay equivalence |
| fuzz | policy YAML, protobuf frames, Docker/ADB/emulator events, paths, input/change manifests, ledger recovery |
| race/stress | concurrent leases, subscriptions, collectors, exports, driver reconciliation |
| contract | every fake and real driver passes one behavior suite |
| security | escape corpus, confused deputy, path race, cross-lease/device access, secret/sensitivity leaks |
| fault injection | process kill, error, delay, partial I/O, disk full, OOM, clock jump at each named boundary |
| integration | real Docker, OverlayFS, cgroup v2, PSI, namespace, eBPF, artifact store |
| emulator | KVM boot/snapshot/crash/ADB/logcat/Perfetto/network behavior |
| hardware | physical-device reservation, scoping, reconnect, reboot, cleanup, quarantine |
| performance | physical cache/view allocation, proxy latency/throughput, collector overhead, event rate, sealing cost, admission response |
| chaos/soak | multi-day mixed workloads, random daemon/runtime/device failures, pressure oscillation |
| compatibility | recorded Engine/emulator/ADB/event fixtures and supported schema migrations |

### 12.2 Named fault points

At minimum inject immediately before and after:

- policy acceptance and effective-policy publication;
- lease admission and reservation;
- SQLite commit and control-record append/sync;
- input-manifest resolution and cache lookup;
- cache content staging, hash verification, atomic publication, view clone,
  pin/release, and garbage collection;
- upper/work creation and OverlayFS mount/bind;
- cgroup creation and limit application;
- Docker create/start and guest handshake;
- each observer start and readiness acknowledgement;
- exec acceptance, process spawn, stream attach, terminal record, and cleanup;
- emulator data clone, port reservation, boot, ADB readiness, snapshot save/load;
- observation frame write, rotation, sync, index, and artifact finalization;
- workspace freeze, diff, file open, hash, staging copy, artifact import, and ack;
- incident acceptance, minimum evidence, publish, recovery decision, and
  generation creation; and
- unmount, container/device release, cgroup removal, and local deletion.

The test harness kills whole processes, not only returns injected errors, to
exercise actual recovery.

### 12.3 Security escape corpus

Run a deliberately malicious container that attempts:

- `/proc`, `/sys`, host PID, namespace, cgroup, kernel-log, and device access;
- Docker/containerd sockets and common credential/config paths;
- mount, unshare, setns, ptrace of host/other lease, privileged BPF, and raw
  packet operations;
- symlink/hardlink/magic-link/export TOCTOU, `..`, absolute/UNC-like paths,
  Unicode/case collisions, whiteout/opaque tricks, FIFOs/sockets/devices, and
  output floods;
- direct cache-path guessing, cross-view enumeration, excluded-entry access,
  cache-key collision, and malformed or inconsistent input manifests;
- cross-lease IP/Unix-socket access and management API guessing;
- access to another emulator/phone serial or the raw host ADB server;
- fork bombs, descriptor exhaustion, disk/inode exhaustion, memory bombs, CPU
  spin, and packet floods; and
- instrumentation disable/evasion and malformed collector output.

The suite proves the boundary remains intact and that the attempt itself is
visible. It does not claim immunity to unknown host-kernel/runtime exploits;
those motivate the stronger isolation tier and node containment.

### 12.4 CI topology

- Pull request: offline unit/model/fuzz-seed/race/vet/generated-contract tests
  using fake drivers.
- Linux integration: real Docker and unprivileged behaviors on every merge.
- Privileged disposable VM: OverlayFS, cgroups, PSI, namespaces, eBPF, security
  corpus, and forced host-level failures.
- KVM runner: emulator boot/snapshot/crash suite.
- Hardware lab: scheduled and explicit physical-device suite.
- Nightly: race, extended fuzz, gVisor/rootless matrix, compatibility fixtures.
- Weekly: chaos/soak, high-rate telemetry, pressure/fairness, and disaster
  recovery.

No developer laptop or shared CI host should run destructive pressure, raw USB,
or privileged escape tests directly; use disposable dedicated nodes.

## 13. Provisional performance budgets

Phase 0 must validate or revise these before they become acceptance criteria:

- local control API p95 below 50 ms for non-provisioning operations at 100
  concurrent clients;
- shim added p99 stream latency below 10 ms for 4 KiB frames on one node and no
  stdout/stderr corruption at 50 MiB/s aggregate;
- normal metric interval 2 seconds, incident interval 250 ms for a bounded
  window, and PSI threshold reaction within one configured kernel window plus
  250 ms control latency;
- baseline observation below 3% CPU and 128 MiB manager memory per active lease
  at the representative workload event rate, with host-global observer cost
  reported separately;
- zero dropped control/incident records; every bulk loss produces a gap record
  no later than the current segment rotation;
- confirmed exec cancellation within 5 seconds under a healthy node; otherwise
  container teardown starts immediately and is incident-visible;
- sealing time scales with changed files/bytes, not the entire immutable lower
  tree on the common path;
- physical data allocation for overlapping input views scales with unique
  content plus view metadata, not the sum of their logical sizes; cache and
  upper-layer bytes are separately attributed per scope and lease; and
- no unbounded queues, files, cardinality, labels, stderr, pcaps, logcat,
  profiles, or retries.

## 14. Contract propagation matrix

When these structures change, all listed consumers must be searched and tested.
Silent omission is a release blocker.

| Data | Required propagation |
| --- | --- |
| policy field | YAML DTO -> strict validation -> canonical digest -> capability resolution -> effective policy -> runtime/target/workspace/observer plan -> persisted view -> ledger -> incident/export provenance |
| identity/correlation | API request -> domain command -> state row -> node plan -> guest/observer context -> every event/metric/capture -> artifact activity -> host VR command metadata |
| resource request/limit | policy -> admission -> cgroup/emulator/container plan -> metric labels -> pressure decision -> incident feedback -> final activity environment |
| capability | driver probe -> fingerprint -> policy decision -> public lease view -> downgrade/failure event -> generation provenance |
| input view | artifact selection -> canonical manifest -> cache scope/lookup/population -> read-only view -> pin -> mount -> release/GC -> activity and incident provenance |
| workspace member | input-view manifest -> lower mount -> mutation event -> sealed change set -> export -> artifact occurrence -> VR activity material |
| clock information | source collector -> sync epoch -> event envelope -> segment index -> timeline query -> incident window -> captured trace metadata |
| sensitivity/retention | policy -> collector -> live authorization -> local segment -> OTLP/export filter -> artifact role -> cleanup acknowledgement |
| failure/recovery | driver fact -> classifier -> incident -> shim outcome -> runner error join -> generation transition -> artifact evidence -> host observation command |

Add automated descriptor/field coverage tests where possible. Generated
protobuf-to-domain/view adapters use common mapping helpers, and tests fail when
a new field is not deliberately mapped or ignored with rationale.

## 15. Cross-repository work

### This repository

Own all phases above, including the first-party forensic adapter and integration
tests that import the adjacent modules at released versions or temporary local
replacements in development.

### `go-agent-runner`

No initial change is required. After the shim vertical slice, propose a narrow
execution-backend interface only if it improves cancellation/incident handling
without exposing provider adapter internals. Remove the shim path completely if
that interface replaces it; do not retain two half-supported transports.

The runner currently contains source-compatibility deprecation remnants. They
are unrelated to this project and should not be copied into new APIs.

### `go-forensic-artifacts`

Prefer the existing selection, projection metadata, public object-reader,
activity, capture, and verification APIs. If canonical manifest resolution or
descriptor-based capture cannot be expressed safely, propose the narrowest new
API there and update every consumer. Do not make this world layer open or
derive internal CAS paths.

### `go-vr-research-framework`

The existing `RecordActivity`, `RecordObservation`, artifact-ref, correlation,
and causation contracts appear sufficient. The campaign manager performs the
translation. Add no direct dependency from world to VR.

## 16. V1 acceptance criteria

1. One hundred mixed container leases and the configured emulator capacity run
   concurrently under the race detector and real-node stress without broken
   state, cross-lease visibility, or unbounded growth.
2. Provider probes and executions through `worldexec` are byte-for-byte
   compatible with direct fake-provider execution, including failure and
   cancellation semantics.
3. The malicious escape corpus cannot access host management authority,
   another lease, another input view, another device, the node cache, or the
   artifact repository.
4. Different frozen selections expose exactly their authorized entries. Exact
   views are reused; overlapping views share physical content allocation; and
   release, crash reconciliation, and bounded GC leave neither deleted live
   inputs nor unbounded abandoned copies.
5. Every workspace change is represented as added/modified/deleted/renamed/
   metadata/opaque state; every exported byte is safely opened, hashed, and
   traceable to immutable inputs and one execution.
6. Killing each component at every named fault boundary never yields a false
   success, hidden crash, silently reused generation, or committed reference to
   missing bytes.
7. Container OOM, process crash, emulator death, Android app crash/ANR, ADB
   loss, physical-device disconnect, host pressure eviction, collector loss,
   and workspace integrity failure are distinguishable and evidence-backed.
8. Snapshot restore/cold boot always creates a new generation. Physical-device
   incomplete reset always quarantines.
9. Live activity and performance subscriptions are resumable; slow consumers
   cannot stall work; every loss/compaction is explicit.
10. Admission prevents configured control-plane reserve exhaustion, and pressure
   shedding follows priority/preemptibility policy with a durable rationale.
11. Every injected observer has bounded resource use, sensitivity metadata,
    output provenance, gap reporting, and verified teardown.
12. Ledger replay after random process termination matches materialized state or
    stops with a precise, recoverable inconsistency; it never repairs
    destructively without an explicit action.
13. A full end-to-end run can be recorded as a forensic activity and then as a
    VR activity/observation by the host, preserving shared correlation and
    causation IDs.
14. `make verify` (or the final cross-platform equivalent) is the single
    non-interactive verification entry point and emits machine-readable test,
    race, fuzz, security, integration, and benchmark summaries.

## 17. Decisions to close during phase 0

The architecture recommends a direction but requires evidence for these exact
choices:

- rootless Docker as the standard profile versus userns-remapped rootful Docker
  where dedicated OverlayFS/cgroup control needs privilege;
- whether `world-node` owns a tiny separate mount helper or keeps mount/open
  operations in its already trusted process;
- the production reflink-capable backing filesystem, cache integrity mode,
  security-scope defaults, high/low watermarks, and whether high view
  cardinality warrants packed immutable images;
- gVisor compatibility/performance for the common agent toolchain and which
  observer features become unavailable;
- the scoped ADB proxy implementation and smallest safe service set;
- the exact protobuf segment framing/compression and local segment size;
- safe provider credential delivery that supports each CLI without mounting a
  general host home directory;
- whether declared outputs are usually returned in the agent's structured
  result, submitted through `world-export`, or both with one shared validation
  path; and
- production retention tiers and authorization for decrypted traffic,
  screenshots, memory, and device identifiers.

These choices can alter driver implementations and deployment profiles. They do
not alter the lease, generation, explicit-incident, safe-export, or causal-ledger
invariants.

# Implementation plan

- Status: historical delivery plan; see `README.md` and `docs/design.md` for the
  shipped boundary
- Date: 2026-07-24
- Revised: 2026-07-27
- Target module: `github.com/philcantcode/go-world-management-layer`
- Proposed Go floor: 1.25.12

## 1. Delivery strategy

Build one real, hostile-input vertical slice around a persistent agent workspace
and a disposable Linux target before adding Android or multiple collector
implementations. The first slice must prove the difficult seams together:

1. a host acquires a lease with one frozen forensic input selection;
2. the manager admits it against host capacity and creates an aggregate lease
   cgroup with agent, target, and observer leaves;
3. the selection resolves to a canonical input view, whose missing bytes are
   streamed once into the scoped node cache and exposed as a verified OverlayFS
   lower layer;
4. Docker starts a hardened agent workspace and `world-guest` becomes ready;
5. `go-agent-runner` invokes a fake provider through a lease-bound
   `ExecutionEnvironment`, including version/help probes and remote temporary
   input materialization;
6. the provider requests a policy-named Linux target; `world-node` creates a
   sibling OCI/runc container without exposing the Docker socket;
7. the manager stages one specimen, starts required Tracee or Inspektor Gadget
   collectors plus a bounded packet ring, verifies coverage, and starts one
   target run;
8. the provider uses `world-target` to issue arbitrary target commands and
   push tooling while a client watches target process/syscall/file/network
   events, intervention attribution, and metrics;
9. finalization seals raw captures, normalized events, target changes, coverage
   and gaps, and a derived summary into one observation bundle;
10. the provider consumes that bundle, writes a derived file, and declares it
    for export;
11. the manager captures the agent output, target observation bundle, logs,
    ledger, and provenance in `go-forensic-artifacts`;
12. a forced target cgroup OOM produces an explicit incident and failed target
    run while the same agent workspace receives the evidence and successfully
    requests a clean new target generation; and
13. teardown leaves no process, mount, cgroup, network namespace, temporary
    descriptor, observer, or writable upper layer behind; released input views
    remain only according to their bounded cache policy.

No Android, Frida, mitmproxy, stronger target runtime, physical-device, or
distributed scheduling work should precede this slice. They depend on its
workspace/target separation, independent generation, observation-bundle,
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
adapters/agentrunner/            lease-bound runner ExecutionEnvironment
cmd/
  worldd/                        logical control-plane daemon
  world-node/                    trusted Linux node authority
  world-guest/                   agent-workspace PID 1 and exec supervisor
  world-target/                  scoped arbitrary target exec/transfer client
  world-observe/                 scoped live/bundle observation client
  worldctl/                      operator/debug CLI over the public API
internal/
  application/                   commands, queries, transactions, orchestration
  domain/                        session/workspace/target/run/incident invariants
  admission/                     capacity, pressure, priority, shedding
  ledger/                        control records, segments, cursors, replay
  transport/                     framed exec and bounded stream helpers
  inputcache/                    scoped CAS, view construction, pins, GC
  workspace/                     OverlayFS plan, diff, seal, export
  observationbundle/             raw refs, normalization, coverage, summaries
  drivers/
    agent/docker/                Docker agent-workspace adapter
    target/linuxcontainer/       visibility-first OCI/runc target adapter
    target/cuttlefish/           Android virtual-device adapter
    deviceproxy/                 scoped ADB protocol gateway
    observer/                    Tracee/IG, packet, Perfetto, Frida adapters
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
VMs. Record kernel, filesystem, Docker Engine/API, runc, rootless, KVM,
SELinux/AppArmor, eBPF/BTF, `openat2`, fanotify, and PSI capabilities. Keep
gVisor/Kata measurements in a future non-shipping research matrix.

Prove:

- agent, Linux target, emulator, and observer processes can be placed in
  separate leaves under one manager-created aggregate cgroup;
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

### 4.3 Runner execution-environment spike

Use the existing `go-agent-runner` deterministic fake CLIs. Run capability
probes, one successful provider protocol, large simultaneous stdout/stderr,
stdin close, cancellation, process descendants, terminal disagreement, Grok's
temporary prompt-file path, executable replacement, and a transport disconnect
through a custom `ExecutionEnvironment` into a container.

Prove:

- no byte changes in successful stdout/stderr;
- backpressure does not deadlock either side;
- runner cancellation kills the remote process group;
- an unconfirmed kill tears down the container;
- working directories and executables resolve inside the container without a
  matching host path;
- temporary inputs arrive as bytes, are materialized inside the container, and
  are removed during confirmed cleanup;
- the environment ID prevents capability-cache reuse across agent generations;
  and
- a structured execution error links to an incident without corrupting either
  provider stream.

### 4.4 Linux target and open-source observer spike

Create a Linux OCI/runc target as a sibling of the agent workspace. Run hostile
fixtures that fork, exec, open/read/write/rename/delete files, use sockets, crash,
and exhaust quotas. Compare current Tracee and Inspektor Gadget versions against
one normalized collector contract and record licensing, privileges, kernel/BTF
requirements, event schema stability, container scoping, startup races, drop
counters, throughput, and measured overhead.

Prove the selected stack can start before the specimen, attribute events to the
correct target run and process identity, maintain a bounded packet ring, seal an
authoritative target change manifest, report all lost coverage, and finalize a
raw-plus-normalized observation bundle after success, crash, OOM, cancellation,
collector death, or daemon restart. As future non-shipping research, compare
the same fixtures under gVisor/Kata and document the exact visibility lost or
moved into the guest.

### 4.5 Android virtual-device spike

Resolution note (2026-07-28): the shipped local backend selects the Android SDK
Emulator with full-tree image identity, durable exact-port allocation, clean
boot, one mutable run per generation, and replacement-generation reset.
Daemon-selected Cuttlefish, snapshot restore, custom-AOSP instrumentation, and
physical devices remain future work.

On a KVM host, compare Cuttlefish and Android Emulator behind one target-driver
contract. Boot an instrumented rooted/debuggable AOSP device with isolated state
and fixed endpoints. Reset to an immutable baseline, kill the runtime during
boot and reset, corrupt or invalidate snapshot metadata, and verify cold-boot or
powerwash behavior. Capture runtime stderr, logcat with monotonic and epoch
clocks, network packets, a bounded Perfetto trace, CPU/memory/thermal state, ADB
readiness, package/activity/process lifecycle, and screenshots.

Inject a test APK and compare Frida app-API intent hooks with framework-level
instrumentation in a custom AOSP image. Name their coverage separately. Exercise
MobSF through its API and decide which static/dynamic outputs are reused as
external evidence without delegating world lifecycle or provenance to it.

### 4.6 Scoped ADB spike

Resolution note (2026-07-28): the shipped protocol-aware gateway binds one
literal-loopback upstream and exact serial, denies host-global/cross-serial
actions, and preserves assigned-device services. Further protocol fuzzing is a
continuing qualification requirement, not an unimplemented driver seam.

Prototype a protocol-aware proxy that exposes one assigned serial. Verify that
device listing and transport selection cannot address another attached device
or host ADB control while arbitrary assigned-device services still work. The
test corpus includes shell, sync/push/pull, install/uninstall, root/remount,
reverse/forward, logcat, reboot, reconnect, and installing/running Frida server.
Fuzz ADB framing and race lease closure against long-lived services before
selecting or writing the production implementation.

### 4.7 Artifact and observation-bundle round-trip spike

Against a disposable `go-forensic-artifacts` case:

- freeze a selection and resolve it to a canonical input-view manifest;
- stream missing objects through the public artifact reader, populate the
  scoped cache, verify them, and mount the resulting view as lower;
- run a specimen in a sibling Linux target and capture its observation bundle,
  selected agent-derived files, stdout/stderr, incident JSON, metric/event
  segments, and packet/trace objects under linked activities;
- trace every output to the input occurrence, agent exec, and target run; and
- prove a modified workspace never changes a managed blob or cached input.

### Phase 0 exit gate

Check in spike code or reproducible test harnesses, machine-readable results,
selected capability requirements, benchmark numbers, and revised ADRs. Do not
proceed if safe export, remote cancellation, target-run finalization, collector
coverage/gaps, aggregate accounting, independent agent/target generations, or
Android reset remains ambiguous. Do not proceed without a verified
reflink-capable storage profile or an explicit decision to accept copy cost for
a named non-production profile.

## 5. Phase 1: deterministic control core

Implement without Docker or Android dependencies:

- typed IDs, revisions, errors, and immutable public views;
- research-session, lease, agent-workspace/generation, exec, target/
  generation/run/operation, observation-bundle, input-view, cache-scope,
  cache-entry, incident, capture, export, and policy models;
- separate declarative agent-workspace, target-generation, and target-run state
  transition tables and replay;
- strict YAML decoding, canonical policy digest, defaults, cross-field
  validation, capability requirements, and effective-policy output;
- SQLite schema/migrations, transaction boundaries, idempotency, leases, and
  recovery ownership;
- append-only segment framing, checksums/hash chain, cursors, rotation, replay,
  compaction markers, and gap records;
- deterministic live-snapshot reduction, subject topology, staleness and
  collector-coverage states, plus authorization-filtered projections;
- admission model and deterministic pressure decision engine;
- fake agent-workspace/target/input-cache/workspace/observer/material/node
  drivers and a deterministic observation-bundle builder;
- gRPC control/query API, target exec/transfer stream contracts, optional MCP
  facade, Go client, Unix socket authentication, and `worldctl`; and
- one `make verify` or equivalent command that runs format, unit, fuzz seed,
  race, vet, generated-contract, and migration checks.

### Phase 1 exit gate

- Model-based tests traverse every legal and illegal transition.
- A target reset advances only `TargetGeneration`; property tests prove it never
  changes a healthy `AgentGeneration` or exposes agent workspace state.
- Replaying the ledger reconstructs exactly the materialized state.
- Replaying any event prefix produces the same `LiveSnapshot`; authorization
  projections cannot reveal another lease or disallowed signal family.
- Killing the daemon at every database/segment fault point reopens to a valid,
  explicitly repaired or blocked state.
- One thousand concurrent fake acquire/renew/release/subscribe flows pass under
  the race detector with bounded goroutine and memory growth.
- Unknown policy fields, capability unknowns, conflicting idempotency keys, and
  stale revisions fail deterministically.

## 6. Phase 2: agent workspace, runner environment, and live metrics

Implement:

- authenticated `worldd` to `world-node` protocol;
- Docker Engine probe and version negotiation;
- hardened agent-container plan and inspection reconciliation;
- per-lease aggregate plus agent/observer leaf cgroups, hard limits, raw
  cgroup metrics, PSI triggers, Docker events/stats, and host metrics;
- scoped content cache population, canonical read-only view construction,
  reference pins, bounded TTL/LRU collection, and startup reconciliation;
- OverlayFS prepare/mount/diff/seal/unmount with descriptor-safe exports;
- `world-guest` PID 1, process-group supervision, and guest heartbeat;
- framed exec transport and the lease-bound `agentrunner.ExecutionEnvironment`
  adapter, including executable identity and temporary-input materialization;
- agent process/file/network-flow collectors and collector health;
- current-state snapshot plus resumable event/metric fan-out, `worldctl watch`,
  `worldctl top`, and `world-observe snapshot|watch|top` clients;
- a policy-bounded, non-blocking raw exec-stream tee plus lease-scoped
  `world-observe`, scoped `world-target`, named append-only `world-capture
  request`, and append-only `world-export` guest helpers; and
- cleanup/reconciliation after node, Docker daemon, guest, exec transport, or agent
  container failure.

The vertical slice uses a fake `MaterialAuthority` first, followed immediately
by the real adapter in phase 3.

### Phase 2 exit gate

- The runner execution-environment spike becomes a permanent end-to-end test.
- A malicious tool cannot see host paths, Docker socket, host processes,
  management credentials, node cache, excluded artifacts, another input view,
  another lease, target runtime authority, raw ADB, or another network namespace.
- Exact views are reused, overlapping views do not duplicate their full physical
  data allocation, and each agent generation still has an independent writable
  upper.
- Cancellation and every abnormal execution-transport disconnect eliminate the
  remote process tree within the selected bound or destroy the container and
  report failure.
- Live metrics separately attribute host, lease aggregate, agent workspace,
  observers, and selected processes; forced CPU throttling, pids exhaustion,
  memory high, OOM, and I/O pressure are correctly distinguished.
- Snapshot-then-stream reconnect has no silent interval. Unsupported, stale,
  lost, and zero-valued signals remain distinguishable in clients and exports.
- Slow/disconnected observation consumers neither block the provider nor lose
  unreported data; gap records are correct.
- Teardown leak checks find zero remaining processes, containers, mounts,
  cgroups, namespaces, capture files, and lease descriptors.

## 7. Phase 3: forensic artifact authority integration

Implement the first-party adapter at the repository edge. Map:

- qualified input occurrences or a frozen selection to a canonical
  `InputViewManifest`, then stream only cache misses through public object
  readers;
- agent generation/exec and target generation/run to linked forensic activities;
- execution image/tool/policy/capability/host descriptors to allowlisted
  provenance fields;
- declared outputs, stdout, stderr, incidents, agent and target change
  manifests, observation bundles, ledger segments, pcaps, traces, profiles,
  screenshots, and tombstones to named output roles; and
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
- Every captured occurrence traces to the agent exec or target run and original
  inputs; derived summaries cite their raw event ranges and artifacts.
- Kill injection before/after every staging, hash, artifact import, ledger
  reference, acknowledgement, and cleanup point leaves either a reconciled
  capture or an explicit incomplete export—never an unreferenced success.
- Managed blobs, cached content, and read-only views remain unchanged after
  arbitrary upper-layer mutation.
- Replaying the same export idempotency key returns the same occurrences;
  changed path/role/hash conflicts.

## 8. Phase 4: visibility-first Linux target driver

Implement:

- Linux target templates, image/runtime/capability fingerprints, independent
  target generations, and bounded target-run state machines;
- sibling OCI/runc containers with dedicated cgroup/network/mount namespaces,
  exact specimen/fixture materialization, target-private writable layers, and no
  agent workspace or management-authority mount;
- arbitrary direct-argv and explicit-shell streams, target-relative push/pull,
  scoped network endpoints, run stop, health, reset, destroy, and startup
  reconciliation;
- one selected open-source eBPF adapter—Tracee or Inspektor Gadget—with required
  process/syscall/file/network metadata, early-start ordering, container/run
  attribution, coverage, drop counters, and versioned raw output;
- bounded packet rings, protocol summaries, cgroup/process metrics, stdout/
  stderr, authoritative filesystem changes, and crash/OOM evidence;
- deterministic normalization and observation-bundle finalization with raw refs,
  coverage/gaps, changes, incidents, and a cited derived summary; and
- future, non-shipped gVisor/Kata research that would publish reduced or
  guest-provided visibility rather than claiming parity with host eBPF.

### Phase 4 exit gate

- A target cannot read the agent workspace, provider credentials, Docker socket,
  control plane, cache, or another target. Arbitrary in-target commands cannot
  escape their lease/target/run transport scope or acquire infrastructure
  authority.
- Installing packages and debugging tools, pushing a newly built binary,
  opening an interactive shell, and launching multiple processes work without
  adding command-specific API operations. Each action is attributed to a
  `TargetOperationID` and appears in the observation bundle.
- The permanent vertical slice proves a target can fail, finalize evidence,
  reset to a new target generation, and rerun while the same agent workspace and
  provider session continue.
- Fork/exec, representative syscall results, file open/read/write/mutation, DNS/
  connect, packet, exit/signal, OOM, and filesystem changes are attributed to the
  correct target run with documented completeness and latency.
- Collector startup races, overload, kill, target crash, daemon restart, and
  disk full create explicit coverage transitions or gaps and never false
  completeness.
- Each successful or failed run produces exactly one idempotently finalized
  observation bundle whose summary can be regenerated from retained evidence.

## 9. Phase 5: instrumented Android target driver

Implement:

- the selected Cuttlefish/Android Emulator backend, instrumented AOSP image,
  runtime/system-image/device/snapshot capability fingerprints, and independent
  target generations;
- collision-free endpoint/state allocation, virtual-device leaf-cgroup
  placement, headless boot, multi-signal readiness, health, stop, reset,
  destruction, and reconciliation;
- scoped ADB gateway with one-serial visibility and policy-recorded services;
- arbitrary assigned-device ADB services, including shell, sync, install,
  forward/reverse, root/remount, reboot, and agent-managed Frida installation;
- APK/fixture materialization, target-relative transfers, target-run
  finalization, and intervention manifests;
- pinned logcat/dumpsys/Perfetto/packet/screenshot/tombstone/ANR collectors plus
  host runtime and selected-app metrics;
- Frida Java/native hooks for app-process intent APIs, file, socket, crypto, and
  selected security-relevant behavior with precise attached-process coverage;
- optional custom-framework intent/Binder observation in the AOSP image with a
  distinct `android.intent.framework` capability;
- MobSF external static/dynamic adapter with license, provenance, and result
  mapping; and
- Android-specific normalization and observation-bundle summaries.

### Phase 5 exit gate

- Parallel devices never collide on endpoints, writable state, snapshots,
  cgroups, ADB identities, or observation attribution.
- One lease cannot list, select, forward to, or otherwise affect another device.
- An ordinary ADB client can perform arbitrary services against its one assigned
  rooted/debuggable emulator, including installing and replacing Frida server,
  without per-command approval or access to the host ADB server.
- Deliberate Java/native crash, ANR, Android reboot, ADB offline, runtime kill,
  disk full, collector loss, and host OOM have distinct tested outcomes and
  sealed observation bundles.
- App-API intent hooks and framework intent coverage pass different fixtures and
  are never reported as interchangeable.
- Reset advances only target generation; the same agent workspace can compare
  multiple clean Android runs and query their separate bundles.
- Known CPU/memory/I/O, Binder, intent, file, and network workloads are
  attributed within documented completeness, accuracy, latency, and overhead.

## 10. Phase 6: advanced observers and feedback

Add each observer as an independent driver only after its contract and teardown
test exist:

- additional eBPF gadgets/events beyond the required Linux metadata profile;
- targeted `strace` process-tree capture;
- deeper Zeek analysis and packet-payload extraction beyond the required ring;
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

- rootless agent-workspace hardening, plus future non-shipped gVisor/Kata
  research where phase 0 proves compatibility and a new visibility contract;
- signed/pinned images and policy distribution;
- node registration, drain, maintenance, quarantine, upgrade, and version skew;
- backup/snapshot/restore and migration for world state and unfinalized segments;
- dashboards and alerts derived from the same public observation schema;
- capacity benchmarks, warm-pool policy, fairness, and starvation prevention;
- scheduled chaos and KVM Android suites; and
- operator runbooks for leaked mounts, Docker loss, corrupt target state,
  observation-bundle finalization, artifact outage, disk pressure, and state
  reconciliation.

### Phase 7 exit gate

All v1 acceptance criteria below pass on the documented node matrix. Security
review must include the node protocol, Docker authority, scoped ADB gateway,
mount/export path, observer privileges, policy compiler, and artifact adapter.

## 12. Test architecture

### 12.1 Test classes

| Class | Required evidence |
| --- | --- |
| unit | invariants, transitions, policy, resource math, error mapping, event conversion |
| model/property | independent agent/target state sequences, admission fairness, cache pin/GC safety, change/bundle algebra, replay equivalence |
| fuzz | policy YAML, protobuf frames, Docker/ADB/Android/collector events, paths, input/change/bundle manifests, ledger recovery |
| race/stress | concurrent leases, subscriptions, collectors, exports, driver reconciliation |
| contract | every fake and real driver passes one behavior suite; runner host/default and world environment produce equivalent provider outcomes |
| security | agent-target separation, escape corpus, confused deputy, path race, cross-lease/target access, secret/sensitivity leaks |
| fault injection | process kill, error, delay, partial I/O, disk full, OOM, clock jump at each named boundary |
| integration | real Docker, OverlayFS, cgroup v2, PSI, namespace, eBPF, artifact store |
| Android | KVM boot/reset/crash/ADB/logcat/Perfetto/Frida/intent/network behavior |
| performance | metric accuracy/delivery latency, syscall/file/network completeness, subject attribution, cache/view allocation, proxy throughput, collector overhead, bundle sealing, admission response |
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
- agent Docker create/start and guest handshake;
- target create, materialization, collector readiness, run start/stop, target
  change sealing, observation-bundle normalization/summary, reset, and destroy;
- each observer start and readiness acknowledgement;
- metric collection, snapshot reduction, segment publication, subscriber
  fan-out, cursor handoff, staleness transition, and gap publication;
- exec acceptance, process spawn, stream attach, terminal record, and cleanup;
- Android state clone, endpoint reservation, boot, ADB readiness, reset/restore;
- observation frame write, rotation, sync, index, and artifact finalization;
- workspace freeze, diff, file open, hash, staging copy, artifact import, and ack;
- incident acceptance, minimum evidence, publish, recovery decision, and the
  correct independent generation creation; and
- unmount, agent/target release, cgroup removal, and local deletion.

The test harness kills whole processes, not only returns injected errors, to
exercise actual recovery.

### 12.3 Security escape corpus

Run deliberately malicious agent and target containers that attempt:

- `/proc`, `/sys`, host PID, namespace, cgroup, kernel-log, and device access;
- Docker/containerd sockets and common credential/config paths;
- mount, unshare, setns, ptrace of host/other lease, privileged BPF, and raw
  packet operations;
- symlink/hardlink/magic-link/export TOCTOU, `..`, absolute/UNC-like paths,
  Unicode/case collisions, whiteout/opaque tricks, FIFOs/sockets/devices, and
  output floods;
- direct cache-path guessing, cross-view enumeration, excluded-entry access,
  cache-key collision, and malformed or inconsistent input manifests;
- target access to agent workspace files/credentials and agent attempts to mount
  its workspace into a target or turn scoped push/pull into host-path access;
- cross-lease IP/Unix-socket access and management API guessing;
- cross-lease live queries, forged observation filters, capture-profile argument
  injection, and attempts to stop or reconfigure required collectors;
- access to another target/Android serial or the raw host ADB server;
- fork bombs, descriptor exhaustion, disk/inode exhaustion, memory bombs, CPU
  spin, and packet floods; and
- instrumentation disable/evasion and malformed collector output.

The same suite positively proves that command scoping is not a semantic
allowlist: arbitrary shell/argv, pushed binaries, package installation, ADB
shell/sync/install/forward/reverse/reboot, and Frida replacement succeed inside
the assigned disposable target and receive correct operation attribution.

The suite proves the boundary remains intact and that the attempt itself is
visible. It does not claim immunity to unknown host-kernel/runtime exploits;
those motivate the stronger isolation tier and node containment.

### 12.4 CI topology

- Pull request: offline unit/model/fuzz-seed/race/vet/generated-contract tests
  using fake drivers.
- Linux integration: real Docker and unprivileged behaviors on every merge.
- Privileged disposable VM: OverlayFS, cgroups, PSI, namespaces, eBPF, security
  corpus, and forced host-level failures.
- KVM runner: Cuttlefish/emulator boot/reset/crash/collector suite.
- Nightly: race, extended fuzz, Tracee/Inspektor Gadget compatibility fixtures,
  plus a non-shipping gVisor/Kata research visibility matrix.
- Weekly: chaos/soak, high-rate telemetry, pressure/fairness, and disaster
  recovery.

No developer laptop or shared CI host should run destructive pressure or
privileged escape tests directly; use disposable dedicated nodes.

## 13. Provisional performance budgets

Phase 0 must validate or revise these before they become acceptance criteria:

- local control API p95 below 50 ms for non-provisioning operations at 100
  concurrent clients;
- execution-environment transport adds p99 stream latency below 10 ms for 4 KiB
  frames on one node and no stdout/stderr corruption at 50 MiB/s aggregate;
- normal metric interval 2 seconds, incident interval 250 ms for a bounded
  window, and PSI threshold reaction within one configured kernel window plus
  250 ms control latency;
- baseline observation below 3% CPU and 128 MiB manager memory per active lease
  at the representative workload event rate, with host-global observer cost
  reported separately;
- normal live samples reach a local subscriber within 500 ms p95 after
  collection and incident-resolution samples within 100 ms p95, with collection
  time and delivery delay separately reported;
- zero dropped control/incident records; every bulk loss produces a gap record
  no later than the current segment rotation;
- required Linux metadata coverage includes the target process tree and selected
  syscall/file/network fixture set at a phase-0-measured event rate; any loss is
  reported before bundle finalization;
- target-run finalization publishes a queryable bundle within 5 seconds p95 for
  metadata-only fixtures after workload exit, excluding separately reported
  artifact-upload latency;
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
| agent/target generation | session -> agent workspace or target transition -> node resource -> event envelope -> incident/bundle -> artifact/VR activity, without updating the sibling generation |
| target run | typed run request -> materialization manifest -> collector plan/readiness -> workload -> raw/normalized events -> coverage/change seal -> observation bundle -> agent/host view |
| resource request/limit | policy -> admission -> cgroup/emulator/container plan -> metric labels -> pressure decision -> incident feedback -> final activity environment |
| capability | driver probe -> fingerprint -> policy decision -> public lease view -> downgrade/failure event -> affected agent/target generation provenance |
| input view | artifact selection -> canonical manifest -> cache scope/lookup/population -> read-only view -> pin -> mount -> release/GC -> activity and incident provenance |
| workspace member | input-view manifest -> lower mount -> mutation event -> sealed change set -> export -> artifact occurrence -> VR activity material |
| clock information | source collector -> sync epoch -> event envelope -> segment index -> timeline query -> incident window -> captured trace metadata |
| sensitivity/retention | policy -> collector -> live authorization -> local segment -> OTLP/export filter -> artifact role -> cleanup acknowledgement |
| failure/recovery | driver fact -> classifier -> incident -> affected exec/run outcome -> correct independent generation transition -> bundle/artifact evidence -> host observation command |

Add automated descriptor/field coverage tests where possible. Generated
protobuf-to-domain/view adapters use common mapping helpers, and tests fail when
a new field is not deliberately mapped or ignored with rationale.

## 15. Cross-repository work

### This repository

Own all phases above, including the first-party forensic adapter and integration
tests that import the adjacent modules at released versions or temporary local
replacements in development.

### `go-agent-runner`

The runner now has the required `ExecutionEnvironment` contract. Its nil value
is local-host execution. The world adapter uses the same interface for probes
and attempts, resolves executable identity within the agent workspace,
transports temporary inputs as bytes, preserves separate ordered streams and
callback backpressure, and confirms process-tree and temporary-input cleanup.

Keep provider-specific flag construction, protocol decoding, retries, sessions,
schema validation, and terminal authority in the runner. Compatibility tests in
both repositories must pin the shared interface behavior. Do not add a second
execution path or provider-aware logic to this repository.

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

1. One hundred agent workspaces with mixed Linux target runs and the configured
   Android capacity run concurrently under the race detector and real-node
   stress without broken state, cross-lease/target visibility, or unbounded
   growth.
2. Provider probes and executions through the world `ExecutionEnvironment` are
   byte-for-byte compatible with the runner's host default, including remote
   temporary inputs, executable-generation invalidation, failure, limits,
   cancellation, and cleanup semantics.
3. The malicious escape corpus cannot access host management authority, another
   lease, another input view, another target, the node cache, artifact
   repository, or agent workspace from a target. The agent cannot obtain the
   Docker socket, raw ADB authority, or privileged collector control.
4. Inside its assigned target, the agent can execute arbitrary Linux commands,
   transfer arbitrary workspace-visible tools, and use arbitrary device-scoped
   ADB services—including Frida installation—without semantic approval. These
   actions cannot select another target or host service and are recorded with
   operation and intervention provenance.
5. Different frozen selections expose exactly their authorized entries. Exact
   views are reused; overlapping views share physical content allocation; and
   release, crash reconciliation, and bounded GC leave neither deleted live
   inputs nor unbounded abandoned copies.
6. Agent and target changes are represented separately as added/modified/
   deleted/renamed/metadata/opaque state; every exported or automatically
   captured byte is safely opened, hashed, and traceable to immutable inputs and
   one agent exec or target run.
7. Killing each component at every named fault boundary never yields a false
   success, hidden crash, silently reused generation, or committed reference to
   missing bytes.
8. Agent workspace failure, target process exit, Linux target OOM/failure,
   Android runtime death, Android app crash/ANR, ADB loss, host pressure
   eviction, collector loss, and workspace integrity failure are distinguishable
   and evidence-backed.
9. Target reset/restore/cold boot always creates a new `TargetGeneration`
   without changing a healthy `AgentGeneration`. A failed agent workspace
   creates a new agent generation without rewriting target history.
10. Live snapshots and activity/performance subscriptions separately attribute
   agent workspace, Linux target/run, Android host/guest/app, observer, and
   selected-process work. They are resumable; slow consumers cannot stall work;
   every stale, unsupported, lost, or compacted interval is explicit.
11. Admission prevents configured control-plane reserve exhaustion, and pressure
   shedding follows priority/preemptibility policy with a durable rationale.
12. Every target run produces exactly one idempotently finalized observation
    bundle with native/raw refs, normalized events, target changes, coverage and
    gaps, incidents, and a reproducible cited summary. Every observer has bounded
    resource use, sensitivity metadata, provenance, and verified teardown.
13. Ledger replay after random process termination matches materialized state or
    stops with a precise, recoverable inconsistency; it never repairs
    destructively without an explicit action.
14. A full end-to-end run can be recorded as a forensic activity and then as a
    VR activity/observation by the host, preserving shared correlation and
    causation IDs.
15. `make verify` (or the final cross-platform equivalent) is the single
    non-interactive verification entry point and emits machine-readable test,
    race, fuzz, security, integration, and benchmark summaries.

## 17. Decisions to close during phase 0

The architecture recommends a direction but requires evidence for these exact
choices:

- rootless Docker versus userns-remapped rootful Docker for the agent workspace,
  and the exact hardened runc profile for visibility-first Linux targets;
- whether `world-node` owns a tiny separate mount helper or keeps mount/open
  operations in its already trusted process;
- the production reflink-capable backing filesystem, cache integrity mode,
  security-scope defaults, high/low watermarks, and whether high view
  cardinality warrants packed immutable images;
- Tracee versus Inspektor Gadget for the required Linux metadata contract,
  including license, embedding/process boundary, startup races, completeness,
  schema stability, drop accounting, and overhead;
- future non-shipping gVisor/Kata compatibility/performance research and the
  exact target visibility lost or replaced by in-guest collection;
- Cuttlefish, snapshots, scale-out operation, and custom-AOSP framework
  instrumentation beyond the shipped Android SDK Emulator clean-boot backend;
- continuing scoped-ADB protocol fuzzing and qualification of the shipped
  host-global/cross-serial deny boundary;
- the exact protobuf segment framing/compression and local segment size;
- safe provider credential delivery that supports each CLI without mounting a
  general host home directory;
- whether declared outputs are usually returned in the agent's structured
  result, submitted through `world-export`, or both with one shared validation
  path; and
- production retention tiers and authorization for decrypted traffic,
  screenshots, memory, and device identifiers.

These choices can alter driver implementations and deployment profiles. They do
not alter agent/target separation, independent generations, required visibility,
observation-bundle, explicit-incident, safe-export, or causal-ledger invariants.

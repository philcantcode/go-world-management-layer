# World management layer design

- Status: proposed v1 architecture
- Date: 2026-07-24
- Initial deployment: dedicated Linux hosts

## 1. Summary

The world management layer is the operational boundary between an autonomous
agent and the machines, containers, emulators, physical phones, workspaces, and
observers on which it acts. It must make a healthy environment feel ordinary to
the agent while retaining exclusive control over provisioning, containment,
resource budgets, evidence movement, monitoring, crash recovery, and teardown.

The system is a single-host control plane first. A trusted host application
acquires an environment lease, gives `go-agent-runner` a lease-specific
`worldexec` executable, consumes live observations, and releases the lease.
`worldexec` forwards the provider CLI's ordinary process interface into the
leased container. The provider and every tool it starts remain inside the
container.

Version 1 targets dedicated Linux nodes. OverlayFS, cgroup v2 pressure signals,
Linux namespaces, eBPF, and KVM are part of the actual design, not portable
implementation details. A Windows or macOS client may call a remote Linux node,
but Docker Desktop is not the initial security or performance reference.

## 2. Ecosystem boundaries

| System | Owns | Does not own |
| --- | --- | --- |
| `go-agent-runner` | provider selection, CLI protocol normalization, provider session recovery, schema validation, provider process result | container/device lifecycle, host containment, workspace custody, system telemetry |
| this repository | environment leases and generations, runtime/device/workspace lifecycle, containment, live operational telemetry, incidents, pressure decisions | analytical conclusions, provider protocol interpretation, immutable forensic custody |
| `go-forensic-artifacts` | immutable bytes, occurrence identity, selections, projections, capture, fixity, provenance | hostile-code sandboxing, container/device lifecycle, live scheduling |
| `go-vr-research-framework` | observations, hypotheses, claims, findings, research activities, review, analytical history | processes, agents, raw bytes, environment scheduling |
| campaign manager / host | authentication, authorization, policy selection, correlations across systems, translating accepted results into domain commands | bypassing any subsystem's invariants |

The dependencies point inward through narrow host-owned interfaces. The world
manager does not import the VR research core. A first-party artifact adapter may
import `go-forensic-artifacts`, but the domain and driver packages depend on a
world-owned `MaterialAuthority` interface.

## 3. Goals

- Run arbitrary agent-selected tools inside a leased environment without a
  per-command allowlist or host execution path.
- Ensure no provider or tool process executes on the host through the bridge.
- Materialize immutable inputs without exposing the artifact repository.
- Give each invocation a different frozen artifact view without repeatedly
  copying shared bytes or leaking entries from another view.
- Preserve an authoritative change set and capture only safe, explicitly
  selected outputs back into the artifact authority.
- Manage Docker containers, Android emulators, and physical Android devices
  through one capability-aware lease model without pretending their guarantees
  are identical.
- Stream live health, lifecycle, activity, and performance data from the host,
  container, emulator, Android guest, and important process scopes.
- Use host and per-lease pressure to control admission and reduce concurrency
  before the host becomes unusable.
- Detect environment, workload, observer, and device failures; record them as
  incidents; capture evidence; and provide actionable feedback.
- Restore emulator snapshots or recreate containers only as explicit new
  generations. Never hide a crash behind an apparently continuous session.
- Make every mutation idempotent, every long operation cancellable, every
  stream bounded, and every state reconstructable after a daemon crash.
- Support adversarial, crash, race, resource-exhaustion, and observer-effect
  testing from the first vertical slice.

## 4. Non-goals

- Interpreting Claude, Codex, Grok, or another provider's streaming protocol.
- Acting as an analytical knowledge graph or deciding whether a vulnerability
  conclusion is valid.
- Replacing the forensic artifact store with a second byte authority.
- Claiming that ordinary Docker containers are a perfect boundary against a
  hostile kernel exploit.
- Claiming exact causality from timestamps alone. Explicit parentage and trace
  context are causal; time proximity is correlation with stated confidence.
- Transparently continuing an agent process after its container, emulator, or
  device failed.
- Promising snapshot rollback for a physical phone.
- Capturing every payload by default. High-volume or invasive observation is
  activated by an authorized policy or request within that policy.

## 5. System invariants

1. Every executing provider and descendant belongs to exactly one lease,
   environment generation, container, and cgroup subtree.
2. The agent never receives the Docker socket, host PID namespace, host root,
   artifact repository path, raw USB bus, control-plane credentials, or a
   general lifecycle/management socket. The only guest-facing management write
   is an append-only, lease-scoped export declaration.
3. A workspace lower layer is immutable. Writes go only to the lease's upper
   layer. A new run never reuses an unsealed upper layer unless the host
   explicitly requests the same generation.
4. An input view contains exactly the entries in its canonical manifest. The
   agent cannot enumerate the node cache, another view, or excluded artifact
   metadata. Shared physical extents do not imply shared logical visibility.
5. Output capture accepts logical relative paths, not host destinations. It
   opens paths beneath the workspace without following symlinks and copies bytes
   through a host-owned artifact adapter.
6. Provider stdout is never mixed with management records. Doing so would
   corrupt the provider's machine-readable protocol.
7. A crash, OOM, disconnect, forced eviction, restore, cold boot, or observer
   gap is externally visible and durably recorded.
8. Recovery creates a new monotonically increasing generation. The failed
   generation remains addressable in the ledger and incident record.
9. Control and incident events are loss-intolerant. Bulk collectors may be
   sampled or dropped only if a typed gap event states the source and range.
10. The effective policy and capability fingerprint are frozen for a generation.
   A runtime, image, emulator, snapshot, tool, or collector change requires a
   new generation.
11. Host pressure decisions are policy decisions with inputs and rationale, not
    invisible scheduler behavior.

## 6. Component architecture

```text
trusted host application
  | acquire / release / incidents / observations / exports
  v
worldd (unprivileged control plane)
  |-- scheduler and admission controller
  |-- lease/generation state machines
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
  |-- emulator/ADB/device adapters
  |-- network namespace and scoped device proxy
  `-- observer supervisor
             |
             +-- container: world-guest (PID 1) -> provider CLI -> tools
             +-- emulator process + Android guest
             `-- physical phone lease

go-agent-runner -> lease-specific worldexec -> worldd -> world-guest
```

### 6.1 `worldd`

`worldd` owns logical truth: requests, leases, generations, effective policies,
idempotency, transitions, incidents, export intents, observation cursors, and
admission decisions. It has no general command-execution API and does not accept
arbitrary host paths from clients.

### 6.2 `world-node`

`world-node` is the only component that can access Docker Engine, mount
workspaces, place processes in cgroups, allocate emulator ports, talk to raw ADB
or USB, and attach privileged observers. Its local API accepts a resolved plan,
not arbitrary shell commands. It independently validates image allowlists,
mount roots, device assignments, cgroup roots, observer permissions, and lease
ownership.

Access to the Docker socket is root-equivalent and must never be delegated to
the agent container. Rootless Docker is preferred where compatible. A stronger
runtime such as gVisor is a policy-selectable isolation tier for hostile
toolchains; capability probing must reveal incompatibilities with ptrace, eBPF,
or device access rather than silently weakening either isolation or observation.

### 6.3 `world-guest`

`world-guest` is a small, versioned container supervisor running as PID 1. It:

- starts the requested pinned provider executable directly with an argument
  vector and explicit working directory;
- creates and kills a process group for each exec;
- forwards raw stdin, stdout, stderr, resize, signal, and exit status;
- reports fork/exec/exit identity needed for causal attribution;
- exposes a narrow read-only observation helper and a separate append-only
  export-declaration helper;
- never has host credentials or artifact-store access; and
- exits if its lease identity or host heartbeat becomes invalid.

The guest protocol is framed and versioned, but the provider byte streams inside
the frames remain opaque.

### 6.4 `worldexec`

`worldexec` is a lease-specific host executable passed through
`agentrunner.Request.Executable`. It lets the current runner continue to probe
and invoke an executable normally. A private adjacent descriptor binds the shim
to one lease, generation, and logical provider executable. The descriptor is
owned by the trusted host user, expires with the lease, and is never mounted in
the container.

The working directory exists at the same absolute path on the host and inside
the container, so provider flags referring to that directory remain valid.
The host path contains only the mounted lease workspace, not a broader host
directory.

On a normal run, the shim is byte-transparent. A policy-controlled host-side tee
may retain bounded raw stdin/stdout/stderr evidence without delaying or changing
delivery; capture truncation or failure is explicit and never changes provider
semantics. On management failure the shim:

1. closes the provider streams;
2. requests cancellation of the remote process group;
3. waits for bounded cancellation confirmation;
4. writes one bounded diagnostic containing an incident ID to stderr; and
5. exits non-zero.

The detailed incident and evidence remain available out of band. A later native
execution-backend interface in `go-agent-runner` can remove the shim without
changing world leases or guest semantics.

## 7. Domain model

### 7.1 Primary identities

- `EnvironmentID`: logical requested environment across generations.
- `Generation`: monotonically increasing realization number.
- `LeaseID`: exclusive authorization to use one generation.
- `ExecID`: one provider or management execution inside a generation.
- `TargetID`: container, emulator, or physical-device resource identity.
- `InputViewID`: domain-separated digest of the canonical selection, layout,
  metadata, and visibility manifest.
- `WorkspaceID`: immutable lower plus one upper/work/merged set.
- `ObservationCursor`: durable position in the observation ledger.
- `IncidentID`: immutable failure or hazardous-state record.
- `CaptureID`: packet, trace, log, profile, or filesystem capture.
- `ExportID`: explicit output-capture transaction.

Resource and operation IDs are opaque UUIDv7 values with human-readable type
prefixes. `InputViewID` is content-addressed so an exact view is reusable.
Docker IDs, PIDs, ADB serials, emulator ports, cgroup paths, and host paths are
attributes, never public identity.

### 7.2 Environment state machine

```text
requested -> admitted -> provisioning -> booting -> ready -> leased
                                                       |
                                                       v
                                                    running
                                                       |
                                  +--------------------+-------------------+
                                  v                    v                   v
                               quiescing           capturing            failed
                                  |                    |                   |
                                  +-----------> releasing <---------------+
                                                   |
                                                   v
                                                released

Any non-terminal state may enter quarantined or lost after a recorded incident.
Restore/recreate begins a new generation at provisioning; it is not a backward
transition of the failed generation.
```

The implementation uses one shared transition guard and a declarative table.
Drivers report observations; they do not update state directly. Replaying
accepted transition records must reconstruct the same current state.

### 7.3 Capability fingerprint

Before admission, each driver reports tri-state capabilities (`supported`,
`unsupported`, `unknown`) plus constraints and immutable version evidence:

- Docker Engine/API/runtime/storage/cgroup versions;
- kernel, cgroup v2, OverlayFS, user namespace, KVM, eBPF, fanotify, and
  `openat2` support;
- emulator binary, system image digest, AVD configuration digest, snapshot
  fingerprint, acceleration, and Perfetto availability;
- physical-device build fingerprint, API level, root/debuggable state, battery,
  authorization, and supported recovery actions;
- observer versions and required privileges; and
- artifact-adapter instance and protocol versions.

Required unknown capabilities fail admission. Preferred features may downgrade
only through an explicit effective-policy record and client-visible warning.

## 8. Public control contract

The canonical API is versioned protobuf over mutually authenticated gRPC. A
Unix-domain socket is the first deployment. A small Go client and `worldctl`
use the same contract.

### 8.1 Lifecycle operations

- `AcquireEnvironment(request) -> lease`
- `GetEnvironment(environment, generation) -> view`
- `WaitEnvironment(lease, desired_state) -> view`
- `RenewLease(lease, expected_revision, ttl) -> lease`
- `ReleaseEnvironment(lease, reason) -> outcome`
- `RequestRecovery(incident, mode) -> new_generation`
- `QuarantineTarget(target, reason) -> target`

Every mutation requires an idempotency key, expected revision where applicable,
correlation ID, causation ID when explicit, authorized policy reference, and
deadline. Retrying the same key and payload returns the original result;
reusing a key with different input conflicts.

The acquire request contains an `InputViewSpec`, not a host path. It identifies
a frozen selection or explicit immutable occurrences, layout/path mapping,
allowed sidecars, cache security scope, and whether zero-copy view construction
is required. The returned lease freezes the resolved `InputViewID` and complete
manifest digest.

### 8.2 Execution transport

`OpenExec` is a bidirectional framed stream. The start frame names a logical,
policy-allowed executable and carries an argv vector, working directory,
terminal settings, and exec idempotency key. Subsequent frames carry raw stream
bytes, signals, resize events, heartbeats, and a single terminal outcome.

The API never offers `host_shell`, arbitrary host executable paths, Docker API
passthrough, or arbitrary mount creation.

### 8.3 Observation and performance

- `SubscribeObservations(filter, after_cursor)` returns ordered ledger records.
- `SubscribeMetrics(filter, resolution, after_cursor)` returns live metric
  samples and pressure transitions.
- `StartCapture(lease, capture_spec)` activates only capabilities allowed by the
  effective policy.
- `StopCapture(capture)` finalizes and returns capture metadata.
- `GetIncident(incident)` returns facts, correlations, recovery, and evidence
  references.

Subscriptions are resumable. A slow subscriber never blocks collectors or an
agent process. It receives a typed gap/compaction record and resumes from a
durable cursor or an artifact-backed segment.

Inside the container, `world-observe` can read only the current lease's
authorized event and metric views through a short-lived guest capability. It
cannot change collectors, lifecycle, policy, resources, or other leases. This
gives an agent useful live feedback without making the observation channel part
of ordinary tool execution.

### 8.4 Output capture

- `DeclareExport(lease, relative_paths, roles)` records an export intent.
- `PreviewChangeSet(lease)` returns added, modified, deleted, renamed, and
  metadata-only changes.
- `CommitExport(export, expected_workspace_revision)` seals selected files,
  copies them to the artifact adapter, and returns qualified occurrence refs.

The agent may declare paths through a narrow `world-export` guest helper, or the
host may translate the agent's structured result into the same API. Declaration
does not itself copy or trust a file.

## 9. Workspace and artifact flow

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
5. A dedicated OverlayFS is mounted with that view as `lowerdir` and new
   lease-owned `upperdir` and `workdir` directories on the same filesystem.
6. Only the merged workspace is bind-mounted into the container. The cache,
   view parent, artifact repository, lower-layer parent, upper-layer parent,
   and mount-control paths are not visible.

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

Each active or recoverable generation pins its `InputViewID`. Release removes
the generation's upper/work tree after output and incident finalization, then
drops the view pin. Unpinned views are retained by TTL/LRU and removed under a
high-water policy. Content entries are removed only after no view or in-flight
builder references them. Reference counts are transactionally recorded but are
reconciled after a crash from leases, mount state, view manifests, and the
filesystem; correctness never depends on timely garbage collection.

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

### 9.3 Workspace layout

```text
<node-root>/leases/<lease-id>/<generation>/
  lower -> pinned cached input view
  upper/          agent changes only
  work/           OverlayFS work directory
  merged/         sole workspace mount exposed to the container
  captures/       host-owned temporary capture staging
  ledger/         host-owned observation segment staging
```

Mount propagation is private. The merged mount is `nodev` and `nosuid`; the
container root filesystem is read-only except for explicit tmpfs locations and
the workspace. `upper` and `work` are never reused across generations.

### 9.4 Authoritative change set

OverlayFS deletions and opaque directories use whiteout semantics, so a naive
directory diff is insufficient. Finalization combines:

- the immutable lower manifest;
- an overlay-aware upper scan, including whiteouts and opaque directories;
- the stable merged-tree view;
- file-event observations; and
- hashes and metadata obtained from already-open file descriptors.

Incremental events make the diff fast and explain timing; the final sealed scan
is authoritative. Disagreement becomes an incident rather than being silently
resolved.

### 9.5 Safe export

Export paths must be normalized relative paths. The node opens each component
under a pre-opened workspace directory using `openat2`-style beneath/no-magic-
link/no-symlink constraints, rejects non-regular files unless a future typed
export supports them, checks file/byte quotas, hashes the open descriptor, and
copies from that descriptor. It never resolves a user path and later reopens it.

Only explicit paths and policy-mandated crash/trace outputs are committed. The
complete change set is still retained as metadata so an analyst can see what
was not exported. Managed lower bytes are reverified after execution. Artifact
capture records the world environment, generation, exec, image digest, policy
digest, input refs, output roles, and observation-segment refs as provenance.

## 10. Container isolation and lifecycle

The standard hostile-workload profile uses:

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
- one container per active lease so container destruction is a valid final
  kill domain.

Instrumentation privileges do not go into the agent container by default. A
host observer or separate observer namespace attaches by cgroup, PID namespace,
or network namespace. An invasive profile that needs `ptrace` is separately
named, capability-probed, and recorded.

Container readiness requires Docker start, `world-guest` protocol handshake,
workspace verification, observer readiness, and provider executable probe. A
health check alone is not readiness authority.

## 11. Android target management

### 11.1 Emulator

Each emulator lease uses an isolated writable AVD data directory or clone, fixed
console/ADB ports, a recorded system-image and emulator fingerprint, a cgroup
under the lease, and a dedicated network path. Readiness requires:

- the emulator process is alive;
- ADB reports the expected serial as `device`;
- boot completion and package manager readiness checks pass;
- Android build fingerprint matches the plan; and
- baseline collectors have started or explicitly downgraded.

The manager can save, list, load, and delete named snapshots. A baseline
snapshot is immutable by policy. Before a restore, the failed generation is
sealed and crash evidence is captured. Restore always increments the generation
and emits an incident/recovery pair. Snapshot validity is tied to emulator,
system image, AVD configuration, and feature fingerprints; invalidity triggers
an explicit cold boot or fails according to policy.

Baseline emulator observation includes host-process/cgroup metrics, emulator
stderr, ADB state, all permitted logcat buffers, boot properties, and network
flow metadata. Policy may add the emulator's packet-capture facility, Perfetto,
bugreport, tombstones, ANR traces, screenshots/screen recordings, Frida, or
mitmproxy.

### 11.2 Physical phone

A physical device is exclusively reserved before an agent can address it. The
agent never sees raw USB. A scoped ADB gateway exposes only the assigned serial,
hides all other devices, rejects host-level ADB services, records service
requests, and closes when the lease ends. Network policy prevents bypassing the
gateway to another host ADB server.

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

Every lease exposes:

1. an activity stream: lifecycle, input-view/cache, process, file, network,
   device, policy, incident, capture, and export events; and
2. a performance stream: host, aggregate lease, container, emulator, Android
   guest, physical device, and selected process metrics.

Both streams carry the same environment, generation, lease, exec, subject, and
clock-domain attributes. Metric samples can link to active spans or execs and
can be queried around an incident without pretending the sample caused it.

### 12.2 Baseline collectors

The recommended always-on baseline is low-overhead and metadata-first:

- world lifecycle and policy decisions;
- input-view resolution, cache hit/miss/population/verification, pin/release,
  eviction, and reconciliation events;
- Docker Engine event stream and detailed container stats;
- host and per-cgroup CPU, memory, I/O, pids, OOM, and pressure-stall signals;
- emulator process metrics, ADB state, Android logcat crash/system/events
  buffers where permitted, battery, thermal, and disk state;
- process fork/exec/exit/signal/OOM events keyed by cgroup;
- workspace mutations and authoritative final diff;
- DNS and connection/flow metadata without payloads; and
- collector health, queue depth, bytes produced, and dropped-record counters.

The host itself is a monitored subject. CPU run queue, memory availability,
swap, PSI, disk bytes/inodes, I/O latency, network saturation, thermal state,
KVM availability, Docker daemon health, and collector overhead feed admission.

### 12.3 Triggered and on-demand collectors

| Collector | Best use | Important constraint |
| --- | --- | --- |
| eBPF/fanotify | targeted process, syscall, file, and network metadata | kernel/capability dependent; report ring-buffer loss |
| `strace` | exact syscall diagnosis for one process tree | high overhead and timing effect; not baseline |
| `tcpdump`/tshark | packet evidence and protocol diagnosis | sensitive and high volume; rotate bounded ring files |
| mitmproxy | authorized HTTP/TLS semantic capture | changes trust/network behavior; pinning may prevent it |
| Perfetto | Android scheduling, CPU, Binder, app/system timelines | buffers and tracing services vary by Android version |
| Frida | authorized function/runtime instrumentation | root, repackaging, gadget, or debugger may be required |
| screenshot/screen recording | visible Android state | sensitive, bandwidth-heavy, and not causal alone |
| heap/profile capture | resource root-cause analysis | stop-the-world or sampling overhead varies |

Every injected collector has a manifest: purpose, question, target, version,
configuration digest, required privileges, start/stop times, expected overhead,
actual overhead, outputs, teardown result, and observer-effect warning.
Instrumentation teardown is verified. An orphaned capture is an incident.

### 12.4 Metric model

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
the underlying measurement.

## 13. Causal ledger

### 13.1 Event envelope

Every observation has:

```text
schema_version, event_id, kind
environment_id, generation, lease_id, exec_id
correlation_id, causation_id, trace_id, span_id
source, source_instance, source_sequence, source_cursor
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

## 14. Pressure-aware admission and shedding

Each lease has resource requests, hard limits, priority, preemptibility, and a
cost estimate. Containers and emulator host processes are placed under one
lease cgroup subtree so aggregate pressure is visible. Physical-device host
helpers also belong to it, though device-side consumption is reported
separately.

Admission evaluates allocatable CPU/memory/storage/pids/devices, warm-pool cost,
current PSI trends, requested observers, snapshot memory spikes, and safety
headroom. It records all inputs and the selected effective budget.

Pressure response is ordered:

1. increase metric resolution and verify the signal;
2. stop admitting new work for the constrained resource;
3. expire unused reservations and reduce unleased warm pools;
4. stop or snapshot idle emulators within policy;
5. ask preemptible active leases to quiesce before a deadline;
6. capture minimum incident evidence and revoke the lowest-priority
   preemptible lease if the host remains at risk; and
7. protect the control plane and quarantine the node if safety cannot be
   restored.

Active work is not silently `docker pause`d because that can look like an agent
stall and corrupt timeout semantics. A forced eviction fails the active exec,
creates a `resource_eviction` incident, preserves the generation, and supplies
the incident ID to the runner/host.

Hard cgroup limits remain the final containment boundary. `memory.events`, OOM
events, pids events, throttling, and PSI distinguish a workload limit from
host-wide contention.

## 15. Failure, incident, and recovery semantics

### 15.1 Classification

- `workload_exit`: provider/tool/app process failed while the environment lived.
- `container_failure`: container died, OOMed, became unhealthy, or lost guest
  supervision.
- `emulator_failure`: QEMU/emulator process died, hung, or lost required ADB
  readiness.
- `android_failure`: app crash, native crash, ANR, system_server restart, or
  Android reboot while the emulator process survived.
- `device_disconnect`: physical device changed authorization/state or vanished.
- `host_pressure` / `resource_eviction`: admission or safety action.
- `observer_failure`: required collector failed, overflowed, or could not tear
  down.
- `workspace_integrity`: lower changed, overlay disagreement, unsafe export,
  or quota violation.
- `control_plane_failure`: lost node, Docker daemon, database, or protocol.

### 15.2 Incident record

An incident contains immutable facts, affected generation and exec, trigger,
last known state, exit/signal/OOM evidence, high-water metrics, relevant event
ranges, artifact refs, recovery actions, and visibility acknowledgements. It
separates:

- proven cause;
- likely correlation with method and confidence; and
- unknown cause.

The generated agent feedback is factual and bounded: what failed, relevant
limits/high-water values, whether the agent's last action is proven or merely
nearby, the incident ID, which outputs survived, and concrete safer retry
options. It never fabricates a successful tool result or blames an agent from
timing alone.

### 15.3 Recovery order

1. Freeze mutation and mark the exec failed.
2. Capture the policy-mandated minimum evidence under a strict emergency budget.
3. Seal ledger and workspace segments, including any capture gaps.
4. Publish the incident to the live stream and terminal shim diagnostic.
5. Decide quarantine, teardown, snapshot restore, cold recreate, reboot, or
   human action according to policy and capabilities.
6. If recovery is authorized, create a new generation with an explicit link to
   the incident and previous generation.
7. Supply the host with recovery context for a new agent invocation. Do not
   resume the old process as though nothing happened.

## 16. Policy model

Policies are host-owned, immutable-by-digest YAML documents. They define
allowed environment shapes and maximum powers, not an agent-editable checklist.
The compiler uses strict decoding, rejects unknown fields, validates cross-field
invariants, resolves defaults, probes capabilities, and emits a canonical
effective-policy document for the generation.

Policy areas are:

- runtime/image/isolation tier;
- input-view selection/layout, cache security scope/construction, workspace
  quotas, and export rules;
- network mode and sensitive capture permissions;
- container and aggregate lease resources;
- emulator or physical-device requirements;
- baseline, triggered, and on-demand observation;
- sampling, buffers, retention, and sensitivity;
- incident evidence minimums and recovery modes; and
- priority, preemptibility, lease TTL, and pressure behavior.

An on-demand observation request can narrow or activate a permitted collector;
it cannot grant itself root, expand network access, choose another device,
increase a hard retention ceiling, or bypass redaction. Denials are observable
policy decisions.

See [the example policy](examples/environment-policy.yaml).

## 17. Driver and helper boundaries

Stable world-owned ports keep vendor APIs out of the domain:

```go
type RuntimeDriver interface {
    Probe(context.Context) (CapabilitySet, error)
    Provision(context.Context, RuntimePlan) (Runtime, error)
    OpenExec(context.Context, ExecPlan) (ExecTransport, error)
    Inspect(context.Context, RuntimeID) (RuntimeStatus, error)
    Stop(context.Context, RuntimeID, StopMode) error
    Destroy(context.Context, RuntimeID) error
}

type TargetDriver interface {
    Probe(context.Context, TargetSelector) (CapabilitySet, error)
    Reserve(context.Context, TargetPlan) (Target, error)
    Ready(context.Context, TargetID) (TargetStatus, error)
    Snapshot(context.Context, TargetID, SnapshotPlan) (Snapshot, error)
    Restore(context.Context, TargetID, SnapshotRef) error
    Release(context.Context, TargetID) error
}

type MaterialAuthority interface {
    ResolveInputView(context.Context, InputPlan) (InputViewManifest, error)
    OpenContent(context.Context, ArtifactOccurrence) (ContentReader, error)
    CaptureOutputs(context.Context, OutputPlan) ([]ArtifactOccurrence, error)
    CaptureObservation(context.Context, CapturePlan) (ArtifactOccurrence, error)
}
```

Workspace, observer, ledger, and resource-controller ports follow the same
shape. Shared patterns are extracted once: transition validation, capability
requirements, safe beneath-path opening, bounded stream pumping, idempotency,
clock conversion, collector supervision, and fault injection. Provider adapters
must not each reimplement these guarantees.

## 18. Cross-repository integration

### 18.1 `go-agent-runner`

Initial integration needs no runner protocol change:

- acquire a world lease first;
- set `Request.Executable` to the lease-specific `worldexec` path;
- set `Request.WorkingDir` to the same-path workspace mount;
- forward runner `OnEvent` observations into the campaign manager with the same
  correlation/exec IDs; and
- combine `RunError` with the world incident when the shim reports one.

A later runner `ExecutionBackend` should preserve its existing process
supervision contract while allowing world-native cancellation and structured
incidents. That change belongs in the runner repository and must not duplicate
provider normalization here.

### 18.2 `go-forensic-artifacts`

The first-party adapter uses existing freeze/projection metadata, `OpenObject`
streaming, activity capture, and verification facilities. The world manager
does not derive or expose forensic CAS paths, and it does not materialize a full
copy for every lease. If the exact canonical view manifest cannot be expressed
through the current artifact API, the narrow addition belongs in the artifact
repository rather than duplicating projection semantics here. World output,
stdout/stderr, ledger segments, pcaps, Perfetto traces, screenshots, tombstones,
and incident reports become immutable occurrences with a single execution
activity and named input/output roles.

The artifact store's wrapped runner is not used as a sandbox; its own design
correctly states that it is only an execution/capture boundary.

### 18.3 `go-vr-research-framework`

The campaign manager records a completed world exec as a VR `RecordActivity`
using artifact refs for inputs, outputs, logs, and attestations. Directly
measured incident and telemetry conclusions become typed `RecordObservation`
commands. The world manager does not create hypotheses, claims, findings, or
authoritative VR events.

Correlation and causation IDs flow unchanged through world, artifact activity,
and VR command metadata so the host can join operational evidence to analytical
history without collapsing the systems into one database.

## 19. Security model

### 19.1 Adversaries

- an agent deliberately or accidentally attempting host access;
- hostile APKs, binaries, source trees, build scripts, and dependencies;
- a compromised tool inside the container;
- malicious paths, symlinks, hardlinks, devices, sockets, whiteouts, archives,
  or output floods;
- a target app attempting anti-instrumentation, traffic evasion, or collector
  exhaustion;
- confused-deputy requests against Docker, ADB, artifact, or export APIs; and
- daemon crashes, host reboots, disk exhaustion, OOM, clock jumps, and partial
  writes.

### 19.2 Trust boundaries

The host application, `worldd`, `world-node`, policy store, and artifact adapter
are trusted. The agent container, provider, tools, inputs, Android apps, and
device responses are untrusted. `world-guest` is trusted for orchestration but
runs in the same compromise domain as the container; host enforcement never
depends solely on it.

Physical devices and emulators may attack ADB, USB, network, parsers, or
collectors. Those adapters are separate processes with bounded inputs and least
privilege. Captured content is untrusted evidence, not safe structured data.

### 19.3 Isolation tiers

- `standard`: rootless/user-namespaced Docker, hardened OCI profile.
- `sandboxed-kernel`: gVisor-compatible workloads with reduced syscall attack
  surface; capability differences are explicit.
- `instrumented`: narrowly grants observer abilities without granting them to
  the agent process.
- `device-lab`: dedicated node/USB/network isolation for physical phones.

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
- Android logcat provides multiple buffers and monotonic/epoch formats, subject
  to device permissions: <https://developer.android.com/tools/logcat>.
- gVisor can be selected as a Docker runtime but has distinct filesystem,
  network, and debugging behavior: <https://gvisor.dev/docs/user_guide/quick_start/docker/>.
- OpenTelemetry correlates traces, metrics, and logs but is an export framework,
  not the durable world ledger: <https://opentelemetry.io/docs/concepts/signals/>.
- Frida and mitmproxy are invasive, capability-dependent collectors rather than
  invisible defaults: <https://frida.re/docs/modes/> and
  <https://docs.mitmproxy.org/stable/concepts/how-mitmproxy-works/>.

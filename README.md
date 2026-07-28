# Go world management layer

`go-world-management-layer` is the operational boundary between autonomous
vulnerability-research agents and the programs or Android apps they investigate.
It keeps a persistent agent workspace separate from disposable target
sandboxes, scopes every operation to a lease and generation, and preserves
failures and observation gaps as evidence.

**Status:** active pre-v1 implementation. `worldd` and `world-node` both ship an
authenticated logical control plane and an opt-in physical Linux composition:
digest-pinned Docker agent workspaces, directory-copy workspaces, Docker Linux
targets, deployment-authorized local material, optional process observers, and
ledger capture. Physical startup requires an immutable version-2 deployment
profile; the daemon probes the selected drivers, compiles every strict policy
against the complete capability fingerprint, preflights every published plan,
and fails closed before opening its listener if the result cannot be enforced.

Android remains a different boundary. The Cuttlefish-family driver and scoped
ADB gateway have an opt-in real Android SDK emulator qualification, including
installing and launching a specimen APK through the run-scoped gateway. Neither
daemon currently accepts an Android target driver selection, and the attached
emulator backend deliberately does not boot, reset, stop, or destroy the
externally owned emulator. Managed Cuttlefish daemon composition, production
collectors, a remote forensic repository, packaging, and a supported
host/version matrix remain release work.

The governing promise is:

> Transparent while healthy, explicit when the environment changes or fails.

## Implemented architecture

```text
trusted operator / client
        |
        | world.v1 gRPC (bounded Protocol Buffers, bearer or mTLS)
        v
  worldd OR world-node
  (independent full daemons; there is no controller/worker link between them)
        |-- revisioned, idempotent logical lifecycle core
        |-- SQLite WAL control records and hash-chain verification
        |-- durable observation ledger and replayed live projections
        |-- local sealed-bundle finalizer and content-addressed material authority
        `-- optional deployment-profile physical composition
              |-- strict policy compile + exact capability binding
              |-- directory-copy workspace + Docker agent -> world-guest
              |-- Docker Linux target -> scoped exec/file transport
              |-- optional process observers -> ledger
              `-- optional policy-authorized ledger captures

qualified library boundary (not a daemon-selectable Android composition)
        Cuttlefish/AttachedEmulator -> scoped one-device ADB

go-agent-runner -> lease-bound ExecutionEnvironment adapter -> world service
go-forensic-artifacts <- scoped MaterialAuthority adapter <- sealed evidence
```

The code is split along those boundaries:

| Area | Implemented contract | Executable/qualification boundary |
| --- | --- | --- |
| Control plane | Typed `world.v1` service/client, authenticated RPC, owner/policy scoping, independent agent/target generations, target runs, execs, incidents, recovery, optimistic revisions, and idempotency | Service management and fleet scheduling remain deployment-owned |
| Persistence | SQLite WAL with full-sync writes, forward migrations, revisioned snapshots, a chained control journal, startup verification, and replay | Online backup/restore and fleet migration tooling are not shipped |
| Execution | Versioned bounded frames, opaque stdin/stdout/stderr, direct argv, temporary inputs, cancellation, heartbeat, and process-tree cleanup in `world-guest` | The default `none` composition has no physical exec; image/provider compatibility still requires node qualification |
| Agent workspace | Fail-closed digest-pinned Docker plan, one configured workspace mount, read-only root, no host namespaces/devices/runtime socket, dropped capabilities, exact plan binding, and a lease-bound runner adapter | The shipped directory workspace is explicitly `directory-copy-non-production`; OverlayFS is not activated by the daemon |
| Targets | Activated Docker Linux lifecycle with collector-readiness gates, exact generation/run plan binding, scoped exec/transfer, quarantine, reset, destruction, and startup inventory/adoption | Android and physical-device selections are rejected by daemon configuration; their drivers remain integration/qualification packages |
| Policy/admission | Strict YAML compilation against a complete probed capability fingerprint; durable effective-policy publication; preflight and per-mutation checks for runtime, network, recovery, capture/export, target concurrency, and aggregate live resources | Host pressure sensing and fleet placement are not daemon-composed |
| Material/workspace | Deployment-authorized local selections and occurrences, digest-verified input projection, directory workspaces, no-follow relative exports, and local content-addressed output/bundle publication | A remote forensic backend, credentials, and cross-system custody are not included |
| Observation | Durable hash-chained segments, explicit gaps/duplicates, resumable bounded fan-out, deterministic live projections, process-backed observer supervision, ledger capture, and one idempotently sealed bundle per run | Adapter programs are supplied by the deployment and must be qualified; a generic process supervisor is not a collector suite |

The default Go suite exercises these boundaries with deterministic fakes,
fault injection, protocol/server integration tests, and command-plan
assertions. Opt-in suites additionally run the Docker drivers and full daemon
lifecycle against a real Docker Engine, and run the Android driver/scoped ADB
gateway against an already-running SDK emulator. The ordinary `go test ./...`
run does not start Docker or an emulator, mount OverlayFS, attach eBPF, or
contact a remote artifact service.

### Shipped-daemon capability matrix

| Surface | Available executable or qualified behavior | Deliberately unavailable |
| --- | --- | --- |
| `worldd` | Full authenticated `world.v1` endpoint; logical-only defaults; opt-in Docker/directory/process/ledger physical composition from a trusted deployment profile | Android and physical-device composition; fleet controller behavior |
| `world-node` | The same full composition with separate node-prefixed state, directories, socket, and Windows port defaults | It is not a worker registered with `worldd`; no controller-to-node protocol is shipped |
| Docker/directory | Agent and Linux-target provisioning, input projection, exec/transfer, export, capture, physical reconciliation, lease drain, reset, quarantine, and teardown | Requires explicit matching driver flags, absolute non-overlapping roots, locally present digest-pinned images, and a version-2 deployment profile |
| Android | Cuttlefish-family driver contracts plus a real AttachedEmulator APK/scoped-ADB qualification test | `android-target-driver` accepts only `none`; the attached backend is not a managed emulator service |
| Observation/material | Durable ledger/live view, process observer supervision, ledger captures, local selection/content/output/bundle authority, and bundle sealing | Deployment observer binaries and a remote forensic authority are not bundled |

With all drivers left at `none`, lifecycle calls create logical records and
capability-dependent RPCs fail explicitly. With the physical Linux composition,
the service returns success only after the exact persisted plan crosses its
physical boundary and the driver returns validated evidence; ambiguous retries
reuse the durable idempotency/plan binding rather than inventing a new
realization.

After a daemon interruption, startup reconciles exact physical identities and
version-5 observer markers before lease cleanup and before RPC. Each marker
binds the persisted run-plan digest and start signature, every external
`CollectorPlan` and its durable start-commit flag, and the intrinsic collector
identity/start time when `target.lifecycle` coverage is required. Its stopped
phase also binds the digest of the complete persisted run result and the digest
of the exact version-2 stop preparation that is allowed to consume it; the
referenced bounded evidence journal makes accepted events, metrics, artifacts,
coverage, gaps, failures, and recovery output independently replayable. On Linux,
the built-in observer starter's parent-death `SIGKILL` proves that its directly
spawned collector process dies with the daemon. Adapters that daemonize or
leave surviving helpers are unsupported unless a deployment supplies an
equivalent external process-tree/cgroup proof.
Recovery then validates every local output transaction and object. Verified
finalized artifacts are retained but continuity is still marked lost,
incomplete output is durably aborted, and foreign or mismatched entries fail
startup. A surviving target execution is force-stopped and proved stopped; the
daemon never resumes the specimen, collectors, or duration timer. It seals the
run as failed with a `control_plane_failure` incident and explicit gaps, marks
active target operations lost, and fails startup if any identity, cleanup,
output, or finalization proof is incomplete.

Run finalization is a crash-resumable saga: durable reservation; byte-identical
version-2 stop preparation containing scope, initial revision/state, complete
result, coverage requirements, and failure-incident intent; observer binding to
both result and preparation digests; hash-chain anchoring; immutable local seal
and artifact publication; canonical public-bundle staging; Core terminal
commit; public file/index; committed observer marker; and finally
`bundle.completed`. Bundle reads remain closed until that final gate. Startup resumes only the exact anchored stage,
removes only recognized unreachable atomic staging files, and fails closed on
tampering or missing/conflicting stages. See the
[startup and reconciliation runbook](docs/operations/startup-and-reconciliation.md).

## Quick start: local control plane

Go 1.23 or later is required by `go.mod`. The following starts the control
service and creates durable logical state; it is a control-plane smoke test, not
a sandbox or collector integration test.

In one shell on Linux:

```sh
export WORLD_BEARER_TOKEN='replace-with-a-long-random-local-token'
export WORLD_POLICY_REFERENCE='sha256:1111111111111111111111111111111111111111111111111111111111111111'

go run ./cmd/worldd \
  -state /tmp/world-quickstart/control.db \
  -ledger-dir /tmp/world-quickstart/ledger \
  -orchestration-state-dir /tmp/world-quickstart/orchestration \
  -bundle-dir /tmp/world-quickstart/bundles \
  -material-dir /tmp/world-quickstart/material \
  -unix-socket /tmp/world-quickstart/worldd.sock
```

In a second shell:

```sh
export WORLD_BEARER_TOKEN='replace-with-a-long-random-local-token'
export WORLD_POLICY_REFERENCE='sha256:1111111111111111111111111111111111111111111111111111111111111111'

go run ./cmd/worldctl -unix-socket /tmp/world-quickstart/worldd.sock acquire \
  -input-view iv_3333333333333333333333333333333333333333333333333333333333333333 \
  -capabilities sha256:2222222222222222222222222222222222222222222222222222222222222222 \
  -ttl 1h

go run ./cmd/worldctl -unix-socket /tmp/world-quickstart/worldd.sock \
  get-session -session 'rs_<id-from-acquire>'
```

The sample digests are syntactically valid placeholders. They do not represent
a compiled policy, a resolved artifact selection, or a probed node. Use real,
immutable digests before enabling execution. This smoke test exercises only
logical state and leaves the observation ledger empty. On Windows the default
endpoint is `127.0.0.1:7777`; pass `-listen` to `worldd` and `-address` to
clients when overriding it.

## Quick start: real Docker end to end

On Windows with Docker Desktop running in Linux-container mode, the repository
harness builds `worldd`, the clients, `world-guest`, `world-idle`, and a native
specimen; builds and digest-pins a Docker image; compiles the strict E2E policy;
writes a version-2 deployment profile; and exercises agent exec, target
exec/push/pull, capture, export, reset, release, and orphan checks. It also
force-kills the daemon while a long-lived target exec is present, then requires
startup to terminate that execution, seal the run as failed with an incident
and continuity gap, deny reopening it, and retain both normal and interrupted
bundles across a final restart:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\testdata\e2e\run-world-e2e.ps1
```

The script leaves a machine-readable `evidence.json` beneath
`.cache/e2e-runs/<timestamp>/`. Docker Desktop is useful integration evidence,
but it is not the dedicated-Linux-host security or filesystem-permission
reference.

Before authoring a physical deployment profile, probe the exact Docker
composition and optionally compile a strict policy against the resulting
complete fingerprint:

```powershell
go run ./cmd/world-capabilities `
  -policy .\policy\deployment\e2e-directory-copy.yaml
```

Use `effective_policy.digest` as the authorized policy digest and
`effective_policy.capability_fingerprint_digest` as the generation capability
digest. `worldd` repeats the probe and compile at startup and rejects a profile
whose plans, policy, physical facts, or image identities do not match. See the
[deployment and policy runbook](docs/operations/deployment-and-policy.md).

## Commands

All client connection flags precede the subcommand. Run
`go run ./cmd/<command> -h` and `go run ./cmd/worldctl <subcommand> -h` for the
authoritative flags.

| Command | Purpose |
| --- | --- |
| `worldd` | Open/verify/migrate the control database and serve the authenticated `world.v1` API. Linux defaults to `/tmp/worldd.sock`; Windows defaults to loopback TCP. |
| `world-node` | Run the same independent full service with node-prefixed durable paths. Linux defaults to `/tmp/world-node.sock`; Windows defaults to `127.0.0.1:7778`. It is not connected to `worldd` as a worker. |
| `worldctl` | Broad operator/debug client covering session/lease, target/run, exec, incident/recovery, observation, capture/export, and trusted lifecycle-transition operations. Unary responses are indented JSON. |
| `world-target` | Target-scoped data plane: `exec`, explicit `shell`, `push`, `pull`, and a loopback-only `adb`/`adb-proxy` for one active target run. |
| `world-observe` | Read side for `snapshot`, one-shot table `top`, NDJSON `watch`, NDJSON `metrics`, and sealed `bundle` retrieval. |
| `world-capture` | Agent-facing `request PROFILE` command for a policy-authorized named capture profile. |
| `world-export` | Agent-facing declaration of one or more relative `PATH[=ROLE]` workspace outputs; it never accepts a host destination. |
| `world-capabilities` | Probe the Docker agent/Linux-target composition without provisioning; with `-policy`, compile strict YAML against the complete fingerprint and print its canonical effective-policy digest pair. |
| `world-guest` | Framed exec supervisor intended for agent and target images. It reads protocol frames on stdin and must not be used as an interactive shell. Docker supplies the container init process. |
| `world-idle` | Inert target-container entrypoint used beneath Docker's `--init`; it exposes no command or management surface. |
| `verify` | Run the repository's deterministic module, format, schema, fuzz-seed, contract, security, integration, full-test, race, vet, and Linux cross-build gates. |

Common `worldctl` connection settings are `-unix-socket`, `-address`, `-token`,
`-timeout`, and the four mTLS flags. `WORLD_BEARER_TOKEN` supplies the local
token; `WORLD_POLICY_REFERENCE` and the command-specific ID environment
variables can reduce repetition in operator scripts. Mutating commands create a
fresh idempotency/correlation identity and an absolute deadline; `-causation`
can link a mutation to an existing causal event. API clients must send one
canonical, non-empty, trim-stable UTF-8 idempotency key of at most 1024 bytes.
Internal child steps use the shared domain-separated, length-framed derivation;
when a readable `parent/suffix` would be ambiguous or too long, a SHA-256-bound
form preserves the exact parent/suffix identity within the same limit.

The client-global timeout (30 seconds by default) also bounds streams. The
daemon's independent `-control-timeout`/`WORLD_CONTROL_TIMEOUT` setting defaults
to 30 seconds and bounds detached controller cleanup, including guest and
observer process cleanup; work that cannot finish within that window remains
durably recoverable for the next reconciliation pass. Target exec and shell
currently support fixed initial terminal geometry, not interactive
signal/resize forwarding. Push copies a server-side workspace-relative source
to a target-relative destination; pull atomically publishes a target-relative
source at a server-side workspace-relative destination. The daemon owns byte
limits, digest verification, and replacement policy, and neither command
accepts a client-host file path. The ADB proxy accepts only a loopback listener
and 1-16 sequential connections.

Both daemon binaries default to `agent-driver=none`, every target driver set to
`none`, `observer-driver=none`, `capture-driver=none`,
`workspace-driver=none`, and `material-driver=local`. The supported physical
combination enables `agent-driver=docker` and `workspace-driver=directory`
together; `linux-target-driver=docker` additionally requires that pair.
`observer-driver=process` is valid only when the deployment profile references
observer adapters, and `capture-driver=ledger` requires a physical local
material composition. Physical mode also requires an absolute version-2
`deployment-profile` and absolute, pairwise non-overlapping state, material,
workspace, target, observer, capture, ledger, bundle, and socket roots as
applicable. Android and physical-device flags currently accept only `none`.
Invalid combinations or unverifiable plans fail before the listener opens.

## Verification

Schema generation uses Buf plus the pinned Go generators. Install those tools
with `make generate-tools`; regenerate checked-in bindings with `make generate`.

The single local entry point is:

```sh
make verify
```

Without `make`, run the equivalent directly:

```sh
go run ./cmd/verify
```

It runs, in order:

1. module-graph drift detection with `go mod tidy -diff`;
2. `gofmt -l` over repository Go files;
3. Buf lint, clean-generation drift detection, and protobuf schema/transport tests;
4. every checked-in fuzz seed;
5. driver/port contract tests;
6. security-boundary tests;
7. RPC/orchestration integration tests;
8. `go test ./...`;
9. `go test -race ./...`;
10. `go vet ./...`; and
11. a CGO-free Linux/amd64 cross-compile of every package and test binary.

Use `go run ./cmd/verify -only=<gate>` for one named gate; `-h` lists the
accepted names. Every invocation atomically writes its gate status and timing
to `verification/summary.json` by default; `-summary` selects another path.
Real-node Docker, privileged Linux, KVM/Android, collector, artifact-service, security
escape, performance, and soak suites are separate release evidence and are not
implied by a local `verify` pass.

## Security boundaries

- `worldd` and `world-node` refuse to start without a bearer token or complete
  mTLS configuration. Plaintext bearer TCP is restricted to loopback;
  non-loopback service requires TLS 1.3 with a verified client certificate.
  Operators must also protect the Unix-socket directory and token environment.
- Each daemon exclusively owns its canonical control-database path through the
  sibling `<canonical-control-path>.worldd.lock`. On supported platforms it
  acquires this nonblocking lock before credentials, the store, ledger, drivers,
  reconciliation, or listener are opened and releases it after RPC shutdown
  and local cleanup. Regular single-link control/lock files and opened-handle
  identity are required; hard links, leaf symlinks/reparse points, and special
  files fail closed. Ancestor-directory aliases resolve to the canonical
  parent. Linux, Darwin, BSD, and Solaris serialize that parent directory to
  stabilize the lock namespace, so place each daemon state in a dedicated
  directory; Windows denies deletion of the held lock file. AIX startup fails
  closed because its available sibling-file primitive cannot provide stable
  namespace ownership. These are advisory controls for conforming daemon
  acquisition, not a defense against arbitrary same-user filesystem mutation.
- Mutable orchestration journals, compact markers, sealed bundle records, and
  local content-addressed publications are rooted in opened safe namespaces.
  Namespace entries are canonical single-component regular files; symlink or
  reparse indirection, special files, and multiply linked files fail closed.
  Publications use same-directory staging and atomic replacement while the
  namespace directory identity is held and revalidated.
- The authenticated subject owns the session it acquires. Reads and mutations
  resolve one resource back to that owner, and mutations must carry the frozen
  policy reference. Driver-fact transitions are restricted to configured
  trusted node subjects.
- Agent workspaces and targets are sibling resources. Plan validation forbids
  privileged mode, host PID/IPC/network/cgroup namespaces, devices, runtime
  sockets, and arbitrary host mounts. Agent plans require a read-only root,
  no-new-privileges, no capabilities, and exactly one workspace mount beneath
  the configured root.
- Target commands are not semantically allowlisted. Arbitrary command and ADB
  service bytes may reach the assigned disposable target, but lease, target,
  generation, run, serial, path, and host-service selection are structurally
  scoped. Host-global ADB authority and other serials are denied.
- Artifact credentials and physical paths remain behind the host-owned adapter.
  Inputs are digest-verified; exports accept logical relative paths and use
  descriptor-safe opens; bundle control state is committed only after the
  artifact digest matches the local seal.
- Bounded frames, messages, streams, queues, temporary inputs, output, cache,
  and collector processes are part of the contract. Loss, stale data,
  truncation, collector failure, and recovery are represented explicitly.

These controls reduce confused-deputy and cross-lease risk; they are not a claim
that ordinary Docker containers resist an unknown host-kernel exploit. Use
dedicated nodes and complete the real-host escape/security suite before hostile
production workloads.

## External node prerequisites

Only Go and local filesystem access are needed for the logical control-plane
smoke test. The shipped Linux physical composition additionally has the
following prerequisites. Flags activate the composition only when its trusted
deployment profile, policy compile, capability probes, exact image inspection,
physical-policy preflight, and root validation all succeed. No supported
production version matrix is asserted yet.

| Prerequisite | Required for | Operator responsibility |
| --- | --- | --- |
| Dedicated Linux host | Production hostile execution | Use cgroup v2, service isolation, safe configured roots, quotas/reserve, and a qualified Docker/runtime/filesystem tuple. The shipped workspace mode is directory copy; Docker Desktop is integration evidence, not the security/performance reference. |
| Docker Engine and CLI | Shipped agent and Linux-target physical mode | Keep Docker authority in the daemon service account; never mount the socket into agent/target containers. Qualify Engine/API/runc, cgroup-v2 enforcement, security options, daemon restart behavior, and locally present digest-pinned images. Agent images need the configured `world-guest`; target images need `world-idle` as their inert entrypoint and `world-guest` for framed exec. |
| Absolute managed roots | Physical mode | Give each configured state, source, workspace, target, capture, observer, ledger, bundle, material, profile, and Unix-socket path its intended ownership and a non-overlapping root. Do not place one beneath another. |
| Collector binaries/adapters | Deployment-profile observers | Select and pin each executable/configuration, readiness command, version probe, placement, coverage, resource estimate, and byte limit. On Linux the process supervisor's parent-death signal covers the directly spawned process only. Adapters that daemonize or leave helpers behind are unsupported without an equivalent external process-tree/cgroup supervisor; the mechanism does not turn an arbitrary program into a trustworthy collector. |
| Local material catalog | Shipped physical mode | Authorize exact regular source files, digests, sizes, logical paths, modes, sensitivity, selections, and security scope in the deployment profile. Protect the source and publication roots from untrusted mutation. |
| Remote forensic backend | External custody, if required | The local authority supports scoped inputs and content-addressed local output/bundle publication. A remote repository, credentials, replication, and cross-system custody remain adapter/deployment work. |
| KVM/SDK emulator/Cuttlefish/ADB | Android driver qualification only | The opt-in AttachedEmulator test requires an already-running SDK emulator and exact ADB serial. It does not activate Android in either daemon or prove managed Cuttlefish boot/reset/destruction. |
| Pinned policies and images | Any physical admission | Compile policy against the complete probed fingerprint, keep its immutable `name@revision` source in the profile, and create a new generation whenever runtime, image, observer, or device facts change. |

Start with the [startup and reconciliation runbook](docs/operations/startup-and-reconciliation.md)
and keep the [upgrade/version-skew procedure](docs/operations/upgrades-and-version-skew.md)
with the node release evidence.

## Ecosystem and documentation

This repository owns operational environment truth. It does not own provider
protocol interpretation (`go-agent-runner`), immutable forensic byte custody
(`go-forensic-artifacts`), or vulnerability conclusions
(`go-vr-research-framework`).

- [Architecture](docs/design.md) — target design and invariants
- [Implementation plan](docs/implementation-plan.md) — staged delivery and exit gates
- [Architecture decisions](docs/adr/README.md)
- [Example policy](docs/examples/environment-policy.yaml)
- [Operator runbooks](docs/operations/README.md)

The design and implementation plan describe the intended full v1 system. The
status table in this README and executable verification are authoritative for
what this repository currently demonstrates.

# Startup and reconciliation

Use this after a normal restart, daemon crash, or host reboot.

## Automated today

Before loading credentials or opening SQLite, a ledger, an orchestration root,
any physical driver, the startup reconciler, or a listener, `worldd` and
`world-node` acquire a nonblocking process-ownership lock at
`<canonical-control-path>.worldd.lock`. The lock is held until RPC shutdown and
all local cleanup complete, then released last. A second daemon using the same
state fails immediately, whether it selects the same listener or another
listener. Existing control and lock files must be regular, non-symlink files,
still match their opened handles, and have exactly one filesystem link;
hard-linked, reparse, and special-file aliases fail closed.

On Linux, Darwin, BSD, and Solaris, the owner locks the canonical parent
directory before its sibling `.worldd.lock` file and releases the file before
the directory. This deliberately prevents a conforming second daemon from
following an unlinked/replaced lock pathname; it also means independent control
databases in the same directory cannot run concurrently. Windows instead uses
`LockFileEx` and holds the lock file without delete sharing, then verifies path
identity and the single-link invariant. AIX fails closed as unsupported because
its available sibling-file lock cannot provide replacement-resistant namespace
ownership. These advisory controls constrain conforming daemon acquisition;
they are not protection against arbitrary same-user mutation that bypasses the
daemon or ignores advisory locks. Give each daemon a dedicated,
access-controlled state directory.

After ownership is established, the control store creates forward
migrations, verifies the complete SQLite control-record hash chain, and
replays accepted records into application state. Startup fails on a newer
database schema, hash-chain damage, an unknown aggregate kind, or invalid
replay data. The daemon also opens the configured observation ledger,
reconstructs its segment index, and may truncate only an incomplete tail in
the single open segment. Every such repair is written to the daemon log. The
orchestration service replays its durable state before accepting RPCs.

Physical composition is loaded before reconciliation: the daemon probes the
selected drivers, compiles strict policy against the complete capability
fingerprint, preflights every configured physical plan, and publishes the exact
effective-policy pairs. Orchestration replay loads reservations, canonical
public-bundle stages and their hash-chain anchors, bundle files/indexes,
and completion records. The serialized gate then examines run-observer markers
and observer output transactions against the replayed run plans. Only
recognized regular atomic staging files may be removed; an unknown, unsafe,
tampered, missing, or conflicting entry fails startup.

Before creating an RPC server or listener, the daemon then runs one serialized
startup gate in this order:

1. Resume every canonical, hash-chain-anchored public-bundle stage before
   observer markers are promoted. For a nonterminal run, replay its exact
   staged Core terminal commit only at the recorded revision; for an already
   terminal run, require the terminal identity, artifact, digest, incidents,
   and outcome to match. Recreate only a missing exact public file/index. A
   reservation without a stage remains owned by its original finalization
   namespace/key/signature and is continued when interrupted-run recovery
   reaches the stop path.
2. Enumerate every durable session and require complete, canonical physical
   bindings for nonterminal generations and runs. Reconstruct each plan through
   the trusted resolver and re-apply effective-policy admission. A changed plan
   digest, provisioning key, policy pair, resource plan, or physical report is
   an integrity/admission failure, not a reason to create something new.
3. Parse every run-observer marker as format version 5 and match its filename
   and recorded run ID to the exact durable target run. Require its immutable
   persisted run-plan digest and full observer start signature, every complete
   external `CollectorPlan`, and each `start_committed` flag to match the
   authority-selected run. When `target.lifecycle` is required, also require
   the exact intrinsic collector ID/start time. A foreign or
   malformed marker, mismatched binding, impossible phase, or nonterminal run
   with a committed marker fails startup. An absent marker is allowed only
   while the run is `requested` or `preparing`; a later phase requires the
   ownership marker. A stopped/committed marker must also bind both the complete
   persisted result digest and its version-2 stop-preparation digest, plus its
   bounded evidence-journal reference. The two stop digests are inseparable and
   must agree with the canonical preparation file and hash-chain record. A
   terminal stopped marker is promoted only after the exact public bundle is
   verified; the matching `bundle.completed` read gate is then appended. Each
   successful journal checkpoint atomically advances the marker before removing
   the superseded checkpoint. Startup removes a crash-window orphan only when it
   is an unreferenced, canonical, digest-matching journal; malformed or foreign
   entries fail reconciliation closed.
4. Inventory agent and target runtimes. An expected container is adopted only
   when its complete structural labels and physical configuration match the
   reconstructed plan. Missing, foreign, uncertain, conflicting, mismatched,
   or unprovable orphan resources fail startup. A uniquely owned orphan is
   removed only when its bound generation is durably terminal (or cleanup is
   already durably authorized), and a second inventory must prove it absent.
   Docker inventory remains global and fail-closed, but inspection is issued in
   bounded groups of 32 IDs to avoid one host process per container. Every
   requested ID must appear exactly once; a partial, duplicate, or substituted
   batch invalidates the entire snapshot. Direct one-container inspection uses
   the same exact requested-ID check.
5. Finalize every durably nonterminal target run as interrupted. For the Docker
   Linux target, the crash reconciler validates the adopted runtime identity,
   force-stops leftover execution, re-inspects it to prove stopped, restarts the
   clean inert target container, revalidates identity, and reconstructs only a
   prepared run. It never calls `StartRun`, resumes the specimen, starts a
   collector, or creates a new maximum-duration timer.
6. Before observer evidence is reconstructed, old external collector ownership
   must be provably dead. The built-in Linux process starter's parent-death
   `SIGKILL`, including the parent-exit race, proves death of its directly
   spawned process only. An adapter that daemonizes or leaves surviving helpers
   is unsupported unless an external cgroup/process-tree supervisor supplies an
   equivalent proof; custom starters and other platforms fail closed. The
   output reconciler then classifies exactly one transaction per persisted
   collector. It verifies and
   retains valid finalized artifacts, durably aborts incomplete output and
   removes its partial files, and accepts an absent transaction only when
   `start_committed` is false. A present valid finalized transaction remains
   authoritative even if the marker flag is false. It validates manifests,
   roles, digests, sizes,
   byte limits, object reachability, file identity, and exact directory
   membership. Foreign/mismatched collectors, files, objects, or transaction
   states fail startup. Even retained finalized output is accompanied by lost
   continuity and explicit control-plane-loss gaps.
7. Continue the ordered bundle saga for the failed run: immutable seal and
   artifact, canonical hash-chain-anchored public stage, Core terminal state,
   public file/index, committed observer marker, then `bundle.completed`.
   Link a `control_plane_failure` incident. Reads remain closed before the
   completion record; startup never rebuilds or invents evidence from a later
   state.
8. Mark each nonterminal target operation attached to the interrupted run as
   `lost`. Verify all observer markers are committed. A repeated reconciliation
   leaves the already-terminal run and its existing bundle untouched.
9. Only after physical/run recovery succeeds, resume every unfinished release
   or due expiry through the durable lease-termination drain.

Any failure aborts startup before an RPC can observe, reuse, or add work. A
logical-only restart also fails if durable state contains physical bindings for
which no inventory/recovery driver is configured.

After startup, lease termination reconciliation runs serially on a bounded
ticker. Configure its cadence with `WORLD_RECONCILIATION_INTERVAL` or
`-reconciliation-interval`, and bound each startup or periodic attempt with
`WORLD_RECONCILIATION_TIMEOUT` or `-reconciliation-timeout`. Both values must
be positive durations; their defaults are 30 seconds and 10 seconds.

Detached physical cleanup, exec cleanup grace, capture/export control work,
and controller release cleanup use `WORLD_CONTROL_TIMEOUT` or
`-control-timeout`, default 30 seconds. Set it above the runtime's graceful-stop
bound and the measured worst-case bounded inventory time. Release persists
`releasing` before cleanup; if this shared budget ends, the controller stops
calling later drivers with the expired context and the startup/periodic reaper
retries the same durable child identities. Increasing the client RPC timeout
does not change this server-side cleanup budget.

## Procedure

1. Keep admission closed. Record the binary version, configuration digest,
   canonical state path, sibling `.worldd.lock` path, node root, and external
   runtime versions. A lock-held error means another owner still controls the
   state; do not bypass or delete the lock file.
2. Verify that the state and node-root filesystems are mounted at the expected
   locations, have safe ownership/modes, and have free bytes and inodes. Do not
   start against an empty replacement directory by accident.
3. Start the applicable daemon. Treat migration, control-chain verification,
   orchestration replay, or ledger-open failure as a hard stop; preserve the
   durable state and logs unchanged. Record every reported incomplete-tail
   repair as a gap/incident.
4. Verify the configured physical driver selections, immutable deployment
   profile digest, compiled policy/capability pairs, image digests, and
   non-overlapping roots. A physical selection without authoritative inventory,
   exact-plan reporting, run crash recovery, or observer cleanup proof fails.
5. Retain the startup report fields for expected/unclaimed/conflicting agent and
   target resources, removed terminal orphans, interrupted runs failed, target
   operations lost, and lease terminations examined/begun/completed.
6. For an interrupted run, query the durable target after startup and require
   state `failed`, a non-empty sealed bundle/artifact/digest, a linked
   `control_plane_failure` incident, and at least one explicit
   control-plane-loss gap. Require every formerly active target operation to be
   `lost`. Verify the version-5 marker is committed, the public stage/file/index
   and `bundle.completed` record agree, and each persisted collector output is
   exactly finalized or aborted. Never label the run resumed, even when
   finalized collector artifacts were retained.
7. Classify any resource family outside the current daemon composition
   separately. Android/Cuttlefish/physical devices have no daemon startup
   reconciler; do not infer their safety from Linux reconciliation.
8. Confirm no stale target transport remains, no old specimen/collector process
   survived, terminal runs were not rewritten, live cursors are monotonic, and
   artifact staging remains pinned.
9. Confirm that the listener appeared only after all startup gates succeeded
   and that control-plane reserve is healthy. Reopen external admission only
   after retaining this evidence and passing a canary lifecycle.

## Exit evidence

Retain the ownership-lock path/result, startup/reconciliation report,
control-store verification result, ledger repair list, compiled
policy/capability and deployment-profile digests, runtime inventories and
orphan decisions, observer marker/output classifications, bundle-stage/index/
completion identities, failed-run bundle/gaps/incidents, lost-operation IDs,
lease-drain report, and the operator who reopened admission.

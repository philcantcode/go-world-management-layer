# Startup and reconciliation

Use this after a normal host-process restart, crash, or host reboot.

## Automated today

Before opening SQLite, a ledger, an orchestration root, any physical driver, or
the startup reconciler, `world.Open` / `OpenHost` acquire a nonblocking
process-ownership lock at `<canonical-control-path>.worldd.lock`. The lock is
held until `Manager.Close` / `Host.Close` and all local cleanup complete, then
released last. A second Open using the same state fails immediately. Existing
control and lock files must be regular, non-symlink files, still match their
opened handles, and have exactly one filesystem link; hard-linked, reparse, and
special-file aliases fail closed.

On Linux, Darwin, BSD, and Solaris, the owner locks the canonical parent
directory before its sibling `.worldd.lock` file and releases the file before
the directory. This deliberately prevents a conforming second Open from
following an unlinked/replaced lock pathname; it also means independent control
databases in the same directory cannot run concurrently. Windows instead uses
`LockFileEx` and holds the lock file without delete sharing, then verifies path
identity and the single-link invariant. AIX fails closed as unsupported because
its available sibling-file lock cannot provide replacement-resistant namespace
ownership. These advisory controls constrain conforming Open acquisition; they
are not protection against arbitrary same-user mutation that bypasses the host
or ignores advisory locks. Give each control-state tree a dedicated,
access-controlled state directory.

After ownership is established, the control store creates forward
migrations, verifies the complete SQLite control-record hash chain, and
replays accepted records into application state. Startup fails on a newer
database schema, hash-chain damage, an unknown aggregate kind, or invalid
replay data. Open also opens the configured observation ledger, reconstructs
its segment index, and may truncate only an incomplete tail in the single open
segment. Every such repair is written to the host log. The orchestration
service replays its durable state before Open returns a Manager.

Physical composition is loaded before reconciliation: Open probes the
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
2. Enumerate every durable session and reconstruct physical plans through the
   trusted resolver, then re-apply effective-policy admission. Complete
   bindings must match their exact digest, provisioning keys, policy pair, and
   resource plan. Before preparing a workspace, replaying provisioning, or
   performing any other ordinary physical mutation, collect every nonterminal
   agent exec and invoke the exact persisted generation's crash reconciler.
   It must stop or prove absent the old execution boundary, start the same
   container generation, complete a fresh framed readiness exchange, and only
   then mark the exec lost; an incomplete or wrong-protocol proof keeps RPC
   admission closed. The one safe unbound exception is the first generation still
   in `provisioning`: its workspace/agent keys are deterministically recovered
   from the immutable session acquisition key, and its target key from the
   immutable target creation key. Startup persists the binding, replays the
   idempotent physical operation, validates the real result, and advances the
   logical generation to ready before inventory. Bound active Docker agent
   generations also replay `Prepare`/`Mount`/`Provision`, including a fresh
   framed `world-guest` readiness exchange; a cached ready record is not proof
   after process restart. A later unbound recovery generation, or an unbound
   target reset whose mode/snapshot request cannot be reconstructed, is
   reported as pending and must be continued by the exact original client
   request. Startup never guesses those inputs. A partially present binding is
   an integrity failure.
3. Parse every run-observer marker as format version 6 and match its filename
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
   reconstructed plan. Inventory receives active plans separately from
   cleanup-only plans. Every cleanup-only entry is still the complete
   historical plan reconstructed through the trusted resolver, including its
   immutable digests and physical configuration; a generation reference,
   runtime name, or labels alone never authorize deletion. Drivers retain
   cleanup-only matches solely for `Stop`/`Destroy` and never expose them to
   execution, transfer, run, or ordinary adoption paths. Missing, foreign,
   uncertain, conflicting, mismatched,
   or unprovable orphan resources fail startup. The current quarantined
   generation is an expected stopped resource, never a terminal orphan; only a
   durable lease cleanup/release decision makes it cleanup-eligible. A uniquely owned orphan is
   removed only when its bound generation is durably terminal (or cleanup is
   already durably authorized), and a second inventory must prove it absent.
   If the runtime is authoritatively absent but exact driver-local target state
   remains, inventory reports cleanup-required residue; startup removes it only
   through that cleanup-only plan and requires the follow-up inventory to show
   both runtime and residue absent. When a terminal Docker agent container is
   absent, startup read-only inspects its exact persisted workspace identity,
   then destroys and seals/releases any remaining workspace through the normal
   generation-bound cleanup path. A failed or partial cleanup keeps admission
   closed.
   Docker inventory remains global and fail-closed, but inspection is issued in
   bounded groups of 32 IDs to avoid one host process per container. Every
   requested ID must appear exactly once; a partial, duplicate, or substituted
   batch invalidates the entire snapshot. Direct one-container inspection uses
   the same exact requested-ID check. A `destroy_target` reservation is bound
   to one exact target generation. If a crash leaves that generation ready or
   resettable and physically adopted or already absent, startup re-establishes
   the resettable boundary, repeats idempotent destruction, proves absence with
   a second inventory, and commits `destroyed`. Missing is accepted for no
   other expected generation. Driver reconciliation also validates durable
   reset-transition receipts before rebuilding an exact Reset replay result;
   a changed reset key or payload remains a conflict after restart.
   A reset interrupted after Core created its successor is represented as an
   exact predecessor/successor pair. Startup accepts only predecessor adopted
   with successor authoritatively missing, or predecessor authoritatively
   missing with successor adopted. It preserves both identities, destroys
   neither, reports the successor as pending, and lets only the byte-equivalent
   reset retry complete it. Both present, both absent, foreign, uncertain, or
   conflicting observations fail closed. A recovery already marked resolved is
   accepted only when its complete predecessor/successor plans and observations
   still form the exact adopted/missing pair and the incident contains the
   strategy-derived trusted physical completion action; a generic recovery
   string cannot satisfy this proof.
5. Resume an exact generation-bound quarantine intent without losing run
   evidence. Close the `ready -> resettable` admission boundary first. Finalize
   every bound interrupted run and require every current-generation run,
   including an unbound crash-window record, to be terminal with its public
   bundle and committed observer marker. Only then invoke target-wide physical
   quarantine, persist its exact containment evidence, and commit the logical
   quarantined state. If containment was already persisted while any run is
   nonterminal, startup fails integrity checks; it never reopens the target to
   manufacture a missing bundle. The contained target remains an expected
   adopted resource.
6. Finalize every other durably nonterminal target run as interrupted. For the Docker
   Linux target, the crash reconciler validates the adopted runtime identity,
   force-stops the exact container and every process inside its boundary,
   re-inspects it to prove stopped, and reconstructs only the persisted prepared
   run for failed evidence. It never restarts the container, calls `StartRun`,
   resumes the specimen, starts a collector, or creates a new duration timer.
   For a managed Android SDK Emulator target, require the exact target/runtime
   manifests, durable allocation and serial, runtime identity, generation-use
   claim, and run/start records. The host process is adoptable only after its
   canonical executable, generation-unique `-pidfile` argument, PID, and start
   token have been durably committed. Stop and prove that exact tainted AVD
   unreachable, reconstruct its persisted prepared run without reboot or ADB
   cleanup, and finalize it with explicit control-plane-loss and opaque-change
   evidence. Launch intent plus a PID can become ownership only when the live
   canonical emulator/QEMU image has exactly one generation-specific `-pidfile`
   argument; its PID and start token are committed before any use. If intent has
   no such exactly bound live successor, startup does not infer ownership, kill
   a candidate process, delete the AVD, or admit work. It reports an unresolved
   physical conflict for exact operator containment or host reboot.
7. Before observer evidence is reconstructed, old external collector ownership
   must be provably dead. The built-in Windows starter atomically assigns each
   collector tree to a private kill-on-close Job whose sole handle belongs to
   the daemon; the built-in Linux starter's parent-death `SIGKILL`, including
   the parent-exit race, proves death of its directly spawned process only. A
   Linux adapter that daemonizes or leaves surviving helpers is unsupported
   unless an external cgroup/process-tree supervisor supplies an equivalent
   proof; custom starters and unsupported platforms fail closed. The output
   reconciler then classifies exactly one transaction per persisted collector.
   It retains valid finalized artifacts; for a start-committed exact partial
   pair it fsyncs, bounds, and publishes the same canonical immutable artifacts
   as normal finalization; and it durably aborts only uncommitted output. It
   accepts an absent transaction only when `start_committed` is false. A present
   valid finalized transaction remains authoritative even if the marker flag is
   false. It validates manifests,
   roles, digests, sizes,
   byte limits, object reachability, file identity, and exact directory
   membership. Foreign/mismatched collectors, files, objects, or transaction
   states fail startup. Even retained or crash-finalized output is accompanied by lost
   continuity and explicit control-plane-loss gaps.
8. Continue the ordered bundle saga for the failed run: immutable seal and
   artifact, canonical hash-chain-anchored public stage, Core terminal state,
   public file/index, committed observer marker, then `bundle.completed`.
   Link a `control_plane_failure` incident. Reads remain closed before the
   completion record; startup never rebuilds or invents evidence from a later
   state.
9. Mark each nonterminal target operation attached to the interrupted run as
   `lost`. Verify all observer markers are committed. A repeated reconciliation
   leaves the already-terminal run and its existing bundle untouched.
10. Only after physical/run recovery succeeds, resume every unfinished release
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
   target resources, recovered and pending agent/target provisioning, pending
   unbound runs, removed terminal orphans, resumed exact quarantines and target
   destructions, interrupted runs failed, target operations lost, and lease
   terminations examined/begun/completed.
6. For an interrupted run, query the durable target after startup and require
   state `failed`, a non-empty sealed bundle/artifact/digest, a linked
   `control_plane_failure` incident, and at least one explicit
   control-plane-loss gap. Require every formerly active target operation to be
   `lost`. Verify the version-6 marker is committed, the public stage/file/index
   and `bundle.completed` record agree, and each persisted collector output is
   exactly finalized or aborted. Never label the run resumed, even when
   finalized collector artifacts were retained.
7. Classify any resource family outside the current daemon composition
   separately. Managed Android SDK Emulator targets have their own exact
   startup reconciler; daemon-selected Cuttlefish and physical devices do not.
   Do not infer one backend's safety from another backend's reconciliation.
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

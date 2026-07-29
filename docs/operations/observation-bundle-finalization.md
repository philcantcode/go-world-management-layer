# Observation-bundle finalization

Use when a target run has stopped, finalization is retrying, or local sealed
state and control state disagree.

## Required ordering

1. Append the exact per-run finalization reservation before crossing the
   physical stop boundary. It binds the operation namespace, lease, target,
   run, bundle ID, canonical idempotency key, and request signature. Startup
   recovery reuses an existing reservation; it never substitutes a fresh
   `startup_run_recovery` identity for an earlier stop owner.
2. Stop the run and close direct transports. Stop collectors with bounded
   cleanup deadlines, rotate/sync their output, and record any teardown loss as
   a gap.
3. Assemble the run result: cursor range, native artifact references,
   normalized events, metrics, target change set, collector coverage/gaps,
   incidents, and a cited derived summary. Missing required coverage must be
   explicit; it must not be represented as complete.
4. Atomically write the canonical version-2 stopped-run preparation. It binds
   the reservation, mutation metadata, initial run state/revision, target and
   agent generations, run creation time, required coverage, complete persisted
   run result, compact failure-incident intent, and result digest.
5. Update the version-6 stopped observer marker with both the result digest and
   the complete preparation-file digest. Only after that marker is durable,
   append `bundle.stop_prepared` with the file identity, size, both digests,
   lease/target/run, and bundle ID to the hash-chained control ledger. A restart
   may retain and anchor a marker-bound file; an unbound unanchored file is not
   authoritative and may be removed.
6. If the preparation contains failure intent, create or resume exactly that
   incident before sealing. Retries do not reconstruct a different cause from
   later state.
7. Call the finalizer with a deadline. It validates identity/generation scope,
   coverage/gap consistency, incident requirements, changes, and citations.
   It publishes content-addressed metadata, then a per-run `sealed.json` and
   `committed` marker.
8. Publish the sealed bundle through the artifact authority with the same
   idempotency lineage. Verify the returned artifact digest equals the sealed
   metadata content digest.
9. Before changing Core state, atomically write the canonical public wire
   bundle plus its exact terminal-commit request beneath
   `bundle-publications/<target-run-id>.json`, then append
   `bundle.publication_staged` with that file's digest, size, and identity to
   the hash-chained control ledger. This anchor is the point after which a
   restart may resume the public projection without rebuilding evidence.
10. Commit the target run's terminal state, bundle ID, artifact reference,
   digest, outcome, and incident IDs from that exact stage.
11. Atomically publish `bundles/<target-run-id>.json`, verify it byte-for-byte
   against the canonical stage, and append its `bundle.indexed` identity.
12. Commit the stopped version-6 observer marker only after the indexed bundle
   is durable.
13. Append `bundle.completed` only after the bundle index and committed
    observer marker agree. `GetObservationBundle` remains closed until this
    completion record matches the bundle ID and wire digest.

## Retry and mismatch handling

- Repeating an identical request is safe: the local finalizer and run service
  return the existing identity, and every saga step verifies or resumes the
  same durable reservation and bytes.
- A different digest for an already sealed run is a conflict. Preserve both
  inputs, quarantine the run staging, and investigate; never replace
  `sealed.json` or the commit marker.
- Local seal without artifact reference means retry publication, not local
  resealing. A stop preparation whose marker binding or hash-chain identity
  differs is an integrity failure; never rebuild it from later state. Artifact
  publication without a hash-chain-anchored public stage
  means recreate that stage only through the exact reservation. An anchored
  stage without terminal Core state is resumed at its recorded revision;
  terminal Core state must agree exactly with the stage.
- A public bundle file without `bundle.indexed`, or an indexed bundle without
  a committed observer marker/`bundle.completed`, is interrupted progress and
  is not readable. Startup repairs it from the exact stage. Do not hand-edit a
  stage, bundle file, index, marker, or completion.
- At service construction, only regular `.staging-*` files left by the atomic
  writer in `bundle-publications/` or `bundles/` are removed. A pre-terminal
  stage file with no hash-chain anchor is rolled back for exact-reservation
  retry. A terminal unanchored stage, anchored stage with no exact file,
  non-canonical/tampered stage, foreign bundle file, or conflicting index or
  completion is an integrity failure and aborts startup.
- Do not remove run staging until the artifact, canonical public stage, Core
  terminal record, public file/index, observer marker, and completion agree.

## Interrupted-run branch

An interrupted run is never resumed. During the pre-RPC startup gate, the
daemon first requires the version-6 observer marker filename/run ID, immutable
persisted run-plan digest, full observer start signature, exact external
`CollectorPlan` bindings, and start-commit flags to match the durable run. When
`target.lifecycle` is required, the marker must also carry the exact intrinsic
collector ID/start time. The daemon then adopts the exact persisted
target identity, force-stops any surviving execution, proves it stopped, and
requires the platform's collector-cleanup authority to have removed everything
it owns. Windows uses a preflighted private kill-on-close Job for the whole
collector tree. Linux parent-death `SIGKILL` covers only the directly spawned
process; daemonized or surviving helpers require an external cgroup/process-tree
authority. Missing ownership evidence in a phase that requires it, an ambiguous
physical binding, or an unsupported collector-cleanup guarantee fails startup;
the daemon does not guess at continuity.

After the platform cleanup proof, local output reconciliation verifies each exact
collector transaction and its object reachability. Valid finalized artifacts
are retained, but the collector still receives lost coverage and a continuity
gap. Incomplete output is durably marked `aborted` and its partial files are
removed. Missing output after a committed start, foreign/mismatched entries,
unsafe file types, invalid manifests, invalid object names, and missing or
digest-mismatched referenced objects abort startup. Complete unreferenced
objects may be removed as verified garbage. A truncated pending object is
removed only when it is a verified prefix tied to an exact interrupted partial;
startup then requires precisely the reachable finalized object set.

After those proofs, recovery may reconstruct only a prepared, inert target so
the normal stop/finalization path can run. It does not call `StartRun`, restart
the specimen or collectors, or create a replacement maximum-duration timer.
Finalization must:

- record every planned signal as lost and include explicit
  control-plane-loss coverage gaps;
- seal and publish a failed observation bundle with its artifact reference and
  digest;
- link a `control_plane_failure` incident to the failed run;
- commit the final observer marker and `bundle.completed` read gate; and
- transition every nonterminal target operation for that run to `lost`.

The failed run, bundle, incident, gaps, and lost operations are the durable
recovery result. A retry must return that same terminal result without
rewriting or resealing it. Any failure to prove physical stop/cleanup or to
complete finalization aborts daemon startup before RPC and before lease
cleanup.

The local finalizer, control-store ordering, and content-addressed local bundle
publication are implemented and tested. The deployment must still supply and
qualify its observer adapters and any remote evidence repository, replication,
or cross-system custody workflow.

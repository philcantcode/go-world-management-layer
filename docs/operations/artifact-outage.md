# Artifact-service outage

Use when immutable input resolution, object reads, output capture, or
observation-bundle publication fails at the artifact authority boundary.

## Procedure

1. Stop admissions that require uncached inputs and stop new high-volume
   captures when policy permits. Existing cached, pinned work may continue only
   if its eventual evidence obligations can still fit within local reserve.
2. Open a `control_plane_failure` or deployment-specific artifact incident.
   Record the operation, security scope, occurrence/reference, expected digest
   and size, backend error, and affected leases/runs without logging credentials
   or sensitive payloads.
3. Keep resolved input-view pins, sealed bundle objects, export staging,
   canonical public-bundle stages, public files/indexes/completions, observer
   markers/outputs, and ledger segments. Do not acknowledge publication or
   terminal run success without the authoritative artifact reference and
   verified digest. Do not synthesize `bundle.completed` to bypass an
   unfinished publication saga.
4. Retry with the original idempotency identity and bounded backoff. Do not
   switch repositories, security scopes, or destinations implicitly.
5. After service recovery, re-resolve metadata, publish staged objects, and
   verify returned occurrence identity, digest, size, roles, and scope. The run
   finalization service commits the terminal control transition only after the
   bundle artifact is accepted with the sealed metadata digest.
6. Release staging and pins only after the artifact, hash-chain-anchored public
   stage, Core terminal record, public bundle file/index, committed observer
   marker, and `bundle.completed` read gate agree. Finalize any overrun or lost
   staging as an explicit failure/gap.

The physical deployment's local material authority resolves only the exact
scope-bound catalog entries and selections declared in the deployment profile,
opens digest-verified content, and publishes workspace outputs and sealed
bundles to its configured content-addressed publication root. It is a local
authority, not a remote forensic service: replication, repository credentials,
cross-system occurrence identity, external availability policy, and outage
capacity still have to be supplied and integration-tested by the deployment.

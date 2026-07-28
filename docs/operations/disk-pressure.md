# Disk pressure

Use for low free space, inode exhaustion, quota events, or growth that threatens
the control database or unfinalized evidence.

## Response order

1. Stop admission for the constrained resource and open a `host_pressure`
   incident. Record free bytes, inodes, filesystem/quota limits, top-level
   usage beneath configured roots, and active run/capture counts.
2. Protect control-plane reserve. Do not delete the SQLite database, its WAL or
   shared-memory sidecars, open ledger segments, finalization reservations,
   canonical public-bundle stages, public bundle files/indexes/completions,
version-5 observer markers or output transactions/objects, local seal/commit
   markers, writable layers for active generations, or unacknowledged artifact
   staging.
3. Reduce producers according to policy: increase observation of the pressure,
   expire unused reservations, stop unleased warm resources, then quiesce
   preemptible work. Forced eviction must be incident-visible and finalize the
   affected run/exec as failed.
4. Finalize/rotate complete segments and publish already sealed evidence where
   the artifact service is healthy. Bound retries so outage staging cannot grow
   without limit.
5. Run input-cache collection only through the cache API. It removes expired,
   unpinned views before unreferenced content and honors high/low watermarks.
   Never manually delete a pinned view or content object.
6. Apply deployment retention to acknowledged, finalized staging only. Record
   every removed class, size, retention decision, and authoritative reference.
7. If safety cannot be restored, quarantine the node and move future work
   elsewhere; do not silently pause active containers.

## Exit criteria

Free bytes and inodes must remain above the configured low-water threshold for
the observation window, SQLite and ledger verification must pass, cache pins
must resolve, and every eviction or evidence gap must have a durable incident.

Effective-policy admission now inventories all durable sessions and enforces
aggregate CPU, memory, and capture limits before physical mutation. The generic
admission and cache-GC decision logic is also implemented and tested. Automatic
host pressure monitoring, quotas, alerts, eviction selection, and actuation are
still deployment prerequisites.

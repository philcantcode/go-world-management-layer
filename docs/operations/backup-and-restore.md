# Backup and restore

The repository does not currently provide an online backup command. Use a
deployment-owned, tested snapshot procedure. Copying only the live SQLite main
file is not a valid backup while WAL writes may be active.

## Backup

1. Drain admission and quiesce mutations. Gracefully stop the applicable
   host process for the safest portable procedure. Independent control-state
   trees are
   independent services; back up each separately and never merge their state.
2. Record the world binaries, Go/module build identity, configuration and policy
   digests, SQLite `user_version`, external runtime versions, and filesystem
   snapshot identifier.
3. Take one consistent snapshot of the control database and any sidecars;
   observation ledger segments/indexes; orchestration finalization
   reservations, hash-chain-anchored canonical files in
   `bundle-publications/`, public bundle files/indexes and
`bundle.completed` records; version-6 run-observer markers; observer
   `runs/` transactions and content-addressed `objects/`; capture/export state;
   local finalizer seals/commits and other unfinalized staging; the exact
   deployment profile and policy sources; and deployment secrets through their
   own secure backup path. These files form one publication history and must
   not be snapshotted or restored independently.
4. Agent upper layers and target writable state are optional incident material,
   not restartable process state. If retained, bind them to their exact old
   generation and never resume them invisibly after restore.
5. The input cache is reconstructible and may be excluded, but retain its pin
   metadata if the deployment expects to reconcile staged work.
6. Hash and inventory the backup, protect it according to evidence sensitivity,
   and test restoration on an isolated node.

## Restore

1. Restore onto an empty, quarantined node root. Never merge two control
   histories or restore over a running daemon.
2. Use a binary that supports the backup schema. Startup intentionally rejects
   a database with a newer `user_version`; forward migrations run at open, so
   retain the pre-migration backup until validation completes.
3. Start with admission closed. Require the sibling process-ownership lock,
   control-record hash verification, and full replay to pass. Open ledgers and
   record any permitted incomplete-tail repair; all other corruption is a hard
   stop. Startup may remove only recognized regular atomic `.staging-*` files.
   It must verify every canonical public stage against its reservation and
   hash-chain anchor, every public file/index/completion, every version-6
   observer binding, and every observer transaction/object.
4. Reconcile external resources. The physical daemon will accept an active
   agent/target container only when the exact durable plan, policy pair, labels,
   and physical configuration can be reconstructed and matched. A nonterminal
   target run is not resumed: startup terminates leftover execution, records
   control-plane loss and coverage gaps, seals a failed bundle/incident, and
   leaves later recovery to an explicit new run or generation. Unprovable state
   is a hard startup failure.
5. Verify artifact references, bundle stages/files/indexes/completions,
   committed observer markers, finalized/aborted observer outputs, cache pins,
   and incidents before reopening admission. A retained finalized collector
   artifact does not restore run continuity.

Success requires a retained restore report and a read-only query of known
sessions/incidents matching the backup inventory.

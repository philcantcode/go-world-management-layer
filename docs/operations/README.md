# Operations runbooks

These runbooks describe the recovery invariants for a world-management node.
They deliberately separate repository behavior from deployment-owned host
operations.

The shipped daemons execute crash-consistent logical control transitions,
ledger recovery/live projection, local observation-bundle finalization, and an
opt-in physical Linux and Android composition. A trusted version-3 deployment
profile can activate directory-copy workspaces, Docker-backed agent and
Linux-target drivers, managed Android SDK Emulator targets,
deployment-authorized local material, process observers, and ledger capture.
Startup compiles strict policy against probed capabilities, preflights
every physical plan, and reconciles durable ownership before opening the
listener. The deployment must still supply and qualify the host, pinned images
and observer programs, service management, monitoring, and any remote artifact
backend.

`android-target-driver=android-emulator` owns headless AVD creation, clean boot,
exact-serial scoped ADB/file transport, quarantine, replacement-generation
reset, destruction, durable allocation, and startup reconciliation. It accepts
one exact full-tree system-image digest/package identity per deployment and one
mutable run per generation. Managed lifecycle resource containment is Windows-
only: a named Job caps the whole host process tree, guest RAM is configured
separately, and `writableState` binds exact guest `/data` capacity. Other hosts
fail closed. The Docker Linux driver has the same one-run rule:
run stop proves the exact container stopped, and only replacement reset creates
authority for another run. The
AttachedEmulator test remains a separate qualification for externally owned
devices; do not treat it as proof of managed lifecycle behavior.

## Common rules

- Stop admission before changing host resources. Keep read-only control access
  available when it is safe to do so.
- Never bypass a held `<canonical-control-path>.worldd.lock` or start two
  daemons against one control history. Keep each daemon's state in a dedicated
  directory on platforms whose namespace lock intentionally excludes siblings.
- Preserve IDs, revisions, policy and capability digests, timestamps, and
  command output in the incident record.
- Never present a restarted process, restored device, or recreated container
  as continuous active work. An interrupted run is finalized failed with its
  gap/incident; resource recovery creates a new generation linked to the
  incident. A runtime restart used only to prove cleanup must not resume the
  specimen or its duration window.
- Do not remove local evidence, cache pins, writable layers, or staging data
  until the immutable artifact, canonical public stage, Core terminal record,
  public bundle/index, observer commit, and completion gate agree or an
  authoritative abandonment decision is recorded.
- Reconcile by structural ownership labels and configured roots. Never use an
  unscoped bulk `docker rm`, recursive unmount, cgroup kill, or filesystem
  deletion.
- Corruption and uncertain ownership fail closed. Quarantine the affected
  target or node and escalate rather than repairing history by hand.

## Runbooks

- [Startup and reconciliation](startup-and-reconciliation.md)
- [Deployment profiles and effective policy](deployment-and-policy.md)
- [Real-system qualification](qualification.md)
- [Docker loss](docker-loss.md)
- [Leaked mounts and cgroups](leaked-mounts-and-cgroups.md)
- [Disk pressure](disk-pressure.md)
- [Artifact-service outage](artifact-outage.md)
- [Observation-bundle finalization](observation-bundle-finalization.md)
- [Backup and restore](backup-and-restore.md)
- [Quarantine](quarantine.md)
- [Upgrades and version skew](upgrades-and-version-skew.md)

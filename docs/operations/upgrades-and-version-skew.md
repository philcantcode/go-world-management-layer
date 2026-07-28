# Upgrades and version skew

Use for world binary, schema, Docker/runtime, image, Cuttlefish/ADB, collector,
or kernel changes.

## Before upgrade

1. For the shipped physical Linux composition, qualify the complete tuple on a
   disposable node: world binaries/API, deployment-profile version, strict
   policy and complete capability digest, control schema, guest framing and
bundle publication-stage/index/completion formats, version-5 observer marker
   and output-transaction formats, Docker Engine/API/security options,
   filesystem/cgroup facts, images, observer programs, and local or remote
   material authority. Treat
   Android SDK emulator/Cuttlefish/ADB qualification as a separate tuple.
2. Drain the node and finalize active runs. Take and verify a backup. Do not
   upgrade a live target or collector underneath an existing generation.
3. Record old/new versions and capability fingerprints. Runtime, image,
   emulator, snapshot, tool, or required-collector changes require a new
   affected generation; policy/capability digests must not be silently reused.

## Rollout

1. Upgrade one quarantined canary with admission closed. Start the control
   service and require migrations, hash verification, replay, ledger open, and
   external-resource reconciliation to pass.
2. Verify API/RPC authentication, the daemon ownership lock, bounded messages,
   guest byte-stream separation, Docker create/exec/teardown, mount/cgroup
   cleanup, collector readiness/gap reporting, interrupted output
   reconciliation, bundle saga recovery/read gating, and artifact digest
   checking.
3. Reject unknown or newer schemas and protocol mismatches. Do not add a
   compatibility shim that hides semantic skew; keep the old component set or
   complete the coordinated upgrade.
4. Admit a canary lease/run, then expand gradually while watching incidents,
   resource pressure, gaps, and cleanup.

## Rollback

Stop admission first. Restore the prior binary/configuration only if it supports
the current database schema; otherwise restore the pre-upgrade snapshot onto an
empty root. Never downgrade a migrated database in place or splice ledger
histories.

The store rejects a newer schema. Physical daemon startup probes runtime and
observer facts, compiles the strict policy against one complete fingerprint,
inspects exact images, preflights plans, and reconciles physical ownership
before listening. A supported node matrix and mixed-version fleet policy remain
external release artifacts.

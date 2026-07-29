# Upgrades and version skew

Use for world binary, schema, Docker/runtime, image, Android SDK Emulator/ADB,
collector, or kernel changes. Daemon-selected Cuttlefish remains a future,
separately qualified backend.

## Before upgrade

1. For the shipped physical composition, qualify the complete tuple on a
   disposable node: world binaries/API, deployment-profile version 3, strict
   policy and complete capability digest, control schema, guest framing and
   bundle publication-stage/index/completion formats, version-6 observer marker
   and output-transaction formats, Docker Engine/API/security options,
   filesystem/cgroup facts, images, observer programs, and local or remote
   material authority. Treat the Android SDK Emulator, platform-tools ADB,
   command-line tools, accelerator, system-image tree, and Android runtime as a
   separate exact tuple; do not substitute Cuttlefish qualification.
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
   guest byte-stream separation, Docker create/exec/stop/teardown, detached and
   session-escaped process containment, mount/cgroup cleanup, managed Android
   Windows Job CPU/memory containment and restart reopen, independently sized
   guest RAM, exact `/data` capacity, create/clean boot/scoped ADB/real APK
   execution, one-run replacement reset, interrupted-run containment/
   reconciliation, and exact AVD destruction. Also
   verify collector readiness/gap reporting, interrupted output reconciliation,
   bundle saga recovery/read gating, and artifact digest checking.
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

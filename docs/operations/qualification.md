# Real-system qualification

The ordinary Go suite is deterministic and does not start external runtimes.
Use these opt-in checks to prove the boundaries that depend on Docker or the
Android SDK Emulator.

## Full daemon and Docker lifecycle

Prerequisites are Windows PowerShell, Go, and a running Docker Engine in Linux
container mode. From the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\testdata\e2e\run-world-e2e.ps1
```

The harness builds the daemon and client binaries, Linux `world-guest`,
`world-idle`, and native specimen binaries, then builds a Docker image and pins
its repository digest into a generated strict policy and deployment profile.
It pins a 45-second daemon control timeout for Docker-only qualification. The
managed-Android form uses one shared eight-minute cold-boot contract and a
ten-minute RPC/control envelope, matching the real driver test while leaving
bounded cleanup headroom. It exercises real agent provisioning/exec, target
creation and readiness, material projection, direct exec, push/pull digest and mode checks,
hostile-path and oversized-transfer denial, capture, export, target reset,
lease release, and absence of owned orphan containers. It proves the first run
consumes its mutable generation, denies a second run before replacement reset,
stops the exact target container before sealing evidence, and prevents detached
or `setsid`-escaped processes from mutating sealed state. The normal run must be
refetched as `completed`; the capture must publish exactly one
`observation-capture` artifact with a canonical digest, durable reference, and
size inside its admitted 4 MiB bound; and export must publish exactly the
selected result and retained `workspace-change-manifest`. Before lifecycle work
it cross-builds the Linux `internal/processlock` test binary, runs it inside the
Docker fixture, and records the replacement-resistance result as
`boundaries.linux_process_lock_replacement_denied`. The primary agent and Linux
target specimen paths also read their own live cgroup-v1 or unified-v2
membership and controller files, report the detected hierarchy, and require
exact 500 milli-CPU, 256 MiB memory, zero swap, and 128 PID limits. A private
cgroup namespace may canonically report its membership as `/`. Its crash
phase leaves long-lived agent and target specimen execs active, force-kills the
daemon, and requires startup to stop/start the exact agent container with a
fresh framed readiness proof before ordinary provisioning, mark that exec
`lost`, and stop
the exact target container without restarting its tainted run. It then
finalizes the run `failed`, seals an incident-bound bundle with an explicit
control-plane-loss gap, denies renewed run authority, and destroys the
recovered target. A final restart must retain both normal and interrupted
bundles. A `status: passed` result is written to
`.cache/e2e-runs/<timestamp>/evidence.json` only after the final fail-closed
cleanup audit succeeds. It records before/after repository-source identity and
the exact tested-binary digests; retain that file and the daemon logs with
release evidence.

The harness snapshots every ambient Docker container, not only recognized World
roles, and compares its exact identity, image, labels, and lifecycle state after
the run. Every test-created/discovered container is retained in an immutable
full-ID cleanup ledger, so final absence does not depend on labels surviving a
crash. The default image tag is unique per run. An explicitly supplied tag must
be unused; the harness records its pre-test mapping, removes only the test tag
and non-preexisting final image, and proves the mapping was restored. It also
records absence of the exact released backing-workspace directory and every
destroyed Linux target generation directory (empty structural target parents
are permitted, but files or owned generation directories are not).

Docker Desktop proves integration behavior and exact controller enforcement
inside its Linux VM, not dedicated-host isolation, native Linux ownership
modes, host-level cgroup topology, performance, or resistance to a host-kernel
escape. In particular, a Windows-host bind mount requested as
mode `0600` can be reported as `0777` inside the Linux container; the driver
records that as a mode boundary and the shipped directory-copy workspace is
explicitly non-production. Directory copy checks byte and inode bounds at
prepare/scan/seal/export boundaries but has no live byte or inode quota while
the agent runs, so both physical support facts must remain `unsupported`.
Admission skips only those two facts in directory mode; CPU, memory, swap, PID,
capture, and container identity/isolation enforcement must still qualify.
OverlayFS/production admission must fail without live workspace byte and inode
enforcement. The Docker inventory remains global and fail-closed,
but inspect requests are issued in exact, substitution-resistant batches of at
most 32 so a host with hundreds of unrelated containers does not consume the
entire cleanup window. Qualify ownership/mode enforcement and the remaining
host properties separately on the production host tuple.

Container inventory derives one cgroup identity only from exact native-Linux
`/proc/<engine-pid>/cgroup` unified-v2 membership whose leaf contains Docker's
full 64-character container ID. A complete cgroup-v1 CPU/memory/PID membership
is accepted, but its multiple hierarchy paths are not collapsed into a false
single identity. On Docker Desktop and other non-Linux hosts the capability
reports `unavailable-on-non-linux-host` and `CgroupID` remains empty; it is
never synthesized from `CgroupParent` or an engine configuration value.

## Managed Android SDK Emulator lifecycle

Prerequisites are Windows, a working hardware accelerator, Android SDK Emulator and
platform tools, the command-line tools, one installed rooted/debuggable system
image, and the signed specimen APK. Build and verify the specimen as shown in
the attached-device section below. Host Job CPU/memory, emulator guest RAM, and
guest `/data` capacity are distinct policy facts. Guest RAM must be an exact
whole MiB in the 1536..8192 MiB range. `/data` must be an exact whole MiB in the
64..2047 MiB range; the example uses a 6 GiB host Job memory cap, 2 GiB guest
RAM, and a 1 GiB `/data` device. Managed lifecycle startup fails closed on
non-Windows hosts because equivalent whole-process-tree containment is not yet
implemented there. Then run:

```powershell
$env:WORLD_ANDROID_MANAGED_E2E = '1'
$env:WORLD_ANDROID_SDK_ROOT = "$env:LOCALAPPDATA\Android\Sdk"
$env:WORLD_ANDROID_SYSTEM_IMAGE_PACKAGE = `
  'system-images;android-35;google_apis;x86_64'
$env:WORLD_ANDROID_SPECIMEN_APK = `
  (Resolve-Path .\testdata\e2e\android-specimen\build\world-specimen.apk)
go test ./internal/drivers/target/cuttlefish `
  -run '^TestManagedEmulatorDriverEndToEnd$' -count=1 -v -timeout 25m
```

`TestManagedEmulatorDriverEndToEnd` hashes the complete installed system-image tree; observes the exact
emulator, ADB, sdkmanager, accelerator, and Android build identities; reserves
real supported console/ADB port pairs; and creates a headless accelerated AVD.
It projects verified material, writes and reads real guest bytes through the
run-scoped ADB endpoint, installs/launches/crashes/relaunches the signed
specimen, and seals the run. It proves that the mutable generation cannot be
reused, creates a separately allocated clean replacement AVD, proves the old
serial and prior mutation/package are absent, closes and reconstructs the
driver, adopts only the exact durably committed runtime, destroys it, and
verifies the serial and AVD directory are gone. The unresolved launch-handoff
cases are deterministic tests, not claims made by this real-emulator test:
`TestManagedEmulatorIntentOnlyNeverBecomesTimedAbsence`,
`TestManagedEmulatorRecoversOnlyPIDBoundToExactLaunchArgument`, and
`TestManagedEmulatorPIDFileRejectsAmbiguousValues` exercise intent-only, PID
binding, and ambiguous/mismatched PID-file behavior respectively.

To qualify the same driver through the shipped daemon, authenticated RPC,
deployment profile, strict policy, material authority, and scoped `world-target`
ADB gateway—while retaining all Docker lifecycle checks—run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\testdata\e2e\run-world-e2e.ps1 -ManagedAndroid
```

The combined script is the real daemon/RPC qualification (it is not a Go test).
Together, the exact real Android tests are
`TestManagedEmulatorDriverEndToEnd`, `TestAttachedEmulatorDriverEndToEnd`, and
`run-world-e2e.ps1 -ManagedAndroid`. The resulting `evidence.json` contains the
exact signed APK and image identities, assigned serial and process ownership,
independent Windows observations of the full emulator argv, named-Job
membership, CPU hard cap and memory limit for both generations, and independent
1 GiB `/data` measurements for both generations. Both generations run the same
immutable `android-exact-adb` logcat adapter: the target driver's typed run
attachment supplies the generation's literal loopback server and independently
allocated serial, while static configuration contains only `logcat` and
`get-state` actions. Each generation emits a distinct marker through the scoped
ADB gateway. Generation 1 seals normally with available partial coverage and a
digest-verified immutable stdout object containing only its marker.

After adopting ready generation 2 across a daemon restart, the script proves
the replacement marker reached that run's live partial and records the exact
new collector PID/path/start token. It opens a separate live scoped ADB stream,
kills the daemon, and requires the per-collector Windows kill-on-close Job to
remove the collector while preserving the exact shared ADB server. Restart
fsyncs and publishes the committed stdout/stderr prefix as canonical immutable
objects, but truthfully seals both required logcat and lifecycle coverage as
`lost`/`none`, each with its own control-plane-loss gap. The recovered stdout
must contain the generation-2 marker and exclude generation 1's marker. Startup
also stops the tainted emulator; the script proves its exact process and serial
unreachable before destroying the target.

Cleanup requires an empty durable emulator-allocation registry, compares the
global inactive AVD inventory and bounded content identities, and preserves the
exact PID/path/start-token set for an ambient ADB server. If port 5037 was
initially absent, the harness starts an exact `tcp:127.0.0.1:5037` server,
records its listener identity, accepts ambient physical/emulator devices it
discovers, and later stops only that unchanged process after all owned serials
are gone. An ambient preexisting ADB server is never stopped.

Production configuration must pin the same full-tree digest. Calculate it with:

```powershell
$image = Join-Path $env:WORLD_ANDROID_SDK_ROOT `
  'system-images\android-35\google_apis\x86_64'
go run ./cmd/world-android-image-digest -path $image
```

Before publishing a policy, run `world-capabilities` with the exact SDK root,
tool paths, loopback ADB server, emulator/runtime versions, image digest and
package, and the same even `5554..5584` base console port configured for
`worldd`. The report
must contain supported managed-Android and hardware-acceleration capabilities;
its combined fingerprint is the one policies bind.

## Attached Android SDK emulator

This test requires an Android SDK emulator that was started outside the test,
an exact ADB serial, and the specimen APK. Build the repository specimen when
needed:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\testdata\e2e\android-specimen\build.ps1
```

The build script creates a local test keystore, signs the APK, and verifies the
signature before returning its path. For release evidence, independently record
the artifact hash and signer verification, for example:

```powershell
Get-FileHash -Algorithm SHA256 `
  .\testdata\e2e\android-specimen\build\world-specimen.apk
& "$env:LOCALAPPDATA\Android\Sdk\build-tools\35.0.0\apksigner.bat" `
  verify --verbose --print-certs `
  .\testdata\e2e\android-specimen\build\world-specimen.apk
```

Then run the opt-in driver test:

```powershell
$env:WORLD_ANDROID_EMULATOR_E2E = '1'
$env:WORLD_ANDROID_EMULATOR_SERIAL = 'emulator-5554'
$env:WORLD_ANDROID_ADB = 'adb'
$env:WORLD_ANDROID_SPECIMEN_APK = `
  (Resolve-Path .\testdata\e2e\android-specimen\build\world-specimen.apk)
go test ./internal/drivers/target/cuttlefish `
  -run '^TestAttachedEmulatorDriverEndToEnd$' -count=1 -v
```

The test attaches only to the exact serial, creates a target/run through the
AttachedEmulator backend, projects run material, and installs and launches
`dev.philcantcode.worldspecimen` through a real ADB client connected to the
run-scoped gateway. It reads the app's report and checks that no host Docker
socket or workspace path is visible; deliberately crashes the app, requires
the crash in logcat and the process to be absent, then relaunches it; denies a
different serial; and stops the run. Teardown must revoke the scoped endpoint
while direct ADB proves the externally owned emulator is still running.
Retain the verbose test output and a screenshot of the ready specimen alongside
the APK hash as the attached-device evidence set.

This is only the non-owning attached-device boundary qualification. Use the
managed lifecycle test above to qualify daemon-selectable AVD creation, reset,
durable allocation, reconciliation, and destruction.

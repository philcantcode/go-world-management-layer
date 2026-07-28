# Real-system qualification

The ordinary Go suite is deterministic and does not start external runtimes.
Use these opt-in checks to prove the boundaries that depend on Docker or ADB.

## Full daemon and Docker lifecycle

Prerequisites are Windows PowerShell, Go, and a running Docker Engine in Linux
container mode. From the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\testdata\e2e\run-world-e2e.ps1
```

The harness builds the daemon and client binaries, Linux `world-guest`,
`world-idle`, and native specimen binaries, then builds a Docker image and pins
its repository digest into a generated strict policy and deployment profile.
It pins a 45-second daemon control timeout for this deliberately destructive
qualification. It exercises real agent provisioning/exec, target creation and
readiness, material projection, direct exec, push/pull digest and mode checks,
hostile-path and oversized-transfer denial, capture, export, target reset,
lease release, and absence of owned orphan containers. The normal run must be
refetched as `completed`; the capture must publish exactly one
`observation-capture` artifact with a canonical digest, durable reference, and
size inside its admitted 4 MiB bound; and export must publish exactly the
selected result and retained `workspace-change-manifest`. Before lifecycle work
it cross-builds the Linux `internal/processlock` test binary, runs it inside the
Docker fixture, and records the replacement-resistance result as
`boundaries.linux_process_lock_replacement_denied`. Its crash phase leaves a
long-lived specimen exec active, force-kills the daemon, and requires startup
reconciliation to adopt the exact container, kill the old exec, finalize the
run `failed`, seal an incident-bound bundle with an explicit control-plane-loss
gap, deny renewed run authority, and destroy the recovered target. A final
restart must retain both normal and interrupted bundles. The result is written
to `.cache/e2e-runs/<timestamp>/evidence.json`; retain that file and the daemon
logs with release evidence.

Docker Desktop proves integration behavior, not dedicated-host isolation,
native Linux ownership modes, cgroup enforcement, performance, or resistance
to a host-kernel escape. In particular, a Windows-host bind mount requested as
mode `0600` can be reported as `0777` inside the Linux container; the driver
records that as a mode boundary and the shipped directory-copy workspace is
explicitly non-production. The Docker inventory remains global and fail-closed,
but inspect requests are issued in exact, substitution-resistant batches of at
most 32 so a host with hundreds of unrelated containers does not consume the
entire cleanup window. Qualify ownership/mode enforcement and the remaining
host properties separately on the production host tuple.

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
Cuttlefish-family driver, projects run material, and installs and launches
`dev.philcantcode.worldspecimen` through a real ADB client connected to the
run-scoped gateway. It reads the app's report and checks that no host Docker
socket or workspace path is visible; deliberately crashes the app, requires
the crash in logcat and the process to be absent, then relaunches it; denies a
different serial; and stops the run. Teardown must revoke the scoped endpoint
while direct ADB proves the externally owned emulator is still running.
Retain the verbose test output and a screenshot of the ready specimen alongside
the APK hash as the attached-device evidence set.

This is a scoped ADB and attached-backend qualification. It does **not** prove
that either daemon can select Android, that the backend can boot or destroy an
AVD, or that managed Cuttlefish allocation/reset/reconciliation is production
ready. Both daemon configurations currently accept only
`android-target-driver=none`.

[CmdletBinding()]
param(
    [string]$ImageTag = "world-e2e:local"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$runName = "{0}-{1}" -f (Get-Date -Format "yyyyMMdd-HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$runRoot = Join-Path $repoRoot ".cache\e2e-runs\$runName"
$toolsRoot = Join-Path $runRoot "tools"
$logsRoot = Join-Path $runRoot "logs"
$profileRoot = Join-Path $runRoot "profile"
$clientRoot = Join-Path $runRoot "client"
$linuxBuildRoot = Join-Path $repoRoot "testdata\e2e\build\linux-amd64"
$processLockTest = Join-Path $linuxBuildRoot "processlock.test"
$sourceRoot = Join-Path $repoRoot "testdata\e2e"
$fixturePath = Join-Path $sourceRoot "fixtures\payload.txt"
$policyTemplatePath = Join-Path $repoRoot "policy\deployment\e2e-directory-copy.yaml"
$policyPath = Join-Path $profileRoot "environment-policy.yaml"
$profilePath = Join-Path $profileRoot "deployment.json"
$evidencePath = Join-Path $runRoot "evidence.json"
$captureByteLimit = 4194304
$maxTransferBytes = $captureByteLimit
$oversizedTransferBytes = $maxTransferBytes * 2

$worldd = Join-Path $toolsRoot "worldd.exe"
$worldctl = Join-Path $toolsRoot "worldctl.exe"
$worldTarget = Join-Path $toolsRoot "world-target.exe"
$worldCapabilities = Join-Path $toolsRoot "world-capabilities.exe"
$daemonProcess = $null
$targetExecProcess = $null
$invocationSequence = 0
$initialContainers = @()
$createdContainers = @()
$completed = $false

function New-Directory {
    param([Parameter(Mandatory)][string]$Path)
    [void](New-Item -ItemType Directory -Path $Path -Force)
}

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Content
    )
    [IO.File]::WriteAllText([IO.Path]::GetFullPath($Path), $Content, [Text.UTF8Encoding]::new($false))
}

function Get-Sha256Reference {
    param([Parameter(Mandatory)][string]$Path)
    return "sha256:" + (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Assert-Condition {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) {
        throw "assertion failed: $Message"
    }
}

function Invoke-ProcessText {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter()][string[]]$Arguments = @(),
        [switch]$ExpectFailure
    )
    $script:invocationSequence++
    $stem = "{0:D3}-{1}" -f $script:invocationSequence, ([IO.Path]::GetFileNameWithoutExtension($Executable))
    $stderrPath = Join-Path $logsRoot "$stem.stderr.txt"
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $nativeOutput = @(& $Executable @Arguments 2> $stderrPath)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }
    $stdoutText = @($nativeOutput) -join [Environment]::NewLine
    $stderrText = ""
    if (Test-Path -LiteralPath $stderrPath) {
        $stderrText = @(Get-Content -LiteralPath $stderrPath) -join [Environment]::NewLine
    }
    $result = [pscustomobject]@{
        executable = $Executable
        arguments = @($Arguments)
        exit_code = $exitCode
        stdout = ([string]$stdoutText).Trim()
        stderr = ([string]$stderrText).Trim()
    }
    if ($ExpectFailure) {
        if ($exitCode -eq 0) {
            throw "expected command failure but it succeeded: $Executable $($Arguments -join ' ')"
        }
    }
    elseif ($exitCode -ne 0) {
        throw "command failed ($exitCode): $Executable $($Arguments -join ' ')`n$stderrText"
    }
    return $result
}

function Invoke-JsonTool {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter()][string[]]$Arguments = @()
    )
    $result = Invoke-ProcessText -Executable $Executable -Arguments $Arguments
    try {
        return $result.stdout | ConvertFrom-Json
    }
    catch {
        throw "command did not return JSON: $Executable $($Arguments -join ' ')`n$($result.stdout)"
    }
}

function Invoke-WorldCtlJSON {
    param([Parameter(Mandatory)][string[]]$Arguments)
    return Invoke-JsonTool -Executable $worldctl -Arguments (@($script:connectionArguments) + $Arguments)
}

function Invoke-WorldTargetText {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$ExpectFailure
    )
    return Invoke-ProcessText -Executable $worldTarget -Arguments (@($script:connectionArguments) + $Arguments) -ExpectFailure:$ExpectFailure
}

function Get-WorldContainerIDs {
    $result = @()
    foreach ($role in @("agent-workspace", "linux-target")) {
        $listing = Invoke-ProcessText -Executable "docker" -Arguments @("ps", "-aq", "--filter", "label=world.role=$role")
        $result += @($listing.stdout -split '\r?\n' | Where-Object { $_ })
    }
    return @($result | Where-Object { $_ } | Sort-Object -Unique)
}

function Get-NewWorldContainers {
    $current = @(Get-WorldContainerIDs)
    $known = @{}
    foreach ($id in $script:initialContainers) {
        $known[$id] = $true
    }
    return @($current | Where-Object { $_ -and -not $known.ContainsKey($_) })
}

function Get-TargetContainerID {
    param([Parameter(Mandatory)][string]$TargetID)
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @(
        "ps", "-aq", "--filter", "label=world.role=linux-target", "--filter", "label=world.target=$TargetID"
    )
    $ids = @($listing.stdout -split '\r?\n' | Where-Object { $_ })
    if ($ids.Count -ne 1) {
        throw "expected exactly one Docker container for target $TargetID, found $($ids.Count)"
    }
    return [string]$ids[0]
}

function Get-ContainerProcesses {
    param([Parameter(Mandatory)][string]$ContainerID)
    return (Invoke-ProcessText -Executable "docker" -Arguments @("top", $ContainerID, "-eo", "pid,args")).stdout
}

function Wait-ContainerProcessState {
    param(
        [Parameter(Mandatory)][string]$ContainerID,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][bool]$Present,
        [int]$TimeoutSeconds = 15
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $processes = Get-ContainerProcesses -ContainerID $ContainerID
        if (($processes -match $Pattern) -eq $Present) {
            return $processes
        }
        Start-Sleep -Milliseconds 200
    } while ((Get-Date) -lt $deadline)
    $expectation = if ($Present) { "appear" } else { "disappear" }
    throw "process pattern $Pattern did not $expectation in container $ContainerID within $TimeoutSeconds seconds"
}

function Get-FreeLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        return ([Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Start-WorldDaemon {
    param([Parameter(Mandatory)][int]$Port)
    $stdoutPath = Join-Path $logsRoot "worldd-$Port.stdout.txt"
    $stderrPath = Join-Path $logsRoot "worldd-$Port.stderr.txt"
    $daemonArguments = @(
        "-state", (Join-Path $runRoot "state\control.db"),
        "-ledger-dir", (Join-Path $runRoot "ledger"),
        "-orchestration-state-dir", (Join-Path $runRoot "orchestration"),
        "-bundle-dir", (Join-Path $runRoot "bundles"),
        "-material-dir", (Join-Path $runRoot "published"),
        "-listen", "127.0.0.1:$Port",
        "-bearer-token", $script:bearerToken,
        "-bearer-subject", "world-e2e-operator",
        "-deployment-profile", $profilePath,
        "-agent-driver", "docker",
        "-workspace-driver", "directory",
        "-agent-workspace-root", (Join-Path $runRoot "workspaces"),
        "-linux-target-driver", "docker",
        "-target-root", (Join-Path $runRoot "targets"),
        "-capture-driver", "ledger",
        "-capture-dir", (Join-Path $runRoot "captures"),
        "-max-transfer-bytes", ([string]$maxTransferBytes),
        "-driver-probe-timeout", "30s",
		"-control-timeout", "45s",
		"-reconciliation-timeout", "45s",
        "-shutdown-timeout", "5s"
    )
    $process = Start-Process -FilePath $worldd -ArgumentList $daemonArguments -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $deadline = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline) {
        if ($process.HasExited) {
            $detail = if (Test-Path -LiteralPath $stderrPath) { Get-Content -LiteralPath $stderrPath -Raw } else { "" }
            throw "worldd exited during startup ($($process.ExitCode)):`n$detail"
        }
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $connect = $client.ConnectAsync([Net.IPAddress]::Loopback, $Port)
            if ($connect.Wait(250) -and $client.Connected) {
                return $process
            }
        }
        catch {
            # The endpoint is expected to reject connections until startup is complete.
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 100
    }
    throw "worldd did not listen on 127.0.0.1:$Port within 120 seconds"
}

function Stop-WorldDaemon {
    param([Diagnostics.Process]$Process)
    if ($null -eq $Process) {
        return
    }
    try {
        if (-not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force
            Wait-Process -Id $Process.Id -Timeout 15 -ErrorAction SilentlyContinue
        }
    }
    catch {
        if (-not $Process.HasExited) {
            throw
        }
    }
}

function Get-MutatedID {
    param([Parameter(Mandatory)][string]$Value)
    $last = $Value.Substring($Value.Length - 1, 1)
    $replacement = if ($last -eq "0") { "1" } else { "0" }
    return $Value.Substring(0, $Value.Length - 1) + $replacement
}

function Write-DeploymentProfile {
    param(
        [Parameter(Mandatory)][string]$PinnedImage,
        [Parameter(Mandatory)][string]$PolicyReference
    )
    $nativePath = Join-Path $linuxBuildRoot "native-specimen"
    $nativeFile = Get-Item -LiteralPath $nativePath
    $fixtureFile = Get-Item -LiteralPath $fixturePath
    $agentResources = [ordered]@{
        cpu_milli = 500
        memory_bytes = 268435456
        storage_bytes = 16777216
        capture_bytes = $captureByteLimit
        inodes = 1024
        pids = 128
    }
    $targetResources = [ordered]@{
        cpu_milli = 500
        memory_bytes = 268435456
        storage_bytes = 16777216
        capture_bytes = 0
        inodes = 0
        pids = 128
    }
    $profile = [ordered]@{
        version = 2
        security_scope = "e2e-scope"
        policies = @(
            [ordered]@{
                reference = $PolicyReference
                source_path = $policyPath
            }
        )
        material = [ordered]@{
            source_root = $sourceRoot
            max_object_bytes = 67108864
            entries = @(
                [ordered]@{
                    reference = "specimen/native"
                    security_scope = "e2e-scope"
                    source_path = "build/linux-amd64/native-specimen"
                    digest = Get-Sha256Reference -Path $nativePath
                    size = $nativeFile.Length
                    logical_path = "inputs/native-specimen"
                    mode = 365
                    role = "specimen"
                    sensitivity = "restricted"
                },
                [ordered]@{
                    reference = "fixture/payload"
                    security_scope = "e2e-scope"
                    source_path = "fixtures/payload.txt"
                    digest = Get-Sha256Reference -Path $fixturePath
                    size = $fixtureFile.Length
                    logical_path = "fixtures/payload.txt"
                    mode = 292
                    role = "fixture"
                    sensitivity = "internal"
                }
            )
            selections = @(
                [ordered]@{
                    reference = "selection:agent-e2e"
                    security_scope = "e2e-scope"
                    occurrences = @("specimen/native", "fixture/payload")
                }
            )
        }
        acquisitions = @(
            [ordered]@{
                selection = [ordered]@{
                    frozen_selection_ref = "selection:agent-e2e"
                    security_scope = "e2e-scope"
                }
                construction = "allow-copy"
                upper_byte_limit = 16777216
                upper_inode_limit = 1024
                policy = $PolicyReference
                agent_image = $PinnedImage
                resources = $agentResources
            }
        )
        targets = @(
            [ordered]@{
                reference = "linux-visible"
                policy = $PolicyReference
                template = [ordered]@{
                    name = "linux-visible"
                    kind = "linux_container"
                    driver = "docker"
                    runtime = "runc"
                    image = $PinnedImage
                    isolation_profile = "observable-container"
                }
                resources = $targetResources
            }
        )
        runs = @(
            [ordered]@{
                target_references = @("linux-visible")
                specimen_occurrence_refs = @("specimen/native")
                fixture_refs = @("fixture/payload")
                required_coverage = @("target.lifecycle")
                material = @(
                    [ordered]@{ reference = "specimen/native"; logical_path = "bin/native-specimen"; mode = 365 },
                    [ordered]@{ reference = "fixture/payload"; logical_path = "payload.txt"; mode = 292 }
                )
                maximum_duration = "2m"
            }
        )
    }
    Write-Utf8NoBom -Path $profilePath -Content ($profile | ConvertTo-Json -Depth 12)
}

function Write-E2EPolicy {
    param([Parameter(Mandatory)][string]$PinnedImage)
    $source = Get-Content -LiteralPath $policyTemplatePath -Raw
    $pattern = 'world-e2e:local@sha256:[0-9a-f]{64}'
    $matches = [regex]::Matches($source, $pattern)
    Assert-Condition -Condition ($matches.Count -eq 2) -Message "E2E policy template contains exactly two pinned image references"
    $rendered = [regex]::Replace($source, $pattern, $PinnedImage)
    Write-Utf8NoBom -Path $policyPath -Content $rendered
}

foreach ($directory in @($runRoot, $toolsRoot, $logsRoot, $profileRoot, $clientRoot, $linuxBuildRoot, (Join-Path $repoRoot ".cache\go-build"), (Join-Path $repoRoot ".cache\go-mod"), (Join-Path $repoRoot ".cache\tmp"))) {
    New-Directory -Path $directory
}

$env:GOCACHE = Join-Path $repoRoot ".cache\go-build"
$env:GOMODCACHE = Join-Path $repoRoot ".cache\go-mod"
$env:GOTMPDIR = Join-Path $repoRoot ".cache\tmp"
$env:TEMP = $env:GOTMPDIR
$env:TMP = $env:GOTMPDIR
$env:CGO_ENABLED = "0"

try {
    Push-Location $repoRoot
    try {
        $initialContainers = @(Get-WorldContainerIDs)

        $env:GOOS = "linux"
        $env:GOARCH = "amd64"
        [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", (Join-Path $linuxBuildRoot "world-guest"), "./cmd/world-guest"))
        [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", (Join-Path $linuxBuildRoot "world-idle"), "./cmd/world-idle"))
        [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", (Join-Path $linuxBuildRoot "native-specimen"), "./testdata/e2e/native-specimen"))
        [void](Invoke-ProcessText -Executable "go" -Arguments @("test", "-c", "-o", $processLockTest, "./internal/processlock"))
        $processLockTestDigest = Get-Sha256Reference -Path $processLockTest
        Remove-Item Env:GOOS
        Remove-Item Env:GOARCH

        foreach ($build in @(
            @($worldd, "./cmd/worldd"),
            @($worldctl, "./cmd/worldctl"),
            @($worldTarget, "./cmd/world-target"),
            @($worldCapabilities, "./cmd/world-capabilities")
        )) {
            [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", $build[0], $build[1]))
        }

        [void](Invoke-ProcessText -Executable "docker" -Arguments @("build", "--pull=false", "-t", $ImageTag, "-f", "testdata/e2e/docker/Dockerfile", "."))
        $processLockQualification = Invoke-ProcessText -Executable "docker" -Arguments @(
            "run", "--rm", "--entrypoint", "/processlock.test",
            "--mount", "type=bind,source=$processLockTest,target=/processlock.test,readonly",
            "--tmpfs", "/tmp:rw,nosuid,nodev,size=16m",
            $ImageTag, "-test.v"
        )
        $processLockOutputLog = Join-Path $logsRoot "linux-processlock.stdout.txt"
        Write-Utf8NoBom -Path $processLockOutputLog -Content $processLockQualification.stdout
        $inspection = (Invoke-ProcessText -Executable "docker" -Arguments @("image", "inspect", $ImageTag)).stdout | ConvertFrom-Json
        Assert-Condition -Condition ($inspection.Count -eq 1) -Message "Docker image inspection returned exactly one image"
        $repository = $ImageTag.Split(":")[0]
        $pinnedImage = @($inspection[0].RepoDigests | Where-Object { $_ -like "$repository@sha256:*" } | Select-Object -First 1)
        if ($pinnedImage.Count -eq 0) {
            throw "Docker did not expose a repository digest for $ImageTag; exact pinned startup cannot be proven"
        }
        $pinnedImage = [string]$pinnedImage[0]

        Write-E2EPolicy -PinnedImage $pinnedImage
        $capabilityReport = Invoke-JsonTool -Executable $worldCapabilities -Arguments @("-timeout", "30s", "-capture-driver", "ledger", "-policy", $policyPath)
        $policyReference = [string]$capabilityReport.effective_policy.reference
        $policyDigest = [string]$capabilityReport.effective_policy.digest
        $capabilityDigest = [string]$capabilityReport.effective_policy.capability_fingerprint_digest
        Assert-Condition -Condition ($policyReference -eq "e2e-directory-copy@1") -Message "capability probe compiled the intended immutable policy reference"
        Assert-Condition -Condition ($capabilityDigest -eq $capabilityReport.combined.digest) -Message "effective policy is bound to the complete probed capability fingerprint"
        Write-DeploymentProfile -PinnedImage $pinnedImage -PolicyReference $policyReference

        $bearerToken = "world-e2e-" + [guid]::NewGuid().ToString("N")
        $env:WORLD_POLICY_REFERENCE = $policyDigest
        $firstPort = Get-FreeLoopbackPort
        $connectionArguments = @("-address", "127.0.0.1:$firstPort", "-token", $bearerToken, "-timeout", "60s")
        $daemonProcess = Start-WorldDaemon -Port $firstPort

        $acquired = Invoke-WorldCtlJSON -Arguments @(
            "acquire", "-frozen-selection", "selection:agent-e2e", "-cache-scope", "e2e-scope",
            "-policy", $policyDigest, "-capabilities", $capabilityDigest, "-ttl", "1h"
        )
        $leaseID = [string]$acquired.lease.lease_id
        $sessionID = [string]$acquired.view.session.research_session_id
        Assert-Condition -Condition ($leaseID -like "lease_*") -Message "acquisition returned a lease"
        Assert-Condition -Condition ($acquired.view.agent_workspace.generations[-1].state -eq "ready") -Message "agent generation reached ready"

        $fixtureDigest = Get-Sha256Reference -Path $fixturePath
        $agentExec = Invoke-ProcessText -Executable $worldctl -Arguments (@($connectionArguments) + @(
            "open-exec", "-lease", $leaseID, "-policy", $policyDigest,
            "-executable", "/workspace/inputs/native-specimen",
            "-temporary-input", "1:e2e-payload=$fixturePath", "--",
            "-input", "placeholder", "-output", "/workspace/agent-result.json"
        ))
        Assert-Condition -Condition ($agentExec.stdout.Trim() -eq $fixtureDigest) -Message "agent guest executed the native specimen with exact temporary-input substitution"

		# Exercise the startup crash boundary while the target really owns a
		# long-lived Docker exec. Recovery must terminate physical execution,
		# preserve no fictitious observation continuity, and finalize the durable
		# run as failed before the replacement daemon admits RPC traffic.
		$crashTarget = Invoke-WorldCtlJSON -Arguments @(
			"create-target", "-lease", $leaseID, "-template", "linux-visible", "-kind", "linux_container",
			"-policy", $policyDigest, "-capabilities", $capabilityDigest
		)
		$crashTargetID = [string]$crashTarget.target_id
		$crashRun = Invoke-WorldCtlJSON -Arguments @(
			"start-run", "-target", $crashTargetID, "-policy", $policyDigest,
			"-specimens", "specimen/native", "-fixtures", "fixture/payload"
		)
		$crashRunID = [string]$crashRun.target_run_id
		Assert-Condition -Condition ($crashRun.state -eq "running") -Message "crash fixture run reached running"
		$crashContainerID = Get-TargetContainerID -TargetID $crashTargetID
		$longExecStdout = Join-Path $logsRoot "active-crash-exec.stdout.txt"
		$longExecStderr = Join-Path $logsRoot "active-crash-exec.stderr.txt"
		$longExecArguments = @($connectionArguments) + @(
			"exec", "-target", $crashTargetID, "-run", $crashRunID, "-policy", $policyDigest, "--",
			"/target/input/bin/native-specimen", "-sleep", "10m", "-input", "/target/input/payload.txt", "-output", "/target/crash-result.json"
		)
		$targetExecProcess = Start-Process -FilePath $worldTarget -ArgumentList $longExecArguments -PassThru -WindowStyle Hidden -RedirectStandardOutput $longExecStdout -RedirectStandardError $longExecStderr
		$activeProcesses = Wait-ContainerProcessState -ContainerID $crashContainerID -Pattern "/target/input/bin/native-specimen.*-sleep.*10m" -Present $true
		Assert-Condition -Condition (-not $targetExecProcess.HasExited) -Message "long-lived target exec was active at the crash boundary"

		Stop-WorldDaemon -Process $daemonProcess
		$daemonProcess = $null
		Wait-Process -Id $targetExecProcess.Id -Timeout 15 -ErrorAction SilentlyContinue
		$targetExecProcess.Refresh()
		Assert-Condition -Condition $targetExecProcess.HasExited -Message "scoped target client exited when the daemon was force-killed"
		Assert-Condition -Condition ($targetExecProcess.ExitCode -ne 0) -Message "interrupted target exec client reported failure"
		$targetExecProcess.Dispose()
		$targetExecProcess = $null

		$recoveryPort = Get-FreeLoopbackPort
		$connectionArguments = @("-address", "127.0.0.1:$recoveryPort", "-token", $bearerToken, "-timeout", "60s")
		$daemonProcess = Start-WorldDaemon -Port $recoveryPort
		$recoveredCrashTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $crashTargetID)
		$recoveredCrashRuns = @($recoveredCrashTarget.runs | Where-Object { $_.target_run_id -eq $crashRunID })
		Assert-Condition -Condition ($recoveredCrashRuns.Count -eq 1) -Message "startup recovered the exact interrupted target run"
		$recoveredCrashRun = $recoveredCrashRuns[0]
		Assert-Condition -Condition ($recoveredCrashRun.state -eq "failed") -Message "startup finalized the interrupted run as failed instead of resuming it"
		Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace([string]$recoveredCrashRun.bundle_id)) -Message "interrupted run published a durable bundle identity"
		Assert-Condition -Condition (@($recoveredCrashRun.incident_ids).Count -gt 0) -Message "interrupted run recorded a failure incident"
		$recoveredCrashBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $crashRunID)
		Assert-Condition -Condition ($recoveredCrashBundle.state -eq "sealed") -Message "interrupted run produced a sealed observation bundle"
		Assert-Condition -Condition (@($recoveredCrashBundle.gaps | Where-Object { $_.detail -match "control-plane loss" }).Count -gt 0) -Message "interrupted run explicitly records the control-plane continuity gap"
		Assert-Condition -Condition (@($recoveredCrashBundle.coverage | Where-Object { $_.signal_family -eq "target.lifecycle" -and $_.required -and $_.status -ne "available" }).Count -eq 1) -Message "interrupted run does not claim complete lifecycle coverage"
		$crashIncidentID = [string]$recoveredCrashRun.incident_ids[0]
		$crashIncident = Invoke-WorldCtlJSON -Arguments @("get-incident", "-incident", $crashIncidentID)
		Assert-Condition -Condition ($crashIncident.state -eq "evidence_sealed") -Message "startup sealed evidence for the control-plane-loss incident"
		Assert-Condition -Condition ($crashIncident.target_run_id -eq $crashRunID -and $crashIncident.observation_bundle_id -eq $recoveredCrashBundle.bundle_id) -Message "failure incident is bound to the exact interrupted run and bundle"

		$recoveredCrashContainerID = Get-TargetContainerID -TargetID $crashTargetID
		Assert-Condition -Condition ($recoveredCrashContainerID -eq $crashContainerID) -Message "startup adopted the exact target container"
		$runningState = (Invoke-ProcessText -Executable "docker" -Arguments @("inspect", "--format", "{{.State.Running}}", $recoveredCrashContainerID)).stdout
		Assert-Condition -Condition ($runningState -eq "true") -Message "recovered target container returned to its inert running state"
		[void](Wait-ContainerProcessState -ContainerID $recoveredCrashContainerID -Pattern "/target/input/bin/native-specimen" -Present $false)
		$reopenInterruptedRun = Invoke-WorldTargetText -ExpectFailure -Arguments @(
			"exec", "-target", $crashTargetID, "-run", $crashRunID, "-policy", $policyDigest, "--", "/target/input/bin/native-specimen"
		)
		Assert-Condition -Condition (($reopenInterruptedRun.stdout + $reopenInterruptedRun.stderr) -match "(?i)(failed|terminal|state|run)") -Message "the failed interrupted run could not reopen target authority"
		$destroyedCrashTarget = Invoke-WorldCtlJSON -Arguments @(
			"destroy", "-target", $crashTargetID, "-revision", ([string]$recoveredCrashTarget.revision),
			"-reason", "active crash E2E cleanup", "-policy", $policyDigest
		)
		Assert-Condition -Condition ($destroyedCrashTarget.state -eq "destroyed") -Message "recovered crash target was physically destroyed"
		$crashContainerListing = Invoke-ProcessText -Executable "docker" -Arguments @("ps", "-aq", "--filter", "label=world.target=$crashTargetID")
		Assert-Condition -Condition ([string]::IsNullOrWhiteSpace($crashContainerListing.stdout)) -Message "recovered crash target left no Docker container"

		$capture = Invoke-WorldCtlJSON -Arguments @(
            "start-capture", "-lease", $leaseID, "-policy", $policyDigest, "-profile", "worldLifecycle",
            "-signals", "target.lifecycle", "-duration", "1m", "-byte-limit", ([string]$captureByteLimit)
        )

        $target = Invoke-WorldCtlJSON -Arguments @(
            "create-target", "-lease", $leaseID, "-template", "linux-visible", "-kind", "linux_container",
            "-policy", $policyDigest, "-capabilities", $capabilityDigest
        )
        $targetID = [string]$target.target_id
        Assert-Condition -Condition ($target.generations[-1].state -eq "ready") -Message "Linux target reached ready"

        $run = Invoke-WorldCtlJSON -Arguments @(
            "start-run", "-target", $targetID, "-policy", $policyDigest,
            "-specimens", "specimen/native", "-fixtures", "fixture/payload"
        )
        $runID = [string]$run.target_run_id
        Assert-Condition -Condition ($run.state -eq "running") -Message "target run reached running"

        $targetExec = Invoke-WorldTargetText -Arguments @(
            "exec", "-target", $targetID, "-run", $runID, "-policy", $policyDigest, "--",
            "/target/input/bin/native-specimen", "-input", "/target/input/payload.txt", "-output", "/target/result.json"
        )
        Assert-Condition -Condition ($targetExec.stdout.Trim() -eq $fixtureDigest) -Message "target executed the pinned specimen against exact material"

		$resultRelative = "e2e/target-result.json"
        [void](Invoke-WorldTargetText -Arguments @(
            "pull", "-target", $targetID, "-run", $runID, "-policy", $policyDigest,
            "-source", "result.json", "-destination", $resultRelative
        ))
		$verifiedTargetResult = Invoke-ProcessText -Executable $worldctl -Arguments (@($connectionArguments) + @(
			"open-exec", "-lease", $leaseID, "-policy", $policyDigest,
			"-executable", "/workspace/inputs/native-specimen", "--",
			"-input", "/workspace/e2e/target-result.json", "-verify-result",
			"-expected-input-digest", $fixtureDigest, "-output", "/workspace/e2e/verified-target-result.json"
		))
		Assert-Condition -Condition ($verifiedTargetResult.exit_code -eq 0) -Message "agent verified the pulled target result and every required boundary probe"

        [void](Invoke-WorldTargetText -Arguments @(
            "push", "-target", $targetID, "-run", $runID, "-policy", $policyDigest,
			"-source", "fixtures/payload.txt", "-destination", "pushed/payload.txt", "-mode", "292"
        ))
		$pushedRelative = "e2e/pushed-roundtrip.txt"
        [void](Invoke-WorldTargetText -Arguments @(
            "pull", "-target", $targetID, "-run", $runID, "-policy", $policyDigest,
            "-source", "pushed/payload.txt", "-destination", $pushedRelative
        ))
		$roundTrip = Invoke-ProcessText -Executable $worldctl -Arguments (@($connectionArguments) + @(
			"open-exec", "-lease", $leaseID, "-policy", $policyDigest,
			"-executable", "/workspace/inputs/native-specimen", "--",
			"-input", "/workspace/e2e/pushed-roundtrip.txt", "-output", "/workspace/e2e/roundtrip-result.json"
		))
		Assert-Condition -Condition ($roundTrip.stdout.Trim() -eq $fixtureDigest) -Message "workspace-backed push/pull round trip preserved bytes"

        $wrongRun = Get-MutatedID -Value $runID
        $wrongScope = Invoke-WorldTargetText -ExpectFailure -Arguments @(
            "exec", "-target", $targetID, "-run", $wrongRun, "-policy", $policyDigest, "--", "/target/input/bin/native-specimen"
        )
        $scopeFailure = ($wrongScope.stdout + "`n" + $wrongScope.stderr)
        Assert-Condition -Condition ($scopeFailure -match "(?i)(scope|not found|outside|run)") -Message "wrong target-run scope failed explicitly"

        $pathEscape = Invoke-WorldTargetText -ExpectFailure -Arguments @(
            "push", "-target", $targetID, "-run", $runID, "-policy", $policyDigest,
            "-source", "testdata/e2e/fixtures/payload.txt", "-destination", "../escape"
        )
        Assert-Condition -Condition (($pathEscape.stdout + $pathEscape.stderr) -match "(?i)(relative|escape|path|traversal)") -Message "path traversal failed before transfer"

		$oversizedSource = Invoke-ProcessText -Executable $worldctl -Arguments (@($connectionArguments) + @(
			"open-exec", "-lease", $leaseID, "-policy", $policyDigest,
			"-executable", "/workspace/inputs/native-specimen", "--",
			"-input", "/workspace/fixtures/payload.txt", "-output", "/workspace/e2e/oversized.bin", "-output-bytes", ([string]$oversizedTransferBytes)
		))
		Assert-Condition -Condition ($oversizedSource.stdout.Trim() -eq $fixtureDigest) -Message "agent created an oversized workspace-backed transfer fixture"
		$oversized = Invoke-WorldTargetText -ExpectFailure -Arguments @(
			"push", "-target", $targetID, "-run", $runID, "-policy", $policyDigest,
			"-source", "e2e/oversized.bin", "-destination", "oversized.bin"
		)
		Assert-Condition -Condition (($oversized.stdout + $oversized.stderr) -match "(?i)(exceeds|resource exhausted|byte limit)") -Message "bounded transfer rejected an oversized workspace source"

        $targetView = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $targetID)
        $currentRun = @($targetView.runs | Where-Object { $_.target_run_id -eq $runID })
        Assert-Condition -Condition ($currentRun.Count -eq 1) -Message "target view contains the active run"
        $bundle = Invoke-WorldCtlJSON -Arguments @(
            "stop-run", "-target", $targetID, "-run", $runID, "-revision", ([string]$currentRun[0].revision),
            "-reason", "world E2E complete", "-policy", $policyDigest
        )
        Assert-Condition -Condition ($bundle.state -eq "sealed") -Message "target run produced a sealed observation bundle"
        Assert-Condition -Condition (@($bundle.coverage | Where-Object { $_.signal_family -eq "target.lifecycle" -and $_.required }).Count -eq 1) -Message "bundle contains required intrinsic lifecycle coverage"
        Assert-Condition -Condition (@($bundle.normalized_events).Count -gt 0) -Message "bundle contains normalized lifecycle events"
		$stoppedTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $targetID)
		$stoppedRun = @($stoppedTarget.runs | Where-Object { $_.target_run_id -eq $runID })
		Assert-Condition -Condition ($stoppedRun.Count -eq 1 -and $stoppedRun[0].state -eq "completed") -Message "stop-run completed rather than merely sealing a failed run"

        $captureResult = Invoke-WorldCtlJSON -Arguments @(
            "stop-capture", "-lease", $leaseID, "-capture", $capture.capture_id,
            "-revision", ([string]$capture.revision), "-policy", $policyDigest
        )
		Assert-Condition -Condition ($captureResult.state -eq "completed") -Message "ledger capture completed"
		Assert-Condition -Condition (@($captureResult.artifacts).Count -eq 1) -Message "capture published one bounded artifact"
		$captureArtifact = $captureResult.artifacts[0]
		Assert-Condition -Condition ($captureArtifact.role -eq "observation-capture") -Message "capture artifact has the observation-capture role"
		Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace([string]$captureArtifact.reference)) -Message "capture artifact has a durable reference"
		Assert-Condition -Condition ([string]$captureArtifact.digest -match '^sha256:[0-9a-f]{64}$') -Message "capture artifact has a canonical digest"
		Assert-Condition -Condition ([uint64]$captureArtifact.size -le [uint64]$captureByteLimit) -Message "capture artifact remains within its admitted byte bound"

		# Export commit is intentionally last among agent-workspace mutations: it
		# quiesces the agent generation before sealing a point-in-time snapshot.
		$preview = Invoke-WorldCtlJSON -Arguments @("preview-export", "-lease", $leaseID)
		Assert-Condition -Condition ([uint64]$preview.workspace_revision -gt 0) -Message "workspace preview returned a revision"
		Assert-Condition -Condition (@($preview.changes | Where-Object { $_.workspace_relative_path -eq "agent-result.json" }).Count -eq 1) -Message "workspace preview observed the agent result"
		$declared = Invoke-WorldCtlJSON -Arguments @("declare-export", "-lease", $leaseID, "-policy", $policyDigest, "-path", "agent-result.json=result")
		$committed = Invoke-WorldCtlJSON -Arguments @(
			"commit-export", "-lease", $leaseID, "-policy", $policyDigest,
			"-export", $declared.export_id, "-workspace-revision", ([string]$preview.workspace_revision)
		)
		Assert-Condition -Condition ($committed.state -eq "committed") -Message "workspace export committed after quiescing the agent"
		Assert-Condition -Condition (@($committed.artifacts).Count -eq 2) -Message "export published the selected result and retained change manifest"
		Assert-Condition -Condition (@($committed.artifacts | Where-Object { $_.role -eq "result" }).Count -eq 1) -Message "export published exactly one selected result artifact"
		Assert-Condition -Condition (@($committed.artifacts | Where-Object { $_.role -eq "workspace-change-manifest" }).Count -eq 1) -Message "export published exactly one retained workspace change manifest"

		$beforeReset = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $targetID)
        $reset = Invoke-WorldCtlJSON -Arguments @(
            "reset", "-target", $targetID, "-revision", ([string]$beforeReset.revision),
            "-mode", "recreate", "-policy", $policyDigest
        )
        Assert-Condition -Condition ($reset.current_generation -eq 2) -Message "target reset advanced generation"
        Assert-Condition -Condition ($reset.generations[-1].state -eq "ready") -Message "reset target became ready"
        $destroyed = Invoke-WorldCtlJSON -Arguments @(
            "destroy", "-target", $targetID, "-revision", ([string]$reset.revision),
            "-reason", "world E2E cleanup", "-policy", $policyDigest
        )
        Assert-Condition -Condition ($destroyed.state -eq "destroyed") -Message "target was physically destroyed"

        $session = Invoke-WorldCtlJSON -Arguments @("get-session", "-session", $sessionID)
        $released = Invoke-WorldCtlJSON -Arguments @(
            "release", "-lease", $leaseID, "-revision", ([string]$session.lease.revision),
            "-reason", "world E2E cleanup", "-policy", $policyDigest
        )
        Assert-Condition -Condition ($released.lease_id -eq $leaseID) -Message "lease release completed"

        $createdContainers = @(Get-NewWorldContainers)
        Assert-Condition -Condition ($createdContainers.Count -eq 0) -Message "release left no new world containers"

        Stop-WorldDaemon -Process $daemonProcess
        $daemonProcess = $null
        $finalRestartPort = Get-FreeLoopbackPort
        $connectionArguments = @("-address", "127.0.0.1:$finalRestartPort", "-token", $bearerToken, "-timeout", "60s")
        $daemonProcess = Start-WorldDaemon -Port $finalRestartPort

        $recoveredSession = Invoke-WorldCtlJSON -Arguments @("get-session", "-session", $sessionID)
        Assert-Condition -Condition ($recoveredSession.lease.state -eq "released") -Message "crash restart recovered released lease state"
        $recoveredBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $runID)
        Assert-Condition -Condition ($recoveredBundle.bundle_id -eq $bundle.bundle_id) -Message "crash restart recovered the exact sealed bundle"
		$durableCrashBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $crashRunID)
		Assert-Condition -Condition ($durableCrashBundle.bundle_id -eq $recoveredCrashBundle.bundle_id) -Message "final restart retained the exact interrupted-run evidence bundle"

        $evidence = [ordered]@{
            schema_version = 2
            completed_at = (Get-Date).ToUniversalTime().ToString("o")
            run_directory = $runRoot
            image = [ordered]@{ tag = $ImageTag; pinned_reference = $pinnedImage; id = $inspection[0].Id }
            linux_process_lock = [ordered]@{ binary_digest = $processLockTestDigest; output_log = $processLockOutputLog }
            capabilities = $capabilityReport
            policy_digest = $policyDigest
            session_id = $sessionID
            lease_id = $leaseID
            target_id = $targetID
            target_run_id = $runID
            bundle_id = $bundle.bundle_id
			crash_recovery = [ordered]@{
				target_id = $crashTargetID
				target_run_id = $crashRunID
				container_id = $crashContainerID
				bundle_id = $recoveredCrashBundle.bundle_id
				incident_id = $crashIncidentID
				final_state = $recoveredCrashRun.state
				incident_state = $crashIncident.state
				active_exec_observed_before_crash = $true
				active_exec_absent_after_recovery = $true
				continuity_gap_recorded = $true
				target_destroyed = $true
			}
            capture_id = $capture.capture_id
            export_id = $declared.export_id
            artifact_references = @($committed.artifacts.reference)
            capture_artifacts = @($captureResult.artifacts.reference)
            boundaries = [ordered]@{
                linux_process_lock_replacement_denied = $true
                target_host_probes_denied = $true
                wrong_run_scope_denied = $true
                path_traversal_denied = $true
                oversized_transfer_denied = $true
				interrupted_run_reopen_denied = $true
                no_new_world_containers = $true
            }
			restart = [ordered]@{
				abrupt_process_stop_during_active_run = $true
				interrupted_run_failed_before_rpc_admission = $true
				released_lease_recovered = $true
				sealed_bundles_recovered = $true
			}
        }
        Write-Utf8NoBom -Path $evidencePath -Content ($evidence | ConvertTo-Json -Depth 12)
        $completed = $true
        Write-Output $evidencePath
    }
    finally {
        Pop-Location
    }
}
catch {
	Write-Warning "E2E failure line=$($_.InvocationInfo.ScriptLineNumber) stack=$($_.ScriptStackTrace)"
	throw
}
finally {
    Stop-WorldDaemon -Process $daemonProcess
	if ($null -ne $targetExecProcess) {
		try {
			if (-not $targetExecProcess.HasExited) {
				Stop-Process -Id $targetExecProcess.Id -Force
				Wait-Process -Id $targetExecProcess.Id -Timeout 15 -ErrorAction SilentlyContinue
			}
		}
		catch {
			Write-Warning "failed to stop active-crash target client: $_"
		}
	}
    try {
        $createdContainers = @(Get-NewWorldContainers)
        foreach ($containerID in $createdContainers) {
            if ($containerID -match "^[0-9a-f]{12,64}$") {
                & docker rm --force --volumes $containerID | Out-Null
            }
        }
    }
    catch {
        Write-Warning "failed to inspect or remove E2E-owned containers: $_"
    }
    if (-not $completed) {
        Write-Warning "E2E failed; preserving diagnostic run directory $runRoot"
    }
}

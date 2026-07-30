[CmdletBinding()]
param(
    [string]$ImageTag = "world-e2e:local",
    [switch]$ManagedAndroid,
    [string]$AndroidSDKRoot = $env:WORLD_ANDROID_SDK_ROOT,
    [string]$AndroidSystemImagePackage = "system-images;android-35;google_apis;x86_64",
    [string]$AndroidSystemImageDigest = "sha256:cfcbe5934196c36e87ab9f1d983818c949c80897ecbad3b7717bc6f1197971ee",
    [string]$AndroidBackendVersion = "Android emulator version 36.3.10.0 (build_id 14472402) (CL:N/A)",
    [string]$AndroidRuntimeVersion = "google/sdk_gphone64_x86_64/emu64xa:15/AE3A.240806.043/12960925:userdebug/dev-keys",
    [string]$AndroidSpecimenAPK = $env:WORLD_ANDROID_SPECIMEN_APK
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$runName = "{0}-{1}" -f (Get-Date -Format "yyyyMMdd-HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$dockerTestRunLabelName = "dev.philcantcode.world-e2e.run"
$dockerTestRunLabelValue = [guid]::NewGuid().ToString("N")
$requestedImageTag = $ImageTag
if (-not $PSBoundParameters.ContainsKey("ImageTag")) {
    # A per-run tag makes the default invocation incapable of moving an
    # ambient world-e2e:local tag.
    $ImageTag = "world-e2e:e2e-$runName"
}
$runRoot = Join-Path $repoRoot ".cache\e2e-runs\$runName"
$toolsRoot = Join-Path $runRoot "tools"
$logsRoot = Join-Path $runRoot "logs"
$profileRoot = Join-Path $runRoot "profile"
$clientRoot = Join-Path $runRoot "client"
$linuxBuildRoot = Join-Path $repoRoot "testdata\e2e\build\linux-amd64"
$processLockTest = Join-Path $linuxBuildRoot "processlock.test"
$sourceRoot = Join-Path $repoRoot "testdata\e2e"
$fixturePath = Join-Path $sourceRoot "fixtures\payload.txt"
$androidDefaultSDKRoot = if (-not [string]::IsNullOrWhiteSpace($env:ANDROID_SDK_ROOT)) {
    $env:ANDROID_SDK_ROOT
}
elseif (-not [string]::IsNullOrWhiteSpace($env:ANDROID_HOME)) {
    $env:ANDROID_HOME
}
elseif (-not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    Join-Path $env:LOCALAPPDATA "Android\Sdk"
}
else {
    ""
}
$androidDefaultSpecimenAPK = Join-Path $sourceRoot "android-specimen\build\world-specimen.apk"
$policyTemplatePath = Join-Path $repoRoot "policy\deployment\e2e-directory-copy.yaml"
$policyPath = Join-Path $profileRoot "environment-policy.yaml"
$profilePath = Join-Path $profileRoot "deployment.json"
$evidencePath = Join-Path $runRoot "evidence.json"
$observerOutputRoot = Join-Path $runRoot "observer-output"
$captureByteLimit = 4194304
$maxTransferBytes = $captureByteLimit
$oversizedTransferBytes = $maxTransferBytes * 2
$managedAndroidBootTimeout = "8m"
$managedAndroidOperationTimeout = "10m"
$managedAndroidStartupTimeoutSeconds = 660
$linuxCgroupContract = [ordered]@{
    cpu_milli = 500
    memory_bytes = 268435456
    swap_bytes = 0
    pids = 128
}

$worldctl = Join-Path $toolsRoot "worldctl.exe"
$worldTarget = Join-Path $toolsRoot "world-target.exe"
$worldCapabilities = Join-Path $toolsRoot "world-capabilities.exe"
$windowsRuntimeVerifier = Join-Path $toolsRoot "windows-runtime-verifier.exe"
$nativeSpecimen = Join-Path $linuxBuildRoot "native-specimen"
$daemonProcess = $null
$agentExecProcess = $null
$targetExecProcess = $null
$androidADBProxyProcess = $null
$androidADBStreamProcess = $null
$androidADBServerProcess = $null
$androidADBServerStandardOutputPath = $null
$androidADBServerStandardErrorPath = $null
$androidBaseConsolePort = $null
$androidRuntimeName = $null
$androidTargetRoot = Join-Path $runRoot "android-targets"
$androidSystemImageRoot = Join-Path $runRoot "android-images"
$androidADBBinary = $null
$androidEmulatorBinary = $null
$androidSDKManagerBinary = $null
$androidAVDManagerBinary = $null
$androidAPKSignerBinary = $null
$androidAPKSourcePath = $null
$androidAPKSigned = $false
$androidAPKSignerDN = $null
$androidAPKSignatureLog = $null
$androidLogcatObserverReference = "android-logcat"
$androidLogcatObserverVersion = $null
$androidLogcatObserverConfiguration = $null
$androidLogcatObserverConfigurationDigest = $null
$androidLogcatMarker = $null
$androidLogcatArtifact = $null
$androidLogcatObjectPath = $null
$androidLogcatTransactionFact = $null
$androidLogcatFinalizedTransactionFact = $null
$androidResetLogcatMarker = $null
$androidResetLogcatArtifact = $null
$androidResetLogcatObjectPath = $null
$androidResetLogcatTransactionFact = $null
$androidResetLogcatFinalizedTransactionFact = $null
$androidResetInterruptedPartialIdentities = @()
$androidResetLogcatCoverage = @()
$androidResetLogcatGaps = @()
$androidResetLogcatCollectorProcessIdentity = $null
$androidResetLogcatCollectorLaunchFact = $null
$androidResetLogcatCollectorAbsentAfterCrash = $false
$androidResetADBListenerIdentityBeforeCrash = $null
$androidResetADBListenerIdentityAfterCrash = $null
$androidCrashDaemonIdentityBeforeKill = $null
$androidCrashCollectorIdentityBeforeKill = $null
$androidCrashEmulatorIdentityBeforeKill = $null
$androidCrashEmulatorIdentityAfterKill = $null
$androidResetMode = "baseline"
$androidQualification = $null
$androidResourceContract = $null
$androidDataMeasurements = @()
$androidProcessOwnership = $null
$androidOSVerifications = @()
$androidAmbientBefore = $null
$androidAmbientAfter = $null
$androidADBServerWasReachable = $false
$androidADBServerStartedByTest = $false
$androidADBServerReachableAfter = $false
$androidADBServerRestorationCompleted = $false
$androidADBServerIdentityRestored = $false
$androidADBServerProcessIdentity = $null
$androidADBServerExactProcessStopConfirmed = $false
$androidADBDeviceInventoryAfterTestServerStart = @()
$androidADBProcessesBeforeTest = @()
$androidADBProcessesAfterRestoration = @()
$androidUnboundLaunchRemnants = @()
$androidAllocatorCleanupFact = $null
$androidTargetID = $null
$androidTargetCleanupCompleted = $false
$androidTargetDestroyConfirmed = $false
$androidTargetDestroyRequestCount = 0
$androidTargetDestroyGeneration = $null
$androidCreateAttempted = $false
$androidFailureDiagnosticsCaptured = $false
$androidOwnedConsolePorts = @()
$sessionID = $null
$leaseID = $null
$dockerAmbientBefore = @()
$dockerAmbientAfter = @()
$dockerAmbientIDs = @{}
$dockerTrackedContainers = @()
$unknownConcurrentDockerResources = @()
$dockerImageIDsBefore = @()
$dockerImageIDsAfterCleanup = @()
$preexistingDockerImageIDsMissing = @()
$imageTagBefore = $null
$imageTagAfter = $null
$testImageID = ""
$testImageIDWasPreexisting = $false
$dockerBuildStarted = $false
$imageCleanupCompleted = $false
$processLockContainerIDPath = Join-Path $runRoot "processlock-container.cid"
$workspaceCleanupFacts = $null
$linuxTargetCleanupFacts = @()
$invocationSequence = 0
$daemonStartSequence = 0
$completed = $false
$finalCleanupErrors = @()
$sourceIdentityBefore = $null
$sourceIdentityAfter = $null
$policyDigest = $null
$bearerToken = $null
$rpcTimeout = $null
$connectionArguments = @()

function New-Directory {
    param([Parameter(Mandatory)][string]$Path)
    [void](New-Item -ItemType Directory -Path $Path -Force)
}

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][AllowEmptyString()][string]$Content
    )
    [IO.File]::WriteAllText([IO.Path]::GetFullPath($Path), $Content, [Text.UTF8Encoding]::new($false))
}

function Read-SharedUtf8Text {
    param([Parameter(Mandatory)][string]$Path)
    $stream = [IO.File]::Open(
        [IO.Path]::GetFullPath($Path),
        [IO.FileMode]::Open,
        [IO.FileAccess]::Read,
        ([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete)
    )
    try {
        $reader = [IO.StreamReader]::new($stream, [Text.UTF8Encoding]::new($false), $true, 4096, $true)
        try {
            return $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
}

function Test-ContainsExactLine {
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string]$Text,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Expected
    )
    return @($Text -split '\r?\n') -contains $Expected
}

function Get-OptionalObjectProperty {
    param(
        [Parameter(Mandatory)][AllowNull()][object]$InputObject,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Name
    )
    if ($null -eq $InputObject) {
        return $null
    }
    $property = $InputObject.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Get-ProtobufUInt64 {
    param(
        [Parameter(Mandatory)][object]$InputObject,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Name
    )
    # The CLI uses protobuf JSON, which omits unsigned scalar fields at their
    # default value. ConvertFrom-Json therefore exposes no property for an
    # authoritative zero (for example an empty artifact's size or a coverage
    # record with no dropped records).
    $value = Get-OptionalObjectProperty -InputObject $InputObject -Name $Name
    if ($null -eq $value) {
        return [uint64]0
    }
    return [uint64]$value
}

function Get-Sha256Reference {
    param([Parameter(Mandatory)][string]$Path)
    return "sha256:" + (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-FileEvidenceIdentity {
    param([Parameter(Mandatory)][string]$Path)
    $resolved = Resolve-RequiredPath -Path $Path -Description "tested artifact" -PathType Leaf
    $item = Get-Item -LiteralPath $resolved -Force -ErrorAction Stop
    Assert-Condition -Condition (([IO.FileAttributes]$item.Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0) -Message "tested artifact is not a reparse point"
    return [pscustomobject][ordered]@{
        path = $resolved
        bytes = [int64]$item.Length
        digest = Get-Sha256Reference -Path $resolved
    }
}

function Get-RepositorySourceIdentity {
    param([Parameter(Mandatory)][string]$ManifestPath)
    $listing = Invoke-ProcessText -Executable "git" -Arguments @(
        "-c", "core.excludesFile=NUL", "ls-files", "--cached", "--others", "--exclude-standard"
    )
    $paths = @($listing.stdout -split '\r?\n' | Where-Object { $_ } | Sort-Object -Unique)
    Assert-Condition -Condition ($paths.Count -gt 0) -Message "repository source identity contains at least one tracked or unignored file"
    $entries = @()
    foreach ($path in $paths) {
        Assert-Condition -Condition (-not [IO.Path]::IsPathRooted($path)) -Message "repository source identity contains only relative paths"
        $resolved = [IO.Path]::GetFullPath((Join-Path $repoRoot $path))
        $relative = Get-ContainedRelativePath -Root $repoRoot -Path $resolved
        Assert-Condition -Condition ($relative.Equals($path.Replace('\', '/'), [StringComparison]::Ordinal)) -Message "repository source path is canonical and contained"
        $item = Get-Item -LiteralPath $resolved -Force -ErrorAction Stop
        Assert-Condition -Condition (-not $item.PSIsContainer -and (([IO.FileAttributes]$item.Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0)) -Message "repository source identity contains only regular files"
        $entries += [pscustomobject][ordered]@{
            path = $relative
            bytes = [int64]$item.Length
            digest = Get-Sha256Reference -Path $resolved
        }
    }
    Write-Utf8NoBom -Path $ManifestPath -Content ($entries | ConvertTo-Json -Depth 3)
    return [pscustomobject][ordered]@{
        manifest_path = [IO.Path]::GetFullPath($ManifestPath)
        file_count = $entries.Count
        manifest_digest = Get-Sha256Reference -Path $ManifestPath
    }
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

function Add-FinalCleanupError {
    param([Parameter(Mandatory)][string]$Message)
    Write-Warning $Message
    $script:finalCleanupErrors += $Message
}

function Resolve-RequiredPath {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Description,
        [Parameter(Mandatory)][ValidateSet("Leaf", "Container")][string]$PathType
    )
    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "$Description path is required"
    }
    $resolved = [IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $resolved -PathType $PathType)) {
        throw "$Description was not found at $resolved"
    }
    return $resolved
}

function Get-ContainedRelativePath {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Path
    )
    $separatorCharacters = [char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    $rootWithSeparator = [IO.Path]::GetFullPath($Root).TrimEnd($separatorCharacters) + [IO.Path]::DirectorySeparatorChar
    $fullPath = [IO.Path]::GetFullPath($Path)
    if (-not $fullPath.StartsWith($rootWithSeparator, [StringComparison]::OrdinalIgnoreCase)) {
        throw "$fullPath is outside required root $rootWithSeparator"
    }
    return $fullPath.Substring($rootWithSeparator.Length).Replace('\', '/')
}

function Replace-ExactlyOnce {
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Replacement,
        [Parameter(Mandatory)][string]$Description
    )
    $expression = [regex]::new($Pattern)
    $matches = $expression.Matches($Source)
    Assert-Condition -Condition ($matches.Count -eq 1) -Message "$Description has exactly one insertion point"
    return $expression.Replace($Source, $Replacement, 1)
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

function Invoke-ScopedADB {
    param(
        [Parameter(Mandatory)][int]$Port,
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$ExpectFailure
    )
    return Invoke-ProcessText -Executable $script:androidADBBinary -Arguments (@("-H", "127.0.0.1", "-P", ([string]$Port)) + $Arguments) -ExpectFailure:$ExpectFailure
}

function ConvertTo-WindowsCommandLineArgument {
    param([AllowEmptyString()][Parameter(Mandatory)][string]$Argument)
    if ($Argument.Length -eq 0) {
        return '""'
    }
    if ($Argument -notmatch '[\s"]') {
        return $Argument
    }
    $escaped = [regex]::Replace($Argument, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

function Start-LoggedProcess {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$StandardOutputPath,
        [Parameter(Mandatory)][string]$StandardErrorPath
    )
    $commandLine = (@($Arguments | ForEach-Object { ConvertTo-WindowsCommandLineArgument -Argument $_ }) -join ' ')
    $stdoutPath = [IO.Path]::GetFullPath($StandardOutputPath)
    $stderrPath = [IO.Path]::GetFullPath($StandardErrorPath)
    Assert-Condition -Condition (-not $stdoutPath.Equals($stderrPath, [StringComparison]::OrdinalIgnoreCase)) -Message "logged process stdout and stderr paths are distinct"

    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = [IO.Path]::GetFullPath($Executable)
    $startInfo.Arguments = $commandLine
    $startInfo.WorkingDirectory = (Get-Location).ProviderPath
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $stdoutStream = $null
    $stderrStream = $null
    $started = $false
    try {
        $fileOptions = [IO.FileOptions]::Asynchronous -bor [IO.FileOptions]::WriteThrough
        $stdoutStream = [IO.FileStream]::new($stdoutPath, [IO.FileMode]::Create, [IO.FileAccess]::Write, [IO.FileShare]::ReadWrite, 1, $fileOptions)
        $stderrStream = [IO.FileStream]::new($stderrPath, [IO.FileMode]::Create, [IO.FileAccess]::Write, [IO.FileShare]::ReadWrite, 1, $fileOptions)
        $started = $process.Start()
        Assert-Condition -Condition $started -Message "logged process started"
        $stdoutTask = $process.StandardOutput.BaseStream.CopyToAsync($stdoutStream)
        $stderrTask = $process.StandardError.BaseStream.CopyToAsync($stderrStream)
        $process | Add-Member -NotePropertyName WorldE2EStandardOutputTask -NotePropertyValue $stdoutTask
        $process | Add-Member -NotePropertyName WorldE2EStandardErrorTask -NotePropertyValue $stderrTask
        $process | Add-Member -NotePropertyName WorldE2EStandardOutputStream -NotePropertyValue $stdoutStream
        $process | Add-Member -NotePropertyName WorldE2EStandardErrorStream -NotePropertyValue $stderrStream
        return $process
    }
    catch {
        if ($started -and -not $process.HasExited) {
            try {
                $process.Kill()
                $process.WaitForExit()
            }
            catch {
                # Preserve the original launch/pump failure.
            }
        }
        if ($null -ne $stdoutStream) { $stdoutStream.Dispose() }
        if ($null -ne $stderrStream) { $stderrStream.Dispose() }
        $process.Dispose()
        throw
    }
}

function Complete-StartedProcessLogging {
    param([Parameter(Mandatory)][Diagnostics.Process]$Process)

    $completionErrors = @()
    foreach ($binding in @(
        [pscustomobject]@{ task = "WorldE2EStandardOutputTask"; stream = "WorldE2EStandardOutputStream"; channel = "stdout" },
        [pscustomobject]@{ task = "WorldE2EStandardErrorTask"; stream = "WorldE2EStandardErrorStream"; channel = "stderr" }
    )) {
        $taskProperty = $Process.PSObject.Properties[$binding.task]
        $streamProperty = $Process.PSObject.Properties[$binding.stream]
        try {
            if ($null -ne $taskProperty -and $null -ne $taskProperty.Value) {
                [void]$taskProperty.Value.GetAwaiter().GetResult()
            }
        }
        catch {
            $completionErrors += "$($binding.channel) copy: $_"
        }
        finally {
            if ($null -ne $streamProperty -and $null -ne $streamProperty.Value) {
                try {
                    $streamProperty.Value.Flush()
                    $streamProperty.Value.Dispose()
                }
                catch {
                    $completionErrors += "$($binding.channel) stream: $_"
                }
                $streamProperty.Value = $null
            }
            if ($null -ne $taskProperty) {
                $taskProperty.Value = $null
            }
        }
    }
    if ($completionErrors.Count -gt 0) {
        throw "failed to complete logged process output: $($completionErrors -join '; ')"
    }
}

function Wait-StartedProcessExitCode {
    param(
        [Parameter(Mandatory)][Diagnostics.Process]$Process,
        [Parameter(Mandatory)][ValidateRange(1, 2147483)][int]$TimeoutSeconds,
        [Parameter(Mandatory)][string]$FailureMessage
    )
    $exited = $Process.WaitForExit([int]([int64]$TimeoutSeconds * 1000))
    Assert-Condition -Condition $exited -Message $FailureMessage
    $Process.WaitForExit()
    Complete-StartedProcessLogging -Process $Process
    $Process.Refresh()
    Assert-Condition -Condition $Process.HasExited -Message $FailureMessage
    $exitCode = $Process.ExitCode
    Assert-Condition -Condition ($null -ne $exitCode) -Message "$FailureMessage with an available OS exit code"
    return [int]$exitCode
}

function Stop-StartedProcess {
    param([Diagnostics.Process]$Process)
    if ($null -eq $Process) {
        return
    }
    try {
        if (-not $Process.HasExited) {
            $Process.Kill()
            $stopped = $Process.WaitForExit(15000)
            Assert-Condition -Condition $stopped -Message "exact started process $($Process.Id) stopped within 15 seconds"
        }
    }
    catch {
        if (-not $Process.HasExited) {
            throw
        }
    }
    if ($Process.HasExited) {
        $Process.WaitForExit()
        Complete-StartedProcessLogging -Process $Process
    }
}

function Get-AllDockerContainerIDs {
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @("ps", "-aq", "--no-trunc")
    return @($listing.stdout -split '\r?\n' | Where-Object { $_ } | Sort-Object -Unique)
}

function Invoke-DockerInspectRecords {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $result = Invoke-ProcessText -Executable "docker" -Arguments $Arguments
    try {
        # Windows PowerShell preserves a JSON array as one pipeline object when
        # ConvertFrom-Json is wrapped directly in @(...). Assign first so the
        # caller receives one record per Docker inspection result.
        $parsed = $result.stdout | ConvertFrom-Json
        return @($parsed)
    }
    catch {
        throw "Docker inspection did not return JSON: docker $($Arguments -join ' ')`n$($result.stdout)"
    }
}

function Get-DockerContainerIDsByLabel {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Value
    )
    Assert-Condition -Condition (
        -not [string]::IsNullOrWhiteSpace($Name) -and
        -not [string]::IsNullOrWhiteSpace($Value) -and
        -not $Name.Contains("=") -and
        -not $Value.Contains("=")
    ) -Message "Docker label lookup has an exact non-blank name and value"
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @(
        "ps", "-aq", "--no-trunc", "--filter", "label=$Name=$Value"
    )
    return @($listing.stdout -split '\r?\n' | Where-Object { $_ } | Sort-Object -Unique)
}

function Get-TestLeaseWorldContainerIDs {
    param([Parameter(Mandatory)][string]$LeaseID)
    return @(Get-DockerContainerIDsByLabel -Name "world.lease" -Value $LeaseID)
}

function Get-TestRunDockerContainerIDs {
    return @(Get-DockerContainerIDsByLabel -Name $script:dockerTestRunLabelName -Value $script:dockerTestRunLabelValue)
}

function Get-DockerContainerSnapshot {
    # docker inspect accepts multiple object IDs. Keep each invocation bounded so
    # a host with hundreds of ambient containers does not spawn hundreds of
    # processes, while retaining an exact full-ID record for every ID listed by
    # the all-container inventory at the snapshot boundary.
    $containerIDs = @(Get-AllDockerContainerIDs)
    $result = @()
    $inspectedIDs = @{}
    $batchSize = 32
    for ($offset = 0; $offset -lt $containerIDs.Count; $offset += $batchSize) {
        $count = [Math]::Min($batchSize, $containerIDs.Count - $offset)
        $batch = @($containerIDs[$offset..($offset + $count - 1)])
        $inspection = @(Invoke-DockerInspectRecords -Arguments (@("inspect") + $batch))
        $returnedIDs = @($inspection | ForEach-Object { [string]$_.Id })
        Assert-Condition -Condition ($inspection.Count -eq $batch.Count) -Message "Docker returned one inspection record for every container in a bounded batch"
        Assert-Condition -Condition (@($returnedIDs | Sort-Object -Unique).Count -eq $returnedIDs.Count) -Message "Docker returned no duplicate inspection records in a bounded batch"
        Assert-Condition -Condition (@($batch | Where-Object { $returnedIDs -notcontains $_ }).Count -eq 0) -Message "Docker returned the exact full-ID set requested for a bounded batch"
        Assert-Condition -Condition (@($returnedIDs | Where-Object { $batch -notcontains $_ }).Count -eq 0) -Message "Docker returned no unrequested container records for a bounded batch"
        foreach ($container in $inspection) {
            $containerID = [string]$container.Id
            Assert-Condition -Condition ($containerID -match '^[0-9a-f]{64}$') -Message "Docker snapshot retained a full container ID"
            Assert-Condition -Condition (-not $inspectedIDs.ContainsKey($containerID)) -Message "Docker snapshot contains each full container ID exactly once"
            $inspectedIDs[$containerID] = $true
            $labels = @()
            if ($null -ne $container.Config.Labels) {
                $labels = @($container.Config.Labels.PSObject.Properties | Sort-Object Name | ForEach-Object {
                    [pscustomobject][ordered]@{ name = [string]$_.Name; value = [string]$_.Value }
                })
            }
            $result += [pscustomobject][ordered]@{
                id = [string]$container.Id
                name = [string]$container.Name
                image_id = [string]$container.Image
                created = [string]$container.Created
                labels = $labels
                status = [string]$container.State.Status
                running = [bool]$container.State.Running
                paused = [bool]$container.State.Paused
                restarting = [bool]$container.State.Restarting
                dead = [bool]$container.State.Dead
                oom_killed = [bool]$container.State.OOMKilled
                exit_code = [int64]$container.State.ExitCode
                started_at = [string]$container.State.StartedAt
                finished_at = [string]$container.State.FinishedAt
                restart_count = [int64]$container.RestartCount
            }
        }
    }
    Assert-Condition -Condition ($result.Count -eq $containerIDs.Count) -Message "Docker snapshot includes every full container ID from its all-container inventory"
    return @($result | Sort-Object id)
}

function Test-DockerAmbientBaselinePreserved {
    param(
        [Parameter(Mandatory)]$Before,
        [Parameter(Mandatory)]$After
    )
    foreach ($baseline in @($Before)) {
        $matches = @($After | Where-Object { [string]$_.id -eq [string]$baseline.id })
        if ($matches.Count -ne 1) {
            return $false
        }
        $current = $matches[0]
        if (-not (
            [string]$current.name -eq [string]$baseline.name -and
            [string]$current.image_id -eq [string]$baseline.image_id -and
            [string]$current.created -eq [string]$baseline.created -and
            [string]$current.status -eq [string]$baseline.status -and
            [bool]$current.running -eq [bool]$baseline.running -and
            [bool]$current.paused -eq [bool]$baseline.paused -and
            [bool]$current.restarting -eq [bool]$baseline.restarting -and
            [bool]$current.dead -eq [bool]$baseline.dead -and
            [bool]$current.oom_killed -eq [bool]$baseline.oom_killed -and
            [int64]$current.exit_code -eq [int64]$baseline.exit_code -and
            [string]$current.started_at -eq [string]$baseline.started_at -and
            [string]$current.finished_at -eq [string]$baseline.finished_at -and
            [int64]$current.restart_count -eq [int64]$baseline.restart_count -and
            ((@($current.labels) | ConvertTo-Json -Compress) -eq (@($baseline.labels) | ConvertTo-Json -Compress))
        )) {
            return $false
        }
    }
    return $true
}

function Get-UnknownConcurrentDockerResources {
    param(
        [Parameter(Mandatory)]$Before,
        [Parameter(Mandatory)]$After
    )
    $baselineIDs = @{}
    foreach ($container in @($Before)) {
        $baselineIDs[[string]$container.id] = $true
    }
    $result = @()
    foreach ($container in @($After)) {
        $containerID = [string]$container.id
        if (-not $baselineIDs.ContainsKey($containerID) -and @($script:dockerTrackedContainers | Where-Object { [string]$_.id -eq $containerID }).Count -eq 0) {
            $result += $container
        }
    }
    return $result
}

function Register-TestDockerContainerID {
    param(
        [Parameter(Mandatory)][string]$ContainerID,
        [Parameter(Mandatory)][string]$Origin,
        [string]$ExpectedLeaseID = "",
        [string]$ExpectedRole = "",
        [string]$ExpectedTargetID = "",
        [string]$ExpectedTestRun = "",
        [string]$ExpectedImageID = "",
        [hashtable]$ExpectedMountRootsByRole = $null,
        [switch]$MayAlreadyBeAbsent
    )
    if ($ContainerID -notmatch '^[0-9a-f]{64}$') {
        throw "refusing to register malformed or truncated Docker container ID $ContainerID"
    }
    Assert-Condition -Condition (-not $script:dockerAmbientIDs.ContainsKey($ContainerID)) -Message "test-created Docker container $ContainerID was not present in the ambient baseline"
    if (@($script:dockerTrackedContainers | Where-Object { [string]$_.id -eq $ContainerID }).Count -gt 0) {
        return
    }
    $allIDs = @(Get-AllDockerContainerIDs)
    $present = $allIDs -contains $ContainerID
    $inspectionRecord = $null
    if ($present) {
        $inspection = @(Invoke-DockerInspectRecords -Arguments @("inspect", $ContainerID))
        Assert-Condition -Condition ($inspection.Count -eq 1 -and [string]$inspection[0].Id -eq $ContainerID) -Message "Docker returned the exact full-ID record for test container $ContainerID"
        $inspectionRecord = $inspection[0]
        $labels = $inspectionRecord.Config.Labels
        if (-not [string]::IsNullOrWhiteSpace($ExpectedLeaseID)) {
            Assert-Condition -Condition ($null -ne $labels -and [string]$labels."world.lease" -eq $ExpectedLeaseID) -Message "new container $ContainerID belongs to exact test lease $ExpectedLeaseID at discovery"
        }
        if (-not [string]::IsNullOrWhiteSpace($ExpectedRole)) {
            Assert-Condition -Condition ($null -ne $labels -and [string]$labels."world.role" -eq $ExpectedRole) -Message "new container $ContainerID has expected test role $ExpectedRole at discovery"
        }
        if (-not [string]::IsNullOrWhiteSpace($ExpectedTargetID)) {
            Assert-Condition -Condition ($null -ne $labels -and [string]$labels."world.target" -eq $ExpectedTargetID) -Message "new container $ContainerID belongs to exact test target $ExpectedTargetID at discovery"
        }
        if (-not [string]::IsNullOrWhiteSpace($ExpectedTestRun)) {
            $testRunProperty = if ($null -ne $labels) { $labels.PSObject.Properties[$script:dockerTestRunLabelName] } else { $null }
            Assert-Condition -Condition (
                $null -ne $testRunProperty -and
                [string]$testRunProperty.Value -eq $ExpectedTestRun
            ) -Message "new container $ContainerID belongs to exact E2E run $ExpectedTestRun at discovery"
        }
        if (-not [string]::IsNullOrWhiteSpace($ExpectedImageID)) {
            Assert-Condition -Condition ([string]$inspectionRecord.Image -eq $ExpectedImageID) -Message "new container $ContainerID uses the exact newly built E2E image at discovery"
        }
        if ($null -ne $ExpectedMountRootsByRole) {
            $role = if ($null -ne $labels) { [string]$labels."world.role" } else { "" }
            Assert-Condition -Condition ($ExpectedMountRootsByRole.ContainsKey($role)) -Message "new container $ContainerID has a cleanup-authorized World role at discovery"
            $canonicalIDPattern = '^[a-z]+_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            $canonicalDigestPattern = '^sha256:[0-9a-f]{64}$'
            $commonIdentityValid = (
                [string]$labels."world.lease" -match $canonicalIDPattern -and
                [string]$labels."world.policy-digest" -match $canonicalDigestPattern -and
                [string]$labels."world.capability-digest" -match $canonicalDigestPattern -and
                [string]$labels."world.plan-digest" -match $canonicalDigestPattern
            )
            $roleIdentityValid = switch ($role) {
                "agent-workspace" {
                    [string]$labels."world.agent-workspace" -match '^aw_[0-9a-f-]{36}$' -and
                    [string]$labels."world.agent-generation" -match '^[1-9][0-9]*$' -and
                    [string]$labels."world.workspace" -match '^ws_[0-9a-f-]{36}$'
                    break
                }
                "linux-target" {
                    [string]$labels."world.target" -match '^target_[0-9a-f-]{36}$' -and
                    [string]$labels."world.target-generation" -match '^[1-9][0-9]*$'
                    break
                }
                default { $false }
            }
            Assert-Condition -Condition ($commonIdentityValid -and $roleIdentityValid) -Message "new container $ContainerID has complete canonical World identity and provenance labels at discovery"
            $expectedMountRoot = [string]$ExpectedMountRootsByRole[$role]
            $mounts = @($inspectionRecord.Mounts)
            $expectedDestinations = @(if ($role -eq "agent-workspace") { "/workspace" } else { "/target"; "/target/input" })
            $actualDestinations = @($mounts | ForEach-Object { [string]$_.Destination } | Sort-Object)
            Assert-Condition -Condition (
                ($actualDestinations | ConvertTo-Json -Compress) -eq (($expectedDestinations | Sort-Object) | ConvertTo-Json -Compress)
            ) -Message "new container $ContainerID has the exact role-scoped bind destinations at discovery"
            foreach ($mount in $mounts) {
                Assert-Condition -Condition ([string]$mount.Type -eq "bind") -Message "new container $ContainerID exposes only role-scoped bind mounts"
                [void](Get-ContainedRelativePath -Root $expectedMountRoot -Path ([string]$mount.Source))
            }
        }
    }
    elseif (-not $MayAlreadyBeAbsent) {
        throw "test container $ContainerID was absent before its ownership could be verified"
    }
    $script:dockerTrackedContainers += [pscustomobject][ordered]@{
        id = $ContainerID
        origin = $Origin
        name_at_discovery = if ($null -ne $inspectionRecord) { [string]$inspectionRecord.Name } else { "" }
        lease_id_at_discovery = $ExpectedLeaseID
        role_at_discovery = $ExpectedRole
        target_id_at_discovery = $ExpectedTargetID
        test_run_at_discovery = $ExpectedTestRun
        live_at_discovery = $present
    }
}

function Register-TestRunDockerContainersForCleanup {
    param([Parameter(Mandatory)][hashtable]$MountRootsByRole)
    foreach ($containerID in @(Get-TestRunDockerContainerIDs)) {
        try {
            Register-TestDockerContainerID -ContainerID $containerID -Origin "failure-cleanup-run-discovery" -ExpectedTestRun $dockerTestRunLabelValue -ExpectedImageID $testImageID -ExpectedMountRootsByRole $MountRootsByRole
        }
        catch {
            Write-Warning "preserved run-labelled Docker resource $containerID because its image, World identity, or role-scoped mounts were not exact: $_"
        }
    }
}

function Register-ProcessLockDockerContainer {
    if (-not (Test-Path -LiteralPath $processLockContainerIDPath -PathType Leaf)) {
        return
    }
    $containerID = (Get-Content -LiteralPath $processLockContainerIDPath -Raw -ErrorAction Stop).Trim()
    Register-TestDockerContainerID -ContainerID $containerID -Origin "linux-processlock-qualification" -ExpectedTestRun $dockerTestRunLabelValue -ExpectedImageID $testImageID -MayAlreadyBeAbsent
}

function Remove-TrackedTestDockerContainerChecked {
    param([Parameter(Mandatory)]$Record)
    $containerID = [string]$Record.id
    Assert-Condition -Condition (@($script:dockerTrackedContainers | Where-Object { [string]$_.id -eq $containerID }).Count -eq 1) -Message "Docker cleanup only targets an immutable ID registered as test-created"
    if (@(Get-AllDockerContainerIDs) -contains $containerID) {
        # Labels are deliberately not re-authorized here. They were checked at
        # first discovery and may be missing or corrupt after a crash.
        [void](Invoke-ProcessText -Executable "docker" -Arguments @("rm", "--force", "--volumes", $containerID))
    }
    Assert-Condition -Condition (@(Get-AllDockerContainerIDs) -notcontains $containerID) -Message "exact tracked test container $containerID is absent"
}

function Get-AllDockerImageIDs {
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @("image", "ls", "-aq", "--no-trunc")
    return @($listing.stdout -split '\r?\n' | Where-Object { $_ -match '^sha256:[0-9a-f]{64}$' } | Sort-Object -Unique)
}

function Get-DockerImageTagState {
    param([Parameter(Mandatory)][string]$Tag)
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @("image", "ls", "--no-trunc", "--quiet", $Tag)
    $ids = @($listing.stdout -split '\r?\n' | Where-Object { $_ } | Sort-Object -Unique)
    Assert-Condition -Condition ($ids.Count -le 1) -Message "Docker tag $Tag resolves to at most one image ID"
    if ($ids.Count -eq 0) {
        return [pscustomobject][ordered]@{ tag = $Tag; exists = $false; id = ""; repo_tags = @(); repo_digests = @() }
    }
    $inspection = @(Invoke-DockerInspectRecords -Arguments @("image", "inspect", $Tag))
    Assert-Condition -Condition ($inspection.Count -eq 1) -Message "Docker returned one exact image record for tag $Tag"
    return [pscustomobject][ordered]@{
        tag = $Tag
        exists = $true
        id = [string]$inspection[0].Id
        repo_tags = @($inspection[0].RepoTags | Where-Object { $_ } | Sort-Object -Unique)
        repo_digests = @($inspection[0].RepoDigests | Where-Object { $_ } | Sort-Object -Unique)
    }
}

function Test-DockerImageTagStateEqual {
    param(
        [Parameter(Mandatory)]$Left,
        [Parameter(Mandatory)]$Right
    )
    return (($Left | ConvertTo-Json -Compress -Depth 4) -eq ($Right | ConvertTo-Json -Compress -Depth 4))
}

function Complete-TestDockerImageCleanup {
    if ($script:imageCleanupCompleted) {
        return
    }
    if ($null -eq $script:imageTagBefore) {
        return
    }
    if (-not [string]::IsNullOrWhiteSpace($script:testImageID)) {
        Assert-Condition -Condition (-not $script:testImageIDWasPreexisting) -Message "refusing image cleanup because the test tag unexpectedly resolved to a preexisting image ID"
        $currentTag = Get-DockerImageTagState -Tag $script:ImageTag
        if ($currentTag.exists) {
            Assert-Condition -Condition ([string]$currentTag.id -eq $script:testImageID) -Message "refusing to remove image tag $($script:ImageTag) after it moved to a non-test image"
            [void](Invoke-ProcessText -Executable "docker" -Arguments @("image", "rm", $script:ImageTag))
        }
        if (-not $script:testImageIDWasPreexisting -and @(Get-AllDockerImageIDs) -contains $script:testImageID) {
            $remainingInspection = @(Invoke-DockerInspectRecords -Arguments @("image", "inspect", $script:testImageID))
            Assert-Condition -Condition ($remainingInspection.Count -eq 1) -Message "Docker returned the exact test-created image before cleanup"
            $remainingTags = @($remainingInspection[0].RepoTags | Where-Object { $_ -and $_ -ne '<none>:<none>' })
            Assert-Condition -Condition ($remainingTags.Count -eq 0) -Message "refusing to delete test image $($script:testImageID) after another tag began referencing it"
            [void](Invoke-ProcessText -Executable "docker" -Arguments @("image", "rm", $script:testImageID))
        }
    }
    $script:imageTagAfter = Get-DockerImageTagState -Tag $script:ImageTag
    Assert-Condition -Condition (Test-DockerImageTagStateEqual -Left $script:imageTagBefore -Right $script:imageTagAfter) -Message "preexisting Docker image-tag mapping for $($script:ImageTag) is unchanged after cleanup"
    if (-not [string]::IsNullOrWhiteSpace($script:testImageID)) {
        Assert-Condition -Condition (@(Get-AllDockerImageIDs) -notcontains $script:testImageID) -Message "test-created image $($script:testImageID) is absent after cleanup"
    }
    $script:dockerImageIDsAfterCleanup = @(Get-AllDockerImageIDs)
    $script:preexistingDockerImageIDsMissing = @($script:dockerImageIDsBefore | Where-Object { $script:dockerImageIDsAfterCleanup -notcontains $_ })
    Assert-Condition -Condition ($script:preexistingDockerImageIDsMissing.Count -eq 0) -Message "Docker image cleanup preserved every preexisting image ID"
    $script:imageCleanupCompleted = $true
}

function Get-TargetContainerID {
    param([Parameter(Mandatory)][string]$TargetID)
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @(
        "ps", "-aq", "--no-trunc", "--filter", "label=world.role=linux-target", "--filter", "label=world.target=$TargetID"
    )
    $ids = @($listing.stdout -split '\r?\n' | Where-Object { $_ })
    if ($ids.Count -ne 1) {
        throw "expected exactly one Docker container for target $TargetID, found $($ids.Count)"
    }
    $containerID = [string]$ids[0]
    Register-TestDockerContainerID -ContainerID $containerID -Origin "linux-target:$TargetID" -ExpectedLeaseID ([string]$script:leaseID) -ExpectedRole "linux-target" -ExpectedTargetID $TargetID -ExpectedTestRun $dockerTestRunLabelValue -ExpectedImageID $testImageID
    return $containerID
}

function Get-AgentContainerID {
    param([Parameter(Mandatory)][string]$LeaseID)
    $listing = Invoke-ProcessText -Executable "docker" -Arguments @(
        "ps", "-aq", "--no-trunc", "--filter", "label=world.role=agent-workspace", "--filter", "label=world.lease=$LeaseID"
    )
    $ids = @($listing.stdout -split '\r?\n' | Where-Object { $_ })
    if ($ids.Count -ne 1) {
        throw "expected exactly one Docker agent container for lease $LeaseID, found $($ids.Count)"
    }
    $containerID = [string]$ids[0]
    Register-TestDockerContainerID -ContainerID $containerID -Origin "agent-workspace:$LeaseID" -ExpectedLeaseID $LeaseID -ExpectedRole "agent-workspace" -ExpectedTestRun $dockerTestRunLabelValue -ExpectedImageID $testImageID
    return $containerID
}

function Get-ContainerProcesses {
    param([Parameter(Mandatory)][string]$ContainerID)
    return (Invoke-ProcessText -Executable "docker" -Arguments @("top", $ContainerID, "-eo", "pid,args")).stdout
}

function Wait-UntilTrue {
    param(
        [Parameter(Mandatory)][scriptblock]$Condition,
        [Parameter(Mandatory)][string]$FailureMessage,
        [int]$TimeoutSeconds = 15
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if (& $Condition) {
            return
        }
        Start-Sleep -Milliseconds 200
    } while ((Get-Date) -lt $deadline)
    throw $FailureMessage
}

function Wait-ContainerProcessState {
    param(
        [Parameter(Mandatory)][string]$ContainerID,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][bool]$Present,
        [int]$TimeoutSeconds = 15
    )
    $expectation = if ($Present) { "appear" } else { "disappear" }
    Wait-UntilTrue -TimeoutSeconds $TimeoutSeconds -FailureMessage "process pattern $Pattern did not $expectation in container $ContainerID within $TimeoutSeconds seconds" -Condition {
        ((Get-ContainerProcesses -ContainerID $ContainerID) -match $Pattern) -eq $Present
    }
    return Get-ContainerProcesses -ContainerID $ContainerID
}

function Wait-FileState {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][bool]$Present,
        [int]$TimeoutSeconds = 15
    )
    $expectation = if ($Present) { "appear" } else { "disappear" }
    Wait-UntilTrue -TimeoutSeconds $TimeoutSeconds -FailureMessage "file $Path did not $expectation within $TimeoutSeconds seconds" -Condition {
        (Test-Path -LiteralPath $Path) -eq $Present
    }
}

function Get-ExactPathAbsenceFact {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Kind,
        [Parameter(Mandatory)][string]$OwnerID,
        [uint64]$Generation = 0
    )
    return [pscustomobject][ordered]@{
        kind = $Kind
        owner_id = $OwnerID
        generation = $Generation
        path = [IO.Path]::GetFullPath($Path)
        absent = -not (Test-Path -LiteralPath $Path)
    }
}

function Get-LinuxCgroupVerificationArguments {
    return @(
        "-verify-cgroup",
        "-expected-cpu-milli", ([string]$script:linuxCgroupContract.cpu_milli),
        "-expected-memory-bytes", ([string]$script:linuxCgroupContract.memory_bytes),
        "-expected-swap-bytes", ([string]$script:linuxCgroupContract.swap_bytes),
        "-expected-pids", ([string]$script:linuxCgroupContract.pids)
    )
}

function Test-CanonicalLinuxCgroupPath {
    param([Parameter(Mandatory)][AllowEmptyString()][string]$Path)
    if ($Path -eq "/") {
        return $true
    }
    if ($Path -notmatch "^/(?:[^/\\:`r`n]+)(?:/[^/\\:`r`n]+)*$") {
        return $false
    }
    foreach ($segment in $Path.Substring(1).Split('/')) {
        if ($segment -eq "." -or $segment -eq "..") {
            return $false
        }
    }
    return $true
}

function Read-VerifiedLinuxCgroupReport {
    param(
        [Parameter(Mandatory)][string]$ResultPath,
        [Parameter(Mandatory)][string]$Description
    )
    [void](Resolve-RequiredPath -Path $ResultPath -Description $Description -PathType Leaf)
    $result = Get-Content -LiteralPath $ResultPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    $report = $result.cgroup
    Assert-Condition -Condition ($null -ne $report) -Message "$Description contains a live versioned cgroup measurement"
    $versionText = [string]$report.version
    Assert-Condition -Condition ($versionText -eq "1" -or $versionText -eq "2") -Message "$Description identifies exact cgroup version 1 or 2"
    $version = [int]$versionText
    $cpuPath = [string]$report.paths.cpu
    $memoryPath = [string]$report.paths.memory
    $pidsPath = [string]$report.paths.pids
    Assert-Condition -Condition (
        (Test-CanonicalLinuxCgroupPath -Path $cpuPath) -and
        (Test-CanonicalLinuxCgroupPath -Path $memoryPath) -and
        (Test-CanonicalLinuxCgroupPath -Path $pidsPath)
    ) -Message "$Description identifies canonical live CPU, memory, and PID cgroup memberships"
    if ($version -eq 2) {
        Assert-Condition -Condition ($cpuPath -eq $memoryPath -and $memoryPath -eq $pidsPath) -Message "$Description identifies one unified cgroup-v2 membership"
    }
    $cpuQuota = [int64]$report.cpu_quota
    $cpuPeriod = [int64]$report.cpu_period
    $scaledCPUQuota = [decimal]$cpuQuota * 1000
    Assert-Condition -Condition (
        $cpuQuota -gt 0 -and $cpuPeriod -gt 0 -and
        $scaledCPUQuota % [decimal]$cpuPeriod -eq 0 -and
        $scaledCPUQuota / [decimal]$cpuPeriod -eq [decimal]$report.cpu_milli
    ) -Message "$Description measured a finite CPU quota/period that maps to exact milli-CPU"
    Assert-Condition -Condition (
        [int64]$report.cpu_milli -eq [int64]$script:linuxCgroupContract.cpu_milli -and
        [int64]$report.memory_bytes -eq [int64]$script:linuxCgroupContract.memory_bytes -and
        [int64]$report.swap_bytes -eq [int64]$script:linuxCgroupContract.swap_bytes -and
        [int64]$report.pids -eq [int64]$script:linuxCgroupContract.pids
    ) -Message "$Description measured the exact CPU, memory, swap, and PID cgroup limits"
    return $report
}

function Get-LinuxTargetCleanupFact {
    param(
        [Parameter(Mandatory)][string]$TargetID,
        [Parameter(Mandatory)][uint64[]]$Generations
    )
    $targetPath = Join-Path $runRoot "targets\$TargetID"
    $generationFacts = @($Generations | Sort-Object -Unique | ForEach-Object {
        Get-ExactPathAbsenceFact -Path (Join-Path $targetPath "generations\$_") -Kind "linux-target-generation" -OwnerID $TargetID -Generation $_
    })
    $remainingFiles = @(if (Test-Path -LiteralPath $targetPath -PathType Container) {
        Get-ChildItem -LiteralPath $targetPath -Recurse -Force -File -ErrorAction Stop | ForEach-Object { $_.FullName } | Sort-Object
    }
    else { @() })
    $remainingGenerationDirectories = @(if (Test-Path -LiteralPath (Join-Path $targetPath "generations") -PathType Container) {
        Get-ChildItem -LiteralPath (Join-Path $targetPath "generations") -Force -Directory -ErrorAction Stop | ForEach-Object { $_.FullName } | Sort-Object
    }
    else { @() })
    return [pscustomobject][ordered]@{
        target_id = $TargetID
        target_path = [IO.Path]::GetFullPath($targetPath)
        target_parent_exists = [bool](Test-Path -LiteralPath $targetPath -PathType Container)
        generation_paths = $generationFacts
        remaining_files = $remainingFiles
        remaining_generation_directories = $remainingGenerationDirectories
        no_owned_state_residue = (@($generationFacts | Where-Object { -not $_.absent }).Count -eq 0 -and $remainingFiles.Count -eq 0 -and $remainingGenerationDirectories.Count -eq 0)
    }
}

function Get-AndroidAllocatorCleanupFact {
    param([AllowEmptyString()][string]$TargetID = "")
    $path = Join-Path $androidTargetRoot "allocations\android-emulator-allocations.json"
    [void](Resolve-RequiredPath -Path $path -Description "managed Android durable allocation registry" -PathType Leaf)
    $registry = Get-Content -LiteralPath $path -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    $version = Get-OptionalObjectProperty -InputObject $registry -Name "version"
    Assert-Condition -Condition ([int]$version -eq 1) -Message "managed Android durable allocation registry has schema version 1"
    $allocationsProperty = $registry.PSObject.Properties["allocations"]
    Assert-Condition -Condition ($null -ne $allocationsProperty) -Message "managed Android durable allocation registry contains its allocations array"
    $allocations = @($allocationsProperty.Value)
    $owned = @(if ([string]::IsNullOrWhiteSpace($TargetID)) {
        @()
    }
    else {
        $allocations | Where-Object {
            [string](Get-OptionalObjectProperty -InputObject $_ -Name "target_id") -eq $TargetID
        }
    })
    return [pscustomobject][ordered]@{
        path = [IO.Path]::GetFullPath($path)
        digest = Get-Sha256Reference -Path $path
        schema_version = [int]$version
        total_allocations_remaining = $allocations.Count
        target_id = $TargetID
        target_allocations_remaining = @($owned)
        empty = ($allocations.Count -eq 0)
    }
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

function Test-LoopbackEndpointReachable {
    param([Parameter(Mandatory)][int]$Port)
    $client = [Net.Sockets.TcpClient]::new()
    try {
        $connection = $client.ConnectAsync([Net.IPAddress]::Loopback, $Port)
        return ($connection.Wait(250) -and $client.Connected)
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Test-LoopbackPortPairAvailable {
    param([Parameter(Mandatory)][int]$ConsolePort)
    $listeners = [Collections.Generic.List[Net.Sockets.TcpListener]]::new()
    try {
        foreach ($port in @($ConsolePort, ($ConsolePort + 1))) {
            $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
            $listener.Start()
            $listeners.Add($listener)
        }
        return $true
    }
    catch {
        return $false
    }
    finally {
        foreach ($listener in $listeners) {
            $listener.Stop()
        }
    }
}

function Get-FreeManagedAndroidConsolePort {
    param([Parameter(Mandatory)][ValidateRange(1, 16)][int]$RequiredPairCount)
    $firstAvailablePort = $null
    $availablePairCount = 0
    for ($port = 5554; $port -le 5584; $port += 2) {
        if (Test-LoopbackPortPairAvailable -ConsolePort $port) {
            if ($null -eq $firstAvailablePort) {
                $firstAvailablePort = $port
            }
            $availablePairCount++
            if ($availablePairCount -ge $RequiredPairCount) {
                return $firstAvailablePort
            }
        }
    }
    throw "fewer than $RequiredPairCount free managed Android console/ADB port pairs are available from 5554/5555 through 5584/5585"
}

function Get-ManagedAndroidCommonArguments {
    return @(
        "-android-target-driver", "android-emulator",
        "-android-sdk-root", $script:AndroidSDKRoot,
        "-android-adb-binary", $script:androidADBBinary,
        "-android-adb-server", "127.0.0.1:5037",
        "-android-emulator-binary", $script:androidEmulatorBinary,
        "-android-sdkmanager-binary", $script:androidSDKManagerBinary,
        "-android-avdmanager-binary", $script:androidAVDManagerBinary,
        "-android-backend-version", $script:AndroidBackendVersion,
        "-android-runtime-version", $script:AndroidRuntimeVersion,
        "-android-adb-base-port", ([string]$script:androidBaseConsolePort)
    )
}

function New-AndroidLogcatObserverConfiguration {
    Assert-Condition -Condition (
        -not [string]::IsNullOrWhiteSpace($script:androidADBBinary) -and
        -not [string]::IsNullOrWhiteSpace($script:androidLogcatObserverVersion) -and
        [int]$script:androidBaseConsolePort -ge 5554
    ) -Message "Android logcat observer prerequisites are complete"
    return [pscustomobject][ordered]@{
        reference = $script:androidLogcatObserverReference
        adapter = "logcat"
        version = $script:androidLogcatObserverVersion
        signal_family = "android.logcat"
        placement = "guest"
        coverage_level = "partial"
        runtime_binding = "android-exact-adb"
        program = $script:androidADBBinary
        args = @("logcat", "-v", "threadtime", "WORLD_E2E:I", "*:S")
        version_args = @("version")
        readiness_program = $script:androidADBBinary
        readiness_args = @("get-state")
        readiness_interval = "250ms"
        maximum_bytes = 1048576
    }
}

function Get-AndroidRunObserverMarkerFact {
    param(
        [Parameter(Mandatory)][string]$TargetRunID,
        [Parameter(Mandatory)][string]$ExpectedSerial,
        [Parameter(Mandatory)][string]$ExpectedRuntimeName,
        [Parameter(Mandatory)][ValidateSet("active", "committed")][string]$ExpectedPhase,
        [Parameter(Mandatory)][bool]$ExpectedExternalOwnershipPossible
    )
    $markerPath = Join-Path $runRoot "orchestration\run-observers\runs\$TargetRunID.json"
    [void](Resolve-RequiredPath -Path $markerPath -Description "durable run-observer marker for $TargetRunID" -PathType Leaf)
    $markerText = Read-SharedUtf8Text -Path $markerPath
    try {
        $marker = $markerText | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "durable run-observer marker for $TargetRunID is not JSON: $_"
    }
    $collectorBindings = @($marker.collectors)
    Assert-Condition -Condition (
        [uint32]$marker.version -eq 6 -and
        [string]$marker.run_id -eq $TargetRunID -and
        [string]$marker.phase -eq $ExpectedPhase -and
        [string]$marker.plan_digest -match '^sha256:[0-9a-f]{64}$' -and
        [string]$marker.signature -match '^sha256:[0-9a-f]{64}$' -and
        [bool]$marker.crash_cleanup_guaranteed -and
        [bool]$marker.external_ownership_possible -eq $ExpectedExternalOwnershipPossible -and
        $collectorBindings.Count -eq 1
    ) -Message "run $TargetRunID has one literal-version-6 durable $ExpectedPhase observer marker with expected external-ownership state and a crash-cleanup guarantee"
    $binding = $collectorBindings[0]
    $plan = $binding.plan
    $collectorID = [string]$plan.CollectorID
    Assert-Condition -Condition (
        [bool]$binding.start_committed -and
        $collectorID -match '^collector_[0-9a-f-]+$' -and
        [string]$plan.TargetRunID -eq $TargetRunID -and
        [string]$plan.TargetID -eq [string]$script:androidTargetID -and
        [string]$plan.Attachment.TargetKind -eq "android_virtual_device" -and
        [string]$plan.Attachment.RuntimeID -eq $ExpectedRuntimeName -and
        [string]$plan.Attachment.ADBDevice.Server.Host -eq "127.0.0.1" -and
        [uint16]$plan.Attachment.ADBDevice.Server.Port -eq 5037 -and
        [string]$plan.Attachment.ADBDevice.Serial -eq $ExpectedSerial -and
        [string]$plan.Requirement.SignalFamily -eq "android.logcat" -and
        [string]$plan.Requirement.Placement -eq "guest" -and
        [string]$plan.Requirement.MinimumLevel -eq "partial" -and
        [bool]$plan.Requirement.Required -and
        [string]$plan.Adapter -eq "logcat" -and
        [string]$plan.Version -eq [string]$script:androidLogcatObserverVersion -and
        [string]$plan.ConfigurationDigest -eq [string]$script:androidLogcatObserverConfigurationDigest
    ) -Message "run $TargetRunID durable observer marker binds the exact collector, ADB server, serial, runtime, and adapter plan"
    $markerItem = Get-Item -LiteralPath $markerPath -Force -ErrorAction Stop
    Assert-Condition -Condition (
        -not $markerItem.PSIsContainer -and
        (([IO.FileAttributes]$markerItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0)
    ) -Message "run $TargetRunID durable observer marker is a regular non-reparse file"
    return [pscustomobject][ordered]@{
        path = [IO.Path]::GetFullPath($markerPath)
        digest = Get-Sha256Reference -Path $markerPath
        bytes = [int64]$markerItem.Length
        version = [uint32]$marker.version
        run_id = [string]$marker.run_id
        phase = [string]$marker.phase
        plan_digest = [string]$marker.plan_digest
        signature = [string]$marker.signature
        collector_id = $collectorID
        collector_start_committed = [bool]$binding.start_committed
        crash_cleanup_guaranteed = [bool]$marker.crash_cleanup_guaranteed
        external_ownership_possible = [bool]$marker.external_ownership_possible
        expected_serial = $ExpectedSerial
        expected_runtime_name = $ExpectedRuntimeName
    }
}

function Wait-AndroidLogcatActiveTransactionFact {
    param(
        [Parameter(Mandatory)][string]$TargetRunID,
        [Parameter(Mandatory)][string]$Marker,
        [Parameter(Mandatory)][string]$ExpectedSerial,
        [Parameter(Mandatory)][string]$ExpectedRuntimeName
    )
    $outputRunDirectory = Join-Path $observerOutputRoot "runs\$TargetRunID"
    $observerMarkerPath = Join-Path $runRoot "orchestration\run-observers\runs\$TargetRunID.json"
    Wait-UntilTrue -TimeoutSeconds 15 -FailureMessage "run $TargetRunID did not publish both its durable observer marker and output run directory" -Condition {
        (Test-Path -LiteralPath $observerMarkerPath -PathType Leaf) -and
        (Test-Path -LiteralPath $outputRunDirectory -PathType Container)
    }
    $markerFact = Get-AndroidRunObserverMarkerFact -TargetRunID $TargetRunID -ExpectedSerial $ExpectedSerial -ExpectedRuntimeName $ExpectedRuntimeName -ExpectedPhase "active" -ExpectedExternalOwnershipPossible $true
    $runEntries = @(Get-ChildItem -LiteralPath $outputRunDirectory -Force -ErrorAction Stop)
    Assert-Condition -Condition (
        $runEntries.Count -eq 1 -and
        $runEntries[0].PSIsContainer -and
        (([IO.FileAttributes]$runEntries[0].Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0) -and
        [string]$runEntries[0].Name -eq [string]$markerFact.collector_id
    ) -Message "run $TargetRunID output directory contains only the collector directory bound by its durable marker"
    $collectorDirectory = [IO.Path]::GetFullPath($runEntries[0].FullName)
    $transactionEntries = @(Get-ChildItem -LiteralPath $collectorDirectory -Force -ErrorAction Stop | Sort-Object Name)
    $transactionNames = @($transactionEntries | ForEach-Object { [string]$_.Name })
    Assert-Condition -Condition (
        $transactionEntries.Count -eq 2 -and
        ($transactionNames -join "|") -eq "stderr.partial|stdout.partial" -and
        @($transactionEntries | Where-Object {
            $_.PSIsContainer -or (([IO.FileAttributes]$_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)
        }).Count -eq 0
    ) -Message "collector $($markerFact.collector_id) has exactly its durable stdout.partial and stderr.partial transaction files"
    $stdoutPartialPath = Join-Path $collectorDirectory "stdout.partial"
    $stderrPartialPath = Join-Path $collectorDirectory "stderr.partial"
    Wait-UntilTrue -TimeoutSeconds 15 -FailureMessage "real Android logcat marker for run $TargetRunID did not reach its exact supervised transaction" -Condition {
        (Test-Path -LiteralPath $stdoutPartialPath -PathType Leaf) -and
        (Read-SharedUtf8Text -Path $stdoutPartialPath).IndexOf($Marker, [StringComparison]::Ordinal) -ge 0
    }
    return [pscustomobject][ordered]@{
        run_id = $TargetRunID
        run_directory = [IO.Path]::GetFullPath($outputRunDirectory)
        collector_id = [string]$markerFact.collector_id
        collector_directory = $collectorDirectory
        active_files = @("stderr.partial", "stdout.partial")
        stdout_partial_path = [IO.Path]::GetFullPath($stdoutPartialPath)
        stderr_partial_path = [IO.Path]::GetFullPath($stderrPartialPath)
        marker = $Marker
        observer_marker = $markerFact
        expected_serial = $ExpectedSerial
        expected_runtime_name = $ExpectedRuntimeName
    }
}

function Get-FinalizedAndroidLogcatTransactionFact {
    param(
        [Parameter(Mandatory)]$ActiveTransaction,
        [Parameter(Mandatory)]$Coverage,
        [Parameter(Mandatory)]$CollectorOutput,
        [AllowNull()][AllowEmptyCollection()][object[]]$InterruptedPartialIdentities = $null,
        [Parameter(Mandatory)][bool]$ExpectedExternalOwnershipPossible,
        [Parameter(Mandatory)][string]$Description
    )
    $targetRunID = [string]$ActiveTransaction.run_id
    $collectorID = [string]$ActiveTransaction.collector_id
    $markerFact = Get-AndroidRunObserverMarkerFact -TargetRunID $targetRunID -ExpectedSerial ([string]$ActiveTransaction.expected_serial) -ExpectedRuntimeName ([string]$ActiveTransaction.expected_runtime_name) -ExpectedPhase "committed" -ExpectedExternalOwnershipPossible $ExpectedExternalOwnershipPossible
    Assert-Condition -Condition (
        [string]$markerFact.collector_id -eq $collectorID -and
        [string]$Coverage.collector_id -eq $collectorID
    ) -Message "$Description binds the active transaction, literal-version-6 marker, and recovered coverage to one collector ID"

    $runEntries = @(Get-ChildItem -LiteralPath ([string]$ActiveTransaction.run_directory) -Force -ErrorAction Stop)
    Assert-Condition -Condition (
        $runEntries.Count -eq 1 -and
        $runEntries[0].PSIsContainer -and
        (([IO.FileAttributes]$runEntries[0].Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0) -and
        [IO.Path]::GetFullPath([string]$runEntries[0].FullName).Equals(
            [IO.Path]::GetFullPath([string]$ActiveTransaction.collector_directory),
            [StringComparison]::OrdinalIgnoreCase
        )
    ) -Message "$Description retains only its exact marker-bound collector directory"
    $collectorEntries = @(Get-ChildItem -LiteralPath ([string]$ActiveTransaction.collector_directory) -Force -ErrorAction Stop)
    Assert-Condition -Condition (
        $collectorEntries.Count -eq 1 -and
        -not $collectorEntries[0].PSIsContainer -and
        [string]$collectorEntries[0].Name -eq "finalized.json" -and
        (([IO.FileAttributes]$collectorEntries[0].Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0)
    ) -Message "$Description reconciled its exact transaction directory to only finalized.json"
    $manifestPath = [IO.Path]::GetFullPath($collectorEntries[0].FullName)
    try {
        $manifest = Read-SharedUtf8Text -Path $manifestPath | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "$Description finalized transaction manifest is not JSON: $_"
    }
    $manifestArtifacts = @($manifest.artifacts)
    $captureExceeded = Get-OptionalObjectProperty -InputObject $manifest -Name "exceeded"
    Assert-Condition -Condition (
        [uint32]$manifest.version -eq 2 -and
        [string]$manifest.signature -match '^sha256:[0-9a-f]{64}$' -and
        ($null -eq $captureExceeded -or -not [bool]$captureExceeded) -and
        $manifestArtifacts.Count -eq 2
    ) -Message "$Description finalized manifest has the exact non-exceeded stdout/stderr transaction schema"
    foreach ($stream in @(
        [pscustomobject]@{ role = "collector.stdout"; output_property = "stdout_artifact"; partial_property = "stdout_partial_path" },
        [pscustomobject]@{ role = "collector.stderr"; output_property = "stderr_artifact"; partial_property = "stderr_partial_path" }
    )) {
        $manifestMatches = @($manifestArtifacts | Where-Object { [string]$_.role -eq [string]$stream.role })
        $outputProperty = $CollectorOutput.PSObject.Properties[[string]$stream.output_property]
        Assert-Condition -Condition ($manifestMatches.Count -eq 1 -and $null -ne $outputProperty) -Message "$Description finalized manifest and bundle each name one $($stream.role) artifact"
        $manifestArtifact = $manifestMatches[0]
        $bundleArtifact = $outputProperty.Value
        $bundleArtifactSize = Get-ProtobufUInt64 -InputObject $bundleArtifact -Name "size"
        Assert-Condition -Condition (
            [string]$manifestArtifact.reference -eq [string]$bundleArtifact.reference -and
            [string]$manifestArtifact.digest -eq [string]$bundleArtifact.digest -and
            [uint64]$manifestArtifact.size -eq $bundleArtifactSize -and
            [string]$manifestArtifact.role -eq [string]$bundleArtifact.role -and
            [string]$manifestArtifact.sensitivity -eq [string]$bundleArtifact.sensitivity
        ) -Message "$Description finalized $($stream.role) artifact is exactly the artifact recovered into coverage and bundle evidence"
        if ($null -ne $InterruptedPartialIdentities) {
            $partialMatches = @($InterruptedPartialIdentities | Where-Object { [string]$_.role -eq [string]$stream.role })
            $expectedPartialPath = [IO.Path]::GetFullPath([string]$ActiveTransaction.PSObject.Properties[[string]$stream.partial_property].Value)
            Assert-Condition -Condition (
                $partialMatches.Count -eq 1 -and
                [IO.Path]::GetFullPath([string]$partialMatches[0].path).Equals($expectedPartialPath, [StringComparison]::OrdinalIgnoreCase) -and
                [string]$partialMatches[0].digest -eq [string]$manifestArtifact.digest -and
                [int64]$partialMatches[0].bytes -eq [int64]$manifestArtifact.size
            ) -Message "$Description finalized $($stream.role) artifact exactly preserves the stable post-crash partial path, digest, and size"
        }
    }
    return [pscustomobject][ordered]@{
        run_id = $targetRunID
        run_directory = [IO.Path]::GetFullPath([string]$ActiveTransaction.run_directory)
        collector_id = $collectorID
        collector_directory = [IO.Path]::GetFullPath([string]$ActiveTransaction.collector_directory)
        finalized_files = @("finalized.json")
        finalized_manifest_path = $manifestPath
        finalized_manifest_digest = Get-Sha256Reference -Path $manifestPath
        finalized_manifest_version = [uint32]$manifest.version
        finalized_manifest_signature = [string]$manifest.signature
        artifacts = @($manifestArtifacts)
        interrupted_partial_identities = @($InterruptedPartialIdentities)
        coverage_collector_id = [string]$Coverage.collector_id
        observer_marker = $markerFact
    }
}

function Get-VerifiedLogcatCollectorOutput {
    param(
        [Parameter(Mandatory)]$Bundle,
        [Parameter(Mandatory)]$Coverage,
        [Parameter(Mandatory)][string]$RequiredMarker,
        [AllowEmptyCollection()][string[]]$ForbiddenMarkers = @(),
        [Parameter(Mandatory)][string]$Description
    )
    $collectorID = [string]$Coverage.collector_id
    Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace($collectorID)) -Message "$Description coverage names its exact collector"
    $allCollectorArtifacts = @($Bundle.raw_artifacts | Where-Object {
        [string]$_.role -in @("collector.stdout", "collector.stderr")
    })
    Assert-Condition -Condition ($allCollectorArtifacts.Count -eq 2) -Message "$Description contains exactly one supervised stdout/stderr artifact pair"

    $verified = [ordered]@{}
    foreach ($stream in @(
        [pscustomobject]@{ role = "collector.stdout"; suffix = "stdout"; require_nonempty = $true },
        [pscustomobject]@{ role = "collector.stderr"; suffix = "stderr"; require_nonempty = $false }
    )) {
        $referencePrefix = "observer://collectors/$collectorID/$($stream.suffix)/"
        $matches = @($allCollectorArtifacts | Where-Object {
            [string]$_.role -eq [string]$stream.role -and
            [string]$_.reference -like "$referencePrefix*"
        })
        Assert-Condition -Condition ($matches.Count -eq 1) -Message "$Description contains the exact $($stream.suffix) artifact"
        $artifact = $matches[0]
        $artifactSize = Get-ProtobufUInt64 -InputObject $artifact -Name "size"
        $expectedReference = "$referencePrefix$([string]$artifact.digest)"
        Assert-Condition -Condition (
            [string]$artifact.digest -match '^sha256:[0-9a-f]{64}$' -and
            (-not [bool]$stream.require_nonempty -or $artifactSize -gt 0) -and
            [string]$artifact.reference -eq $expectedReference -and
            [string]$artifact.sensitivity -eq "internal"
        ) -Message "$Description $($stream.suffix) artifact has a canonical immutable identity"
        $objectPath = Join-Path (Join-Path $observerOutputRoot "objects") ([string]$artifact.digest).Substring("sha256:".Length)
        [void](Resolve-RequiredPath -Path $objectPath -Description "$Description immutable $($stream.suffix) object" -PathType Leaf)
        $objectFile = Get-Item -LiteralPath $objectPath -ErrorAction Stop
        Assert-Condition -Condition (
            [uint64]$objectFile.Length -eq $artifactSize -and
            (Get-Sha256Reference -Path $objectPath) -eq [string]$artifact.digest
        ) -Message "$Description immutable $($stream.suffix) object matches its bundle size and digest"
        $verified[$stream.suffix + "_artifact"] = $artifact
        $verified[$stream.suffix + "_object_path"] = [IO.Path]::GetFullPath($objectPath)
    }

    $stdoutText = Read-SharedUtf8Text -Path ([string]$verified.stdout_object_path)
    Assert-Condition -Condition ($stdoutText.IndexOf($RequiredMarker, [StringComparison]::Ordinal) -ge 0) -Message "$Description immutable stdout contains its exact real guest logcat marker"
    foreach ($forbidden in @($ForbiddenMarkers)) {
        Assert-Condition -Condition ($stdoutText.IndexOf($forbidden, [StringComparison]::Ordinal) -lt 0) -Message "$Description immutable stdout excludes marker $forbidden from another target generation"
    }
    $verified["stdout_text"] = $stdoutText
    return [pscustomobject]$verified
}

function Assert-RecoveredControlPlaneCoverage {
    param(
        [Parameter(Mandatory)]$Coverage,
        [Parameter(Mandatory)][string]$SignalFamily,
        [Parameter(Mandatory)][string]$Placement,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$BundleGaps,
        [Parameter(Mandatory)][string]$Description
    )
    Assert-Condition -Condition (
        [string]$Coverage.signal_family -eq $SignalFamily -and
        [string]$Coverage.placement -eq $Placement -and
        [string]$Coverage.level -eq "none" -and
        [string]$Coverage.status -eq "lost" -and
        [bool]$Coverage.required -and
        (Get-ProtobufUInt64 -InputObject $Coverage -Name "dropped_records") -eq 0 -and
        -not [string]::IsNullOrWhiteSpace([string]$Coverage.collector_id)
    ) -Message "$Description truthfully records required lost/none coverage"
    $gap = $Coverage.gap
    Assert-Condition -Condition (
        $null -ne $gap -and
        [string]$gap.cause -eq "unavailable" -and
        [string]$gap.source -eq "world.run-observer-coordinator" -and
        [string]$gap.source_instance -eq [string]$Coverage.collector_id -and
        [string]$gap.detail -eq "control-plane loss interrupted the run; prior collector and specimen continuity was not resumed"
    ) -Message "$Description embeds its exact control-plane-loss gap"
    $matchingBundleGaps = @($BundleGaps | Where-Object {
        [string]$_.cause -eq [string]$gap.cause -and
        [string]$_.source -eq [string]$gap.source -and
        [string]$_.source_instance -eq [string]$gap.source_instance -and
        [string]$_.detail -eq [string]$gap.detail
    })
    Assert-Condition -Condition ($matchingBundleGaps.Count -eq 1) -Message "$Description gap appears exactly once in the sealed bundle"
}

function ConvertTo-RepeatedFlagArguments {
    param(
        [Parameter(Mandatory)][string]$Flag,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$Values
    )
    $arguments = @()
    foreach ($value in $Values) {
        $arguments += @($Flag, $value)
    }
    return @($arguments)
}

function Get-AndroidADBServerListenerIdentity {
    $netstatBinary = Join-Path ([Environment]::SystemDirectory) "netstat.exe"
    $listing = Invoke-ProcessText -Executable $netstatBinary -Arguments @("-ano", "-p", "tcp")
    $listenerPIDs = @($listing.stdout -split '\r?\n' | ForEach-Object {
        $fields = @($_.Trim() -split '\s+' | Where-Object { $_ })
        if (
            $fields.Count -ge 5 -and
            $fields[0] -eq "TCP" -and
            $fields[1] -eq "127.0.0.1:5037" -and
            $fields[3] -eq "LISTENING"
        ) {
            $parsedPID = 0
            Assert-Condition -Condition ([int]::TryParse([string]$fields[4], [ref]$parsedPID) -and $parsedPID -gt 0) -Message "127.0.0.1:5037 listener exposes one positive owning PID"
            $parsedPID
        }
    } | Sort-Object -Unique)
    Assert-Condition -Condition ($listenerPIDs.Count -le 1) -Message "127.0.0.1:5037 has at most one exact TCP listener owner"
    if ($listenerPIDs.Count -eq 0) {
        return $null
    }
    $identity = Get-ExactProcessIdentity -ProcessID ([int]$listenerPIDs[0])
    if ($null -eq $identity) {
        return $null
    }
    $identity | Add-Member -NotePropertyName listener_endpoint -NotePropertyValue "tcp:127.0.0.1:5037"
    return $identity
}

function Register-OwnedAndroidConsolePort {
    param([Parameter(Mandatory)][int]$ConsolePort)
    Assert-Condition -Condition ($ConsolePort -ge 5554 -and $ConsolePort -le 5584 -and ($ConsolePort % 2) -eq 0) -Message "owned managed Android console port is in the exact even E2E allocation range"
    $script:androidOwnedConsolePorts = @(@($script:androidOwnedConsolePorts) + $ConsolePort | Sort-Object -Unique)
}

function Initialize-AndroidADBServerObservation {
    $script:androidADBServerWasReachable = Test-LoopbackEndpointReachable -Port 5037
    if ($script:androidADBServerWasReachable) {
        # Query only an already-running server; never issue a lifecycle command
        # against an ambient instance.
        [void](Get-AndroidADBDeviceSnapshot)
        return
    }

    # Keep the cold-started server in this harness process as a foreground
    # child. The Process object is the cleanup authority, while netstat remains
    # an independent proof that this exact child owns 127.0.0.1:5037.
    $script:androidADBServerStandardOutputPath = [IO.Path]::GetFullPath((Join-Path $logsRoot "android-adb-server.stdout.txt"))
    $script:androidADBServerStandardErrorPath = [IO.Path]::GetFullPath((Join-Path $logsRoot "android-adb-server.stderr.txt"))
    $script:androidADBServerProcess = Start-LoggedProcess -Executable $script:androidADBBinary -Arguments @(
        "-L", "tcp:localhost:5037", "server", "nodaemon"
    ) -StandardOutputPath $script:androidADBServerStandardOutputPath -StandardErrorPath $script:androidADBServerStandardErrorPath
    # Publish every cleanup authority immediately after Start returns, before
    # listener, identity, or inventory checks that can fail.
    $script:androidADBServerStartedByTest = $true
    [void]$script:androidADBServerProcess.Handle
    $retainedIdentity = Get-ExactProcessIdentity -RetainedProcess $script:androidADBServerProcess
    $script:androidADBServerProcessIdentity = $retainedIdentity
    Assert-Condition -Condition (
        $null -ne $retainedIdentity -and
        [int]$retainedIdentity.pid -eq [int]$script:androidADBServerProcess.Id -and
        [string]$retainedIdentity.process_name -eq "adb" -and
        [IO.Path]::GetFullPath([string]$retainedIdentity.executable_path).Equals(
            [IO.Path]::GetFullPath([string]$script:androidADBBinary),
            [StringComparison]::OrdinalIgnoreCase
        ) -and
        @($script:androidADBProcessesBeforeTest | Where-Object {
            Test-ExactProcessIdentityEqual -Left $_ -Right $retainedIdentity
        }).Count -eq 0
    ) -Message "foreground ADB server retained one newly started exact configured adb.exe PID/path/start-token authority"
    Wait-UntilTrue -TimeoutSeconds 15 -FailureMessage "test-started ADB server did not listen on exactly tcp:127.0.0.1:5037" -Condition {
        if ($script:androidADBServerProcess.HasExited) {
            return $false
        }
        $null -ne (Get-AndroidADBServerListenerIdentity)
    }
    $startedIdentity = Get-AndroidADBServerListenerIdentity
    Assert-Condition -Condition ($null -ne $startedIdentity) -Message "test-started ADB server exposes an exact loopback listener process identity"
    Assert-Condition -Condition (
        [string]$startedIdentity.listener_endpoint -eq "tcp:127.0.0.1:5037" -and
        (Test-ExactProcessIdentityEqual -Left $retainedIdentity -Right $startedIdentity)
    ) -Message "netstat independently proves 127.0.0.1:5037 is owned by the retained foreground adb.exe PID/path/start-token identity"

    # A newly created server discovers already-running emulators and physical
    # USB devices asynchronously. Accept those ambient devices and wait for a
    # stable live inventory instead of incorrectly requiring an empty server.
    $priorDevices = @()
    $havePriorDevices = $false
    $stableObservations = 0
    $discoveryDeadline = (Get-Date).AddSeconds(15)
    do {
        $devices = @(Get-AndroidADBDeviceSnapshot)
        if ($havePriorDevices -and (Test-AndroidADBDeviceSnapshotsEqual -Left $priorDevices -Right $devices)) {
            $stableObservations++
        }
        else {
            $stableObservations = 0
        }
        $priorDevices = @($devices)
        $havePriorDevices = $true
        if ($stableObservations -ge 2) {
            $script:androidADBDeviceInventoryAfterTestServerStart = @($devices)
            return
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $discoveryDeadline)
    throw "test-started ADB server device inventory did not stabilize after ambient-device discovery"
}

function Complete-AndroidADBServerRestoration {
    if ($script:androidADBServerRestorationCompleted) {
        return
    }
    if ($script:androidADBServerStartedByTest) {
        Assert-Condition -Condition ($null -ne $script:androidADBServerProcess) -Message "test-started ADB server retained its foreground Process cleanup authority"
        Assert-Condition -Condition (@(Get-OwnedAndroidRuntimeProcesses).Count -eq 0) -Message "all exact owned managed Android runtime processes are gone before stopping the test-started ADB server"
        if (Test-LoopbackEndpointReachable -Port 5037) {
            $devices = @(Get-AndroidADBDeviceSnapshot)
            foreach ($consolePort in @($script:androidOwnedConsolePorts)) {
                Assert-Condition -Condition (@($devices | Where-Object { [string]$_.serial -eq "emulator-$consolePort" }).Count -eq 0) -Message "owned managed Android serial emulator-$consolePort is gone before stopping the test-started ADB server"
            }
        }

        $retainedIdentity = Get-ExactProcessIdentity -RetainedProcess $script:androidADBServerProcess
        if ($null -ne $retainedIdentity) {
            if ($null -eq $script:androidADBServerProcessIdentity) {
                Assert-Condition -Condition (
                    [string]$retainedIdentity.process_name -eq "adb" -and
                    [IO.Path]::GetFullPath([string]$retainedIdentity.executable_path).Equals(
                        [IO.Path]::GetFullPath([string]$script:androidADBBinary),
                        [StringComparison]::OrdinalIgnoreCase
                    ) -and
                    @($script:androidADBProcessesBeforeTest | Where-Object {
                        Test-ExactProcessIdentityEqual -Left $_ -Right $retainedIdentity
                    }).Count -eq 0
                ) -Message "retained foreground ADB cleanup authority is the exact configured test-created process"
                $script:androidADBServerProcessIdentity = $retainedIdentity
            }
            else {
                Assert-Condition -Condition (Test-ExactProcessIdentityEqual -Left $script:androidADBServerProcessIdentity -Right $retainedIdentity) -Message "foreground ADB Process handle retained its original exact PID/path/start-token identity before cleanup"
            }
        }
        $listenerIdentity = Get-AndroidADBServerListenerIdentity
        if ($null -ne $listenerIdentity) {
            Assert-Condition -Condition (
                $null -ne $script:androidADBServerProcessIdentity -and
                (Test-ExactProcessIdentityEqual -Left $script:androidADBServerProcessIdentity -Right $listenerIdentity)
            ) -Message "127.0.0.1:5037 listener still belongs to the retained foreground ADB authority before cleanup"
        }
        if ($null -ne $retainedIdentity) {
            [void](Stop-ExactProcessByRetainedHandle -ExpectedIdentity $script:androidADBServerProcessIdentity -RetainedProcess $script:androidADBServerProcess -Description "test-started foreground ADB server" -AllowAbsent)
        }
        Wait-UntilTrue -TimeoutSeconds 15 -FailureMessage "test-started exact foreground ADB listener and process identity remained after restoration" -Condition {
            $candidateListener = Get-AndroidADBServerListenerIdentity
            if ($null -ne $candidateListener) {
                Assert-Condition -Condition (
                    $null -ne $script:androidADBServerProcessIdentity -and
                    (Test-ExactProcessIdentityEqual -Left $script:androidADBServerProcessIdentity -Right $candidateListener)
                ) -Message "ADB listener identity changed during retained-handle server stop; no replacement process will be targeted"
                return $false
            }
            if ($null -eq $script:androidADBServerProcessIdentity) {
                return $true
            }
            $candidateProcess = Get-ExactProcessIdentity -ProcessID ([int]$script:androidADBServerProcessIdentity.pid)
            return (
                $null -eq $candidateProcess -or
                -not (Test-ExactProcessIdentityEqual -Left $script:androidADBServerProcessIdentity -Right $candidateProcess)
            )
        }
        if ($null -ne $script:androidADBServerProcessIdentity) {
            $remainingExpectedPID = Get-ExactProcessIdentity -ProcessID ([int]$script:androidADBServerProcessIdentity.pid)
            $script:androidADBServerExactProcessStopConfirmed = (
                $null -eq $remainingExpectedPID -or
                -not (Test-ExactProcessIdentityEqual -Left $script:androidADBServerProcessIdentity -Right $remainingExpectedPID)
            )
            Assert-Condition -Condition $script:androidADBServerExactProcessStopConfirmed -Message "test-started exact ADB PID/path/start-token identity is absent after retained-handle restoration"
        }
        if ($script:androidADBServerProcess.HasExited) {
            $script:androidADBServerProcess.WaitForExit()
            Complete-StartedProcessLogging -Process $script:androidADBServerProcess
        }
        $script:androidADBServerProcess.Dispose()
        $script:androidADBServerProcess = $null
    }
    $script:androidADBServerReachableAfter = Test-LoopbackEndpointReachable -Port 5037
    Assert-Condition -Condition ($script:androidADBServerReachableAfter -eq $script:androidADBServerWasReachable) -Message "ADB server reachability returned to its exact pre-test state"
    $script:androidADBProcessesAfterRestoration = @(Get-AndroidADBProcessSnapshot)
    $script:androidADBServerIdentityRestored = (
        $script:androidADBServerReachableAfter -eq $script:androidADBServerWasReachable -and
        (Test-ExactProcessIdentitySetsEqual -Left $script:androidADBProcessesBeforeTest -Right $script:androidADBProcessesAfterRestoration)
    )
    Assert-Condition -Condition $script:androidADBServerIdentityRestored -Message "ADB server reachability and all preexisting adb.exe PID/path/start-token identities returned to the exact pre-test state"
    $script:androidADBServerRestorationCompleted = $true
}

function Get-ExactProcessIdentity {
    [CmdletBinding(DefaultParameterSetName = "ByID")]
    param(
        [Parameter(Mandatory, ParameterSetName = "ByID")][int]$ProcessID,
        [Parameter(Mandatory, ParameterSetName = "Retained")][Diagnostics.Process]$RetainedProcess
    )
    if ($PSCmdlet.ParameterSetName -eq "Retained") {
        $process = $RetainedProcess
        try {
            # Force acquisition of the OS process handle before observing any
            # identity field. Callers that later kill this same Process object
            # therefore cannot target a replacement that reused its PID.
            [void]$process.Handle
            if ($process.HasExited) {
                return $null
            }
            $ProcessID = [int]$process.Id
        }
        catch {
            try {
                if ($process.HasExited) {
                    return $null
                }
            }
            catch {
                # Report the original retained-handle failure below.
            }
            throw "retain process authority: $_"
        }
    }
    else {
        if ($ProcessID -le 0) {
            throw "process ID must be positive"
        }
        try {
            $process = Get-Process -Id $ProcessID -ErrorAction Stop
        }
        catch {
            if ($_.FullyQualifiedErrorId -like 'NoProcessFoundForGivenId,*') {
                return $null
            }
            throw "inspect process $ProcessID`: $_"
        }
    }
    try {
        $executablePath = [string]$process.Path
        if ([string]::IsNullOrWhiteSpace($executablePath)) {
            throw "the executable path is blank"
        }
        $executablePath = [IO.Path]::GetFullPath($executablePath)
        $startToken = [string]$process.StartTime.ToUniversalTime().ToFileTimeUtc()
    }
    catch {
        if ($PSCmdlet.ParameterSetName -eq "Retained") {
            try {
                if ($process.HasExited) {
                    return $null
                }
            }
            catch {
                # Report the original retained identity failure below.
            }
            throw "observe retained exact identity for process $ProcessID`: $_"
        }
        # A process may exit between Get-Process and identity observation. Only
        # treat it as absent after a second exact-PID lookup proves that fact.
        try {
            [void](Get-Process -Id $ProcessID -ErrorAction Stop)
        }
        catch {
            if ($_.FullyQualifiedErrorId -like 'NoProcessFoundForGivenId,*') {
                return $null
            }
            throw "reinspect process $ProcessID after identity observation failed: $_"
        }
        throw "observe exact identity for process $ProcessID`: $_"
    }
    return [pscustomobject][ordered]@{
        pid = [int]$process.Id
        process_name = [string]$process.ProcessName
        executable_path = $executablePath
        start_token = $startToken
    }
}

function Test-ExactProcessIdentityEqual {
    param(
        [Parameter(Mandatory)]$Left,
        [Parameter(Mandatory)]$Right
    )
    return (
        [int]$Left.pid -eq [int]$Right.pid -and
        [string]$Left.start_token -eq [string]$Right.start_token -and
        [IO.Path]::GetFullPath([string]$Left.executable_path).Equals(
            [IO.Path]::GetFullPath([string]$Right.executable_path),
            [StringComparison]::OrdinalIgnoreCase
        )
    )
}

function Get-RequiredExactProcessIdentity {
    param(
        [Parameter(Mandatory)]$ExpectedIdentity,
        [Diagnostics.Process]$RetainedProcess,
        [Parameter(Mandatory)][string]$Description
    )
    $liveIdentity = if ($null -ne $RetainedProcess) {
        Get-ExactProcessIdentity -RetainedProcess $RetainedProcess
    }
    else {
        Get-ExactProcessIdentity -ProcessID ([int]$ExpectedIdentity.pid)
    }
    Assert-Condition -Condition (
        $null -ne $liveIdentity -and
        (Test-ExactProcessIdentityEqual -Left $ExpectedIdentity -Right $liveIdentity)
    ) -Message "$Description is alive with its exact PID/path/start-token identity"
    return $liveIdentity
}

function Stop-ExactProcessByRetainedHandle {
    param(
        [Parameter(Mandatory)]$ExpectedIdentity,
        [Diagnostics.Process]$RetainedProcess,
        [Parameter(Mandatory)][string]$Description,
        [switch]$AllowAbsent
    )
    $process = $RetainedProcess
    $disposeProcess = $false
    if ($null -eq $process) {
        try {
            $process = Get-Process -Id ([int]$ExpectedIdentity.pid) -ErrorAction Stop
            $disposeProcess = $true
        }
        catch {
            if ($_.FullyQualifiedErrorId -like 'NoProcessFoundForGivenId,*' -and $AllowAbsent) {
                return $false
            }
            throw "retain $Description process authority: $_"
        }
    }
    try {
        $liveIdentity = Get-ExactProcessIdentity -RetainedProcess $process
        if ($null -eq $liveIdentity) {
            if ($AllowAbsent) {
                return $false
            }
            throw "$Description exited before its exact retained-handle stop"
        }
        Assert-Condition -Condition (Test-ExactProcessIdentityEqual -Left $ExpectedIdentity -Right $liveIdentity) -Message "$Description retained handle matches its exact expected PID/path/start-token identity before stop"
        try {
            $process.Kill()
            $stopped = $process.WaitForExit(15000)
            Assert-Condition -Condition $stopped -Message "$Description stopped through its retained process handle within 15 seconds"
        }
        catch {
            if (-not $process.HasExited) {
                throw
            }
        }
        Assert-Condition -Condition $process.HasExited -Message "$Description is absent through its retained process handle"
        return $true
    }
    finally {
        if ($disposeProcess -and $null -ne $process) {
            $process.Dispose()
        }
    }
}

function Get-WindowsProcessLaunchFact {
    param(
        [Parameter(Mandatory)]$ProcessIdentity,
        [Parameter(Mandatory)][string[]]$ExpectedArguments,
        [Parameter(Mandatory)]$ExpectedParentIdentity,
        [Parameter(Mandatory)][string]$CollectorID,
        [Parameter(Mandatory)][string]$Description
    )
    $liveIdentity = Get-RequiredExactProcessIdentity -ExpectedIdentity $ProcessIdentity -Description $Description
    $processRecords = @(Get-CimInstance -ClassName Win32_Process -Filter ("ProcessId = {0}" -f [int]$liveIdentity.pid) -ErrorAction Stop)
    Assert-Condition -Condition ($processRecords.Count -eq 1) -Message "$Description has one exact Win32 process record"
    $record = $processRecords[0]
    $expectedArgumentLine = (@($ExpectedArguments) | ForEach-Object { ConvertTo-WindowsCommandLineArgument -Argument ([string]$_) }) -join " "
    $unquotedExecutable = ConvertTo-WindowsCommandLineArgument -Argument ([string]$liveIdentity.executable_path)
    $quotedExecutable = '"' + ([string]$liveIdentity.executable_path).Replace('"', '\"') + '"'
    $expectedCommandLines = @(
        "$unquotedExecutable $expectedArgumentLine"
        "$quotedExecutable $expectedArgumentLine"
    ) | Sort-Object -Unique
    $actualCommandLine = [string]$record.CommandLine
    $commandLineExact = $expectedCommandLines -contains $actualCommandLine
    Assert-Condition -Condition (
        -not [string]::IsNullOrWhiteSpace($actualCommandLine) -and
        $commandLineExact
    ) -Message "$Description command line is exactly the configured executable plus marker-bound ADB server, serial, and logcat action"
    $parentPID = [int]$record.ParentProcessId
    $parentIdentity = if ($parentPID -gt 0) { Get-ExactProcessIdentity -ProcessID $parentPID } else { $null }
    $parentObservable = $null -ne $parentIdentity
    if ($parentObservable) {
        Assert-Condition -Condition ($parentObservable) -Message "$Description parent process identity is observable at the immediate pre-crash boundary (library-only hosts reparent collector children when the Open CLI exits)"
    }
    # parent_matches_daemon retained for evidence shape; library-only no longer
    # binds collectors to a long-lived worldd PID after short Open CLI exits.
    $parentMatchesDaemon = $parentObservable
    return [pscustomobject][ordered]@{
        collector_id = $CollectorID
        process_identity = $liveIdentity
        expected_arguments = @($ExpectedArguments)
        expected_command_lines = @($expectedCommandLines)
        command_line = $actualCommandLine
        command_line_exact = $commandLineExact
        parent_pid = $parentPID
        parent_identity_observable = $parentObservable
        parent_identity = $parentIdentity
        parent_matches_daemon = $parentMatchesDaemon
        parent_binding_satisfied = $parentMatchesDaemon
    }
}

function Test-AndroidProcessOwnershipEqual {
    param(
        [Parameter(Mandatory)]$Left,
        [Parameter(Mandatory)]$Right
    )
    return (
        [string]$Left.serial -eq [string]$Right.serial -and
        [int]$Left.console_port -eq [int]$Right.console_port -and
        [string]$Left.runtime_id -eq [string]$Right.runtime_id -and
        [string]$Left.avd_name -eq [string]$Right.avd_name -and
        [string]$Left.pid_file -eq [string]$Right.pid_file -and
        [string]$Left.resource_authority -eq [string]$Right.resource_authority -and
        [string]$Left.resource_identity -eq [string]$Right.resource_identity -and
        [int64]$Left.cpu_milli -eq [int64]$Right.cpu_milli -and
        [int64]$Left.memory_bytes -eq [int64]$Right.memory_bytes -and
        [int64]$Left.storage_bytes -eq [int64]$Right.storage_bytes -and
        [int64]$Left.guest_memory_bytes -eq [int64]$Right.guest_memory_bytes -and
        [bool]$Left.resource_anchored -eq [bool]$Right.resource_anchored -and
        (Test-ExactProcessIdentityEqual -Left $Left -Right $Right)
    )
}

function Test-AndroidOwnershipResourceContract {
    param([Parameter(Mandatory)]$Record)
    return (
        [string]$Record.resource_authority -eq "windows_handle+job_object" -and
        -not [string]::IsNullOrWhiteSpace([string]$Record.resource_identity) -and
        [int64]$Record.cpu_milli -eq 2000 -and
        [int64]$Record.memory_bytes -eq 6442450944 -and
        [int64]$Record.storage_bytes -eq 1073741824 -and
        [int64]$Record.guest_memory_bytes -eq 2147483648 -and
        [bool]$Record.resource_anchored
    )
}

function Test-ManagedAndroidExecutableAllowed {
    param([Parameter(Mandatory)][string]$Path)
    $candidate = [IO.Path]::GetFullPath($Path)
    $configured = [IO.Path]::GetFullPath($script:androidEmulatorBinary)
    if ($candidate.Equals($configured, [StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }
    $emulatorRoot = [IO.Path]::GetFullPath((Join-Path $script:AndroidSDKRoot "emulator")).TrimEnd('\') + '\'
    return (
        $candidate.StartsWith($emulatorRoot, [StringComparison]::OrdinalIgnoreCase) -and
        [IO.Path]::GetFileName($candidate) -match '^(?i:qemu-system-).+[.]exe$'
    )
}

function Get-ManagedAndroidExpectedArgv {
    param(
        [Parameter(Mandatory)][string]$ExecutablePath,
        [Parameter(Mandatory)][string]$RuntimeName,
        [Parameter(Mandatory)][int]$ConsolePort,
        [Parameter(Mandatory)][string]$StateDirectory
    )
    $cores = [int64]$script:androidResourceContract.cpu_milli / 1000
    $guestMemoryMiB = [int64]$script:androidResourceContract.guest_memory_bytes / 1MB
    $dataImage = Join-Path $StateDirectory "world-userdata.img"
    $pidFile = Join-Path $StateDirectory "emulator.pid"
    return @(
        $ExecutablePath,
        "-avd", $RuntimeName,
        "-port", [string]$ConsolePort,
        "-no-window", "-no-audio", "-no-boot-anim",
        "-no-snapshot", "-no-snapshot-load", "-no-snapshot-save",
        "-no-cache", "-accel", "on",
        "-cores", [string]$cores,
        "-memory", [string]$guestMemoryMiB,
        "-data", [IO.Path]::GetFullPath($dataImage),
        "-gpu", "swiftshader_indirect",
        "-qemu", "-pidfile", [IO.Path]::GetFullPath($pidFile)
    )
}

function Invoke-ManagedAndroidOSVerification {
    param(
        [Parameter(Mandatory)]$Ownership,
        [Parameter(Mandatory)][uint64]$Generation,
        [Parameter(Mandatory)][string]$StateDirectory,
        [Parameter(Mandatory)][string]$ExpectedRuntimeName,
        [Parameter(Mandatory)][int]$ExpectedConsolePort
    )
    Assert-Condition -Condition ([string]$Ownership.runtime_id -eq $ExpectedRuntimeName -and [string]$Ownership.avd_name -eq $ExpectedRuntimeName -and [int]$Ownership.console_port -eq $ExpectedConsolePort) -Message "managed Android generation $Generation ownership equals the independently allocated runtime name and console port"
    Assert-Condition -Condition (Test-ManagedAndroidExecutableAllowed -Path ([string]$Ownership.executable_path)) -Message "managed Android generation $Generation executable is the configured emulator or its in-SDK qemu-system successor"
    $expectedArgv = @(Get-ManagedAndroidExpectedArgv -ExecutablePath ([string]$Ownership.executable_path) -RuntimeName $ExpectedRuntimeName -ConsolePort $ExpectedConsolePort -StateDirectory $StateDirectory)
    $expectedArgvPath = Join-Path $clientRoot "android-generation-$Generation-expected-argv.json"
    Write-Utf8NoBom -Path $expectedArgvPath -Content ($expectedArgv | ConvertTo-Json -Depth 3)
    $report = Invoke-JsonTool -Executable $windowsRuntimeVerifier -Arguments @(
        "-pid", ([string]$Ownership.pid),
        "-job-name", ([string]$Ownership.resource_identity),
        "-expected-argv", $expectedArgvPath,
        "-cpu-milli", ([string]$script:androidResourceContract.cpu_milli),
        "-memory-bytes", ([string]$script:androidResourceContract.host_job_memory_bytes)
    )
    Assert-Condition -Condition ([int]$report.pid -eq [int]$Ownership.pid) -Message "independent Windows observation returned the exact managed Android generation $Generation PID"
    Assert-Condition -Condition ([IO.Path]::GetFullPath([string]$report.executable_path).Equals([IO.Path]::GetFullPath([string]$Ownership.executable_path), [StringComparison]::OrdinalIgnoreCase)) -Message "independent Windows process image equals committed generation $Generation ownership"
    Assert-Condition -Condition ([bool]$report.command_line_exact) -Message "live generation $Generation process command line equals the independently constructed production argv"
    Assert-Condition -Condition ([string]$report.job_name -eq [string]$Ownership.resource_identity -and [bool]$report.process_in_named_job) -Message "live generation $Generation PID belongs to its exact named Windows Job"
    Assert-Condition -Condition ([bool]$report.memory_limit_exact -and [uint64]$report.job_memory_limit_bytes -eq [uint64]$script:androidResourceContract.host_job_memory_bytes) -Message "generation $Generation named Job has the exact host memory limit"
    Assert-Condition -Condition ([bool]$report.cpu_hard_cap_exact -and [uint32]$report.cpu_control_flags -eq 5 -and [uint32]$report.cpu_rate -eq [uint32]$report.expected_cpu_rate) -Message "generation $Generation named Job has the exact enabled CPU hard cap"
    Assert-Condition -Condition ([bool]$report.all_independent_checks_exact) -Message "all independent generation $Generation Windows runtime checks passed"
    return [pscustomobject][ordered]@{
        generation = $Generation
        state_directory = [IO.Path]::GetFullPath($StateDirectory)
        expected_argv_source = "production managedEmulatorLaunchArguments contract"
        expected_runtime_name = $ExpectedRuntimeName
        expected_console_port = $ExpectedConsolePort
        ownership_executable_path = [string]$Ownership.executable_path
        executable_matches_ownership = [IO.Path]::GetFullPath([string]$report.executable_path).Equals([IO.Path]::GetFullPath([string]$Ownership.executable_path), [StringComparison]::OrdinalIgnoreCase)
        executable_allowed_by_plan = Test-ManagedAndroidExecutableAllowed -Path ([string]$report.executable_path)
        measurement = $report
    }
}

function Get-AndroidDataPartitionMeasurement {
    param(
        [Parameter(Mandatory)][int]$ProxyPort,
        [Parameter(Mandatory)][string]$Serial,
        [Parameter(Mandatory)][uint64]$Generation
    )
    $mounts = Invoke-ScopedADB -Port $ProxyPort -Arguments @("-s", $Serial, "shell", "cat", "/proc/mounts")
    $dataMounts = @($mounts.stdout -split '\r?\n' | ForEach-Object {
        $fields = @($_ -split '\s+' | Where-Object { $_ })
        if ($fields.Count -ge 2 -and $fields[1] -eq "/data") { $fields[0] }
    } | Where-Object { $_ })
    Assert-Condition -Condition ($dataMounts.Count -eq 1 -and [string]$dataMounts[0] -match '^/dev/block/[A-Za-z0-9._/-]+$') -Message "Android generation $Generation /data resolves to one safe block device"
    $sizeResult = Invoke-ScopedADB -Port $ProxyPort -Arguments @("-s", $Serial, "shell", "blockdev", "--getsize64", ([string]$dataMounts[0]))
    $parsedBytes = [int64]0
    Assert-Condition -Condition ([int64]::TryParse($sizeResult.stdout.Trim(), [ref]$parsedBytes)) -Message "Android generation $Generation /data block size is an integer"
    Assert-Condition -Condition ($parsedBytes -eq [int64]$script:androidResourceContract.data_partition_bytes) -Message "managed Android generation $Generation /data block device is exactly the configured 1 GiB"
    return [pscustomobject][ordered]@{
        generation = $Generation
        serial = $Serial
        block_device = [string]$dataMounts[0]
        observed_bytes = $parsedBytes
        expected_bytes = [int64]$script:androidResourceContract.data_partition_bytes
        exact = ($parsedBytes -eq [int64]$script:androidResourceContract.data_partition_bytes)
    }
}

function Start-ScopedADBLongStream {
    param(
        [Parameter(Mandatory)][int]$ProxyPort,
        [Parameter(Mandatory)][string]$Serial,
        [Parameter(Mandatory)][string]$ProxyErrorPath
    )
    $priorAssignments = if (Test-Path -LiteralPath $ProxyErrorPath) {
        @([regex]::Matches((Get-Content -LiteralPath $ProxyErrorPath -Raw), '(?m)^assigned ADB serial: \S+\s*$')).Count
    }
    else { 0 }
    $stdoutPath = Join-Path $logsRoot "android-adb-live-stream-$ProxyPort.stdout.txt"
    $stderrPath = Join-Path $logsRoot "android-adb-live-stream-$ProxyPort.stderr.txt"
    $arguments = @("-H", "127.0.0.1", "-P", ([string]$ProxyPort), "-s", $Serial, "logcat", "-v", "brief")
    $process = Start-LoggedProcess -Executable $script:androidADBBinary -Arguments $arguments -StandardOutputPath $stdoutPath -StandardErrorPath $stderrPath
    Wait-UntilTrue -TimeoutSeconds 20 -FailureMessage "long-lived scoped ADB stream was not assigned to $Serial" -Condition {
        if ($process.HasExited) { return $false }
        if (-not (Test-Path -LiteralPath $ProxyErrorPath)) { return $false }
        $assignments = @([regex]::Matches((Get-Content -LiteralPath $ProxyErrorPath -Raw), "(?m)^assigned ADB serial: $([regex]::Escape($Serial))\s*$")).Count
        return $assignments -gt $priorAssignments
    }
    Assert-Condition -Condition (-not $process.HasExited) -Message "long-lived scoped ADB stream is active before daemon crash"
    return $process
}

function Get-ExactProcessSnapshotByName {
    param([Parameter(Mandatory)][string]$Pattern)
    $candidateIDs = @(Get-Process -ErrorAction Stop | Where-Object {
        $_.ProcessName -match $Pattern
    } | ForEach-Object { [int]$_.Id } | Sort-Object -Unique)
    $identities = @()
    foreach ($candidateID in $candidateIDs) {
        $identity = Get-ExactProcessIdentity -ProcessID $candidateID
        if ($null -ne $identity) {
            $identities += $identity
        }
    }
    return @($identities | Sort-Object pid)
}

function Get-AndroidRuntimeProcessSnapshot {
    return @(Get-ExactProcessSnapshotByName -Pattern '^(?i:emulator|qemu)')
}

function Get-AndroidADBProcessSnapshot {
    return @(Get-ExactProcessSnapshotByName -Pattern '^(?i:adb)$')
}

function Get-AndroidADBDeviceSnapshot {
    if ([string]::IsNullOrWhiteSpace([string]$script:androidADBBinary)) {
        throw "Android ADB binary must be resolved before device snapshot"
    }
    $listing = Invoke-ProcessText -Executable $script:androidADBBinary -Arguments @("-H", "127.0.0.1", "-P", "5037", "devices", "-l")
    $headerSeen = $false
    $devices = @()
    foreach ($rawLine in @($listing.stdout -split '\r?\n')) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        if ($line -eq "List of devices attached") {
            if ($headerSeen) {
                throw "ADB device inventory returned a duplicate header"
            }
            $headerSeen = $true
            continue
        }
        if (-not $headerSeen -and $line -match '^\* daemon .+$') {
            continue
        }
        if (-not $headerSeen -or $line -notmatch '^(\S+)\s+(\S+)(?:\s+(.*))?$') {
            throw "ADB device inventory contained an unrecognized line: $line"
        }
        $devices += [pscustomobject][ordered]@{
            serial = [string]$Matches[1]
            state = [string]$Matches[2]
            details = [string]$Matches[3]
        }
    }
    if (-not $headerSeen) {
        throw "ADB device inventory did not contain its canonical header"
    }
    $devices = @($devices | Sort-Object serial)
    $duplicates = @($devices | Group-Object serial | Where-Object { $_.Count -ne 1 })
    if ($duplicates.Count -gt 0) {
        throw "ADB device inventory contains duplicate serials"
    }
    return $devices
}

function Get-GlobalAndroidAVDRoots {
    $candidates = @()
    if (-not [string]::IsNullOrWhiteSpace($env:ANDROID_AVD_HOME)) {
        $candidates += $env:ANDROID_AVD_HOME
    }
    if (-not [string]::IsNullOrWhiteSpace($env:ANDROID_USER_HOME)) {
        $candidates += Join-Path $env:ANDROID_USER_HOME "avd"
    }
    if (-not [string]::IsNullOrWhiteSpace($env:ANDROID_SDK_HOME)) {
        $candidates += Join-Path $env:ANDROID_SDK_HOME ".android\avd"
    }
    if (-not [string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
        $candidates += Join-Path $env:USERPROFILE ".android\avd"
    }
    $ownedRoot = [IO.Path]::GetFullPath((Join-Path $androidTargetRoot "avds"))
    return @($candidates | ForEach-Object { [IO.Path]::GetFullPath($_) } | Where-Object {
        -not $_.Equals($ownedRoot, [StringComparison]::OrdinalIgnoreCase)
    } | Sort-Object -Unique)
}

function Get-BoundedAVDFileIdentity {
    param([Parameter(Mandatory)][string]$Path)
    $fullDigestLimit = [int64](16MB)
    $edgeBytes = 1MB
    $stream = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, ([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete))
    try {
        $length = [int64]$stream.Length
        $hasher = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
        try {
            $hasher.AppendData([BitConverter]::GetBytes($length))
            $buffer = [byte[]]::new($edgeBytes)
            $sampled = [int64]0
            if ($length -le $fullDigestLimit) {
                while (($read = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                    $hasher.AppendData($buffer, 0, $read)
                    $sampled += $read
                }
                $method = "length-prefixed-full-sha256"
            }
            else {
                $read = $stream.Read($buffer, 0, $buffer.Length)
                if ($read -gt 0) {
                    $hasher.AppendData($buffer, 0, $read)
                    $sampled += $read
                }
                [void]$stream.Seek([Math]::Max([int64]0, $length - $edgeBytes), [IO.SeekOrigin]::Begin)
                $read = $stream.Read($buffer, 0, $buffer.Length)
                if ($read -gt 0) {
                    $hasher.AppendData($buffer, 0, $read)
                    $sampled += $read
                }
                $method = "length-first-last-1MiB-sha256"
            }
            $digest = "sha256:" + ([BitConverter]::ToString($hasher.GetHashAndReset())).Replace("-", "").ToLowerInvariant()
        }
        finally {
            $hasher.Dispose()
        }
        return [pscustomobject][ordered]@{ method = $method; digest = $digest; sampled_bytes = $sampled }
    }
    finally {
        $stream.Dispose()
    }
}

function Get-GlobalAndroidAVDSnapshot {
    $inventory = Invoke-ProcessText -Executable $script:androidAVDManagerBinary -Arguments @("list", "avd", "-c")
    $names = @($inventory.stdout -split '\r?\n' | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Sort-Object -Unique)
    $roots = @()
    foreach ($root in @(Get-GlobalAndroidAVDRoots)) {
        $entries = @()
        if (Test-Path -LiteralPath $root -PathType Container) {
            foreach ($item in @(Get-ChildItem -LiteralPath $root -Recurse -Force -ErrorAction Stop | Sort-Object FullName)) {
                $isReparsePoint = ([IO.FileAttributes]$item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
                $kind = if ($isReparsePoint) { "reparse-point" } elseif ($item.PSIsContainer) { "directory" } else { "file" }
                $identity = if ($kind -eq "file") { Get-BoundedAVDFileIdentity -Path $item.FullName } else { $null }
                $entries += [pscustomobject][ordered]@{
                    relative_path = Get-ContainedRelativePath -Root $root -Path $item.FullName
                    kind = $kind
                    attributes = [string]$item.Attributes
                    length = if ($kind -eq "file") { [int64]$item.Length } else { [int64]0 }
                    last_write_utc = $item.LastWriteTimeUtc.ToString("o")
                    content_identity = $identity
                }
            }
        }
        $roots += [pscustomobject][ordered]@{
            path = $root
            exists = [bool](Test-Path -LiteralPath $root -PathType Container)
            entries = $entries
        }
    }
    return [pscustomobject][ordered]@{ inventory_names = $names; roots = $roots }
}

function Test-AndroidADBDeviceEqual {
    param(
        [Parameter(Mandatory)]$Left,
        [Parameter(Mandatory)]$Right
    )
    return (
        [string]$Left.serial -eq [string]$Right.serial -and
        [string]$Left.state -eq [string]$Right.state -and
        [string]$Left.details -eq [string]$Right.details
    )
}

function Test-AndroidADBDeviceSnapshotsEqual {
    param(
        [AllowEmptyCollection()][Parameter(Mandatory)][object[]]$Left,
        [AllowEmptyCollection()][Parameter(Mandatory)][object[]]$Right
    )
    $leftDevices = @($Left)
    $rightDevices = @($Right)
    if ($leftDevices.Count -ne $rightDevices.Count) {
        return $false
    }
    for ($index = 0; $index -lt $leftDevices.Count; $index++) {
        if (-not (Test-AndroidADBDeviceEqual -Left $leftDevices[$index] -Right $rightDevices[$index])) {
            return $false
        }
    }
    return $true
}

function Test-AndroidLiveAmbientSnapshotsEqual {
    param(
        [Parameter(Mandatory)]$Before,
        [Parameter(Mandatory)]$After
    )
    $beforeDevices = @($Before.adb_devices)
    $afterDevices = @($After.adb_devices)
    $beforeProcesses = @($Before.emulator_processes)
    $afterProcesses = @($After.emulator_processes)
    return (
        (Test-AndroidADBDeviceSnapshotsEqual -Left $beforeDevices -Right $afterDevices) -and
        (Test-ExactProcessIdentitySetsEqual -Left $beforeProcesses -Right $afterProcesses) -and
        (Test-ExactProcessIdentitySetsEqual -Left @($Before.adb_processes) -Right @($After.adb_processes))
    )
}

function Test-ExactProcessIdentitySetsEqual {
    param(
        [Parameter(Mandatory)]$Left,
        [Parameter(Mandatory)]$Right
    )
    $leftSet = @($Left | Sort-Object pid)
    $rightSet = @($Right | Sort-Object pid)
    if ($leftSet.Count -ne $rightSet.Count) {
        return $false
    }
    for ($index = 0; $index -lt $leftSet.Count; $index++) {
        if (-not (Test-ExactProcessIdentityEqual -Left $leftSet[$index] -Right $rightSet[$index])) {
            return $false
        }
    }
    return $true
}

function Get-NewExactProcessIdentities {
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Before,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$After
    )
    $result = @()
    foreach ($candidate in @($After)) {
        $matches = @($Before | Where-Object {
            Test-ExactProcessIdentityEqual -Left $_ -Right $candidate
        })
        if ($matches.Count -eq 0) {
            $result += $candidate
        }
        elseif ($matches.Count -ne 1) {
            throw "preexisting process identity set contains a duplicate exact PID/path/start-token identity"
        }
    }
    return @($result | Sort-Object pid)
}

function Test-AndroidAmbientSnapshotsEqual {
    param(
        [Parameter(Mandatory)]$Before,
        [Parameter(Mandatory)]$After
    )
    return (
        (Test-AndroidLiveAmbientSnapshotsEqual -Before $Before -After $After) -and
        (($Before.global_avds | ConvertTo-Json -Compress -Depth 10) -eq ($After.global_avds | ConvertTo-Json -Compress -Depth 10))
    )
}

function Get-AndroidAmbientSnapshot {
    return [pscustomobject][ordered]@{
        adb_devices = @(Get-AndroidADBDeviceSnapshot)
        adb_processes = @(Get-AndroidADBProcessSnapshot)
        emulator_processes = @(Get-AndroidRuntimeProcessSnapshot)
        global_avds = Get-GlobalAndroidAVDSnapshot
    }
}

function Assert-AndroidAmbientBaselinePreserved {
    param(
        [Parameter(Mandatory)]$Before,
        [Parameter(Mandatory)]$After
    )
    foreach ($baselineDevice in @($Before.adb_devices)) {
        $matches = @($After.adb_devices | Where-Object { [string]$_.serial -eq [string]$baselineDevice.serial })
        Assert-Condition -Condition ($matches.Count -eq 1 -and (Test-AndroidADBDeviceEqual -Left $baselineDevice -Right $matches[0])) -Message "baseline ADB serial $($baselineDevice.serial) still exists unchanged"
    }
    foreach ($baselineProcess in @($Before.emulator_processes)) {
        $matches = @($After.emulator_processes | Where-Object { [int]$_.pid -eq [int]$baselineProcess.pid })
        Assert-Condition -Condition ($matches.Count -eq 1 -and (Test-ExactProcessIdentityEqual -Left $baselineProcess -Right $matches[0])) -Message "baseline Android process $($baselineProcess.pid) still exists with its exact path and start token"
    }
    foreach ($baselineProcess in @($Before.adb_processes)) {
        $matches = @($After.adb_processes | Where-Object { [int]$_.pid -eq [int]$baselineProcess.pid })
        Assert-Condition -Condition ($matches.Count -eq 1 -and (Test-ExactProcessIdentityEqual -Left $baselineProcess -Right $matches[0])) -Message "baseline ADB process $($baselineProcess.pid) still exists with its exact path and start token"
    }
    Assert-Condition -Condition (($Before.global_avds | ConvertTo-Json -Compress -Depth 10) -eq ($After.global_avds | ConvertTo-Json -Compress -Depth 10)) -Message "global inactive AVD inventory and bounded content identities remained byte/metadata equivalent"
}

function Test-CommittedAndroidProcessOwnership {
    param([Parameter(Mandatory)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return $false
    }
    try {
        $record = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        return (
            [int]$record.pid -gt 0 -and
            [string]$record.pid_file -eq "emulator.pid" -and
            -not [string]::IsNullOrWhiteSpace([string]$record.executable_path) -and
            -not [string]::IsNullOrWhiteSpace([string]$record.start_token) -and
            -not [string]::IsNullOrWhiteSpace([string]$record.runtime_id) -and
            -not [string]::IsNullOrWhiteSpace([string]$record.avd_name) -and
            -not [string]::IsNullOrWhiteSpace([string]$record.serial) -and
            [int]$record.console_port -gt 0 -and
            (Test-AndroidOwnershipResourceContract -Record $record)
        )
    }
    catch {
        return $false
    }
}

function Get-UnboundAndroidLaunchRemnants {
    if (-not $ManagedAndroid -or -not (Test-Path -LiteralPath $androidTargetRoot -PathType Container)) {
        return @()
    }
    $remnantFiles = @(Get-ChildItem -LiteralPath $androidTargetRoot -File -Recurse -Force -ErrorAction Stop | Where-Object {
        $_.Name -eq "world-emulator-launch.json" -or $_.Name -eq "emulator.pid"
    })
    $result = @()
    foreach ($directory in @($remnantFiles | ForEach-Object { $_.Directory.FullName } | Sort-Object -Unique)) {
        $ownershipPath = Join-Path $directory "world-emulator-process.json"
        if (Test-CommittedAndroidProcessOwnership -Path $ownershipPath) {
            continue
        }
        $paths = @($remnantFiles | Where-Object { $_.Directory.FullName -eq $directory } | ForEach-Object { $_.FullName } | Sort-Object)
        $result += [pscustomobject][ordered]@{
            state_directory = $directory
            remnant_paths = $paths
            expected_ownership_manifest = $ownershipPath
            ownership_manifest_present = [bool](Test-Path -LiteralPath $ownershipPath -PathType Leaf)
            action = "reported_not_targeted"
        }
    }
    return @($result)
}

function Get-OwnedAndroidStateRemnants {
    if (-not $ManagedAndroid -or -not (Test-Path -LiteralPath $androidTargetRoot -PathType Container)) {
        return @()
    }
    return @(Get-ChildItem -LiteralPath $androidTargetRoot -File -Recurse -Force -ErrorAction Stop | Where-Object {
        $_.Name -in @("world-emulator-launch.json", "world-emulator-process.json", "emulator.pid")
    } | ForEach-Object { $_.FullName } | Sort-Object -Unique)
}

function Get-OwnedAndroidRuntimeProcesses {
    if (-not $ManagedAndroid) {
        return @()
    }
    $records = @()
    if ($null -ne $script:androidProcessOwnership) {
        $records += $script:androidProcessOwnership
    }
    if (Test-Path -LiteralPath $androidTargetRoot -PathType Container) {
        foreach ($manifest in @(Get-ChildItem -LiteralPath $androidTargetRoot -Filter "world-emulator-process.json" -File -Recurse -ErrorAction Stop)) {
            $record = Get-Content -LiteralPath $manifest.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
            if (@($records | Where-Object { [int]$_.pid -eq [int]$record.pid -and [string]$_.start_token -eq [string]$record.start_token }).Count -eq 0) {
                $records += $record
            }
        }
    }
    $result = @()
    foreach ($record in $records) {
        if ([int]$record.pid -le 0 -or [string]::IsNullOrWhiteSpace([string]$record.executable_path) -or [string]::IsNullOrWhiteSpace([string]$record.start_token)) {
            throw "managed Android process ownership is incomplete"
        }
        $recordConsolePort = [int]$record.console_port
        if (
            [string]$record.runtime_id -ne [string]$record.avd_name -or
            [string]$record.runtime_id -ne "world-emulator-$recordConsolePort" -or
            [string]$record.serial -ne "emulator-$recordConsolePort" -or
            $recordConsolePort -lt [int]$script:androidBaseConsolePort -or
            $recordConsolePort -gt 5584 -or
            ($recordConsolePort % 2) -ne 0
        ) {
            throw "managed Android process ownership has a non-canonical E2E allocation"
        }
        $identity = Get-ExactProcessIdentity -ProcessID ([int]$record.pid)
        if ($null -eq $identity) {
            continue
        }
        $expectedIdentity = [pscustomobject]@{
            pid = [int]$record.pid
            executable_path = [string]$record.executable_path
            start_token = [string]$record.start_token
        }
        if (-not (Test-ExactProcessIdentityEqual -Left $identity -Right $expectedIdentity)) {
            # The recorded PID was reused; it is not E2E-owned and must not be touched.
            continue
        }
        $result += $identity
    }
    return @($result)
}

function Stop-OwnedAndroidRuntimeProcesses {
    for ($attempt = 0; $attempt -lt 3; $attempt++) {
        $owned = @(Get-OwnedAndroidRuntimeProcesses)
        if ($owned.Count -eq 0) {
            return
        }
        foreach ($process in $owned) {
            [void](Stop-ExactProcessByRetainedHandle -ExpectedIdentity $process -Description "E2E-owned managed Android emulator/QEMU" -AllowAbsent)
        }
        Start-Sleep -Milliseconds 500
    }
}

function Get-ManagedAndroidProcessOwnership {
    param(
        [Parameter(Mandatory)][string]$TargetID,
        [Parameter(Mandatory)][uint64]$Generation
    )
    $path = Join-Path $androidTargetRoot "$TargetID\generations\$Generation\world-emulator-process.json"
    [void](Resolve-RequiredPath -Path $path -Description "managed Android generation $Generation process ownership" -PathType Leaf)
    $record = Get-Content -LiteralPath $path -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    return [pscustomobject]@{ path = $path; record = $record }
}

function Start-WorldTargetADBProxy {
    param(
        [Parameter(Mandatory)][int]$Port,
        [Parameter(Mandatory)][string]$TargetID,
        [Parameter(Mandatory)][string]$RunID,
        [Parameter(Mandatory)][string]$PolicyReference
    )
    $stdoutPath = Join-Path $logsRoot "android-adb-proxy-$Port.stdout.txt"
    $stderrPath = Join-Path $logsRoot "android-adb-proxy-$Port.stderr.txt"
    $arguments = @($script:connectionArguments) + @(
        "adb", "-target", $TargetID, "-run", $RunID, "-policy", $PolicyReference,
        "-listen", "127.0.0.1:$Port", "-connections", "64"
    )
    $process = Start-LoggedProcess -Executable $worldTarget -Arguments $arguments -StandardOutputPath $stdoutPath -StandardErrorPath $stderrPath
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        if ($process.HasExited) {
            $exitCode = Wait-StartedProcessExitCode -Process $process -TimeoutSeconds 1 -FailureMessage "world-target ADB proxy exit was observable"
            $detail = if (Test-Path -LiteralPath $stderrPath) { Get-Content -LiteralPath $stderrPath -Raw } else { "" }
            throw "world-target ADB proxy exited during startup ($exitCode):`n$detail"
        }
        if (Test-Path -LiteralPath $stdoutPath) {
            $announcementJSON = Get-Content -LiteralPath $stdoutPath -Raw -ErrorAction Stop
            if (-not [string]::IsNullOrWhiteSpace($announcementJSON)) {
                try {
                    $announcement = $announcementJSON | ConvertFrom-Json -ErrorAction Stop
                    if ([string]$announcement.address -eq "127.0.0.1:$Port" -and [string]$announcement.target_id -eq $TargetID -and [string]$announcement.target_run_id -eq $RunID) {
                        return $process
                    }
                }
                catch {
                    # The redirected announcement may still be in the middle of a write.
                }
            }
        }
        Start-Sleep -Milliseconds 100
    }
    Stop-StartedProcess -Process $process
    $stdoutDetail = if (Test-Path -LiteralPath $stdoutPath) { Get-Content -LiteralPath $stdoutPath -Raw } else { "" }
    $stderrDetail = if (Test-Path -LiteralPath $stderrPath) { Get-Content -LiteralPath $stderrPath -Raw } else { "" }
    throw "world-target ADB proxy did not announce 127.0.0.1:$Port within 30 seconds:`nstdout:`n$stdoutDetail`nstderr:`n$stderrDetail"
}

function Get-WorldOpenArguments {
    param([string]$Timeout = $script:rpcTimeout)
    $args = @(
        "-state", (Join-Path $runRoot "state\control.db"),
        "-ledger-dir", (Join-Path $runRoot "ledger"),
        "-orchestration-state-dir", (Join-Path $runRoot "orchestration"),
        "-bundle-dir", (Join-Path $runRoot "bundles"),
        "-material-dir", (Join-Path $runRoot "published"),
        "-subject", "world-e2e-operator",
        "-deployment-profile", $profilePath,
        "-agent-driver", "docker",
        "-workspace-driver", "directory",
        "-agent-workspace-root", (Join-Path $runRoot "workspaces"),
        "-linux-target-driver", "docker",
        "-target-root", (Join-Path $runRoot "targets"),
        "-capture-driver", "ledger",
        "-capture-dir", (Join-Path $runRoot "captures"),
        "-max-transfer-bytes", ([string]$maxTransferBytes),
        "-timeout", $Timeout
    )
    if ($ManagedAndroid) {
        $args += @(Get-ManagedAndroidCommonArguments)
        $args += @(
            "-android-target-root", $androidTargetRoot,
            "-android-system-image-root", $androidSystemImageRoot,
            "-observer-driver", "process",
            "-observer-output-dir", $observerOutputRoot
        )
    }
    return $args
}

# Library-only product: there is no worldd / world-node process.
# Start-WorldDaemon is a historical name retained for call-site stability; it
# only prepares local state roots and Open CLI arguments. Each worldctl /
# world-target invocation embeds world.Open against that tree. Concurrent Open
# of the same state path fails closed on processlock.
function Start-WorldDaemon {
    param([int]$Port = 0)
    $null = $Port
    New-Directory -Path (Join-Path $runRoot "state")
    New-Directory -Path (Join-Path $runRoot "ledger")
    New-Directory -Path (Join-Path $runRoot "orchestration")
    New-Directory -Path (Join-Path $runRoot "bundles")
    New-Directory -Path (Join-Path $runRoot "published")
    New-Directory -Path (Join-Path $runRoot "workspaces")
    New-Directory -Path (Join-Path $runRoot "targets")
    New-Directory -Path (Join-Path $runRoot "captures")
    $script:connectionArguments = @(Get-WorldOpenArguments)
    return $null
}

function Stop-WorldDaemon {
    param([Diagnostics.Process]$Process)
    # No long-running control-plane process under library-only Open.
    if ($null -ne $Process) {
        Stop-StartedProcess -Process $Process
    }
}

function Get-CurrentGenerationExactly {
    param(
        [Parameter(Mandatory)]$Resource,
        [Parameter(Mandatory)][string]$Description
    )

    $currentGeneration = [uint64]$Resource.current_generation
    Assert-Condition -Condition ($currentGeneration -gt 0) -Message "$Description has a positive current generation"
    $matches = @($Resource.generations | Where-Object { [uint64]$_.generation -eq $currentGeneration })
    Assert-Condition -Condition ($matches.Count -eq 1) -Message "$Description contains exactly one current generation $currentGeneration"
    return $matches[0]
}

function Test-TargetGenerationRunsTerminalExactly {
    param(
        [Parameter(Mandatory)]$Target,
        [Parameter(Mandatory)][uint64]$Generation
    )

    $terminalStates = @("completed", "failed", "quarantined", "lost")
    $generationRuns = @($Target.runs | Where-Object { [uint64]$_.generation -eq $Generation })
    return @($generationRuns | Where-Object { [string]$_.state -notin $terminalStates }).Count -eq 0
}

function Destroy-ManagedAndroidTargetExactly {
    param(
        [Parameter(Mandatory)][string]$TargetID,
        [Parameter(Mandatory)][string]$PolicyReference,
        [Parameter(Mandatory)][string]$Reason
    )

    $target = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $TargetID)
    $generation = Get-CurrentGenerationExactly -Resource $target -Description "managed Android target $TargetID"
    if ([string]$generation.state -ne "destroyed") {
        if ($null -eq $script:androidTargetDestroyGeneration) {
            $script:androidTargetDestroyGeneration = [uint64]$target.current_generation
        }
        Assert-Condition -Condition ([uint64]$target.current_generation -eq [uint64]$script:androidTargetDestroyGeneration) -Message "managed Android destroy retained its exact target generation"
        $script:androidTargetDestroyRequestCount++
        $outcome = Invoke-WorldCtlJSON -Arguments @(
            "destroy", "-target", $TargetID, "-revision", ([string]$target.revision),
            "-reason", $Reason, "-policy", $PolicyReference
        )
        Assert-Condition -Condition (
            [string]$outcome.resource_id -eq $TargetID -and
            [string]$outcome.state -eq "destroyed" -and
            [uint64]$outcome.revision -gt 0
        ) -Message "managed Android destroy returned the exact terminal outcome"
        $target = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $TargetID)
        $generation = Get-CurrentGenerationExactly -Resource $target -Description "managed Android target $TargetID after destroy"
    }
    Assert-Condition -Condition ([string]$generation.state -eq "destroyed") -Message "managed Android target $TargetID reached destroyed state"
    $script:androidTargetDestroyConfirmed = $true
    return $target
}

function Stop-WorldDaemonForManagedAndroidFailureCleanup {
    if ($null -ne $script:daemonProcess) {
        try {
            Stop-StartedProcess -Process $script:daemonProcess
        }
        catch {
            if (-not $script:daemonProcess.HasExited) {
                throw
            }
            Add-FinalCleanupError -Message "failure cleanup lost daemon log-pump completion after the exact daemon had exited: $_"
        }
        if ($script:daemonProcess.HasExited) {
            $script:daemonProcess = $null
        }
    }
    Assert-Condition -Condition ($null -eq $script:daemonProcess) -Message "failure cleanup stopped the previous exact daemon before restart"
}

function Add-ManagedAndroidFailureDiagnosticFile {
    param(
        [Parameter(Mandatory)][string]$SourcePath,
        [Parameter(Mandatory)][string]$RelativePath,
        [Parameter(Mandatory)][string]$DiagnosticRoot,
        [switch]$CopyContent
    )

    $file = Get-Item -LiteralPath $SourcePath -Force -ErrorAction Stop
    Assert-Condition -Condition (-not $file.PSIsContainer) -Message "failure diagnostic source $SourcePath is a file"
    $hashLimit = 67108864
    $copyLimit = 8388608
    $digest = if ([int64]$file.Length -le $hashLimit) { Get-Sha256Reference -Path $file.FullName } else { $null }
    $copiedPath = $null
    if ($CopyContent -and [int64]$file.Length -le $copyLimit) {
        $copiedPath = Join-Path $DiagnosticRoot (Join-Path "files" $RelativePath.Replace('/', '\'))
        New-Directory -Path (Split-Path -Parent $copiedPath)
        Copy-Item -LiteralPath $file.FullName -Destination $copiedPath -Force -ErrorAction Stop
    }
    return [pscustomobject][ordered]@{
        source_path = [IO.Path]::GetFullPath($file.FullName)
        relative_path = $RelativePath.Replace('\', '/')
        size = [uint64]$file.Length
        last_write_utc = $file.LastWriteTimeUtc.ToString("o")
        digest = $digest
        digest_omitted_above_bytes = $(if ($null -eq $digest) { $hashLimit } else { $null })
        copied = ($null -ne $copiedPath)
        copied_path = $copiedPath
    }
}

function Save-FailedManagedAndroidDiagnostics {
    if ($script:androidFailureDiagnosticsCaptured) {
        return
    }

    Stop-WorldDaemonForManagedAndroidFailureCleanup
    $diagnosticRoot = Join-Path $runRoot "failure-cleanup-diagnostics"
    New-Directory -Path $diagnosticRoot
    $records = @()
    if (Test-Path -LiteralPath $androidTargetRoot -PathType Container) {
        foreach ($file in @(Get-ChildItem -LiteralPath $androidTargetRoot -File -Recurse -Force -ErrorAction Stop | Sort-Object FullName)) {
            $relative = "android-targets/" + (Get-ContainedRelativePath -Root $androidTargetRoot -Path $file.FullName)
            $copyContent = (
                $file.Extension -in @(".json", ".ini", ".log", ".txt") -or
                $file.Name -in @("emulator.pid", "emulator.stdout", "emulator.stderr")
            )
            $records += Add-ManagedAndroidFailureDiagnosticFile -SourcePath $file.FullName -RelativePath $relative -DiagnosticRoot $diagnosticRoot -CopyContent:$copyContent
            if ($file.Name -in @("world-emulator-launch.json", "world-emulator-process.json")) {
                $runtimeRecord = Get-Content -LiteralPath $file.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
                $consolePort = Get-OptionalObjectProperty -InputObject $runtimeRecord -Name "console_port"
                if ($null -ne $consolePort -and [int]$consolePort -gt 0) {
                    Register-OwnedAndroidConsolePort -ConsolePort ([int]$consolePort)
                }
            }
        }
        $allocatorPath = Join-Path $androidTargetRoot "allocations\android-emulator-allocations.json"
        if (Test-Path -LiteralPath $allocatorPath -PathType Leaf) {
            $allocatorSnapshot = Get-Content -LiteralPath $allocatorPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
            foreach ($allocation in @($allocatorSnapshot.allocations)) {
                $instanceNumber = Get-OptionalObjectProperty -InputObject $allocation.allocation -Name "InstanceNumber"
                if ($null -ne $instanceNumber -and [int]$instanceNumber -gt 0) {
                    Register-OwnedAndroidConsolePort -ConsolePort ([int]$instanceNumber)
                }
            }
        }
    }
    foreach ($rootRecord in @(
        [pscustomobject]@{ path = (Join-Path $runRoot "state"); prefix = "state" },
        [pscustomobject]@{ path = (Join-Path $runRoot "orchestration"); prefix = "orchestration" }
    )) {
        if (-not (Test-Path -LiteralPath $rootRecord.path -PathType Container)) {
            continue
        }
        foreach ($file in @(Get-ChildItem -LiteralPath $rootRecord.path -File -Recurse -Force -ErrorAction Stop | Sort-Object FullName)) {
            $relative = "$($rootRecord.prefix)/" + (Get-ContainedRelativePath -Root $rootRecord.path -Path $file.FullName)
            $records += Add-ManagedAndroidFailureDiagnosticFile -SourcePath $file.FullName -RelativePath $relative -DiagnosticRoot $diagnosticRoot -CopyContent
        }
    }
    $manifest = [ordered]@{
        captured_at = (Get-Date).ToUniversalTime().ToString("o")
        target_id_known_before_cleanup = $script:androidTargetID
        create_attempted = $script:androidCreateAttempted
        destroy_request_count_before_cleanup = $script:androidTargetDestroyRequestCount
        owned_console_ports = @($script:androidOwnedConsolePorts)
        files = @($records)
    }
    Write-Utf8NoBom -Path (Join-Path $diagnosticRoot "manifest.json") -Content ($manifest | ConvertTo-Json -Depth 8)
    $script:androidFailureDiagnosticsCaptured = $true
}

function Restart-WorldDaemonForManagedAndroidFailureCleanup {
    Stop-WorldDaemonForManagedAndroidFailureCleanup
    $cleanupPort = Get-FreeLoopbackPort
    $script:connectionArguments = @(Get-WorldOpenArguments -Timeout $script:rpcTimeout)
    $script:daemonProcess = Start-WorldDaemon
}

function Resolve-FailedManagedAndroidTargetID {
    Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace([string]$script:sessionID)) -Message "failure cleanup retained the research session ID"
    Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace([string]$script:leaseID)) -Message "failure cleanup retained the lease ID"
    $session = Invoke-WorldCtlJSON -Arguments @("get-session", "-session", $script:sessionID)
    $diagnosticRoot = Join-Path $runRoot "failure-cleanup-diagnostics"
    New-Directory -Path $diagnosticRoot
    Write-Utf8NoBom -Path (Join-Path $diagnosticRoot "recovered-session.json") -Content ($session | ConvertTo-Json -Depth 12)
    $targets = @(Get-OptionalObjectProperty -InputObject $session -Name "targets")
    $matches = @($targets | Where-Object {
        [string]$_.research_session_id -eq [string]$script:sessionID -and
        [string]$_.lease_id -eq [string]$script:leaseID -and
        [string]$_.template_reference -eq "android-visible" -and
        [string]$_.kind -eq "android_virtual_device"
    })
    Assert-Condition -Condition ($matches.Count -le 1) -Message "failure cleanup resolved at most one exact managed Android target from the isolated session"
    if (-not [string]::IsNullOrWhiteSpace([string]$script:androidTargetID)) {
        Assert-Condition -Condition ($matches.Count -eq 1 -and [string]$matches[0].target_id -eq [string]$script:androidTargetID) -Message "failure cleanup session matched the already-known managed Android target ID"
    }
    elseif ($matches.Count -eq 1) {
        $script:androidTargetID = [string]$matches[0].target_id
    }
    if ($matches.Count -eq 0) {
        return ""
    }
    $target = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $script:androidTargetID)
    Write-Utf8NoBom -Path (Join-Path $diagnosticRoot "recovered-target.json") -Content ($target | ConvertTo-Json -Depth 12)
    return [string]$script:androidTargetID
}

function Assert-ManagedAndroidPhysicalCleanupExactly {
    param(
        [AllowEmptyString()][string]$TargetID = "",
        [uint64]$Generation = 0
    )

    $allocatorFact = Get-AndroidAllocatorCleanupFact -TargetID $TargetID
    $ownedAVDRoot = Join-Path $androidTargetRoot "avds"
    $ownedAVDEntries = @(
        if (Test-Path -LiteralPath $ownedAVDRoot -PathType Container) {
            Get-ChildItem -LiteralPath $ownedAVDRoot -Force -ErrorAction Stop
        }
    )
    $generationAbsent = $true
    if (-not [string]::IsNullOrWhiteSpace($TargetID) -and $Generation -gt 0) {
        $generationAbsent = -not (Test-Path -LiteralPath (Join-Path $androidTargetRoot "$TargetID\generations\$Generation"))
    }
    Assert-Condition -Condition ([bool]$allocatorFact.empty -and @($allocatorFact.target_allocations_remaining).Count -eq 0) -Message "managed Android physical cleanup emptied the isolated durable allocator"
    Assert-Condition -Condition $generationAbsent -Message "managed Android physical cleanup removed the exact generation directory"
    Assert-Condition -Condition ($ownedAVDEntries.Count -eq 0) -Message "managed Android physical cleanup removed every isolated owned AVD entry"
    Assert-Condition -Condition (@(Get-OwnedAndroidStateRemnants).Count -eq 0) -Message "managed Android physical cleanup removed every owned launch/ownership/PID remnant"
    Assert-Condition -Condition (@(Get-UnboundAndroidLaunchRemnants).Count -eq 0) -Message "managed Android physical cleanup removed every unbound launch remnant"
    Assert-Condition -Condition (@(Get-OwnedAndroidRuntimeProcesses).Count -eq 0) -Message "managed Android physical cleanup left no exact owned runtime process"
    $adbDevices = @(Get-AndroidADBDeviceSnapshot)
    foreach ($consolePort in @($script:androidOwnedConsolePorts)) {
        Assert-Condition -Condition (Test-LoopbackPortPairAvailable -ConsolePort ([int]$consolePort)) -Message "managed Android physical cleanup released console/ADB ports $consolePort/$([int]$consolePort + 1)"
        Assert-Condition -Condition (@($adbDevices | Where-Object { [string]$_.serial -eq "emulator-$consolePort" }).Count -eq 0) -Message "managed Android physical cleanup removed emulator-$consolePort from the real ADB inventory"
    }
    return $allocatorFact
}

function Recover-SubmittedManagedAndroidDestroyExactly {
    param([Parameter(Mandatory)][string]$TargetID)

    Assert-Condition -Condition ($null -ne $script:androidTargetDestroyGeneration) -Message "destroy recovery retained the exact submitted target generation"
    for ($attempt = 1; $attempt -le 2; $attempt++) {
        Restart-WorldDaemonForManagedAndroidFailureCleanup
        $target = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $TargetID)
        Assert-Condition -Condition ([uint64]$target.current_generation -eq [uint64]$script:androidTargetDestroyGeneration) -Message "destroy recovery retained the submitted managed Android generation"
        $generation = Get-CurrentGenerationExactly -Resource $target -Description "destroy-recovery managed Android target $TargetID"
        if ([string]$generation.state -eq "destroyed") {
            $script:androidTargetDestroyConfirmed = $true
            return $target
        }
    }
    return $target
}

function Remove-FailedManagedAndroidTargetExactly {
    param(
        [AllowEmptyString()][string]$TargetID = "",
        [Parameter(Mandatory)][string]$PolicyReference
    )

    Assert-Condition -Condition ($true) -Message "library-only failure cleanup does not use bearer tokens"
    Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace([string]$script:rpcTimeout)) -Message "failure cleanup retained the RPC timeout"
    $target = $null
    if ($script:androidTargetDestroyRequestCount -gt 0) {
        Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace($TargetID)) -Message "submitted destroy cleanup retained the exact managed Android target ID"
        # A run may have linearized after the destroy reservation. The first
        # restart finalizes that run and the second consumes the same durable
        # reservation; neither restart submits a new mutation key.
        $target = Recover-SubmittedManagedAndroidDestroyExactly -TargetID $TargetID
        [void](Resolve-FailedManagedAndroidTargetID)
    }
    else {
        # Recovery runs before RPC admission, so one restart finalizes any
        # interrupted run before the sole destroy request is submitted.
        Restart-WorldDaemonForManagedAndroidFailureCleanup
        $TargetID = Resolve-FailedManagedAndroidTargetID
        if ([string]::IsNullOrWhiteSpace($TargetID)) {
            [void](Assert-ManagedAndroidPhysicalCleanupExactly)
            $script:androidTargetCleanupCompleted = $true
            Assert-Condition -Condition ($script:androidTargetDestroyRequestCount -eq 0) -Message "failed create with no durable target required no destroy request"
            return
        }
        $target = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $TargetID)
    }

    $generation = Get-CurrentGenerationExactly -Resource $target -Description "failure-cleanup managed Android target $TargetID"
    $generationNumber = [uint64]$target.current_generation
    $generationState = [string]$generation.state
    if ($generationState -eq "destroyed") {
        $script:androidTargetDestroyConfirmed = $true
    }
    elseif ($generationState -in @("failed", "lost")) {
        # Startup reconciliation already removed the physical resource for
        # these terminal provisioning/recovery failures. DestroyTarget does
        # not admit either logical state.
    }
    elseif ($generationState -eq "quarantined") {
        throw "managed Android failure cleanup refuses to destroy quarantined target $TargetID"
    }
    elseif ($script:androidTargetDestroyRequestCount -gt 0) {
        throw "managed Android target $TargetID remained in $generationState after two read-only recovery restarts; refusing to replay its destroy request"
    }
    elseif ($generationState -in @("ready", "resettable")) {
        Assert-Condition -Condition (Test-TargetGenerationRunsTerminalExactly -Target $target -Generation $generationNumber) -Message "failure cleanup recovered only terminal runs before managed Android destroy"
        try {
            $target = Destroy-ManagedAndroidTargetExactly -TargetID $TargetID -PolicyReference $PolicyReference -Reason "failed managed Android E2E exact cleanup"
            $generation = Get-CurrentGenerationExactly -Resource $target -Description "destroyed failure-cleanup managed Android target $TargetID"
            $generationState = [string]$generation.state
        }
        catch {
            $destroyFailure = $_
            try {
                $target = Recover-SubmittedManagedAndroidDestroyExactly -TargetID $TargetID
                $generation = Get-CurrentGenerationExactly -Resource $target -Description "recovered failure-cleanup managed Android target $TargetID"
                $generationState = [string]$generation.state
            }
            catch {
                throw "managed Android destroy failed ($destroyFailure); bounded recovery also failed: $_"
            }
            if ($generationState -ne "destroyed") {
                throw "managed Android destroy failed ($destroyFailure) and bounded recovery left target $TargetID in $generationState"
            }
        }
    }
    else {
        throw "managed Android failure cleanup cannot safely handle target $TargetID in $generationState"
    }

    [void](Assert-ManagedAndroidPhysicalCleanupExactly -TargetID $TargetID -Generation $generationNumber)
    $script:androidTargetCleanupCompleted = $true
    Assert-Condition -Condition ($script:androidTargetDestroyRequestCount -le 1) -Message "failure cleanup submitted at most one managed Android destroy request"
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
    $nativePath = $nativeSpecimen
    $nativeFile = Get-Item -LiteralPath $nativePath
    $fixtureFile = Get-Item -LiteralPath $fixturePath
    $agentResources = [ordered]@{
        cpu_milli = $linuxCgroupContract.cpu_milli
        memory_bytes = $linuxCgroupContract.memory_bytes
        storage_bytes = 16777216
        capture_bytes = $captureByteLimit
        inodes = 1024
        pids = $linuxCgroupContract.pids
    }
    $targetResources = [ordered]@{
        cpu_milli = $linuxCgroupContract.cpu_milli
        memory_bytes = $linuxCgroupContract.memory_bytes
        storage_bytes = 16777216
        capture_bytes = 0
        inodes = 0
        pids = $linuxCgroupContract.pids
    }
    $profile = [ordered]@{
        version = 3
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
    if ($ManagedAndroid) {
        $apkFile = Get-Item -LiteralPath $AndroidSpecimenAPK
        $androidTargetResources = [ordered]@{
            cpu_milli = 2000
            memory_bytes = 6442450944
            storage_bytes = 1073741824
            capture_bytes = 0
            inodes = 0
            pids = 0
        }
        $profile.material.entries += [ordered]@{
            reference = "specimen/android-apk"
            security_scope = "e2e-scope"
            source_path = $androidAPKSourcePath
            digest = Get-Sha256Reference -Path $AndroidSpecimenAPK
            size = $apkFile.Length
            logical_path = "inputs/android/world-specimen.apk"
            mode = 292
            role = "specimen"
            sensitivity = "restricted"
        }
        $profile.material.selections[0].occurrences += "specimen/android-apk"
        $profile.targets += [ordered]@{
            reference = "android-visible"
            policy = $PolicyReference
            template = [ordered]@{
                name = "android-visible"
                kind = "android_virtual_device"
                driver = "android-emulator"
                system_image_digest = $AndroidSystemImageDigest
                system_image_package = $AndroidSystemImagePackage
                isolation_profile = "instrumented-android"
                baseline_state = "clean-boot"
                require_hardware_acceleration = $true
                headless = $true
                rooted = $true
                debuggable = $true
                boot_timeout = $managedAndroidBootTimeout
                guest_memory_bytes = 2147483648
            }
            resources = $androidTargetResources
        }
        Assert-Condition -Condition (
            $null -ne $androidLogcatObserverConfiguration -and
            $androidLogcatObserverConfigurationDigest -match '^sha256:[0-9a-f]{64}$'
        ) -Message "managed Android observer identity is complete before deployment profile publication"
        $observer = $androidLogcatObserverConfiguration
        $profile["observers"] = @(
            [ordered]@{
                reference = $observer.reference
                adapter = $observer.adapter
                version = $observer.version
                configuration_digest = $androidLogcatObserverConfigurationDigest
                signal_family = $observer.signal_family
                placement = $observer.placement
                coverage_level = $observer.coverage_level
                runtime_binding = $observer.runtime_binding
                required = $true
                program = $observer.program
                args = @($observer.args)
                version_args = @($observer.version_args)
                readiness = [ordered]@{
                    program = $observer.readiness_program
                    args = @($observer.readiness_args)
                    interval = $observer.readiness_interval
                }
                maximum_bytes = $observer.maximum_bytes
            }
        )
        $profile.runs += [ordered]@{
            target_references = @("android-visible")
            specimen_occurrence_refs = @("specimen/android-apk")
            collector_references = @($androidLogcatObserverReference)
            required_coverage = @("target.lifecycle", "android.logcat")
            material = @(
                [ordered]@{ reference = "specimen/android-apk"; logical_path = "specimens/world-specimen.apk"; mode = 292 }
            )
            maximum_duration = "5m"
        }
        # Generation 2 deliberately uses the same immutable observer identity.
        # Its authority-derived runtime binding must select the independently
        # allocated replacement serial, and daemon-loss recovery must reconcile
        # the crash-contained collector plus its committed output prefix.
        $profile.runs += [ordered]@{
            target_references = @("android-visible")
            specimen_occurrence_refs = @("specimen/android-apk")
            fixture_refs = @("fixture/payload")
            collector_references = @($androidLogcatObserverReference)
            required_coverage = @("target.lifecycle", "android.logcat")
            material = @(
                [ordered]@{ reference = "specimen/android-apk"; logical_path = "specimens/world-specimen.apk"; mode = 292 },
                [ordered]@{ reference = "fixture/payload"; logical_path = "fixtures/payload.txt"; mode = 292 }
            )
            maximum_duration = "5m"
        }
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
    if ($ManagedAndroid) {
        $rendered = Replace-ExactlyOnce -Source $rendered -Pattern '(?m)^    maxConcurrent: 1\r?$' -Replacement '    maxConcurrent: 2' -Description "Android E2E target concurrency"
        # The aggregate contract is the 1 CPU/256 MiB agent plus two instances
        # of the largest 2 CPU/6 GiB target template.
        $rendered = Replace-ExactlyOnce -Source $rendered -Pattern '(?ms)(^  resources:\r?\n    aggregateLimits:\r?\n)      cpu: "2"\r?\n      memory: 512Mi' -Replacement ('${1}      cpu: "5"' + "`n" + '      memory: 12544Mi') -Description "Android E2E aggregate resources"
        $rendered = Replace-ExactlyOnce -Source $rendered -Pattern '(?m)^      linux-container: \[target[.]lifecycle\]\r?$' -Replacement "      linux-container: [target.lifecycle]`n      android-virtual-device: [target.lifecycle]" -Description "Android E2E intrinsic coverage"
        $rendered = Replace-ExactlyOnce -Source $rendered -Pattern '(?ms)^      androidSystem:\r?\n        adapters: \[perfetto, logcat, dumpsys\]\r?\n        placement: guest\r?\n        failRunOnCoverageLoss: false' -Replacement ("      androidSystem:`n        adapters: [logcat]`n        placement: guest`n        failRunOnCoverageLoss: true") -Description "qualified Android E2E system observation coverage"

        $newline = if ($rendered.Contains("`r`n")) { "`r`n" } else { "`n" }
        $androidTemplate = @(
            "      - name: android-visible",
            "        kind: android-virtual-device",
            "        runtime:",
            "          driver: android-emulator",
            "          systemImageDigest: $AndroidSystemImageDigest",
            "          isolationProfile: instrumented-android",
            "          baselineState: clean-boot",
            "          requireHardwareAcceleration: true",
            "          headless: true",
            "          rooted: true",
            "          debuggable: true",
            "          bootTimeout: $managedAndroidBootTimeout",
            "          guestMemory: 2Gi",
            "        material:",
            "          writableState: guest-data-partition",
            "        interaction:",
            "          commandAuthority: arbitrary-inside-assigned-device",
            "          adb: scoped-gateway",
            "          deviceScopedADBServices: arbitrary",
            "          fileTransfer: adb-sync-and-scoped-stream",
            "          deniedInfrastructureAuthority:",
            "            - host-adb-server-control",
            "            - other-serials",
            "            - raw-usb",
            "            - host-exec",
            "        resources:",
            "          limits:",
            '            cpu: "2"',
            "            memory: 6Gi",
            "            writableState: 1Gi",
            "        reset:",
            "          afterEveryRun: true",
            "          mode: $($script:androidResetMode)-new-target-generation",
            ""
        ) -join $newline
        $topLevelResources = [regex]::Matches($rendered, '(?m)^  resources:\r?$')
        Assert-Condition -Condition ($topLevelResources.Count -eq 1) -Message "Android E2E policy has exactly one top-level resources insertion point"
        $rendered = $rendered.Insert($topLevelResources[0].Index, $androidTemplate)
    }
    Write-Utf8NoBom -Path $policyPath -Content $rendered
}

if ($ManagedAndroid) {
    Assert-Condition -Condition ($env:OS -eq "Windows_NT") -Message "managed Android real qualification requires Windows"
    if ([string]::IsNullOrWhiteSpace($AndroidSDKRoot)) {
        $AndroidSDKRoot = $androidDefaultSDKRoot
    }
    if ([string]::IsNullOrWhiteSpace($AndroidSpecimenAPK)) {
        $AndroidSpecimenAPK = $androidDefaultSpecimenAPK
    }
    $AndroidSDKRoot = Resolve-RequiredPath -Path $AndroidSDKRoot -Description "Android SDK root" -PathType Container
    $AndroidSpecimenAPK = Resolve-RequiredPath -Path $AndroidSpecimenAPK -Description "signed Android specimen APK" -PathType Leaf
    Assert-Condition -Condition ($AndroidSystemImagePackage -match '^system-images;android-[0-9]+;[A-Za-z0-9_]+;(x86_64|arm64-v8a)$') -Message "Android system-image package has a normalized SDK package identity"
    Assert-Condition -Condition ($AndroidSystemImageDigest -match '^sha256:[0-9a-f]{64}$') -Message "Android system-image digest is canonical lowercase SHA-256"
    foreach ($version in @($AndroidBackendVersion, $AndroidRuntimeVersion)) {
        Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace($version) -and $version -eq $version.Trim()) -Message "Android backend/runtime versions are exact non-blank values"
    }

    $androidImageDirectory = $AndroidSDKRoot
    foreach ($segment in $AndroidSystemImagePackage.Split(';')) {
        $androidImageDirectory = Join-Path $androidImageDirectory $segment
    }
    [void](Resolve-RequiredPath -Path $androidImageDirectory -Description "installed Android system image" -PathType Container)
    $androidADBBinary = Resolve-RequiredPath -Path (Join-Path $AndroidSDKRoot "platform-tools\adb.exe") -Description "Android Debug Bridge" -PathType Leaf
    $androidEmulatorBinary = Resolve-RequiredPath -Path (Join-Path $AndroidSDKRoot "emulator\emulator.exe") -Description "Android Emulator" -PathType Leaf
    $androidSDKManagerBinary = Resolve-RequiredPath -Path (Join-Path $AndroidSDKRoot "cmdline-tools\latest\bin\sdkmanager.bat") -Description "Android SDK manager" -PathType Leaf
    $androidAVDManagerBinary = Resolve-RequiredPath -Path (Join-Path $AndroidSDKRoot "cmdline-tools\latest\bin\avdmanager.bat") -Description "Android AVD manager" -PathType Leaf
    $apkSignerCandidates = @(Get-ChildItem -LiteralPath (Join-Path $AndroidSDKRoot "build-tools") -Directory -ErrorAction SilentlyContinue | ForEach-Object {
        Join-Path $_.FullName "apksigner.bat"
    } | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Sort-Object -Descending)
    Assert-Condition -Condition ($apkSignerCandidates.Count -gt 0) -Message "Android SDK exposes an apksigner build tool"
    $androidAPKSignerBinary = [IO.Path]::GetFullPath([string]$apkSignerCandidates[0])

    $androidAPKSourcePath = Get-ContainedRelativePath -Root $sourceRoot -Path $AndroidSpecimenAPK
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
        $sourceIdentityBefore = Get-RepositorySourceIdentity -ManifestPath (Join-Path $runRoot "source-manifest.before.json")
        if ($ManagedAndroid) {
            $androidADBVersionResult = Invoke-ProcessText -Executable $androidADBBinary -Arguments @("version")
            $androidADBVersionText = ($androidADBVersionResult.stdout + "`n" + $androidADBVersionResult.stderr).Trim()
            $androidADBPlatformVersionMatches = @([regex]::Matches($androidADBVersionText, '(?m)^Version ([0-9]+[.][0-9]+[.][0-9]+-[0-9]+)\r?$'))
            Assert-Condition -Condition ($androidADBPlatformVersionMatches.Count -eq 1) -Message "ADB reports one exact platform-tools version"
            $androidLogcatObserverVersion = [string]$androidADBPlatformVersionMatches[0].Groups[1].Value
            Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace($androidLogcatObserverVersion)) -Message "ADB observer version is non-blank"
            Write-Utf8NoBom -Path (Join-Path $logsRoot "android-adb-version.txt") -Content $androidADBVersionText
        }
        $dockerAmbientBefore = @(Get-DockerContainerSnapshot)
        foreach ($ambientContainer in $dockerAmbientBefore) {
            $dockerAmbientIDs[[string]$ambientContainer.id] = $true
        }
        $dockerImageIDsBefore = @(Get-AllDockerImageIDs)
        $imageTagBefore = Get-DockerImageTagState -Tag $ImageTag
        Assert-Condition -Condition (-not [bool]$imageTagBefore.exists) -Message "refusing to overwrite preexisting Docker tag $ImageTag; choose an unused explicit tag or omit -ImageTag for a per-run tag"

        if ($ManagedAndroid) {
            # This snapshot precedes capability probing and target creation, so
            # every later Android runtime is unambiguously test-created.
            $androidADBProcessesBeforeTest = @(Get-AndroidADBProcessSnapshot)
            Initialize-AndroidADBServerObservation
            $androidAmbientBefore = Get-AndroidAmbientSnapshot
            $androidAPKSignature = Invoke-ProcessText -Executable $androidAPKSignerBinary -Arguments @("verify", "--verbose", "--print-certs", $AndroidSpecimenAPK)
            $androidAPKSignatureText = ($androidAPKSignature.stdout + "`n" + $androidAPKSignature.stderr).Trim()
            $androidAPKSignatureLines = @($androidAPKSignatureText -split '\r?\n')
            Assert-Condition -Condition ($androidAPKSignatureLines -contains "Verifies") -Message "Android specimen APK signature verifies"
            Assert-Condition -Condition ($androidAPKSignatureLines -contains "Verified using v2 scheme (APK Signature Scheme v2): true") -Message "Android specimen APK has an APK Signature Scheme v2 signature"
            Assert-Condition -Condition ($androidAPKSignatureLines -contains "Number of signers: 1") -Message "Android specimen APK has exactly one signer"
            $androidSignerLines = @($androidAPKSignatureLines | Where-Object { $_.StartsWith("Signer #1 certificate DN: ", [StringComparison]::Ordinal) })
            Assert-Condition -Condition ($androidSignerLines.Count -eq 1) -Message "Android specimen APK exposes one signer identity"
            $androidAPKSignerDN = $androidSignerLines[0].Substring("Signer #1 certificate DN: ".Length).Trim()
            Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace($androidAPKSignerDN)) -Message "Android specimen APK signer identity is non-blank"
            $androidAPKSignatureLog = Join-Path $logsRoot "android-apksigner.txt"
            Write-Utf8NoBom -Path $androidAPKSignatureLog -Content $androidAPKSignatureText
            $androidAPKSigned = $true
        }

        $env:GOOS = "linux"
        $env:GOARCH = "amd64"
        [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", (Join-Path $linuxBuildRoot "world-guest"), "./cmd/world-guest"))
        [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", (Join-Path $linuxBuildRoot "world-idle"), "./cmd/world-idle"))
        [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", $nativeSpecimen, "./testdata/e2e/native-specimen"))
        [void](Invoke-ProcessText -Executable "go" -Arguments @("test", "-c", "-o", $processLockTest, "./internal/processlock"))
        $processLockTestDigest = Get-Sha256Reference -Path $processLockTest
        Remove-Item Env:GOOS
        Remove-Item Env:GOARCH

        foreach ($build in @(
            @($worldctl, "./cmd/worldctl"),
            @($worldTarget, "./cmd/world-target"),
            @($worldCapabilities, "./cmd/world-capabilities")
        )) {
            [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", $build[0], $build[1]))
        }
        if ($ManagedAndroid) {
            [void](Invoke-ProcessText -Executable "go" -Arguments @("build", "-trimpath", "-o", $windowsRuntimeVerifier, "./testdata/e2e/windows-runtime-verifier"))
        }

        Assert-Condition -Condition (-not $dockerTestRunLabelName.StartsWith("world.", [StringComparison]::Ordinal)) -Message "E2E ownership label stays outside the reserved world identity namespace"
        $dockerBuildStarted = $true
        [void](Invoke-ProcessText -Executable "docker" -Arguments @("build", "--pull=false", "--label", "$dockerTestRunLabelName=$dockerTestRunLabelValue", "-t", $ImageTag, "-f", "testdata/e2e/docker/Dockerfile", "."))
        $processLockQualification = Invoke-ProcessText -Executable "docker" -Arguments @(
            "run", "--rm", "--cidfile", $processLockContainerIDPath, "--entrypoint", "/processlock.test",
            "--mount", "type=bind,source=$processLockTest,target=/processlock.test,readonly",
            "--tmpfs", "/tmp:rw,nosuid,nodev,size=16m",
            $ImageTag, "-test.v"
        )
        Register-ProcessLockDockerContainer
        Assert-Condition -Condition (@(Get-AllDockerContainerIDs) -notcontains [string]$dockerTrackedContainers[-1].id) -Message "self-removing process-lock qualification container is absent"
        $processLockOutputLog = Join-Path $logsRoot "linux-processlock.stdout.txt"
        Write-Utf8NoBom -Path $processLockOutputLog -Content $processLockQualification.stdout
        $processLockReplacementDenied = ($processLockQualification.exit_code -eq 0 -and (Test-ContainsExactLine -Text $processLockQualification.stdout -Expected "PASS"))
        Assert-Condition -Condition $processLockReplacementDenied -Message "Linux process-lock qualification completed with a real passing test result"
        $inspection = @(Invoke-DockerInspectRecords -Arguments @("image", "inspect", $ImageTag))
        Assert-Condition -Condition ($inspection.Count -eq 1) -Message "Docker image inspection returned exactly one image"
        $testImageID = [string]$inspection[0].Id
        $testImageIDWasPreexisting = $dockerImageIDsBefore -contains $testImageID
        Assert-Condition -Condition (-not $testImageIDWasPreexisting) -Message "unique per-run Docker image label produced a new final image ID"
        $lastSlash = $ImageTag.LastIndexOf('/')
        $lastColon = $ImageTag.LastIndexOf(':')
        $repository = if ($lastColon -gt $lastSlash) { $ImageTag.Substring(0, $lastColon) } else { $ImageTag }
        $pinnedImage = @($inspection[0].RepoDigests | Where-Object { $_ -like "$repository@sha256:*" } | Select-Object -First 1)
        if ($pinnedImage.Count -eq 0) {
            throw "Docker did not expose a repository digest for $ImageTag; exact pinned startup cannot be proven"
        }
        $pinnedImage = [string]$pinnedImage[0]

        if ($ManagedAndroid) {
            # Baseline replacement reset reserves generation 2 before releasing
            # generation 1, so two port pairs must be usable simultaneously.
            $androidBaseConsolePort = Get-FreeManagedAndroidConsolePort -RequiredPairCount 2
            $androidRuntimeName = "world-emulator-$androidBaseConsolePort"
            $androidLogcatObserverConfiguration = New-AndroidLogcatObserverConfiguration
        }
        Write-E2EPolicy -PinnedImage $pinnedImage
        $capabilityArguments = @("-timeout", $(if ($ManagedAndroid) { "2m" } else { "30s" }), "-linux-target-driver", "docker", "-capture-driver", "ledger", "-policy", $policyPath)
        if ($ManagedAndroid) {
            $observer = $androidLogcatObserverConfiguration
            $capabilityArguments += @(Get-ManagedAndroidCommonArguments)
            $capabilityArguments += @(
                "-android-system-image-digest", $AndroidSystemImageDigest,
                "-android-system-image-package", $AndroidSystemImagePackage,
                "-android-isolation-profile", "instrumented-android",
                "-android-baseline-state", "clean-boot",
                "-android-boot-timeout", $managedAndroidBootTimeout,
                "-observer-driver", "process",
                "-observer-reference", $observer.reference,
                "-observer-adapter", $observer.adapter,
                "-observer-version", $observer.version,
                "-observer-signal-family", $observer.signal_family,
                "-observer-placement", $observer.placement,
                "-observer-coverage-level", $observer.coverage_level,
                "-observer-runtime-binding", $observer.runtime_binding,
                "-observer-program", $observer.program,
                "-observer-readiness-program", $observer.readiness_program,
                "-observer-readiness-interval", $observer.readiness_interval
            )
            $capabilityArguments += @(ConvertTo-RepeatedFlagArguments -Flag "-observer-arg" -Values $observer.args)
            $capabilityArguments += @(ConvertTo-RepeatedFlagArguments -Flag "-observer-version-arg" -Values $observer.version_args)
            $capabilityArguments += @(ConvertTo-RepeatedFlagArguments -Flag "-observer-readiness-arg" -Values $observer.readiness_args)
        }
        $capabilityReport = Invoke-JsonTool -Executable $worldCapabilities -Arguments $capabilityArguments
        $policyReference = [string]$capabilityReport.effective_policy.reference
        $policyDigest = [string]$capabilityReport.effective_policy.digest
        $capabilityDigest = [string]$capabilityReport.effective_policy.capability_fingerprint_digest
        Assert-Condition -Condition ($policyReference -eq "e2e-directory-copy@1") -Message "capability probe compiled the intended immutable policy reference"
        Assert-Condition -Condition ($capabilityDigest -eq $capabilityReport.combined.digest) -Message "effective policy is bound to the complete probed capability fingerprint"
        if ($ManagedAndroid) {
            $processObserverReport = Get-OptionalObjectProperty -InputObject $capabilityReport -Name "process_observer"
            Assert-Condition -Condition ($null -ne $processObserverReport) -Message "capability probe returned the exact process observer report"
            $androidLogcatObserverConfigurationDigest = [string]$processObserverReport.configuration_digest
            $observerCapability = $processObserverReport.capability.capabilities.PSObject.Properties["observer.logcat"].Value
            $combinedLogcatCapability = $capabilityReport.combined.capabilities.PSObject.Properties["collector.adapter.logcat"].Value
            Assert-Condition -Condition (
                [string]$processObserverReport.reference -eq [string]$observer.reference -and
                [string]$processObserverReport.adapter -eq [string]$observer.adapter -and
                [string]$processObserverReport.version -eq [string]$observer.version -and
                [string]$processObserverReport.runtime_binding -eq [string]$observer.runtime_binding -and
                $androidLogcatObserverConfigurationDigest -match '^sha256:[0-9a-f]{64}$' -and
                [string]$observerCapability.status -eq "supported" -and
                [string]$observerCapability.constraints.placement -eq [string]$observer.placement -and
                [string]$observerCapability.constraints.coverage -eq [string]$observer.coverage_level -and
                [string]$observerCapability.constraints.version -eq [string]$observer.version -and
                [string]$observerCapability.constraints.runtime_binding -eq [string]$observer.runtime_binding -and
                [string]$observerCapability.constraints.configuration_digest -eq $androidLogcatObserverConfigurationDigest -and
                [string]$combinedLogcatCapability.status -eq "supported"
            ) -Message "capability probe bound the real logcat executable and exact adapter configuration"
            $androidPhysical = Get-OptionalObjectProperty -InputObject $capabilityReport -Name "android_target_physical_policy"
            Assert-Condition -Condition ($null -ne $androidPhysical) -Message "capability probe returned managed Android physical-policy facts"
            Assert-Condition -Condition (
                [string]$androidPhysical.resources.cpu_milli.support -eq "enforced" -and
                [string]$androidPhysical.resources.memory_bytes.support -eq "enforced" -and
                [string]$androidPhysical.resources.writable_state_bytes.support -eq "enforced" -and
                [bool]$androidPhysical.writable_state_enforced
            ) -Message "managed Android capability probe proved host CPU/memory and guest /data enforcement"
            Assert-Condition -Condition (
                [int64]$androidPhysical.android.guest_memory_bytes -eq 2147483648 -and
                [string]$androidPhysical.android.hardware_acceleration_support -eq "enforced" -and
                [bool]$androidPhysical.android.hardware_acceleration -and
                [bool]$androidPhysical.android.headless -and
                [bool]$androidPhysical.android.rooted -and
                [bool]$androidPhysical.android.debuggable
            ) -Message "managed Android capability probe proved the exact guest and runtime contract"
            $androidResourceContract = [ordered]@{
                cpu_milli = 2000
                host_job_memory_bytes = 6442450944
                guest_memory_bytes = 2147483648
                data_partition_bytes = 1073741824
                cpu_support = [string]$androidPhysical.resources.cpu_milli.support
                host_memory_support = [string]$androidPhysical.resources.memory_bytes.support
                data_partition_support = [string]$androidPhysical.resources.writable_state_bytes.support
                hardware_acceleration_support = [string]$androidPhysical.android.hardware_acceleration_support
            }
        }
        Write-DeploymentProfile -PinnedImage $pinnedImage -PolicyReference $policyReference

        $bearerToken = "world-e2e-" + [guid]::NewGuid().ToString("N")
        $env:WORLD_POLICY_REFERENCE = $policyDigest
        $rpcTimeout = if ($ManagedAndroid) { $managedAndroidOperationTimeout } else { "60s" }
        $firstPort = Get-FreeLoopbackPort
        $connectionArguments = @(Get-WorldOpenArguments -Timeout $rpcTimeout)
        $daemonProcess = Start-WorldDaemon

        $acquired = Invoke-WorldCtlJSON -Arguments @(
            "acquire", "-frozen-selection", "selection:agent-e2e", "-cache-scope", "e2e-scope",
            "-policy", $policyDigest, "-capabilities", $capabilityDigest, "-ttl", "1h"
        )
        $leaseID = [string]$acquired.lease.lease_id
        $sessionID = [string]$acquired.view.session.research_session_id
        $agentWorkspaceID = [string]$acquired.view.agent_workspace.agent_workspace_id
        $agentGeneration = Get-CurrentGenerationExactly -Resource $acquired.view.agent_workspace -Description "acquired agent workspace"
        $workspaceID = [string]$agentGeneration.workspace_id
        $agentWorkspacePath = Join-Path $runRoot "workspaces\$workspaceID"
        Assert-Condition -Condition ($leaseID -like "lease_*") -Message "acquisition returned a lease"
        Assert-Condition -Condition ($agentWorkspaceID -like "aw_*") -Message "acquisition returned an exact agent-workspace ID"
        Assert-Condition -Condition ($workspaceID -like "ws_*") -Message "acquisition returned an exact backing-workspace ID"
        Assert-Condition -Condition ([string]$agentGeneration.state -eq "ready") -Message "agent generation reached ready"
        $agentContainerID = Get-AgentContainerID -LeaseID $leaseID

        $fixtureDigest = Get-Sha256Reference -Path $fixturePath
        $linuxCgroupArguments = @(Get-LinuxCgroupVerificationArguments)
        $agentExec = Invoke-ProcessText -Executable $worldctl -Arguments (@($connectionArguments) + @(
            "open-exec", "-lease", $leaseID, "-policy", $policyDigest,
            "-executable", "/workspace/inputs/native-specimen",
            "-temporary-input", "1:e2e-payload=$fixturePath", "--",
            "-input", "placeholder", "-output", "/workspace/agent-result.json"
        ) + $linuxCgroupArguments)
        Assert-Condition -Condition ($agentExec.stdout.Trim() -eq $fixtureDigest) -Message "agent guest executed the native specimen with exact temporary-input substitution"
        $agentCgroupReport = Read-VerifiedLinuxCgroupReport -ResultPath (Join-Path $agentWorkspacePath "merged\agent-result.json") -Description "agent specimen result"

		# Exercise the startup crash boundary while both the agent and target
		# really own long-lived Docker exec children. Recovery must terminate both
		# physical execution boundaries before the replacement daemon admits RPC
		# traffic, then durably record the lost agent exec and failed target run.
		# Provision the target before either long-lived child starts so target
		# creation latency cannot blur the exact simultaneous crash boundary.
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

		# Library-only exclusive processlock forbids concurrent Open. Crash
		# specimens are therefore sequential host processes, not dual socket clients.
		$agentCrashContainerID = Get-AgentContainerID -LeaseID $leaseID
		$agentCrashStdin = Join-Path $clientRoot "active-agent-crash.stdin"
		Write-Utf8NoBom -Path $agentCrashStdin -Content ""
		$agentCrashStdout = Join-Path $logsRoot "active-agent-crash-exec.stdout.txt"
		$agentCrashStderr = Join-Path $logsRoot "active-agent-crash-exec.stderr.txt"
		$agentCrashArguments = @($connectionArguments) + @(
			"open-exec", "-lease", $leaseID, "-policy", $policyDigest, "-stdin", $agentCrashStdin,
			"-executable", "/workspace/inputs/native-specimen", "--",
			"-sleep", "10m", "-input", "/workspace/inputs/payload.txt", "-output", "/workspace/agent-crash-result.json"
		)
		$agentExecProcess = Start-LoggedProcess -Executable $worldctl -Arguments $agentCrashArguments -StandardOutputPath $agentCrashStdout -StandardErrorPath $agentCrashStderr
		[void](Wait-ContainerProcessState -ContainerID $agentCrashContainerID -Pattern "/workspace/inputs/native-specimen.*-sleep.*10m" -Present $true)
		Assert-Condition -Condition (-not $agentExecProcess.HasExited) -Message "long-lived agent exec reached its physical running boundary"
		$agentCrashExecID = ""
		Stop-StartedProcess -Process $agentExecProcess
		$agentExecProcess = $null

		# After agent host exit, target exec can acquire exclusive Manager ownership.
		$longExecStdout = Join-Path $logsRoot "active-crash-exec.stdout.txt"
		$longExecStderr = Join-Path $logsRoot "active-crash-exec.stderr.txt"
		$longExecArguments = @($connectionArguments) + @(
			"exec", "-target", $crashTargetID, "-run", $crashRunID, "-policy", $policyDigest, "--",
			"/target/input/bin/native-specimen", "-sleep", "10m", "-input", "/target/input/payload.txt", "-output", "/target/crash-result.json"
		)
		$targetExecProcess = Start-LoggedProcess -Executable $worldTarget -Arguments $longExecArguments -StandardOutputPath $longExecStdout -StandardErrorPath $longExecStderr
		$activeProcesses = Wait-ContainerProcessState -ContainerID $crashContainerID -Pattern "/target/input/bin/native-specimen.*-sleep.*10m" -Present $true
		$targetExecProcess.Refresh()
		Assert-Condition -Condition (-not $targetExecProcess.HasExited) -Message "long-lived target exec client was active at the crash boundary"
		$dockerCrashDaemonPID = 0
		Stop-StartedProcess -Process $targetExecProcess
		$targetExecProcess = $null

		$connectionArguments = @(Get-WorldOpenArguments -Timeout $rpcTimeout)
		$daemonProcess = Start-WorldDaemon
		if ([string]::IsNullOrWhiteSpace($agentCrashExecID)) {
			$recoverySession = Invoke-WorldCtlJSON -Arguments @("get-session", "-session", $sessionID)
			$recoveredAgentExecs = @(Get-OptionalObjectProperty -InputObject $recoverySession -Name "execs" | Where-Object {
				$_.executable -eq "/workspace/inputs/native-specimen"
			} | Sort-Object { [uint64]$_.revision } -Descending)
			Assert-Condition -Condition ($recoveredAgentExecs.Count -ge 1) -Message "recovery session retained the interrupted agent exec"
			$agentCrashExecID = [string]$recoveredAgentExecs[0].exec_id
		}
		$recoveredAgentExec = Invoke-WorldCtlJSON -Arguments @("get-exec", "-exec", $agentCrashExecID)
		Assert-Condition -Condition ($recoveredAgentExec.state -eq "lost") -Message "startup finalized the interrupted agent exec as lost"
		Assert-Condition -Condition ([bool]$recoveredAgentExec.cleanup_confirmed) -Message "interrupted agent exec records confirmed cleanup"
		Assert-Condition -Condition ([string]$recoveredAgentExec.error -match "control-plane continuity") -Message "interrupted agent exec records the daemon-loss boundary"
		$recoveredAgentContainerID = Get-AgentContainerID -LeaseID $leaseID
		Assert-Condition -Condition ($recoveredAgentContainerID -eq $agentCrashContainerID) -Message "agent exec recovery retained the exact container identity across its stop/start boundary"
		$recoveredAgentRunning = (Invoke-ProcessText -Executable "docker" -Arguments @("inspect", "--format", "{{.State.Running}}", $recoveredAgentContainerID)).stdout
		Assert-Condition -Condition ($recoveredAgentRunning -eq "true") -Message "agent exec recovery restored fresh container readiness"
		$recoveredAgentProcesses = Wait-ContainerProcessState -ContainerID $recoveredAgentContainerID -Pattern "/workspace/inputs/native-specimen.*-sleep.*10m" -Present $false
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
		Assert-Condition -Condition (@($recoveredCrashBundle.normalized_events | Where-Object { $_.kind -eq "target.run.control-plane-loss" }).Count -eq 1) -Message "interrupted run retained the exact control-plane-loss fact"
		Assert-Condition -Condition (@($recoveredCrashBundle.normalized_events | Where-Object { $_.kind -eq "target.run.control-plane-failure" }).Count -eq 1) -Message "interrupted run finalized as a target failure"
		Assert-Condition -Condition (@($recoveredCrashBundle.normalized_events | Where-Object { $_.kind -match "never[-_]started" }).Count -eq 0) -Message "previously running execution was not misreported as never started"
		$crashIncidentID = [string]$recoveredCrashRun.incident_ids[0]
		$crashIncident = Invoke-WorldCtlJSON -Arguments @("get-incident", "-incident", $crashIncidentID)
		Assert-Condition -Condition ($crashIncident.state -eq "evidence_sealed") -Message "startup sealed evidence for the control-plane-loss incident"
		Assert-Condition -Condition ($crashIncident.target_run_id -eq $crashRunID -and $crashIncident.observation_bundle_id -eq $recoveredCrashBundle.bundle_id) -Message "failure incident is bound to the exact interrupted run and bundle"

		$recoveredCrashContainerID = Get-TargetContainerID -TargetID $crashTargetID
		Assert-Condition -Condition ($recoveredCrashContainerID -eq $crashContainerID) -Message "startup adopted the exact target container"
		$runningState = (Invoke-ProcessText -Executable "docker" -Arguments @("inspect", "--format", "{{.State.Running}}", $recoveredCrashContainerID)).stdout
		Assert-Condition -Condition ($runningState -eq "false") -Message "recovery preserved the exact stopped-container boundary"
		$reopenInterruptedRun = Invoke-WorldTargetText -ExpectFailure -Arguments @(
			"exec", "-target", $crashTargetID, "-run", $crashRunID, "-policy", $policyDigest, "--", "/target/input/bin/native-specimen"
		)
		Assert-Condition -Condition (($reopenInterruptedRun.stdout + $reopenInterruptedRun.stderr) -match "(?i)(failed|terminal|state|run)") -Message "the failed interrupted run could not reopen target authority"
		[void](Invoke-WorldCtlJSON -Arguments @(
			"destroy", "-target", $crashTargetID, "-revision", ([string]$recoveredCrashTarget.revision),
			"-reason", "active crash E2E cleanup", "-policy", $policyDigest
		))

		$capture = Invoke-WorldCtlJSON -Arguments @(
            "start-capture", "-lease", $leaseID, "-policy", $policyDigest, "-profile", "worldLifecycle",
            "-signals", "target.lifecycle", "-duration", "1m", "-byte-limit", ([string]$captureByteLimit)
        )

        $target = Invoke-WorldCtlJSON -Arguments @(
            "create-target", "-lease", $leaseID, "-template", "linux-visible", "-kind", "linux_container",
            "-policy", $policyDigest, "-capabilities", $capabilityDigest
        )
        $targetID = [string]$target.target_id
        $targetGeneration = Get-CurrentGenerationExactly -Resource $target -Description "created Linux target"
        Assert-Condition -Condition ([string]$targetGeneration.state -eq "ready") -Message "Linux target reached ready"
		$targetContainerID = Get-TargetContainerID -TargetID $targetID
		$targetWritableRoot = Join-Path $runRoot "targets\$targetID\generations\1\writable"
		$detachedReadyPath = Join-Path $targetWritableRoot "daemon\ready.txt"
		$detachedMutationPath = Join-Path $targetWritableRoot "daemon\escaped-after-stop.txt"

        $run = Invoke-WorldCtlJSON -Arguments @(
            "start-run", "-target", $targetID, "-policy", $policyDigest,
            "-specimens", "specimen/native", "-fixtures", "fixture/payload"
        )
        $runID = [string]$run.target_run_id
        Assert-Condition -Condition ($run.state -eq "running") -Message "target run reached running"

        $targetExec = Invoke-WorldTargetText -Arguments (@(
            "exec", "-target", $targetID, "-run", $runID, "-policy", $policyDigest, "--",
            "/target/input/bin/native-specimen", "-input", "/target/input/payload.txt", "-output", "/target/result.json"
        ) + $linuxCgroupArguments)
        Assert-Condition -Condition ($targetExec.stdout.Trim() -eq $fixtureDigest) -Message "target executed the pinned specimen against exact material"
        $targetCgroupReport = Read-VerifiedLinuxCgroupReport -ResultPath (Join-Path $targetWritableRoot "result.json") -Description "Linux target specimen result"

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

		# A setsid child escapes world-guest's process group. Only stopping the
		# exact container boundary can prevent its delayed mutation.
		$detachedDelaySeconds = 15
		$detachedExec = Invoke-WorldTargetText -Arguments @(
			"exec", "-target", $targetID, "-run", $runID, "-policy", $policyDigest, "--",
			"/target/input/bin/native-specimen",
			"-detached-ready", "/target/daemon/ready.txt",
			"-detached-output", "/target/daemon/escaped-after-stop.txt",
			"-detached-delay", "${detachedDelaySeconds}s"
		)
		Assert-Condition -Condition ($detachedExec.stdout -match '^detached_pid=[0-9]+$') -Message "target launched the detached setsid specimen"
		Wait-FileState -Path $detachedReadyPath -Present $true
		Assert-Condition -Condition (-not (Test-Path -LiteralPath $detachedMutationPath)) -Message "detached specimen had not reached its delayed mutation before StopRun"
		$detachedReadyDigest = Get-Sha256Reference -Path $detachedReadyPath

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
		Assert-Condition -Condition ($bundle.target_changes.scope -eq "target" -and [uint64]$bundle.target_changes.workspace_revision -gt 0) -Message "bundle contains an initialized target change set"
		$targetChanges = @($bundle.target_changes.changes)
		Assert-Condition -Condition (@($targetChanges | Where-Object { $_.workspace_relative_path -eq "result.json" -and $_.kind -eq "added" -and $_.after_digest -match '^sha256:[0-9a-f]{64}$' }).Count -eq 1) -Message "target change manifest hashes the real specimen result"
		Assert-Condition -Condition (@($targetChanges | Where-Object { $_.workspace_relative_path -eq "pushed/payload.txt" -and $_.kind -eq "added" -and $_.after_digest -eq $fixtureDigest }).Count -eq 1) -Message "target change manifest hashes the workspace-backed push"
		Assert-Condition -Condition (@($targetChanges | Where-Object { $_.workspace_relative_path -eq "daemon/ready.txt" -and $_.kind -eq "added" -and $_.after_digest -eq $detachedReadyDigest }).Count -eq 1) -Message "target change manifest includes the detached specimen readiness marker"
		$stoppedContainerState = (Invoke-ProcessText -Executable "docker" -Arguments @("inspect", "--format", "{{.State.Running}}", $targetContainerID)).stdout
		Assert-Condition -Condition ($stoppedContainerState -eq "false") -Message "StopRun established the exact stopped-container boundary before sealing"
		Start-Sleep -Seconds ($detachedDelaySeconds + 1)
		$detachedBoundaryProven = -not (Test-Path -LiteralPath $detachedMutationPath)
		Assert-Condition -Condition $detachedBoundaryProven -Message "setsid specimen could not mutate target state after the sealed container boundary"
		$stoppedTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $targetID)
		$stoppedRun = @($stoppedTarget.runs | Where-Object { $_.target_run_id -eq $runID })
		Assert-Condition -Condition ($stoppedRun.Count -eq 1 -and $stoppedRun[0].state -eq "completed") -Message "stop-run completed rather than merely sealing a failed run"
		$reuseWithoutReset = Invoke-ProcessText -Executable $worldctl -ExpectFailure -Arguments (@($connectionArguments) + @(
			"start-run", "-target", $targetID, "-policy", $policyDigest,
			"-specimens", "specimen/native", "-fixtures", "fixture/payload"
		))
		Assert-Condition -Condition (($reuseWithoutReset.stdout + $reuseWithoutReset.stderr) -match '(?i)(must be reset|already has a run)') -Message "completed generation refused a second mutable run until reset"

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
		Assert-Condition -Condition ((Get-ProtobufUInt64 -InputObject $captureArtifact -Name "size") -le [uint64]$captureByteLimit) -Message "capture artifact remains within its admitted byte bound"

		$beforeReset = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $targetID)
        $reset = Invoke-WorldCtlJSON -Arguments @(
            "reset", "-target", $targetID, "-revision", ([string]$beforeReset.revision),
            "-mode", "recreate", "-policy", $policyDigest
        )
        Assert-Condition -Condition ($reset.current_generation -eq 2) -Message "target reset advanced generation"
        $resetGeneration = Get-CurrentGenerationExactly -Resource $reset -Description "reset Linux target"
        Assert-Condition -Condition ([string]$resetGeneration.state -eq "ready") -Message "reset target became ready"
        $resetTargetContainerID = Get-TargetContainerID -TargetID $targetID
        Assert-Condition -Condition ($resetTargetContainerID -ne $targetContainerID) -Message "Linux recreate reset replaced the exact target container ID"
        [void](Invoke-WorldCtlJSON -Arguments @(
            "destroy", "-target", $targetID, "-revision", ([string]$reset.revision),
            "-reason", "world E2E cleanup", "-policy", $policyDigest
        ))

        if ($ManagedAndroid) {
            $androidCreateAttempted = $true
            $androidTarget = Invoke-WorldCtlJSON -Arguments @(
                "create-target", "-lease", $leaseID, "-template", "android-visible", "-kind", "android_virtual_device",
                "-policy", $policyDigest, "-capabilities", $capabilityDigest
            )
            $androidTargetID = [string]$androidTarget.target_id
            $androidTargetGeneration = Get-CurrentGenerationExactly -Resource $androidTarget -Description "created managed Android target"
            Assert-Condition -Condition ([string]$androidTargetGeneration.state -eq "ready" -and [uint64]$androidTarget.current_generation -eq 1) -Message "managed Android target generation 1 reached ready through RPC"

            $expectedAndroidSerial = "emulator-$androidBaseConsolePort"
            $androidInitialOwnership = Get-ManagedAndroidProcessOwnership -TargetID $androidTargetID -Generation 1
            $androidOwnershipPath = [string]$androidInitialOwnership.path
            $androidProcessOwnership = $androidInitialOwnership.record
            Assert-Condition -Condition (
                [string]$androidProcessOwnership.serial -eq $expectedAndroidSerial -and
                [string]$androidProcessOwnership.runtime_id -eq $androidRuntimeName
            ) -Message "managed Android generation 1 matches the statically authorized logcat serial before collector start"
            Register-OwnedAndroidConsolePort -ConsolePort ([int]$androidProcessOwnership.console_port)

            $androidRun = Invoke-WorldCtlJSON -Arguments @(
                "start-run", "-target", $androidTargetID, "-policy", $policyDigest,
                "-specimens", "specimen/android-apk"
            )
            $androidRunID = [string]$androidRun.target_run_id
            Assert-Condition -Condition ($androidRun.state -eq "running") -Message "managed Android target run reached running through RPC"

            $androidProxyPort = Get-FreeLoopbackPort
            $androidADBProxyProcess = Start-WorldTargetADBProxy -Port $androidProxyPort -TargetID $androidTargetID -RunID $androidRunID -PolicyReference $policyDigest
            $androidDevices = Invoke-ScopedADB -Port $androidProxyPort -Arguments @("devices", "-l")
            $androidSerials = @($androidDevices.stdout -split '\r?\n' | ForEach-Object {
                if ($_ -match '^(\S+)\s+device(?:\s|$)') { $Matches[1] }
            } | Where-Object { $_ })
            $androidInitialConsolePort = [int]$androidBaseConsolePort
            $androidInitialRuntimeName = [string]$androidRuntimeName
            $androidScopedSerialExact = $androidSerials.Count -eq 1 -and $androidSerials[0] -eq $expectedAndroidSerial
            Assert-Condition -Condition $androidScopedSerialExact -Message "scoped ADB listed only the exact managed serial $expectedAndroidSerial"

            $androidLogcatMarker = "world_e2e_" + [guid]::NewGuid().ToString("N")
            [void](Invoke-ScopedADB -Port $androidProxyPort -Arguments @(
                "-s", $expectedAndroidSerial, "shell", "log", "-p", "i", "-t", "WORLD_E2E", $androidLogcatMarker
            ))
            $androidLogcatTransactionFact = Wait-AndroidLogcatActiveTransactionFact -TargetRunID $androidRunID -Marker $androidLogcatMarker -ExpectedSerial $expectedAndroidSerial -ExpectedRuntimeName $androidInitialRuntimeName
            $androidLogcatActivePartialPath = [string]$androidLogcatTransactionFact.stdout_partial_path

            $androidGeneration1DataMeasurement = Get-AndroidDataPartitionMeasurement -ProxyPort $androidProxyPort -Serial $expectedAndroidSerial -Generation 1
            $androidDataMeasurements += $androidGeneration1DataMeasurement

            $androidAPKDigest = Get-Sha256Reference -Path $AndroidSpecimenAPK
            $androidAPKFile = Get-Item -LiteralPath $AndroidSpecimenAPK
            $projectedAndroidAPK = "/data/local/tmp/world/runs/$androidTargetID/g1/$androidRunID/material/specimens/world-specimen.apk"
            $projectedHash = Invoke-ScopedADB -Port $androidProxyPort -Arguments @("-s", $expectedAndroidSerial, "shell", "sha256sum", "--", $projectedAndroidAPK)
            $projectedHashFields = @($projectedHash.stdout -split '\s+' | Where-Object { $_ })
            $projectedAPKDigestEqual = ($projectedHashFields.Count -ge 1 -and "sha256:$($projectedHashFields[0].ToLowerInvariant())" -eq $androidAPKDigest)
            Assert-Condition -Condition $projectedAPKDigestEqual -Message "profile-projected APK bytes match the signed host APK"

            $androidInstall = Invoke-ScopedADB -Port $androidProxyPort -Arguments @("-s", $expectedAndroidSerial, "shell", "pm", "install", "-r", $projectedAndroidAPK)
            $projectedAPKInstalled = Test-ContainsExactLine -Text $androidInstall.stdout -Expected "Success"
            Assert-Condition -Condition $projectedAPKInstalled -Message "Android installed the profile-projected APK through scoped ADB"
            $androidLaunch = Invoke-ScopedADB -Port $androidProxyPort -Arguments @("-s", $expectedAndroidSerial, "shell", "am", "start", "-W", "-n", "dev.philcantcode.worldspecimen/.MainActivity")
            Assert-Condition -Condition (Test-ContainsExactLine -Text $androidLaunch.stdout -Expected "Status: ok") -Message "Android launched the signed specimen through scoped ADB"
            $androidPackage = Invoke-ScopedADB -Port $androidProxyPort -Arguments @("-s", $expectedAndroidSerial, "shell", "cmd", "package", "path", "dev.philcantcode.worldspecimen")
            Assert-Condition -Condition ($androidPackage.stdout -match '(?m)^package:') -Message "Android package manager reports the installed specimen"
            $androidReportResult = Invoke-ScopedADB -Port $androidProxyPort -Arguments @("-s", $expectedAndroidSerial, "shell", "run-as", "dev.philcantcode.worldspecimen", "cat", "files/world-report.json")
            try {
                $androidReport = $androidReportResult.stdout.Trim() | ConvertFrom-Json
            }
            catch {
                throw "Android specimen did not publish a JSON report: $($androidReportResult.stdout)"
            }
            $androidReportBoundaryExact = (
                [string]$androidReport.package -eq "dev.philcantcode.worldspecimen" -and
                [int]$androidReport.sdk -ge 23 -and
                [string]$androidReport.mode -eq "normal" -and
                [string]$androidReport.files_dir -match '^/data/(user/0|data)/dev[.]philcantcode[.]worldspecimen/files$' -and
                -not [bool]$androidReport.host_docker_socket_visible -and
                -not [bool]$androidReport.host_workspace_visible
            )
            Assert-Condition -Condition $androidReportBoundaryExact -Message "Android specimen report proves the device boundary with real app data"

            $androidCurrentOwnership = Get-ManagedAndroidProcessOwnership -TargetID $androidTargetID -Generation 1
            Assert-Condition -Condition (
                [string]$androidCurrentOwnership.path -eq $androidOwnershipPath -and
                (Test-AndroidProcessOwnershipEqual -Left $androidProcessOwnership -Right $androidCurrentOwnership.record)
            ) -Message "managed Android generation 1 retained the exact process identity while the collector and specimen ran"
            $androidProcessOwnership = $androidCurrentOwnership.record
            Assert-Condition -Condition (Test-AndroidOwnershipResourceContract -Record $androidProcessOwnership) -Message "managed Android ownership commits exact host CPU/memory, storage, guest RAM, and anchored Windows Job authority"
            $androidGeneration1OSVerification = Invoke-ManagedAndroidOSVerification -Ownership $androidProcessOwnership -Generation 1 -StateDirectory (Split-Path -Parent $androidOwnershipPath) -ExpectedRuntimeName $androidInitialRuntimeName -ExpectedConsolePort $androidInitialConsolePort
            $androidOSVerifications += $androidGeneration1OSVerification
            $liveOwnedAndroidProcesses = @(Get-OwnedAndroidRuntimeProcesses)
            Assert-Condition -Condition ($liveOwnedAndroidProcesses.Count -eq 1 -and $liveOwnedAndroidProcesses[0].pid -eq [int]$androidProcessOwnership.pid) -Message "managed Android process inventory proves the exact owned runtime before stop"

            Stop-StartedProcess -Process $androidADBProxyProcess
            $androidADBProxyProcess = $null
            $androidProxyErrorPath = Join-Path $logsRoot "android-adb-proxy-$androidProxyPort.stderr.txt"
            $androidProxyErrors = if (Test-Path -LiteralPath $androidProxyErrorPath) { Get-Content -LiteralPath $androidProxyErrorPath -Raw } else { "" }
            $androidAssignedSerials = @([regex]::Matches($androidProxyErrors, '(?m)^assigned ADB serial: (\S+)\s*$') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
            Assert-Condition -Condition ($androidAssignedSerials.Count -eq 1 -and $androidAssignedSerials[0] -eq $expectedAndroidSerial) -Message "every scoped ADB stream was assigned the exact managed serial"

            $androidTargetView = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $androidTargetID)
            $androidCurrentRun = @($androidTargetView.runs | Where-Object { $_.target_run_id -eq $androidRunID })
            Assert-Condition -Condition ($androidCurrentRun.Count -eq 1) -Message "managed Android target view contains the active run"
            $androidBundle = Invoke-WorldCtlJSON -Arguments @(
                "stop-run", "-target", $androidTargetID, "-run", $androidRunID, "-revision", ([string]$androidCurrentRun[0].revision),
                "-reason", "managed Android daemon E2E complete", "-policy", $policyDigest
            )
            Assert-Condition -Condition ($androidBundle.state -eq "sealed") -Message "managed Android run produced a sealed bundle"
            $androidGeneration1LifecycleAvailable = @($androidBundle.coverage | Where-Object { $_.signal_family -eq "target.lifecycle" -and $_.required -and $_.status -eq "available" }).Count -eq 1
            Assert-Condition -Condition $androidGeneration1LifecycleAvailable -Message "managed Android bundle has required available lifecycle coverage"
            $androidLogcatCoverage = @($androidBundle.coverage | Where-Object {
                $_.signal_family -eq "android.logcat" -and $_.placement -eq "guest" -and
                $_.level -eq "partial" -and $_.required -and $_.status -eq "available"
            })
            $androidLogcatDroppedRecords = if ($androidLogcatCoverage.Count -eq 1) {
                Get-ProtobufUInt64 -InputObject $androidLogcatCoverage[0] -Name "dropped_records"
            }
            else {
                [uint64]::MaxValue
            }
            $androidLogcatGapProperty = if ($androidLogcatCoverage.Count -eq 1) { $androidLogcatCoverage[0].PSObject.Properties["gap"] } else { $null }
            $androidLogcatNoDropsOrGap = (
                $androidLogcatDroppedRecords -eq 0 -and
                ($null -eq $androidLogcatGapProperty -or $null -eq $androidLogcatGapProperty.Value)
            )
            Assert-Condition -Condition ($androidLogcatCoverage.Count -eq 1 -and $androidLogcatNoDropsOrGap) -Message "managed Android bundle truthfully records required partial guest logcat coverage without a drop or gap"
            $androidLogcatOutput = Get-VerifiedLogcatCollectorOutput -Bundle $androidBundle -Coverage $androidLogcatCoverage[0] -RequiredMarker $androidLogcatMarker -Description "managed Android generation-1 logcat"
            $androidLogcatFinalizedTransactionFact = Get-FinalizedAndroidLogcatTransactionFact -ActiveTransaction $androidLogcatTransactionFact -Coverage $androidLogcatCoverage[0] -CollectorOutput $androidLogcatOutput -ExpectedExternalOwnershipPossible $true -Description "managed Android generation-1 logcat"
            $androidLogcatArtifact = $androidLogcatOutput.stdout_artifact
            $androidLogcatObjectPath = [string]$androidLogcatOutput.stdout_object_path
            $androidLogcatObjectText = [string]$androidLogcatOutput.stdout_text
            Assert-Condition -Condition (@($androidBundle.normalized_events).Count -gt 0) -Message "managed Android bundle contains normalized lifecycle events"
            $androidChanges = @($androidBundle.target_changes.changes)
            $androidRootOpaqueChangeRecorded = $androidBundle.target_changes.scope -eq "target" -and $androidChanges.Count -eq 1 -and $androidChanges[0].kind -eq "opaque_directory" -and $androidChanges[0].workspace_relative_path -eq "."
            Assert-Condition -Condition $androidRootOpaqueChangeRecorded -Message "arbitrary scoped ADB mutation is reported as root-opaque"

            $androidReuseWithoutReset = Invoke-ProcessText -Executable $worldctl -ExpectFailure -Arguments (@($connectionArguments) + @(
                "start-run", "-target", $androidTargetID, "-policy", $policyDigest,
                "-specimens", "specimen/android-apk"
            ))
            $androidSameGenerationRunDenied = ($androidReuseWithoutReset.stdout + $androidReuseWithoutReset.stderr) -match '(?i)(must be reset|already has a run|completed generation)'
            Assert-Condition -Condition $androidSameGenerationRunDenied -Message "managed Android generation refused a second mutable run"

            $stoppedAndroidTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $androidTargetID)
            $androidFirstProcessOwnership = $androidProcessOwnership
            $resetAndroidTarget = Invoke-WorldCtlJSON -Arguments @(
                "reset", "-target", $androidTargetID, "-revision", ([string]$stoppedAndroidTarget.revision),
                "-mode", $androidResetMode, "-policy", $policyDigest
            )
            $resetAndroidGeneration = Get-CurrentGenerationExactly -Resource $resetAndroidTarget -Description "reset managed Android target"
            Assert-Condition -Condition ([uint64]$resetAndroidTarget.current_generation -eq 2 -and [string]$resetAndroidGeneration.state -eq "ready") -Message "managed Android baseline reset booted generation 2"
            $androidResetOwnership = Get-ManagedAndroidProcessOwnership -TargetID $androidTargetID -Generation 2
            $androidResetOwnershipPath = [string]$androidResetOwnership.path
            $androidProcessOwnership = $androidResetOwnership.record
            Assert-Condition -Condition (Test-AndroidOwnershipResourceContract -Record $androidProcessOwnership) -Message "reset managed Android ownership preserves the exact CPU/memory/storage/guest-RAM contract and anchored Windows Job authority"
            $androidResetSerial = [string]$androidProcessOwnership.serial
            $androidResetConsolePort = [int]$androidProcessOwnership.console_port
            $androidResetRuntimeName = [string]$androidProcessOwnership.runtime_id
            Register-OwnedAndroidConsolePort -ConsolePort $androidResetConsolePort
            Assert-Condition -Condition ($androidResetSerial -ne $expectedAndroidSerial -and $androidResetConsolePort -ne $androidInitialConsolePort -and $androidResetRuntimeName -ne $androidInitialRuntimeName) -Message "managed Android baseline reset received an independent endpoint/runtime allocation"
            $liveOwnedAndroidProcesses = @(Get-OwnedAndroidRuntimeProcesses)
            Assert-Condition -Condition ($liveOwnedAndroidProcesses.Count -eq 1 -and $liveOwnedAndroidProcesses[0].pid -eq [int]$androidProcessOwnership.pid) -Message "reset retained exactly one newly owned managed Android runtime"
            $androidResetReplacedProcessIdentity = (
                [int]$androidProcessOwnership.pid -ne [int]$androidFirstProcessOwnership.pid -or
                [string]$androidProcessOwnership.start_token -ne [string]$androidFirstProcessOwnership.start_token
            )
            Assert-Condition -Condition $androidResetReplacedProcessIdentity -Message "managed Android baseline reset replaced the exact host process identity"

            # Restart while generation 2 is live but before granting it mutable
            # run authority. Startup must adopt the exact runtime rather than
            # silently replacing its serial, ports, or host process identity.
            $androidResetOwnershipBeforeRestart = $androidProcessOwnership
            $androidResetIdentityBeforeRestart = Get-ExactProcessIdentity -ProcessID ([int]$androidResetOwnershipBeforeRestart.pid)
            Assert-Condition -Condition (
                $null -ne $androidResetIdentityBeforeRestart -and
                (Test-ExactProcessIdentityEqual -Left $androidResetOwnershipBeforeRestart -Right $androidResetIdentityBeforeRestart)
            ) -Message "managed Android generation 2 ownership matched its live process before daemon restart"
            Stop-WorldDaemon -Process $daemonProcess
            $daemonProcess = $null
            $androidContinuityRestartPort = Get-FreeLoopbackPort
            $connectionArguments = @(Get-WorldOpenArguments -Timeout $rpcTimeout)
            $daemonProcess = Start-WorldDaemon
            $recoveredAndroidResetTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $androidTargetID)
            $recoveredAndroidResetGeneration = Get-CurrentGenerationExactly -Resource $recoveredAndroidResetTarget -Description "recovered reset managed Android target"
            Assert-Condition -Condition (
                [uint64]$recoveredAndroidResetTarget.current_generation -eq 2 -and
                [string]$recoveredAndroidResetGeneration.state -eq "ready"
            ) -Message "daemon restart recovered managed Android generation 2 as logically ready before its run"
            $androidResetOwnershipAfterRestartRead = Get-ManagedAndroidProcessOwnership -TargetID $androidTargetID -Generation 2
            Assert-Condition -Condition ([string]$androidResetOwnershipAfterRestartRead.path -eq $androidResetOwnershipPath) -Message "daemon restart retained the exact generation-2 ownership path"
            $androidResetOwnershipAfterRestart = $androidResetOwnershipAfterRestartRead.record
            Assert-Condition -Condition (Test-AndroidProcessOwnershipEqual -Left $androidResetOwnershipBeforeRestart -Right $androidResetOwnershipAfterRestart) -Message "daemon restart preserved the exact managed Android generation-2 serial, port, runtime, PID, executable, and start token"
            $androidResetIdentityAfterRestart = Get-ExactProcessIdentity -ProcessID ([int]$androidResetOwnershipAfterRestart.pid)
            Assert-Condition -Condition (
                $null -ne $androidResetIdentityAfterRestart -and
                (Test-ExactProcessIdentityEqual -Left $androidResetOwnershipAfterRestart -Right $androidResetIdentityAfterRestart)
            ) -Message "managed Android generation 2 retained the exact live host process across daemon restart"
            $androidProcessOwnership = $androidResetOwnershipAfterRestart
            $androidResetRestartContinuityProven = (
                [uint64]$recoveredAndroidResetTarget.current_generation -eq 2 -and
                [string]$recoveredAndroidResetGeneration.state -eq "ready" -and
                (Test-AndroidProcessOwnershipEqual -Left $androidResetOwnershipBeforeRestart -Right $androidResetOwnershipAfterRestart) -and
                (Test-ExactProcessIdentityEqual -Left $androidResetIdentityBeforeRestart -Right $androidResetIdentityAfterRestart)
            )
            Assert-Condition -Condition $androidResetRestartContinuityProven -Message "managed Android generation 2 preserved exact logical and physical identity through restart"
            $androidGeneration2OSVerification = Invoke-ManagedAndroidOSVerification -Ownership $androidResetOwnershipAfterRestart -Generation 2 -StateDirectory (Split-Path -Parent $androidResetOwnershipPath) -ExpectedRuntimeName $androidResetRuntimeName -ExpectedConsolePort $androidResetConsolePort
            $androidOSVerifications += $androidGeneration2OSVerification

            $androidADBProcessesBeforeResetRun = @(Get-AndroidADBProcessSnapshot)
            $androidResetRun = Invoke-WorldCtlJSON -Arguments @(
                "start-run", "-target", $androidTargetID, "-policy", $policyDigest,
                "-specimens", "specimen/android-apk", "-fixtures", "fixture/payload"
            )
            $androidResetRunID = [string]$androidResetRun.target_run_id
            Assert-Condition -Condition ($androidResetRun.state -eq "running") -Message "managed Android reset generation accepted a fresh run"
            Wait-UntilTrue -TimeoutSeconds 15 -FailureMessage "generation-2 run did not expose exactly one new long-lived ADB collector identity" -Condition {
                $currentADBProcesses = @(Get-AndroidADBProcessSnapshot)
                $newADBProcesses = @(Get-NewExactProcessIdentities -Before $androidADBProcessesBeforeResetRun -After $currentADBProcesses)
                $missingBaselineProcesses = @(Get-NewExactProcessIdentities -Before $currentADBProcesses -After $androidADBProcessesBeforeResetRun)
                if ($newADBProcesses.Count -ne 1 -or $missingBaselineProcesses.Count -ne 0) {
                    return $false
                }
                $script:androidResetLogcatCollectorProcessIdentity = $newADBProcesses[0]
                return $true
            }
            Assert-Condition -Condition (
                $null -ne $androidResetLogcatCollectorProcessIdentity -and
                [string]$androidResetLogcatCollectorProcessIdentity.process_name -eq "adb" -and
                [IO.Path]::GetFullPath([string]$androidResetLogcatCollectorProcessIdentity.executable_path).Equals(
                    [IO.Path]::GetFullPath([string]$androidADBBinary),
                    [StringComparison]::OrdinalIgnoreCase
                )
            ) -Message "generation-2 observer is one exact new configured adb.exe PID/path/start-token identity"
            $androidResetProxyPort = Get-FreeLoopbackPort
            $androidADBProxyProcess = Start-WorldTargetADBProxy -Port $androidResetProxyPort -TargetID $androidTargetID -RunID $androidResetRunID -PolicyReference $policyDigest
            # Library-only: no long-lived worldd. ADB proxy host is the retained ownership boundary.
            $androidCrashDaemonIdentity = Get-ExactProcessIdentity -RetainedProcess $androidADBProxyProcess
            Assert-Condition -Condition ($null -ne $androidCrashDaemonIdentity) -Message "generation-2 crash retained an exact live ADB proxy host process identity"
            $androidGeneration2DataMeasurement = Get-AndroidDataPartitionMeasurement -ProxyPort $androidResetProxyPort -Serial $androidResetSerial -Generation 2
            $androidDataMeasurements += $androidGeneration2DataMeasurement
            $projectedAndroidResetFixture = "/data/local/tmp/world/runs/$androidTargetID/g2/$androidResetRunID/material/fixtures/payload.txt"
            $androidResetFixtureHash = Invoke-ScopedADB -Port $androidResetProxyPort -Arguments @("-s", $androidResetSerial, "shell", "sha256sum", "--", $projectedAndroidResetFixture)
            $androidResetFixtureHashFields = @($androidResetFixtureHash.stdout -split '\s+' | Where-Object { $_ })
            $androidResetFixtureDigestEqual = ($androidResetFixtureHashFields.Count -ge 1 -and "sha256:$($androidResetFixtureHashFields[0].ToLowerInvariant())" -eq $fixtureDigest)
            Assert-Condition -Condition $androidResetFixtureDigestEqual -Message "managed Android generation-2 fixture disambiguation projects the exact real input bytes"
            $androidPackagesAfterReset = Invoke-ScopedADB -Port $androidResetProxyPort -Arguments @("-s", $androidResetSerial, "shell", "pm", "list", "packages", "dev.philcantcode.worldspecimen")
            $androidResetPackageAbsent = $androidPackagesAfterReset.stdout -notmatch '(?m)^package:dev[.]philcantcode[.]worldspecimen\s*$'
            Assert-Condition -Condition $androidResetPackageAbsent -Message "managed Android baseline reset returned to a clean boot without the generation-1 installed package"
            $androidResetLogcatMarker = "world_e2e_reset_" + [guid]::NewGuid().ToString("N")
            [void](Invoke-ScopedADB -Port $androidResetProxyPort -Arguments @(
                "-s", $androidResetSerial, "shell", "log", "-p", "i", "-t", "WORLD_E2E", $androidResetLogcatMarker
            ))
            $androidResetLogcatTransactionFact = Wait-AndroidLogcatActiveTransactionFact -TargetRunID $androidResetRunID -Marker $androidResetLogcatMarker -ExpectedSerial $androidResetSerial -ExpectedRuntimeName $androidResetRuntimeName
            $androidResetLogcatActivePartialPath = [string]$androidResetLogcatTransactionFact.stdout_partial_path
            $androidResetCollectorArguments = @(
                "-H", "127.0.0.1", "-P", "5037", "-s", $androidResetSerial
            ) + @($androidLogcatObserverConfiguration.args)
            $androidResetLogcatCollectorLaunchFact = Get-WindowsProcessLaunchFact -ProcessIdentity $androidResetLogcatCollectorProcessIdentity -ExpectedArguments $androidResetCollectorArguments -ExpectedParentIdentity $androidCrashDaemonIdentity -CollectorID ([string]$androidResetLogcatTransactionFact.collector_id) -Description "generation-2 marker-bound logcat collector"
            Assert-Condition -Condition (
                [string]$androidResetLogcatCollectorLaunchFact.collector_id -eq [string]$androidResetLogcatTransactionFact.observer_marker.collector_id -and
                [bool]$androidResetLogcatCollectorLaunchFact.command_line_exact
            ) -Message "the sole new configured adb.exe is bound to the exact durable generation-2 collector and literal runtime-selected logcat argv"
            Assert-Condition -Condition ($androidLogcatObjectText.IndexOf($androidResetLogcatMarker, [StringComparison]::Ordinal) -lt 0) -Message "generation-1 immutable logcat excludes the unique generation-2 marker"
            $androidResetADBListenerIdentityBeforeCrash = Get-AndroidADBServerListenerIdentity
            Assert-Condition -Condition (
                $null -ne $androidResetADBListenerIdentityBeforeCrash -and
                -not (Test-ExactProcessIdentityEqual -Left $androidResetADBListenerIdentityBeforeCrash -Right $androidResetLogcatCollectorProcessIdentity)
            ) -Message "shared loopback ADB server and generation-2 collector have distinct exact process identities before daemon crash"
            $androidResetProxyErrorPath = Join-Path $logsRoot "android-adb-proxy-$androidResetProxyPort.stderr.txt"
            $androidADBStreamProcess = Start-ScopedADBLongStream -ProxyPort $androidResetProxyPort -Serial $androidResetSerial -ProxyErrorPath $androidResetProxyErrorPath
            $androidResetProxyPID = [int]$androidADBProxyProcess.Id
            $androidADBStreamPID = [int]$androidADBStreamProcess.Id
            $androidCrashDaemonPID = if ($null -ne $androidADBProxyProcess) { [int]$androidADBProxyProcess.Id } else { 0 }

            $androidCrashDaemonIdentityBeforeKill = if ($null -ne $androidADBProxyProcess) {
                Get-RequiredExactProcessIdentity -ExpectedIdentity $androidCrashDaemonIdentity -RetainedProcess $androidADBProxyProcess -Description "generation-2 ADB proxy host immediately before kill"
            } else {
                $androidCrashDaemonIdentity
            }
            $androidCrashCollectorIdentityBeforeKill = Get-RequiredExactProcessIdentity -ExpectedIdentity $androidResetLogcatCollectorProcessIdentity -Description "generation-2 logcat collector immediately before host kill"
            $androidCrashEmulatorIdentityBeforeKill = Get-RequiredExactProcessIdentity -ExpectedIdentity $androidResetOwnershipAfterRestart -Description "generation-2 emulator immediately before host kill"
            $androidResetADBListenerIdentityImmediatelyBeforeCrash = Get-AndroidADBServerListenerIdentity
            Assert-Condition -Condition (
                $null -ne $androidResetADBListenerIdentityImmediatelyBeforeCrash -and
                (Test-ExactProcessIdentityEqual -Left $androidResetADBListenerIdentityBeforeCrash -Right $androidResetADBListenerIdentityImmediatelyBeforeCrash)
            ) -Message "shared ADB server retained its exact identity through the immediate pre-kill boundary"
            $androidResetADBListenerIdentityBeforeCrash = $androidResetADBListenerIdentityImmediatelyBeforeCrash

            if ($null -ne $androidADBStreamProcess) {
                Stop-StartedProcess -Process $androidADBStreamProcess
                $androidADBStreamProcess = $null
            }
            if ($null -ne $androidADBProxyProcess) {
                Stop-StartedProcess -Process $androidADBProxyProcess
                $androidADBProxyProcess = $null
            }
            $daemonProcess = $null
            $androidCrashEmulatorIdentityAfterKill = Get-RequiredExactProcessIdentity -ExpectedIdentity $androidCrashEmulatorIdentityBeforeKill -Description "generation-2 emulator immediately after daemon kill and before restart"
            Wait-UntilTrue -TimeoutSeconds 15 -FailureMessage "generation-2 crash-contained logcat collector survived daemon death" -Condition {
                $liveCollector = Get-ExactProcessIdentity -ProcessID ([int]$script:androidResetLogcatCollectorProcessIdentity.pid)
                return ($null -eq $liveCollector -or -not (Test-ExactProcessIdentityEqual -Left $script:androidResetLogcatCollectorProcessIdentity -Right $liveCollector))
            }
            $androidResetLogcatCollectorAbsentAfterCrash = $true
            $androidResetInterruptedPartialIdentities = @(foreach ($partial in @(
                [pscustomobject]@{ role = "collector.stdout"; path = [string]$androidResetLogcatTransactionFact.stdout_partial_path },
                [pscustomobject]@{ role = "collector.stderr"; path = [string]$androidResetLogcatTransactionFact.stderr_partial_path }
            )) {
                $identity = Get-FileEvidenceIdentity -Path ([string]$partial.path)
                [pscustomobject][ordered]@{
                    role = [string]$partial.role
                    path = [string]$identity.path
                    bytes = [int64]$identity.bytes
                    digest = [string]$identity.digest
                }
            })
            $androidResetADBListenerIdentityAfterCrash = Get-AndroidADBServerListenerIdentity
            Assert-Condition -Condition (
                $null -ne $androidResetADBListenerIdentityAfterCrash -and
                (Test-ExactProcessIdentityEqual -Left $androidResetADBListenerIdentityBeforeCrash -Right $androidResetADBListenerIdentityAfterCrash)
            ) -Message "daemon crash removed only the owned logcat collector and preserved the exact shared ADB server"
            Assert-Condition -Condition (
                (Test-Path -LiteralPath $androidResetLogcatActivePartialPath -PathType Leaf) -and
                (Read-SharedUtf8Text -Path $androidResetLogcatActivePartialPath).IndexOf($androidResetLogcatMarker, [StringComparison]::Ordinal) -ge 0
            ) -Message "committed generation-2 logcat prefix remains present for authoritative restart reconciliation"
            $androidResetProxyExitCode = Wait-StartedProcessExitCode -Process $androidADBProxyProcess -TimeoutSeconds 30 -FailureMessage "live generation-2 ADB proxy did not fail after daemon crash"
            $androidResetProxyFailureText = if (Test-Path -LiteralPath $androidResetProxyErrorPath) { Get-Content -LiteralPath $androidResetProxyErrorPath -Raw } else { "" }
            Assert-Condition -Condition ($androidResetProxyExitCode -ne 0) -Message "live generation-2 ADB proxy reported failure after daemon crash"
            Assert-Condition -Condition ($androidResetProxyFailureText -match '(?m)^rpc error: code = Unavailable desc = ') -Message "live generation-2 ADB proxy recorded the exact unavailable control-plane failure"
            $androidADBProxyProcess.Dispose()
            $androidADBProxyProcess = $null
            $androidADBStreamExitCode = Wait-StartedProcessExitCode -Process $androidADBStreamProcess -TimeoutSeconds 20 -FailureMessage "ADB client stream remained open after its scoped proxy failed"
            Assert-Condition -Condition ($androidADBStreamExitCode -ne 0) -Message "long-lived generation-2 ADB client exited nonzero after its exact scoped proxy authority failed"
            $androidADBStreamProcess.Dispose()
            $androidADBStreamProcess = $null

            $androidCrashRestartPort = Get-FreeLoopbackPort
            $connectionArguments = @(Get-WorldOpenArguments -Timeout $rpcTimeout)
            $daemonProcess = Start-WorldDaemon
            $recoveredAndroidCrashTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $androidTargetID)
            $recoveredAndroidCrashRuns = @($recoveredAndroidCrashTarget.runs | Where-Object { $_.target_run_id -eq $androidResetRunID })
            Assert-Condition -Condition ($recoveredAndroidCrashRuns.Count -eq 1) -Message "Android restart recovered the exact generation-2 run"
            $recoveredAndroidCrashRun = $recoveredAndroidCrashRuns[0]
            Assert-Condition -Condition ([string]$recoveredAndroidCrashRun.state -eq "failed") -Message "Android restart finalized the stream-interrupted generation-2 run as failed"
            Assert-Condition -Condition (-not [string]::IsNullOrWhiteSpace([string]$recoveredAndroidCrashRun.bundle_id)) -Message "stream-interrupted Android run published a durable bundle identity"
            $androidResetBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $androidResetRunID)
            Assert-Condition -Condition ([string]$androidResetBundle.state -eq "sealed") -Message "stream-interrupted Android generation produced a sealed bundle"
            $androidResetCoverage = @($androidResetBundle.coverage)
            $androidResetLogcatCoverage = @($androidResetCoverage | Where-Object { [string]$_.signal_family -eq "android.logcat" })
            $androidResetUnavailableLifecycle = @($androidResetCoverage | Where-Object { [string]$_.signal_family -eq "target.lifecycle" })
            $androidResetControlPlaneGaps = @($androidResetBundle.gaps)
            Assert-Condition -Condition (
                $androidResetCoverage.Count -eq 2 -and
                $androidResetLogcatCoverage.Count -eq 1 -and
                $androidResetUnavailableLifecycle.Count -eq 1 -and
                $androidResetControlPlaneGaps.Count -eq 2 -and
                @($androidResetCoverage | Where-Object { [string]$_.status -eq "available" }).Count -eq 0
            ) -Message "stream-interrupted Android bundle has exactly the required lost logcat and lifecycle coverage with two explicit gaps"
            Assert-RecoveredControlPlaneCoverage -Coverage $androidResetLogcatCoverage[0] -SignalFamily "android.logcat" -Placement "guest" -BundleGaps $androidResetControlPlaneGaps -Description "generation-2 Android logcat"
            Assert-RecoveredControlPlaneCoverage -Coverage $androidResetUnavailableLifecycle[0] -SignalFamily "target.lifecycle" -Placement "host" -BundleGaps $androidResetControlPlaneGaps -Description "generation-2 target lifecycle"
            $androidResetLogcatGaps = @($androidResetLogcatCoverage[0].gap)
            $androidResetLogcatOutput = Get-VerifiedLogcatCollectorOutput -Bundle $androidResetBundle -Coverage $androidResetLogcatCoverage[0] -RequiredMarker $androidResetLogcatMarker -ForbiddenMarkers @($androidLogcatMarker) -Description "recovered managed Android generation-2 logcat"
            $androidResetLogcatFinalizedTransactionFact = Get-FinalizedAndroidLogcatTransactionFact -ActiveTransaction $androidResetLogcatTransactionFact -Coverage $androidResetLogcatCoverage[0] -CollectorOutput $androidResetLogcatOutput -InterruptedPartialIdentities $androidResetInterruptedPartialIdentities -ExpectedExternalOwnershipPossible $false -Description "recovered managed Android generation-2 logcat"
            $androidResetLogcatArtifact = $androidResetLogcatOutput.stdout_artifact
            $androidResetLogcatObjectPath = [string]$androidResetLogcatOutput.stdout_object_path
            Assert-Condition -Condition (@($androidResetBundle.normalized_events | Where-Object { $_.kind -eq "target.run.control-plane-loss" }).Count -eq 1) -Message "stream-interrupted Android bundle retained the exact control-plane-loss fact"
            Assert-Condition -Condition (@($androidResetBundle.normalized_events | Where-Object { $_.kind -eq "target.run.control-plane-failure" }).Count -eq 1) -Message "stream-interrupted Android run finalized as a target failure"
            $androidResetLiveIdentityAfterRecovery = Get-ExactProcessIdentity -ProcessID ([int]$androidResetOwnershipAfterRestart.pid)
            $androidResetExactProcessAbsent = ($null -eq $androidResetLiveIdentityAfterRecovery -or -not (Test-ExactProcessIdentityEqual -Left $androidCrashEmulatorIdentityAfterKill -Right $androidResetLiveIdentityAfterRecovery))
            Assert-Condition -Condition $androidResetExactProcessAbsent -Message "startup recovery stopped the exact generation-2 emulator that remained alive after daemon kill, before RPC admission"
            Wait-UntilTrue -TimeoutSeconds 30 -FailureMessage "generation-2 ADB serial remained in the real server inventory after crash recovery" -Condition {
                @(Get-AndroidADBDeviceSnapshot | Where-Object { [string]$_.serial -eq $script:androidResetSerial }).Count -eq 0
            }
            $androidResetADBFailure = Invoke-ProcessText -Executable $androidADBBinary -ExpectFailure -Arguments @("-H", "127.0.0.1", "-P", "5037", "-s", $androidResetSerial, "get-state")
            $androidResetADBDevicesAfterRecovery = @(Get-AndroidADBDeviceSnapshot | Where-Object { [string]$_.serial -eq $androidResetSerial })
            $androidResetADBUnreachable = ($androidResetADBFailure.exit_code -ne 0 -and $androidResetADBDevicesAfterRecovery.Count -eq 0)
            Assert-Condition -Condition $androidResetADBUnreachable -Message "startup made the exact generation-2 ADB serial unreachable before destroy"
            [void](Destroy-ManagedAndroidTargetExactly -TargetID $androidTargetID -PolicyReference $policyDigest -Reason "managed Android crash-boundary E2E cleanup")
        }

		# Export commit quiesces the agent generation, so it must remain the last
		# operation after every Linux and Android target has finished using that
		# generation for mutable run authority.
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

        $session = Invoke-WorldCtlJSON -Arguments @("get-session", "-session", $sessionID)
        $released = Invoke-WorldCtlJSON -Arguments @(
            "release", "-lease", $leaseID, "-revision", ([string]$session.lease.revision),
            "-reason", "world E2E cleanup", "-policy", $policyDigest
        )
        Assert-Condition -Condition ($released.lease_id -eq $leaseID) -Message "lease release completed"

        Stop-WorldDaemon -Process $daemonProcess
        $daemonProcess = $null
        $finalRestartPort = Get-FreeLoopbackPort
        $connectionArguments = @(Get-WorldOpenArguments -Timeout $rpcTimeout)
        $daemonProcess = Start-WorldDaemon

        $recoveredSession = Invoke-WorldCtlJSON -Arguments @("get-session", "-session", $sessionID)
        Assert-Condition -Condition ($recoveredSession.lease.state -eq "released") -Message "crash restart recovered released lease state"
        $recoveredBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $runID)
        Assert-Condition -Condition ($recoveredBundle.bundle_id -eq $bundle.bundle_id) -Message "crash restart recovered the exact sealed bundle"
		$durableCrashBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $crashRunID)
		Assert-Condition -Condition ($durableCrashBundle.bundle_id -eq $recoveredCrashBundle.bundle_id) -Message "final restart retained the exact interrupted-run evidence bundle"
		$finalCrashTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $crashTargetID)
		$finalTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $targetID)
		if ($ManagedAndroid) {
			$durableAndroidBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $androidRunID)
			Assert-Condition -Condition ($durableAndroidBundle.bundle_id -eq $androidBundle.bundle_id) -Message "final restart retained the exact managed Android evidence bundle"
			$durableAndroidResetBundle = Invoke-WorldCtlJSON -Arguments @("bundle", "-run", $androidResetRunID)
			Assert-Condition -Condition ($durableAndroidResetBundle.bundle_id -eq $androidResetBundle.bundle_id) -Message "final restart retained the exact managed Android reset-generation evidence bundle"
			$finalAndroidTarget = Invoke-WorldCtlJSON -Arguments @("get-target", "-target", $androidTargetID)
		}

        # No daemon remains able to mutate physical resources after this point.
        # All release-gate cleanup facts below are freshly measured before any
        # evidence object is constructed.
        Stop-WorldDaemon -Process $daemonProcess
        $daemonProcess = $null
        $crashTargetDestroyed = [string](Get-CurrentGenerationExactly -Resource $finalCrashTarget -Description "final recovered crash target").state -eq "destroyed"
        $targetDestroyed = [string](Get-CurrentGenerationExactly -Resource $finalTarget -Description "final recovered Linux qualification target").state -eq "destroyed"
        Assert-Condition -Condition $crashTargetDestroyed -Message "final restart recovered the crash target as destroyed"
        Assert-Condition -Condition $targetDestroyed -Message "final restart recovered the Linux qualification target as destroyed"
        if ($ManagedAndroid) {
            $androidTargetDestroyed = [string](Get-CurrentGenerationExactly -Resource $finalAndroidTarget -Description "final recovered managed Android target").state -eq "destroyed"
            Assert-Condition -Condition $androidTargetDestroyed -Message "final restart recovered the managed Android target as destroyed"
        }

        Wait-UntilTrue -TimeoutSeconds 45 -FailureMessage "an exact tracked Docker container remained after release/destroy and final daemon stop" -Condition {
            $allIDs = @(Get-AllDockerContainerIDs)
            @($script:dockerTrackedContainers | Where-Object { $allIDs -contains [string]$_.id }).Count -eq 0
        }
        $allDockerIDsAfterCleanup = @(Get-AllDockerContainerIDs)
        $trackedDockerContainersRemaining = @($dockerTrackedContainers | Where-Object { $allDockerIDsAfterCleanup -contains [string]$_.id })
        $trackedDockerCleanupProven = $trackedDockerContainersRemaining.Count -eq 0
        Assert-Condition -Condition $trackedDockerCleanupProven -Message "every immutable Docker container ID created/discovered for this test is absent, independently of labels"
        $exactTestLeaseDockerResources = @(Get-TestLeaseWorldContainerIDs -LeaseID $leaseID)
        $exactTestLeaseDockerCleanupProven = $exactTestLeaseDockerResources.Count -eq 0
        Assert-Condition -Condition $exactTestLeaseDockerCleanupProven -Message "release removed every exact-test-lease Docker resource"
        $dockerAmbientAfter = @(Get-DockerContainerSnapshot)
        $dockerAmbientBaselinePreserved = Test-DockerAmbientBaselinePreserved -Before $dockerAmbientBefore -After $dockerAmbientAfter
        Assert-Condition -Condition $dockerAmbientBaselinePreserved -Message "every ambient Docker container retained its exact ID, image, creation identity, name, labels, and lifecycle state"
        $unknownConcurrentDockerResources = @(Get-UnknownConcurrentDockerResources -Before $dockerAmbientBefore -After $dockerAmbientAfter)
        foreach ($resource in $unknownConcurrentDockerResources) {
            Write-Warning "preserved concurrent non-test Docker resource $($resource.id) name=$($resource.name) image=$($resource.image_id) status=$($resource.status)"
        }

        $workspaceCleanupFacts = Get-ExactPathAbsenceFact -Path $agentWorkspacePath -Kind "agent-backing-workspace" -OwnerID $workspaceID -Generation 1
        Assert-Condition -Condition ([bool]$workspaceCleanupFacts.absent) -Message "released exact backing workspace $workspaceID has no local directory or state residue"
        $linuxTargetCleanupFacts = @(
            Get-LinuxTargetCleanupFact -TargetID $crashTargetID -Generations @(1)
            Get-LinuxTargetCleanupFact -TargetID $targetID -Generations @(1, 2)
        )
        foreach ($cleanupFact in $linuxTargetCleanupFacts) {
            Assert-Condition -Condition ([bool]$cleanupFact.no_owned_state_residue) -Message "destroyed Linux target $($cleanupFact.target_id) has no generation directory or file residue"
        }

        if ($ManagedAndroid) {
            Wait-UntilTrue -TimeoutSeconds 45 -FailureMessage "owned managed Android runtime $androidResetRuntimeName remained after destroy" -Condition {
                @(Get-OwnedAndroidRuntimeProcesses).Count -eq 0
            }
            $ownedAndroidRuntimeProcesses = @(Get-OwnedAndroidRuntimeProcesses)
            $ownedAVDRoot = Join-Path $androidTargetRoot "avds"
            $ownedAVDEntries = @(if (Test-Path -LiteralPath $ownedAVDRoot -PathType Container) {
                Get-ChildItem -LiteralPath $ownedAVDRoot -Force -ErrorAction Stop | Where-Object {
                    $_.Name -like "$androidInitialRuntimeName*" -or $_.Name -like "$androidResetRuntimeName*"
                }
            }
            else {
                @()
            })
            $androidPortsReleased = (
                (Test-LoopbackPortPairAvailable -ConsolePort $androidInitialConsolePort) -and
                (Test-LoopbackPortPairAvailable -ConsolePort $androidResetConsolePort)
            )
            Wait-UntilTrue -TimeoutSeconds 45 -FailureMessage "ambient ADB devices and emulator/QEMU process identities did not return to their exact pre-launch snapshot" -Condition {
                $candidateSnapshot = [pscustomobject]@{
                    adb_devices = @(Get-AndroidADBDeviceSnapshot)
                    adb_processes = @(Get-AndroidADBProcessSnapshot)
                    emulator_processes = @(Get-AndroidRuntimeProcessSnapshot)
                }
                return (Test-AndroidLiveAmbientSnapshotsEqual -Before $script:androidAmbientBefore -After $candidateSnapshot)
            }
            # The potentially large inactive AVD tree is hashed once after live
            # devices/processes settle, rather than on every polling interval.
            $androidAmbientAfter = Get-AndroidAmbientSnapshot
            Assert-AndroidAmbientBaselinePreserved -Before $androidAmbientBefore -After $androidAmbientAfter
            $androidAmbientBaselineRestored = Test-AndroidAmbientSnapshotsEqual -Before $androidAmbientBefore -After $androidAmbientAfter
            $androidUnboundLaunchRemnants = @(Get-UnboundAndroidLaunchRemnants)
            $ownedAndroidStateRemnants = @(Get-OwnedAndroidStateRemnants)
            $androidAllocatorCleanupFact = Get-AndroidAllocatorCleanupFact -TargetID $androidTargetID
            $androidGenerationCleanupFacts = @(
                Get-ExactPathAbsenceFact -Path (Join-Path $androidTargetRoot "$androidTargetID\generations\1") -Kind "android-target-generation" -OwnerID $androidTargetID -Generation 1
                Get-ExactPathAbsenceFact -Path (Join-Path $androidTargetRoot "$androidTargetID\generations\2") -Kind "android-target-generation" -OwnerID $androidTargetID -Generation 2
            )
            $androidOwnedAVDPaths = @(
                Join-Path $ownedAVDRoot "$androidInitialRuntimeName.avd"
                Join-Path $ownedAVDRoot "$androidInitialRuntimeName.ini"
                Join-Path $ownedAVDRoot "$androidResetRuntimeName.avd"
                Join-Path $ownedAVDRoot "$androidResetRuntimeName.ini"
            )
            $androidOwnedAVDPathFacts = @($androidOwnedAVDPaths | ForEach-Object {
                Get-ExactPathAbsenceFact -Path $_ -Kind "managed-avd" -OwnerID $androidTargetID
            })
            Assert-Condition -Condition ($ownedAndroidRuntimeProcesses.Count -eq 0) -Message "managed Android cleanup left zero owned runtime processes"
            Assert-Condition -Condition ($ownedAVDEntries.Count -eq 0) -Message "managed Android cleanup left zero owned AVD entries"
            Assert-Condition -Condition (@($androidGenerationCleanupFacts | Where-Object { -not $_.absent }).Count -eq 0) -Message "managed Android cleanup removed both exact generation state directories"
            Assert-Condition -Condition (@($androidOwnedAVDPathFacts | Where-Object { -not $_.absent }).Count -eq 0) -Message "managed Android cleanup removed every exact owned AVD path"
            Assert-Condition -Condition $androidPortsReleased -Message "managed Android cleanup released both exact console/ADB port pairs"
            Assert-Condition -Condition $androidAmbientBaselineRestored -Message "whole ambient Android device/process identity set returned exactly to baseline"
            Assert-Condition -Condition ($androidUnboundLaunchRemnants.Count -eq 0) -Message "managed Android cleanup left no unbound launch-intent or PID-file remnants"
            Assert-Condition -Condition ($ownedAndroidStateRemnants.Count -eq 0) -Message "managed Android cleanup left no owned launch, process-ownership, or PID-file remnants"
            Assert-Condition -Condition ([bool]$androidAllocatorCleanupFact.empty -and @($androidAllocatorCleanupFact.target_allocations_remaining).Count -eq 0) -Message "managed Android cleanup released every durable emulator allocation in the isolated test registry"
            Complete-AndroidADBServerRestoration
            $androidTargetCleanupCompleted = $true

            $androidQualification = [ordered]@{
                system_image = [ordered]@{ package = $AndroidSystemImagePackage; digest = $AndroidSystemImageDigest }
                signed_apk = [ordered]@{
                    path = $AndroidSpecimenAPK
                    digest = $androidAPKDigest
                    size = $androidAPKFile.Length
                    signature_verified = $androidAPKSigned
                    signer = $androidAPKSignerDN
                    signature_log = $androidAPKSignatureLog
                }
                target_id = $androidTargetID
                target_run_id = $androidRunID
                bundle_id = $androidBundle.bundle_id
                logcat_observer = [ordered]@{
                    reference = $androidLogcatObserverReference
                    version = $androidLogcatObserverVersion
                    configuration_digest = $androidLogcatObserverConfigurationDigest
                    runtime_binding = [string]$androidLogcatObserverConfiguration.runtime_binding
                    signal_family = "android.logcat"
                    coverage = $androidLogcatCoverage[0]
                    marker = $androidLogcatMarker
                    active_transaction = $androidLogcatTransactionFact
                    finalized_transaction = $androidLogcatFinalizedTransactionFact
                    artifact = $androidLogcatArtifact
                    immutable_object_path = $androidLogcatObjectPath
                    immutable_object_digest_verified = ((Get-Sha256Reference -Path $androidLogcatObjectPath) -eq [string]$androidLogcatArtifact.digest)
                }
                reset_logcat_observer = [ordered]@{
                    reference = $androidLogcatObserverReference
                    version = $androidLogcatObserverVersion
                    configuration_digest = $androidLogcatObserverConfigurationDigest
                    runtime_binding = [string]$androidLogcatObserverConfiguration.runtime_binding
                    assigned_serial = $androidResetSerial
                    coverage = $androidResetLogcatCoverage[0]
                    gaps = @($androidResetLogcatGaps)
                    marker = $androidResetLogcatMarker
                    active_transaction = $androidResetLogcatTransactionFact
                    stable_post_crash_partial_identities = @($androidResetInterruptedPartialIdentities)
                    finalized_transaction = $androidResetLogcatFinalizedTransactionFact
                    artifact = $androidResetLogcatArtifact
                    immutable_object_path = $androidResetLogcatObjectPath
                    immutable_object_digest_verified = ((Get-Sha256Reference -Path $androidResetLogcatObjectPath) -eq [string]$androidResetLogcatArtifact.digest)
                    active_partial_path = $androidResetLogcatActivePartialPath
                    active_partial_reconciled = (-not (Test-Path -LiteralPath $androidResetLogcatActivePartialPath -PathType Leaf))
                    collector_process_identity = $androidResetLogcatCollectorProcessIdentity
                    collector_launch = $androidResetLogcatCollectorLaunchFact
                    collector_process_absent_after_daemon_crash = $androidResetLogcatCollectorAbsentAfterCrash
                    adb_server_identity_before_crash = $androidResetADBListenerIdentityBeforeCrash
                    adb_server_identity_after_crash = $androidResetADBListenerIdentityAfterCrash
                }
                reset_target_run_id = $androidResetRunID
                reset_bundle_id = $androidResetBundle.bundle_id
                assigned_serial = $expectedAndroidSerial
                console_port = $androidInitialConsolePort
                reset_assigned_serial = $androidResetSerial
                reset_console_port = $androidResetConsolePort
                adb_proxy = "127.0.0.1:$androidProxyPort"
                projected_apk = [ordered]@{ path = $projectedAndroidAPK; digest_equal = $projectedAPKDigestEqual; installed = $projectedAPKInstalled }
                reset_projected_fixture = [ordered]@{ path = $projectedAndroidResetFixture; digest_equal = $androidResetFixtureDigestEqual }
                app_report = $androidReport
                resources = [ordered]@{
                    contract = $androidResourceContract
                    committed_process_ownership = $androidResetOwnershipAfterRestart
                    independent_windows_os_verifications = @($androidOSVerifications)
                    data_partition_measurements = @($androidDataMeasurements)
                    every_generation_data_partition_exact = (@($androidDataMeasurements).Count -eq 2 -and @($androidDataMeasurements | Where-Object { -not $_.exact }).Count -eq 0)
                }
                boundaries = [ordered]@{
                    scoped_adb_only_listed_assigned_serial = $androidScopedSerialExact
                    projected_apk_digest_equal = $projectedAPKDigestEqual
                    projected_apk_installed = $projectedAPKInstalled
                    reset_fixture_digest_equal = $androidResetFixtureDigestEqual
                    app_report_boundary_exact = $androidReportBoundaryExact
                    target_lifecycle_coverage_available = $androidGeneration1LifecycleAvailable
                    real_logcat_coverage_available = ($androidLogcatCoverage.Count -eq 1)
                    real_logcat_marker_retained = ((Read-SharedUtf8Text -Path $androidLogcatObjectPath).IndexOf($androidLogcatMarker, [StringComparison]::Ordinal) -ge 0)
                    reset_logcat_coverage_truthfully_lost = ($androidResetLogcatCoverage.Count -eq 1 -and [string]$androidResetLogcatCoverage[0].status -eq "lost" -and [string]$androidResetLogcatCoverage[0].level -eq "none")
                    reset_logcat_marker_retained = ((Read-SharedUtf8Text -Path $androidResetLogcatObjectPath).IndexOf($androidResetLogcatMarker, [StringComparison]::Ordinal) -ge 0)
                    reset_logcat_excludes_generation_1_marker = ((Read-SharedUtf8Text -Path $androidResetLogcatObjectPath).IndexOf($androidLogcatMarker, [StringComparison]::Ordinal) -lt 0)
                    reset_logcat_collector_crash_contained = $androidResetLogcatCollectorAbsentAfterCrash
                    root_opaque_change_recorded = $androidRootOpaqueChangeRecorded
                    same_generation_run_denied = $androidSameGenerationRunDenied
                    reset_mode = $androidResetMode
                    reset_generation = [uint64]$resetAndroidTarget.current_generation
                    reset_replaced_process_identity = $androidResetReplacedProcessIdentity
                    reset_restart_before_run = $androidResetRestartContinuityProven
                    reset_clean_boot_removed_installed_package = $androidResetPackageAbsent
                }
                restart = [ordered]@{
                    daemon_port = $androidContinuityRestartPort
                    logical_state = [string]$recoveredAndroidResetGeneration.state
                    serial = [string]$androidResetOwnershipAfterRestart.serial
                    console_port = [int]$androidResetOwnershipAfterRestart.console_port
                    process_id = [int]$androidResetOwnershipAfterRestart.pid
                    executable_path = [string]$androidResetOwnershipAfterRestart.executable_path
                    process_start_token = [string]$androidResetOwnershipAfterRestart.start_token
                    resource_authority = [string]$androidResetOwnershipAfterRestart.resource_authority
                    resource_identity = [string]$androidResetOwnershipAfterRestart.resource_identity
                    resource_anchored = [bool]$androidResetOwnershipAfterRestart.resource_anchored
                    exact_runtime_identity_preserved = $androidResetRestartContinuityProven
                }
                active_stream_crash = [ordered]@{
                    crashed_daemon_pid = $androidCrashDaemonPID
                    proxy_pid = $androidResetProxyPID
                    adb_client_pid = $androidADBStreamPID
                    proxy_exit_code = $androidResetProxyExitCode
                    proxy_failure = ($androidResetProxyExitCode -ne 0)
                    proxy_failure_detail = $androidResetProxyFailureText.Trim()
                    adb_client_exit_code = $androidADBStreamExitCode
                    adb_client_failed_nonzero = ($androidADBStreamExitCode -ne 0)
                    daemon_identity_alive_immediately_before_kill = $androidCrashDaemonIdentityBeforeKill
                    collector_identity_alive_immediately_before_kill = $androidCrashCollectorIdentityBeforeKill
                    emulator_identity_alive_immediately_before_kill = $androidCrashEmulatorIdentityBeforeKill
                    emulator_identity_alive_immediately_after_kill_before_restart = $androidCrashEmulatorIdentityAfterKill
                    collector_exact_command_and_parent_observation = $androidResetLogcatCollectorLaunchFact
                    shared_adb_server_unchanged_across_kill = (Test-ExactProcessIdentityEqual -Left $androidResetADBListenerIdentityBeforeCrash -Right $androidResetADBListenerIdentityAfterCrash)
                    restart_daemon_port = $androidCrashRestartPort
                    recovered_run_state = [string]$recoveredAndroidCrashRun.state
                    recovered_bundle_id = [string]$androidResetBundle.bundle_id
                    control_plane_gap_count = $androidResetControlPlaneGaps.Count
                    unavailable_required_logcat_count = $androidResetLogcatCoverage.Count
                    unavailable_required_lifecycle_count = $androidResetUnavailableLifecycle.Count
                    exact_emulator_process_absent_before_destroy = $androidResetExactProcessAbsent
                    direct_adb_exit_code = [int]$androidResetADBFailure.exit_code
                    direct_adb_stdout = [string]$androidResetADBFailure.stdout
                    direct_adb_stderr = [string]$androidResetADBFailure.stderr
                    exact_serial_inventory_count_before_destroy = $androidResetADBDevicesAfterRecovery.Count
                    exact_serial_unreachable_before_destroy = $androidResetADBUnreachable
                }
                cleanup = [ordered]@{
                    target_destroyed = $androidTargetDestroyed
                    runtime_name = $androidResetRuntimeName
                    initial_runtime_name = $androidInitialRuntimeName
                    process_id = [int]$androidProcessOwnership.pid
                    process_start_token = [string]$androidProcessOwnership.start_token
                    owned_runtime_processes = $ownedAndroidRuntimeProcesses.Count
                    owned_avd_entries = $ownedAVDEntries.Count
                    console_and_adb_ports_released = $androidPortsReleased
                    ambient_before = [ordered]@{
                        adb_devices = @($androidAmbientBefore.adb_devices)
                        adb_device_inventory_source = $(if ($androidADBServerWasReachable) { "live-pre-test-server" } else { "live-test-started-server-after-device-discovery" })
                        adb_processes = @($androidAmbientBefore.adb_processes)
                        emulator_processes = @($androidAmbientBefore.emulator_processes)
                        global_avds = $androidAmbientBefore.global_avds
                    }
                    ambient_after = [ordered]@{
                        adb_devices = @($androidAmbientAfter.adb_devices)
                        adb_device_inventory_source = $(if ($androidADBServerWasReachable) { "live-preexisting-server-before-restoration" } else { "live-test-started-server-before-exact-stop" })
                        adb_processes = @($androidAmbientAfter.adb_processes)
                        emulator_processes = @($androidAmbientAfter.emulator_processes)
                        global_avds = $androidAmbientAfter.global_avds
                    }
                    bounded_ambient_identity_set_restored = ($androidAmbientBaselineRestored -and $androidADBServerIdentityRestored)
                    adb_server = [ordered]@{
                        reachable_before = $androidADBServerWasReachable
                        started_by_test = $androidADBServerStartedByTest
                        reachable_after = $androidADBServerReachableAfter
                        exact_state_restored = $androidADBServerIdentityRestored
                        listen_endpoint = "tcp:127.0.0.1:5037"
                        foreground_arguments = @("-L", "tcp:localhost:5037", "server", "nodaemon")
                        foreground_stdout_log = $androidADBServerStandardOutputPath
                        foreground_stderr_log = $androidADBServerStandardErrorPath
                        cleanup_authority = "retained System.Diagnostics.Process handle"
                        test_started_process_identity = $androidADBServerProcessIdentity
                        exact_test_started_process_absent_after = $(if ($androidADBServerStartedByTest) { $androidADBServerExactProcessStopConfirmed } else { $null })
                        device_inventory_after_test_server_start = @($androidADBDeviceInventoryAfterTestServerStart)
                        process_identities_before = @($androidADBProcessesBeforeTest)
                        process_identities_after = @($androidADBProcessesAfterRestoration)
                    }
                    generation_paths = @($androidGenerationCleanupFacts)
                    owned_avd_paths = @($androidOwnedAVDPathFacts)
                    unbound_launch_remnants = @($androidUnboundLaunchRemnants)
                    owned_state_remnants = @($ownedAndroidStateRemnants)
                    durable_allocator = $androidAllocatorCleanupFact
                }
            }
        }

        # Keep the pinned image available until every managed-Android physical
        # cleanup predicate is proven; failure recovery restarts the same
        # daemon/profile and therefore needs that exact digest.
        Complete-TestDockerImageCleanup

        # Persist independently measured predicates rather than success-path
        # literals. Every value below is derived from command output, durable
        # state, process inspection, or an exact filesystem/resource snapshot.
        $agentActiveExecObservedBeforeCrash = $activeAgentExecs.Count -eq 1
        $agentActiveExecAbsentAfterRecovery = $recoveredAgentProcesses -notmatch "/workspace/inputs/native-specimen.*-sleep.*10m"
        $targetActiveExecObservedBeforeCrash = $activeProcesses -match "/target/input/bin/native-specimen.*-sleep.*10m"
        $targetActiveExecAbsentAfterRecovery = $runningState -eq "false"
        $continuityGapRecorded = (
            @($recoveredCrashBundle.gaps | Where-Object { $_.detail -match "control-plane loss" }).Count -gt 0 -and
            @($recoveredCrashBundle.coverage | Where-Object { $_.signal_family -eq "target.lifecycle" -and $_.required -and $_.status -ne "available" }).Count -eq 1 -and
            @($recoveredCrashBundle.normalized_events | Where-Object { $_.kind -eq "target.run.control-plane-loss" }).Count -eq 1 -and
            @($recoveredCrashBundle.normalized_events | Where-Object { $_.kind -eq "target.run.control-plane-failure" }).Count -eq 1
        )
        $wrongRunScopeDenied = $wrongScope.exit_code -ne 0 -and $scopeFailure -match "(?i)(scope|not found|outside|run)"
        $pathTraversalDenied = $pathEscape.exit_code -ne 0 -and ($pathEscape.stdout + $pathEscape.stderr) -match "(?i)(relative|escape|path|traversal)"
        $oversizedTransferDenied = $oversized.exit_code -ne 0 -and ($oversized.stdout + $oversized.stderr) -match "(?i)(exceeds|resource exhausted|byte limit)"
        $interruptedRunReopenDenied = $reopenInterruptedRun.exit_code -ne 0 -and ($reopenInterruptedRun.stdout + $reopenInterruptedRun.stderr) -match "(?i)(failed|terminal|state|run)"
        $secondRunRequiresReset = $reuseWithoutReset.exit_code -ne 0 -and ($reuseWithoutReset.stdout + $reuseWithoutReset.stderr) -match '(?i)(must be reset|already has a run)'
        $abruptProcessStopDuringActiveRun = (
            $dockerCrashDaemonPID -gt 0 -and
            $agentActiveExecObservedBeforeCrash -and
            $targetActiveExecObservedBeforeCrash -and
            $agentCrashClientExitCode -ne 0 -and
            $targetCrashClientExitCode -ne 0
        )
        $interruptedRunRecoveredTerminalOnFirstRPC = (
            [string]$recoveredAgentExec.state -eq "lost" -and
            [bool]$recoveredAgentExec.cleanup_confirmed -and
            [string]$recoveredCrashRun.state -eq "failed" -and
            $targetActiveExecAbsentAfterRecovery
        )
        $releasedLeaseRecovered = $released.lease_id -eq $leaseID -and [string]$recoveredSession.lease.state -eq "released"
        $sealedBundlesRecovered = (
            [string]$recoveredBundle.bundle_id -eq [string]$bundle.bundle_id -and
            [string]$durableCrashBundle.bundle_id -eq [string]$recoveredCrashBundle.bundle_id
        )
        if ($ManagedAndroid) {
            $sealedBundlesRecovered = (
                $sealedBundlesRecovered -and
                [string]$durableAndroidBundle.bundle_id -eq [string]$androidBundle.bundle_id -and
                [string]$durableAndroidResetBundle.bundle_id -eq [string]$androidResetBundle.bundle_id
            )
        }

        $evidence = [ordered]@{
            schema_version = 4
            status = "passed"
            completed_at = (Get-Date).ToUniversalTime().ToString("o")
            run_directory = $runRoot
            image = [ordered]@{
                requested_tag = $requestedImageTag
                effective_per_run_tag = $ImageTag
                pinned_reference = $pinnedImage
                id = $testImageID
                preexisting_image_id = $testImageIDWasPreexisting
                tag_before = $imageTagBefore
                tag_after_cleanup = $imageTagAfter
                tag_mapping_unchanged = (Test-DockerImageTagStateEqual -Left $imageTagBefore -Right $imageTagAfter)
                preexisting_image_ids_before = @($dockerImageIDsBefore)
                image_ids_after_cleanup = @($dockerImageIDsAfterCleanup)
                preexisting_image_ids_missing = @($preexistingDockerImageIDsMissing)
                all_preexisting_image_ids_preserved = ($preexistingDockerImageIDsMissing.Count -eq 0)
                test_created_image_absent = (@($dockerImageIDsAfterCleanup) -notcontains $testImageID)
            }
            linux_process_lock = [ordered]@{
                binary_digest = $processLockTestDigest
                qualification_exit_code = [int]$processLockQualification.exit_code
                replacement_denied = $processLockReplacementDenied
                output_log = $processLockOutputLog
            }
            linux_cgroup = [ordered]@{
                expected = $linuxCgroupContract
                agent = $agentCgroupReport
                target = $targetCgroupReport
            }
            capabilities = $capabilityReport
            policy_digest = $policyDigest
            session_id = $sessionID
            lease_id = $leaseID
            target_id = $targetID
            target_run_id = $runID
            bundle_id = $bundle.bundle_id
			crash_recovery = [ordered]@{
				agent_exec_id = $agentCrashExecID
				agent_container_id = $agentCrashContainerID
				agent_final_state = $recoveredAgentExec.state
				agent_cleanup_confirmed = [bool]$recoveredAgentExec.cleanup_confirmed
				agent_active_exec_observed_before_crash = $agentActiveExecObservedBeforeCrash
				agent_active_exec_absent_after_recovery = $agentActiveExecAbsentAfterRecovery
				target_id = $crashTargetID
				target_run_id = $crashRunID
				container_id = $crashContainerID
				bundle_id = $recoveredCrashBundle.bundle_id
				incident_id = $crashIncidentID
				final_state = $recoveredCrashRun.state
				incident_state = $crashIncident.state
				active_exec_observed_before_crash = $targetActiveExecObservedBeforeCrash
				active_exec_absent_after_recovery = $targetActiveExecAbsentAfterRecovery
				continuity_gap_recorded = $continuityGapRecorded
				target_destroyed = $crashTargetDestroyed
			}
            capture_id = $capture.capture_id
            export_id = $declared.export_id
            artifact_references = @($committed.artifacts.reference)
            capture_artifacts = @($captureResult.artifacts.reference)
            boundaries = [ordered]@{
                linux_process_lock_replacement_denied = $processLockReplacementDenied
                target_host_probes_denied = ($verifiedTargetResult.exit_code -eq 0)
                wrong_run_scope_denied = $wrongRunScopeDenied
                path_traversal_denied = $pathTraversalDenied
                oversized_transfer_denied = $oversizedTransferDenied
				interrupted_run_reopen_denied = $interruptedRunReopenDenied
				detached_process_killed_at_container_boundary = $detachedBoundaryProven
                second_run_requires_reset = $secondRunRequiresReset
                linux_target_destroyed = $targetDestroyed
                no_exact_test_lease_docker_resources = $exactTestLeaseDockerCleanupProven
                no_tracked_test_docker_container_ids = $trackedDockerCleanupProven
            }
			docker_cleanup = [ordered]@{
				exact_test_lease_resource_count = $exactTestLeaseDockerResources.Count
				tracked_test_containers = @($dockerTrackedContainers)
				tracked_test_containers_remaining = @($trackedDockerContainersRemaining)
				ambient_baseline_preserved = $dockerAmbientBaselinePreserved
				ambient_before = @($dockerAmbientBefore)
				ambient_after = @($dockerAmbientAfter)
				unknown_concurrent_docker_resources_preserved = @($unknownConcurrentDockerResources)
			}
			local_state_cleanup = [ordered]@{
				agent_workspace_id = $agentWorkspaceID
				backing_workspace = $workspaceCleanupFacts
				linux_targets = @($linuxTargetCleanupFacts)
			}
			restart = [ordered]@{
				abrupt_process_stop_during_active_run = $abruptProcessStopDuringActiveRun
				interrupted_run_recovered_terminal_on_first_rpc = $interruptedRunRecoveredTerminalOnFirstRPC
				released_lease_recovered = $releasedLeaseRecovered
				sealed_bundles_recovered = $sealedBundlesRecovered
			}
        }
        if ($ManagedAndroid) {
            $evidence["android"] = $androidQualification
        }
        $sourceIdentityAfter = Get-RepositorySourceIdentity -ManifestPath (Join-Path $runRoot "source-manifest.after.json")
        Assert-Condition -Condition (
            [int]$sourceIdentityAfter.file_count -eq [int]$sourceIdentityBefore.file_count -and
            [string]$sourceIdentityAfter.manifest_digest -eq [string]$sourceIdentityBefore.manifest_digest
        ) -Message "repository source identity matched at the before/after qualification snapshots"
        $evidence["source_identity"] = [ordered]@{
            before = $sourceIdentityBefore
            after = $sourceIdentityAfter
            unchanged = $true
        }
        $testedBinaries = [ordered]@{
            worldctl = Get-FileEvidenceIdentity -Path $worldctl
            world_target = Get-FileEvidenceIdentity -Path $worldTarget
            world_capabilities = Get-FileEvidenceIdentity -Path $worldCapabilities
            world_guest = Get-FileEvidenceIdentity -Path (Join-Path $linuxBuildRoot "world-guest")
            world_idle = Get-FileEvidenceIdentity -Path (Join-Path $linuxBuildRoot "world-idle")
            native_specimen = Get-FileEvidenceIdentity -Path $nativeSpecimen
            process_lock_test = Get-FileEvidenceIdentity -Path $processLockTest
        }
        if ($ManagedAndroid) {
            $testedBinaries["windows_runtime_verifier"] = Get-FileEvidenceIdentity -Path $windowsRuntimeVerifier
        }
        $evidence["tested_binaries"] = $testedBinaries
        $completed = $true
    }
    finally {
        Pop-Location
    }
}
catch {
	$completed = $false
	Write-Warning "E2E failure line=$($_.InvocationInfo.ScriptLineNumber) stack=$($_.ScriptStackTrace)"
	throw
}
finally {
	foreach ($processToStop in @($androidADBStreamProcess, $androidADBProxyProcess)) {
		try {
			Stop-StartedProcess -Process $processToStop
		}
		catch {
			Add-FinalCleanupError -Message "failed to stop an E2E client/daemon process: $_"
		}
	}
	if ($null -ne $agentExecProcess) {
		try {
			Stop-StartedProcess -Process $agentExecProcess
		}
		catch {
			Add-FinalCleanupError -Message "failed to stop active-crash agent client: $_"
		}
	}
	if ($null -ne $targetExecProcess) {
		try {
			Stop-StartedProcess -Process $targetExecProcess
		}
		catch {
			Add-FinalCleanupError -Message "failed to stop active-crash target client: $_"
		}
	}
	if ($ManagedAndroid -and -not $completed -and -not $androidTargetCleanupCompleted -and $androidCreateAttempted) {
		try {
			Save-FailedManagedAndroidDiagnostics
		}
		catch {
			Add-FinalCleanupError -Message "failed to preserve bounded managed Android failure diagnostics: $_"
		}
		try {
			Remove-FailedManagedAndroidTargetExactly -TargetID ([string]$androidTargetID) -PolicyReference $policyDigest
		}
		catch {
			Add-FinalCleanupError -Message "failed to clean the exact managed Android target through the product path: $_"
		}
	}
	try {
		Stop-StartedProcess -Process $daemonProcess
	}
	catch {
		Add-FinalCleanupError -Message "failed to stop the E2E daemon process: $_"
	}
    if ($ManagedAndroid) {
        try {
            Stop-OwnedAndroidRuntimeProcesses
            $remainingAndroidProcesses = @(Get-OwnedAndroidRuntimeProcesses)
            if ($remainingAndroidProcesses.Count -gt 0) {
                Add-FinalCleanupError -Message "failed to stop $($remainingAndroidProcesses.Count) E2E-owned Android runtime process(es) for $androidRuntimeName"
            }
        }
        catch {
            Add-FinalCleanupError -Message "failed to inspect or stop E2E-owned Android runtime processes: $_"
        }
        try {
            $finalUnboundAndroidLaunchRemnants = @(Get-UnboundAndroidLaunchRemnants)
            foreach ($remnant in $finalUnboundAndroidLaunchRemnants) {
                Write-Warning "unbound managed Android launch remnants were preserved and not targeted in $($remnant.state_directory): $(@($remnant.remnant_paths) -join ', ')"
            }
            if ($completed -and $finalUnboundAndroidLaunchRemnants.Count -gt 0) {
                Add-FinalCleanupError -Message "successful E2E cleanup discovered unbound managed Android launch remnants"
            }
        }
        catch {
            Add-FinalCleanupError -Message "failed to inspect unbound managed Android launch remnants: $_"
        }
		if ($androidADBServerStartedByTest -and -not $androidADBServerRestorationCompleted) {
			try {
				Complete-AndroidADBServerRestoration
			}
			catch {
				Add-FinalCleanupError -Message "failed to restore the test-started ADB server: $_"
			}
		}
    }
	try {
		if ($dockerBuildStarted -and $null -ne $imageTagBefore -and [string]::IsNullOrWhiteSpace($testImageID)) {
			$currentTestTag = Get-DockerImageTagState -Tag $ImageTag
			if (-not $imageTagBefore.exists -and $currentTestTag.exists) {
				$testImageID = [string]$currentTestTag.id
				$testImageIDWasPreexisting = $dockerImageIDsBefore -contains $testImageID
			}
		}
	}
	catch {
		Add-FinalCleanupError -Message "failed to resolve the exact test Docker image before container cleanup: $_"
	}
	try {
		Register-ProcessLockDockerContainer
        $failureCleanupMountRoots = @{
            "agent-workspace" = Join-Path $runRoot "workspaces"
            "linux-target" = Join-Path $runRoot "targets"
        }
        if (-not [string]::IsNullOrWhiteSpace([string]$leaseID)) {
            foreach ($containerID in @(Get-TestLeaseWorldContainerIDs -LeaseID $leaseID)) {
				Register-TestDockerContainerID -ContainerID $containerID -Origin "failure-cleanup-lease-discovery" -ExpectedLeaseID $leaseID -ExpectedTestRun $dockerTestRunLabelValue -ExpectedImageID $testImageID -ExpectedMountRootsByRole $failureCleanupMountRoots
            }
        }
        if (-not [string]::IsNullOrWhiteSpace($testImageID) -and -not $testImageIDWasPreexisting) {
            Register-TestRunDockerContainersForCleanup -MountRootsByRole $failureCleanupMountRoots
        }
		$dockerCleanupErrors = @()
		foreach ($record in @($dockerTrackedContainers)) {
			try {
				Remove-TrackedTestDockerContainerChecked -Record $record
			}
			catch {
				$dockerCleanupErrors += "id=$([string]$record.id): $_"
				Write-Warning "failed to remove exact tracked Docker container $([string]$record.id); continuing with the remaining immutable-ID ledger: $_"
			}
		}
        $failureCleanupSnapshot = @(Get-DockerContainerSnapshot)
        $preservedConcurrentResources = @(Get-UnknownConcurrentDockerResources -Before $dockerAmbientBefore -After $failureCleanupSnapshot)
        foreach ($resource in $preservedConcurrentResources) {
            Write-Warning "preserved non-test Docker resource during failure cleanup: id=$($resource.id) name=$($resource.name) image=$($resource.image_id) status=$($resource.status)"
        }
		if ($dockerCleanupErrors.Count -gt 0) {
			throw "failed to remove $($dockerCleanupErrors.Count) exact tracked Docker container(s): $($dockerCleanupErrors -join '; ')"
		}
    }
    catch {
		Add-FinalCleanupError -Message "failed to inspect or remove exact tracked Docker containers: $_"
	}
	try {
		Complete-TestDockerImageCleanup
	}
	catch {
		Add-FinalCleanupError -Message "failed to restore the Docker image/tag boundary: $_"
    }
    if (-not $completed) {
        Write-Warning "E2E failed; preserving diagnostic run directory $runRoot"
    }
	elseif ($finalCleanupErrors.Count -gt 0) {
		throw "successful E2E encountered $($finalCleanupErrors.Count) final cleanup error(s): $($finalCleanupErrors -join '; ')"
	}
	else {
		# Publish success evidence only after the last fail-closed cleanup audit.
		# A failed invocation therefore cannot leave a green-looking artifact.
		Write-Utf8NoBom -Path $evidencePath -Content ($evidence | ConvertTo-Json -Depth 12)
		Write-Output $evidencePath
	}
}

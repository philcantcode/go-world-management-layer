param(
    [string]$AndroidSdk = "$env:LOCALAPPDATA\Android\Sdk",
    [string]$JavaHome = "C:\Program Files\Android\Android Studio\jbr",
    [string]$BuildToolsVersion = "35.0.0",
    [string]$PlatformVersion = "35",
    [string]$OutputDirectory = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path $projectRoot "build"
}
$buildTools = Join-Path $AndroidSdk "build-tools\$BuildToolsVersion"
$androidJar = Join-Path $AndroidSdk "platforms\android-$PlatformVersion\android.jar"
$aapt2 = Join-Path $buildTools "aapt2.exe"
$aapt = Join-Path $buildTools "aapt.exe"
$d8 = Join-Path $buildTools "d8.bat"
$apksigner = Join-Path $buildTools "apksigner.bat"
$javac = Join-Path $JavaHome "bin\javac.exe"
$keytool = Join-Path $JavaHome "bin\keytool.exe"
foreach ($required in @($androidJar, $aapt2, $aapt, $d8, $apksigner, $javac, $keytool)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required Android build tool is missing: $required"
    }
}

$compiled = Join-Path $OutputDirectory "compiled"
$generated = Join-Path $OutputDirectory "generated"
$classes = Join-Path $OutputDirectory "classes"
$dex = Join-Path $OutputDirectory "dex"
New-Item -ItemType Directory -Force -Path $compiled, $generated, $classes, $dex | Out-Null
$localAndroidJar = Join-Path $OutputDirectory "android.jar"
Copy-Item -LiteralPath $androidJar -Destination $localAndroidJar -Force

& $aapt2 compile --dir (Join-Path $projectRoot "res") -o (Join-Path $compiled "resources.zip")
if ($LASTEXITCODE -ne 0) { throw "aapt2 compile failed" }
$unsignedApk = Join-Path $OutputDirectory "world-specimen-unsigned.apk"
& $aapt2 link -o $unsignedApk -I $localAndroidJar --manifest (Join-Path $projectRoot "AndroidManifest.xml") --java $generated --min-sdk-version 23 --target-sdk-version $PlatformVersion --version-code 1 --version-name "1.0" (Join-Path $compiled "resources.zip")
if ($LASTEXITCODE -ne 0) { throw "aapt2 link failed" }

$sources = @(
    (Join-Path $projectRoot "src\dev\philcantcode\worldspecimen\MainActivity.java"),
    (Join-Path $generated "dev\philcantcode\worldspecimen\R.java")
)
& $javac -encoding UTF-8 -source 8 -target 8 -classpath $localAndroidJar -d $classes @sources
if ($LASTEXITCODE -ne 0) { throw "javac failed" }
$classFiles = Get-ChildItem -LiteralPath $classes -Recurse -Filter *.class | ForEach-Object { $_.FullName }
& $d8 --lib $localAndroidJar --min-api 23 --output $dex @classFiles
if ($LASTEXITCODE -ne 0) { throw "d8 failed" }
Push-Location $dex
try {
    & $aapt add $unsignedApk "classes.dex"
    if ($LASTEXITCODE -ne 0) { throw "aapt add failed" }
} finally {
    Pop-Location
}

$keystore = Join-Path $OutputDirectory "e2e-debug.keystore"
if (-not (Test-Path -LiteralPath $keystore)) {
    & $keytool -genkeypair -keystore $keystore -storepass android -keypass android -alias world-e2e -keyalg RSA -keysize 2048 -validity 3650 -dname "CN=World E2E,O=Local Test,C=GB"
    if ($LASTEXITCODE -ne 0) { throw "keytool failed" }
}
$signedApk = Join-Path $OutputDirectory "world-specimen.apk"
& $apksigner sign --ks $keystore --ks-pass pass:android --key-pass pass:android --ks-key-alias world-e2e --out $signedApk $unsignedApk
if ($LASTEXITCODE -ne 0) { throw "apksigner failed" }
& $apksigner verify --verbose $signedApk
if ($LASTEXITCODE -ne 0) { throw "apksigner verification failed" }
Write-Output $signedApk

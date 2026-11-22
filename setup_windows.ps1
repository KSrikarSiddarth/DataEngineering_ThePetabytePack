<#
  NYC MobilityPulse - one-time Windows setup (PowerShell 5.1+ or PowerShell 7).

  * creates a Python 3.11 venv (default %USERPROFILE%\.venvs\mp311) and installs the package + dev tools
  * checks for Java 17 (needed by Spark)
  * downloads winutils.exe + hadoop.dll into .\tools\hadoop\bin (needed by Spark to write files on Windows)

  Usage (from this folder):
      powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1
      powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1 -Venv C:\venvs\mp311
#>
param(
    [string]$Venv = (Join-Path $env:USERPROFILE ".venvs\mp311"),
    [string]$PythonVersion = "3.11",      # use 3.12 for the SetupF cluster: it must match Ubuntu 24.04's Python
    [string]$HadoopVersion = "hadoop-3.3.6"
)
$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Set-Location -LiteralPath $PSScriptRoot

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

# ------------------------------------------------------------------ Python 3.11
Step "Python $PythonVersion virtual environment at $Venv"
$venvPy = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPy)) {
    $created = $false
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -$PythonVersion -m venv "$Venv"
        if ($LASTEXITCODE -eq 0) { $created = $true }
    }
    if (-not $created) {
        $ver = (& python -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>$null)
        if ($ver -notin @("3.10", "3.11", "3.12")) {
            throw "Python $PythonVersion not found. Install it with:  winget install Python.Python.$PythonVersion   (then re-run this script)"
        }
        if ($ver -ne $PythonVersion) { Write-Warning "py -$PythonVersion not available; using 'python' ($ver). Python 3.11 is the tested version." }
        & python -m venv "$Venv"
        if ($LASTEXITCODE -ne 0) { throw "venv creation failed" }
    }
}
& $venvPy -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
& $venvPy -m pip install -e ".[dev]"
if ($LASTEXITCODE -ne 0) { throw "package install failed" }

# ------------------------------------------------------------------ Java 17
Step "Java 17 (Spark runtime)"
$javaOk = $false
if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
    $javaOk = $true; Write-Host "JAVA_HOME = $env:JAVA_HOME"
} else {
    $globs = "C:\Program Files\Eclipse Adoptium\jdk-17*", "C:\Program Files\Microsoft\jdk-17*",
             "C:\Program Files\Java\jdk-17*", "C:\Program Files\Zulu\zulu-17*", "C:\Program Files\Amazon Corretto\jdk17*"
    foreach ($g in $globs) {
        $hit = Get-ChildItem -Path $g -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending | Select-Object -First 1
        if ($hit) { $javaOk = $true; Write-Host "found JDK 17: $($hit.FullName) (picked up automatically)"; break }
    }
}
if (-not $javaOk) {
    Write-Warning "JDK 17 not found. Install it, then open a NEW terminal:"
    Write-Host   "    winget install EclipseAdoptium.Temurin.17.JDK" -ForegroundColor Yellow
}

# ------------------------------------------------------------------ winutils / hadoop.dll
Step "Hadoop Windows binaries (winutils.exe, hadoop.dll) -> tools\hadoop\bin"
$hbin = Join-Path $PSScriptRoot "tools\hadoop\bin"
New-Item -ItemType Directory -Force -Path $hbin | Out-Null
$base = "https://raw.githubusercontent.com/cdarlint/winutils/master/$HadoopVersion/bin"
foreach ($f in "winutils.exe", "hadoop.dll") {
    $dest = Join-Path $hbin $f
    if (Test-Path -LiteralPath $dest) { Write-Host "$f already present"; continue }
    Write-Host "downloading $f"
    Invoke-WebRequest -UseBasicParsing -Uri "$base/$f" -OutFile $dest
}

# ------------------------------------------------------------------ smoke test
Step "Smoke test (imports)"
$env:HADOOP_HOME = Join-Path $PSScriptRoot "tools\hadoop"
& $venvPy -c "import pyspark, pandas, pyarrow, fastapi, streamlit, confluent_kafka, mobilitypulse; print('pyspark', pyspark.__version__, '- imports OK')"
if ($LASTEXITCODE -ne 0) { throw "import check failed" }

Write-Host "`nSetup complete. Next steps:" -ForegroundColor Green
Write-Host "    .\mp test-fast      # unit tests"
Write-Host "    .\mp demo           # synthetic end-to-end run (no internet)"
Write-Host "    .\mp help           # all commands"
if ($Venv -ne (Join-Path $env:USERPROFILE ".venvs\mp311")) {
    Write-Host "Custom venv: set `$env:MP_VENV = '$Venv' before using .\mp" -ForegroundColor Yellow
}

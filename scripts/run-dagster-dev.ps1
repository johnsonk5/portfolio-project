$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$dagsterHome = Join-Path $repoRoot ".dagster_home"
$dagsterYaml = Join-Path $dagsterHome "dagster.yaml"
$dagsterExe = Join-Path $repoRoot ".venv\Scripts\dagster.exe"

if (-not (Test-Path $dagsterExe)) {
    throw "Dagster executable not found at $dagsterExe. Create the project's virtual environment first."
}

New-Item -ItemType Directory -Force -Path $dagsterHome | Out-Null

if (-not (Test-Path $dagsterYaml)) {
    New-Item -ItemType File -Path $dagsterYaml | Out-Null
}

$env:DAGSTER_HOME = $dagsterHome

Write-Host "Using DAGSTER_HOME=$dagsterHome"
& $dagsterExe dev -m portfolio_project.definitions

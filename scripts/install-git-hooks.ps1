$ErrorActionPreference = "Stop"

$repoRoot = (git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) {
    throw "Unable to determine repository root."
}

git config core.hooksPath .githooks
Write-Host "Configured git hooks for $repoRoot using .githooks/pre-commit"

<#
.SYNOPSIS
    Shift-left secret-scan gate. Fails the build when 1ESSecretScanning (SPMI /
    SEC101) reports a credential leak in shipping source, while allowing the
    sample credentials that legitimately live in test / benchmark / pipeline dirs.

.DESCRIPTION
    The 1ESSecretScanning task only emits SARIF *warnings*; on its own it never
    fails the run, and PostAnalysis@2's break filter does not cover it. This
    script reads the emitted SARIF, keeps SEC101* findings, drops anything under
    the paths already excluded by .config/CredScanSuppressions.json, and exits 1
    if any real finding remains. Path policy is kept in sync with that file.
#>
param(
    [string]$SourcesDir = $env:BUILD_SOURCESDIRECTORY,
    [string]$SearchRoot = $env:AGENT_BUILDDIRECTORY
)

$ErrorActionPreference = 'Stop'

# Directories where sample/dummy credentials are allowed (mirror of
# .config/CredScanSuppressions.json). Matched against repo-relative, '/'-normalized paths.
$excludedPrefixes = @('tests/', 'benchmarks/', 'eng/', 'OneBranchPipelines/', '.gdn/', '.git/')

# The scanner writes its SARIF under <BuildDir>/.gdn/.r/**. That directory is
# hidden, so a plain -Recurse would skip it (and could match an unrelated empty
# SARIF elsewhere) - search the known dir first, with -Force, then fall back.
$sarifFiles = @()
$gdnResults = Join-Path $SearchRoot '.gdn/.r'
if (Test-Path $gdnResults) {
    $sarifFiles = @(Get-ChildItem -Path $gdnResults -Recurse -Filter '*.sarif' -Force -ErrorAction SilentlyContinue)
}
if (-not $sarifFiles) {
    $sarifFiles = @(Get-ChildItem -Path $SearchRoot -Recurse -Filter '*.sarif' -Force -ErrorAction SilentlyContinue)
}
if (-not $sarifFiles) {
    Write-Host "No SARIF output found under '$SearchRoot' - secret scanner did not produce results."
    exit 0
}

Write-Host "Found $($sarifFiles.Count) SARIF file(s):"
$sarifFiles | ForEach-Object { Write-Host "  $($_.FullName)" }

$srcNorm = ($SourcesDir -replace '\\', '/').TrimEnd('/')
$violations = New-Object System.Collections.Generic.List[string]

foreach ($file in $sarifFiles) {
    try {
        $sarif = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json
    } catch {
        Write-Host "##[warning]Could not parse SARIF '$($file.FullName)': $_"
        continue
    }

    foreach ($run in @($sarif.runs)) {
        $rules = @($run.tool.driver.rules)
        Write-Host "  $($file.Name): $(@($run.results).Count) result(s), $($rules.Count) rule(s) in driver"
        foreach ($result in @($run.results)) {
            # Resolve the rule id across SARIF variants: result.ruleId, result.rule.id,
            # or result.ruleIndex -> tool.driver.rules[index].id
            $rid = $result.ruleId
            if (-not $rid -and $result.rule) { $rid = $result.rule.id }
            if (($rid -notmatch 'SEC101') -and ($null -ne $result.ruleIndex) -and $rules.Count -gt 0) {
                $idx = [int]$result.ruleIndex
                if ($idx -ge 0 -and $idx -lt $rules.Count) { $rid = $rules[$idx].id }
            }
            if ($rid -notmatch 'SEC101') { continue }

            foreach ($loc in @($result.locations)) {
                $uri = $loc.physicalLocation.artifactLocation.uri
                if (-not $uri) { continue }

                $rel = $uri -replace '^file:///', '' -replace '\\', '/'
                if ($srcNorm) { $rel = $rel -replace ('^' + [regex]::Escape($srcNorm) + '/'), '' }
                $rel = $rel.TrimStart('/')

                $skip = $false
                foreach ($prefix in $excludedPrefixes) {
                    if ($rel.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) { $skip = $true; break }
                }
                if ($skip) { continue }

                $line = $loc.physicalLocation.region.startLine
                $msg = $result.message.text
                $violations.Add("${rel}:${line}  ${rid}  $msg")
            }
        }
    }
}

if ($violations.Count -gt 0) {
    Write-Host "##[error]Secret scan found $($violations.Count) credential leak(s) in shipping source (outside allowed sample paths):"
    foreach ($v in $violations) { Write-Host "##[error]  $v" }
    Write-Host ""
    Write-Host "If a finding is an intentional sample credential, place it under an allowed path"
    Write-Host "(tests/, benchmarks/, eng/, OneBranchPipelines/) or add a Guardian baseline entry."
    exit 1
}

Write-Host "Secret scan gate passed: no SEC101 findings in shipping source."
exit 0

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

$sarifFiles = Get-ChildItem -Path $SearchRoot -Recurse -Filter '*.sarif' -ErrorAction SilentlyContinue
if (-not $sarifFiles) {
    Write-Host "No SARIF output found under '$SearchRoot' - secret scanner did not produce results."
    exit 0
}

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
        foreach ($result in @($run.results)) {
            if ($result.ruleId -notmatch '^SEC101') { continue }

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
                $violations.Add("${rel}:${line}  $($result.ruleId)  $msg")
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

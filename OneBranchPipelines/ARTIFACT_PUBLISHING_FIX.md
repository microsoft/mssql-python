# Artifact Publishing Fix - OneBranch Configuration

## Problem
Artifacts were not being published in NonOfficial builds. The pipeline showed "consumed" artifacts but no "published" artifacts.

## Root Cause
All three platform jobs (Windows, Linux, macOS) were using `isCustom: true` in their pool configuration, which **disables OneBranch's automatic artifact publishing** from `ob_outputDirectory`.

### Why `isCustom: true` Breaks Artifact Publishing
- `isCustom: true` tells OneBranch: "This job uses custom infrastructure outside OneBranch's control"
- OneBranch only automatically publishes `ob_outputDirectory` for **managed jobs** (where `isCustom` is `false` or not specified)
- With `isCustom: true`, you're responsible for explicitly publishing artifacts
- However, OneBranch restricts explicit artifact publishing tasks:
  - ❌ `PublishBuildArtifacts@1` - Old task, not allowed
  - ❌ `PublishPipelineArtifact@2` - Restricted in custom jobs
  - ✅ `PublishPipelineArtifact@1` - Allowed (but only needed for custom jobs)

## Solution
Follow the ODBC team's proven pattern:
1. **Windows/Linux**: Use OneBranch managed pools (remove `isCustom: true`) → Auto-publishing works
2. **macOS**: Keep `isCustom: true` for Microsoft-hosted agents + explicit `PublishPipelineArtifact@1`

## Changes Made

### Windows Job (`build-windows-job.yml`)
**Before:**
```yaml
pool:
  type: windows
  isCustom: true
  name: Django-1ES-pool
  vmImage: WIN22-SQL22
```

**After:**
```yaml
pool:
  type: windows
  # Removed isCustom: true to enable OneBranch auto-publishing from ob_outputDirectory
  # Using OneBranch managed Windows pool instead of custom Django-1ES-pool
```

**Impact:**
- ✅ OneBranch automatically publishes `$(ob_outputDirectory)` as `drop_Build_BuildWindowsWheels`
- ✅ Works for both Official and NonOfficial builds
- ✅ Artifact structure preserved: `wheels/`, `bindings/windows/`, `symbols/`

### Linux Job (`build-linux-job.yml`)
**Before:**
```yaml
pool:
  type: linux
  isCustom: true
  name: Django-1ES-pool
  demands:
  - imageOverride -equals ADO-UB22-SQL22
```

**After:**
```yaml
pool:
  type: linux
  # Removed isCustom: true to enable OneBranch auto-publishing from ob_outputDirectory
  # Using OneBranch managed Linux pool instead of custom Django-1ES-pool
```

**Impact:**
- ✅ OneBranch automatically publishes `$(ob_outputDirectory)` as `drop_Build_BuildLinuxWheels`
- ✅ Works for both Official and NonOfficial builds
- ✅ Artifact structure preserved: `wheels/`, `bindings/manylinux-*/`, `bindings/musllinux-*/`

### macOS Job (`build-macos-job.yml`)
**Before:**
```yaml
pool:
  type: linux
  isCustom: true
  name: Azure Pipelines
  vmImage: 'macOS-14'

# ... steps ...
# Note: OneBranch automatically publishes $(ob_outputDirectory) as artifact
```

**After:**
```yaml
pool:
  type: linux
  isCustom: true    # ← KEPT because using Microsoft-hosted agents
  name: Azure Pipelines
  vmImage: 'macOS-14'

# ... steps ...
- task: PublishPipelineArtifact@1  # ← ADDED explicit publish
  displayName: 'Publish macOS Artifacts'
  inputs:
    targetPath: '$(ob_outputDirectory)'
    artifact: 'drop_Build_BuildMacOSWheels'
    publishLocation: 'pipeline'
```

**Impact:**
- ✅ Explicitly publishes artifacts using `PublishPipelineArtifact@1`
- ✅ Works with Microsoft-hosted macOS agents (Azure Pipelines pool)
- ✅ Artifact structure preserved: `wheels/`, `bindings/macOS/`
- ℹ️ macOS requires `isCustom: true` because it uses Microsoft-hosted agents, not OneBranch infrastructure

## Why This Pattern Works

### OneBranch Managed Pools (Windows/Linux)
- OneBranch provides and controls the agent infrastructure
- Automatically publishes `ob_outputDirectory` to artifacts
- Uses standardized naming: `drop_<StageName>_<JobName>`
- Works for both Official and NonOfficial builds
- No explicit publish tasks needed

### Microsoft-Hosted Agents (macOS)
- Uses Azure Pipelines' Microsoft-hosted macOS agents
- Requires `isCustom: true` to indicate non-OneBranch infrastructure
- Must explicitly publish with `PublishPipelineArtifact@1`
- OneBranch allows `@1` version for custom jobs (but not `@2`)

## Verification
After this change, you should see **published** artifacts for all builds:

### NonOfficial Builds (PRs, Manual Runs)
- ✅ `drop_Build_BuildWindowsWheels` - 7 Python wheels (3.10-3.13, x64/ARM64)
- ✅ `drop_Build_BuildLinuxWheels` - 16 Python wheels (manylinux/musllinux × x86_64/aarch64)
- ✅ `drop_Build_BuildMacOSWheels` - 4 Python wheels (3.10-3.13, universal2)

### Official Builds
All of the above, plus:
- ✅ Symbols published to Azure DevOps Symbol Server
- ✅ Symbols published to Microsoft Symbol Service

## Artifact Structure
Each artifact contains organized subdirectories:

```
drop_Build_BuildWindowsWheels/
├── wheels/              ← .whl files for all Python versions
├── bindings/windows/    ← .pyd files (native extensions)
└── symbols/             ← .pdb files (debug symbols)

drop_Build_BuildLinuxWheels/
├── wheels/              ← .whl files for all Python versions
├── bindings/
│   ├── manylinux-x86_64/    ← .so files
│   ├── manylinux-aarch64/
│   ├── musllinux-x86_64/
│   └── musllinux-aarch64/

drop_Build_BuildMacOSWheels/
├── wheels/              ← .whl files for all Python versions
└── bindings/macOS/      ← .so files (universal2)
```

## Related Documentation
- [ACCESSING_ARTIFACTS.md](ACCESSING_ARTIFACTS.md) - How to download and use artifacts
- [SYMBOLS_EXPLAINED.md](SYMBOLS_EXPLAINED.md) - Understanding debug symbols
- [SYMBOL_PUBLISHING_INTEGRATION.md](SYMBOL_PUBLISHING_INTEGRATION.md) - Symbol publishing setup

## References
- ODBC Team's Pattern: `odbc_eng/templates/windows/windows-build.yml`, `odbc_eng/templates/mac/mac-stage.yml`
- OneBranch Documentation: https://aka.ms/obpipelines
- Pool Configuration: https://aka.ms/obpipelines/pools

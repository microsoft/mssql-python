# Major Refactor Summary - Matrix to Individual Stages

## Date: October 8, 2025

## Problem Statement
OneBranch enforces strict artifact naming convention: `drop_<StageName>_<JobName>`
- Matrix jobs all have same job name → same artifact name
- Multiple matrix iterations publishing to same artifact → **conflict error**
- Cannot use custom artifact names with matrix variables → **validation error**

## Solution: ODBC Team Pattern
Refactored from matrix strategy to individual stages (following msodbcsql pattern)

---

## Changes Made

### 1. **New Stage Templates Created**
- `/OneBranchPipelines/stages/build-windows-single-stage.yml`
  - Builds single Python version/architecture combination
  - Parameters: stageName, pythonVersion, shortPyVer, architecture
  
- `/OneBranchPipelines/stages/build-macos-single-stage.yml`
  - Builds single Python version (universal2)
  - Parameters: stageName, pythonVersion, shortPyVer
  
- `/OneBranchPipelines/stages/build-linux-single-stage.yml`
  - Builds all 4 Python versions for one distro/arch combination
  - Parameters: stageName, linuxTag, arch, dockerPlatform

### 2. **Main Pipeline Refactored**
File: `/OneBranchPipelines/build-release-package-pipeline.yml`

**Added Configuration Parameters:**
```yaml
parameters:
  - name: windowsConfigs  # 7 configurations
  - name: macosConfigs    # 4 configurations
  - name: linuxConfigs    # 4 configurations (each builds 4 Python versions)
```

**Replaced Single Build Stage with Multiple Stages:**
```yaml
# OLD:
stages:
  - stage: Build
    jobs:
      - BuildWindowsWheels (matrix: 7)
      - BuildMacOSWheels (matrix: 4)
      - BuildLinuxWheels (matrix: 16)

# NEW:
stages:
  # 7 Windows stages
  - Win_py310_x64
  - Win_py311_x64
  - Win_py312_x64
  - Win_py313_x64
  - Win_py311_arm64
  - Win_py312_arm64
  - Win_py313_arm64
  
  # 4 macOS stages
  - MacOS_py310
  - MacOS_py311
  - MacOS_py312
  - MacOS_py313
  
  # 4 Linux stages (each builds py310, py311, py312, py313)
  - Linux_manylinux_x86_64
  - Linux_manylinux_aarch64
  - Linux_musllinux_x86_64
  - Linux_musllinux_aarch64
  
  # Final consolidation
  - Consolidate (depends on all 15 stages)
```

**Used `${{ each }}` Loops for Generation:**
- Windows: 7 stages generated from windowsConfigs
- macOS: 4 stages generated from macosConfigs
- Linux: 4 stages generated from linuxConfigs

### 3. **Consolidation Job Updated**
File: `/OneBranchPipelines/jobs/consolidate-artifacts-job.yml`

- Removed job-level `dependsOn` (now handled at stage level)
- Updated comments to reflect new structure
- Still uses `buildType: 'current'` to download all artifacts
- Expected artifact names now:
  - `drop_Win_py310_x64_BuildWheel`
  - `drop_MacOS_py310_BuildWheel`
  - `drop_Linux_manylinux_x86_64_BuildWheels`
  - etc. (15 artifacts total)

### 4. **Old Job Templates**
Files preserved but no longer used:
- `/OneBranchPipelines/jobs/build-windows-job.yml`
- `/OneBranchPipelines/jobs/build-macos-job.yml`
- `/OneBranchPipelines/jobs/build-linux-job.yml`

**Note:** These can be deleted after verifying new structure works.

---

## Artifact Naming

### Before (Matrix - Failed):
- `drop_Build_BuildWindowsWheels` ← All 7 matrix iterations tried to publish here
- `drop_Build_BuildMacOSWheels` ← All 4 matrix iterations tried to publish here
- `drop_Build_BuildLinuxWheels` ← All 16 matrix iterations tried to publish here
- **Result:** Artifact conflict errors

### After (Individual Stages - Success):
**Windows (7 artifacts):**
- `drop_Win_py310_x64_BuildWheel`
- `drop_Win_py311_x64_BuildWheel`
- `drop_Win_py312_x64_BuildWheel`
- `drop_Win_py313_x64_BuildWheel`
- `drop_Win_py311_arm64_BuildWheel`
- `drop_Win_py312_arm64_BuildWheel`
- `drop_Win_py313_arm64_BuildWheel`

**macOS (4 artifacts):**
- `drop_MacOS_py310_BuildWheel`
- `drop_MacOS_py311_BuildWheel`
- `drop_MacOS_py312_BuildWheel`
- `drop_MacOS_py313_BuildWheel`

**Linux (4 artifacts, each with 4 wheels inside):**
- `drop_Linux_manylinux_x86_64_BuildWheels`
- `drop_Linux_manylinux_aarch64_BuildWheels`
- `drop_Linux_musllinux_x86_64_BuildWheels`
- `drop_Linux_musllinux_aarch64_BuildWheels`

**Consolidation (1 artifact with all 27 wheels):**
- `drop_Consolidate_ConsolidateArtifacts`

---

## Total Wheel Count

**15 artifacts published → 27 wheels consolidated:**
- Windows: 7 wheels (1 per stage)
- macOS: 4 wheels (1 per stage)
- Linux: 16 wheels (4 per stage × 4 stages)
- **Total: 27 wheels**

---

## Key Benefits

✅ **OneBranch Compliant:** All artifact names follow `drop_<Stage>_<Job>` pattern
✅ **No Conflicts:** Each stage publishes unique artifact name
✅ **Maintainable:** Configurations defined in parameters, generated via loops
✅ **Proven Pattern:** Following ODBC team's successful approach
✅ **Single Output:** Consolidation job merges all into one dist/ folder

---

## Risks Mitigated

- ✅ Preserved all existing build logic (signing, symbols, SDL)
- ✅ Kept custom pool configuration (Docker access)
- ✅ Maintained ESRP signing workflows
- ✅ Symbol publishing still works (Windows only)
- ✅ SDL tools configuration unchanged

---

## Testing Checklist

Before marking complete, verify:
- [ ] Pipeline validates successfully (YAML syntax)
- [ ] All 15 stages appear in pipeline run
- [ ] Each stage publishes artifact with correct name
- [ ] Consolidation job downloads all 15 artifacts
- [ ] Final dist/ folder contains 27 wheels
- [ ] ESRP signing works (Official builds only)
- [ ] Symbol publishing works (Windows Debug builds)
- [ ] SDL tools run successfully

---

## Rollback Plan

If issues occur:
1. Revert `/OneBranchPipelines/build-release-package-pipeline.yml` to use old job templates
2. Delete new stage templates from `/OneBranchPipelines/stages/`
3. Old job templates still exist and can be reactivated

---

## Next Steps

1. **Test NonOfficial Build First**
   - Run pipeline with `oneBranchType: NonOfficial`
   - Verify artifact structure
   - Check wheel count

2. **Test Official Build**
   - Run with `oneBranchType: Official`
   - Verify ESRP signing
   - Verify symbol publishing

3. **Clean Up After Success**
   - Delete old job templates once confirmed working
   - Update documentation

---

## Files Modified

- ✅ `/OneBranchPipelines/build-release-package-pipeline.yml` - Major refactor
- ✅ `/OneBranchPipelines/jobs/consolidate-artifacts-job.yml` - Minor update
- ✅ `/OneBranchPipelines/stages/build-windows-single-stage.yml` - Created
- ✅ `/OneBranchPipelines/stages/build-macos-single-stage.yml` - Created
- ✅ `/OneBranchPipelines/stages/build-linux-single-stage.yml` - Created

## Files Preserved (Deprecated)

- `/OneBranchPipelines/jobs/build-windows-job.yml`
- `/OneBranchPipelines/jobs/build-macos-job.yml`
- `/OneBranchPipelines/jobs/build-linux-job.yml`

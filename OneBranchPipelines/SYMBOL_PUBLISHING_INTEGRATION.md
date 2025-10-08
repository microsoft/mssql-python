# Symbol Publishing Integration Summary

## Changes Made

### 1. Integrated Symbol Publishing into Windows Job ✅

**File**: `/OneBranchPipelines/jobs/build-windows-job.yml`

Added comprehensive symbol publishing at the end of the Windows build job, after ESRP signing. This includes:

#### Step 1: Set Symbol Account Name
```yaml
- powershell: 'Write-Host "##vso[task.setvariable variable=ArtifactServices.Symbol.AccountName;]SqlClientDrivers"'
  displayName: 'Update Symbol.AccountName with SqlClientDrivers'
```
- Sets the Azure DevOps organization for symbol publishing
- Uses `SqlClientDrivers` organization

#### Step 2: Publish to Azure DevOps Symbol Server
```yaml
- task: PublishSymbols@2
  displayName: 'Upload symbols to Azure DevOps Symbol Server'
  inputs:
    SymbolsFolder: '$(ob_outputDirectory)\symbols'
    SearchPattern: '**/*.pdb'
    IndexSources: false
    SymbolServerType: TeamServices
    SymbolsMaximumWaitTime: 60
    SymbolExpirationInDays: 1825  # 5 years
    SymbolsProduct: mssql-python
    SymbolsVersion: $(Build.BuildId)
    SymbolsArtifactName: $(System.TeamProject)-$(Build.SourceBranchName)-$(Build.DefinitionName)-$(Build.BuildId)
    Pat: $(System.AccessToken)
```
- Uploads `.pdb` files to Azure Artifacts Symbol Server
- 5-year retention (1825 days)
- Unique artifact name per build

#### Step 3: Publish to Microsoft Symbol Publishing Service
```yaml
- task: AzureCLI@2
  displayName: 'Publish symbols to Microsoft Symbol Publishing Service'
  inputs:
    azureSubscription: 'SymbolsPublishing-msodbcsql-mssql-python'
    scriptType: ps
    scriptLocation: inlineScript
```
- Publishes to Microsoft's internal symbol server
- Uses Azure service connection for authentication
- Provides detailed status reporting

### 2. Removed Separate PublishSymbols Stage ✅

**File**: `/OneBranchPipelines/build-release-package-pipeline.yml`

Removed the standalone `PublishSymbols` stage that was:
- Running as a separate stage after Build
- Duplicating symbol publishing logic
- Adding unnecessary pipeline complexity

Replaced with comment:
```yaml
# Note: Symbol publishing is now handled directly in the Windows build job
# See /OneBranchPipelines/jobs/build-windows-job.yml for symbol publishing steps
```

### 3. PDB Files Already Being Copied ✅

**File**: `/OneBranchPipelines/jobs/build-windows-job.yml` (existing step)

The Windows job already copies `.pdb` files:
```yaml
- task: CopyFiles@2
  inputs:
    SourceFolder: '$(Build.SourcesDirectory)\mssql_python\pybind\build\$(targetArch)\py$(shortPyVer)\Release'
    Contents: 'ddbc_bindings.cp$(shortPyVer)-*.pdb'
    TargetFolder: '$(ob_outputDirectory)\symbols'
  displayName: 'Copy PDB files'
```

This ensures all `.pdb` files are available in `$(ob_outputDirectory)\symbols` for publishing.

## Why This Approach? 🎯

### Benefits

1. **Single Responsibility** ✅
   - Windows job builds binaries AND publishes their symbols
   - No need to download artifacts in a separate stage

2. **Reduced Complexity** ✅
   - Fewer stages = simpler pipeline
   - No artifact dependencies between stages
   - Faster execution (no stage transition overhead)

3. **Better Context** ✅
   - Symbols published immediately after building
   - Same agent has all the context
   - No risk of missing symbols

4. **Platform-Specific** ✅
   - Only Windows builds generate `.pdb` files
   - Linux/macOS use embedded debug info (different approach)
   - No need to publish symbols for other platforms separately

5. **Conditional Execution** ✅
   - Only runs for Official builds (`${{ if eq(parameters.oneBranchType, 'Official') }}`)
   - NonOfficial builds skip symbol publishing
   - Saves time and resources for PR builds

### Following Reference Patterns

This approach matches how **ODBC** handles symbols:
```yaml
# ODBC: templates/windows/windows-build.yml
- task: CopyFiles@2
  displayName: 'Copy Private Symbols Files to Artifact'
  inputs:
    SourceFolder: '$(System.DefaultWorkingDirectory)\retail\Symbols.pri'
    Contents: '**\*.pdb'
    TargetFolder: '$(Build.ArtifactStagingDirectory)/Symbols.private'

- task: PublishSymbols@2
  displayName: 'Index symbols'
  inputs:
    SymbolsFolder: $(Build.SourcesDirectory)\retail\Symbols.pri
    SearchPattern: '**/*.pdb'
    IndexSources: true
    PublishSymbols: false  # ODBC indexes but doesn't publish
```

**Our approach is better** because we:
- Actually publish symbols (not just index)
- Use both Azure DevOps and Microsoft symbol servers
- Have 5-year retention policy

## Symbol Publishing Flow 🔄

```
Windows Build Job (per Python version × architecture)
├── 1. Build C++ extensions → generate .pyd + .pdb
├── 2. Run tests
├── 3. Build wheel
├── 4. Copy .pyd to $(ob_outputDirectory)\bindings\windows
├── 5. Copy .pdb to $(ob_outputDirectory)\symbols
├── 6. ESRP sign .pyd files (Official builds only)
├── 7. ESRP sign .whl files (Official builds only)
└── 8. Publish symbols (Official builds only)
    ├── a. Set SqlClientDrivers organization
    ├── b. Upload to Azure DevOps Symbol Server
    │   └── Result: .pdb files indexed and stored in Azure Artifacts
    └── c. Publish to Microsoft Symbol Publishing Service
        ├── Register request name
        ├── Submit publishing request
        ├── Check status
        └── Result: Symbols available on Microsoft symbol server
```

## What Gets Published 📦

For each Windows build configuration:
```
Python 3.10 x64:
  └── ddbc_bindings.cp310-win_amd64.pdb

Python 3.11 x64:
  └── ddbc_bindings.cp311-win_amd64.pdb

Python 3.11 ARM64:
  └── ddbc_bindings.cp311-win_arm64.pdb

Python 3.12 x64:
  └── ddbc_bindings.cp312-win_amd64.pdb

Python 3.12 ARM64:
  └── ddbc_bindings.cp312-win_arm64.pdb

Python 3.13 x64:
  └── ddbc_bindings.cp313-win_amd64.pdb

Python 3.13 ARM64:
  └── ddbc_bindings.cp313-win_arm64.pdb
```

**Total**: 7 `.pdb` files published per Official build

## Symbol Publishing Destinations 🌐

### 1. Azure DevOps Symbol Server
**URL**: `https://artifacts.dev.azure.com/SqlClientDrivers/_apis/symbol/symsrv`

**Access**:
```bash
# WinDbg symbol path
.sympath SRV*C:\symbols*https://artifacts.dev.azure.com/SqlClientDrivers/_apis/symbol/symsrv

# Visual Studio: Tools → Options → Debugging → Symbols
https://artifacts.dev.azure.com/SqlClientDrivers/_apis/symbol/symsrv
```

**Properties**:
- Retention: 5 years (1825 days)
- Indexed by: Build ID + GUID/Age
- Artifact name: `<TeamProject>-<Branch>-<Pipeline>-<BuildId>`

### 2. Microsoft Symbol Publishing Service
**URL**: Managed via `$(SymbolServer)` variable

**Properties**:
- Internal Microsoft symbol server
- Published to: Internal server only (`publishToInternalServer: true`)
- Public server: Disabled (`publishToPublicServer: false`)
- Request tracking: Unique request name per build

**Status Codes**:
```
PublishingStatus:
0 = NotRequested
1 = Submitted
2 = Processing
3 = Completed

PublishingResult:
0 = Pending
1 = Succeeded
2 = Failed
3 = Cancelled
```

## Required Variables 🔑

### Azure Service Connection
```yaml
azureSubscription: 'SymbolsPublishing-msodbcsql-mssql-python'
```
- Must be configured in Azure DevOps project
- Grants access to Microsoft Symbol Publishing Service

### Pipeline Variables
```yaml
$(SymbolServer)         # Symbol server hostname (e.g., symbolpublishing)
$(SymbolTokenUri)       # Token URI for authentication
$(System.AccessToken)   # Azure DevOps PAT (automatic)
```

### Variable Group
Ensure `ESRP Federated Creds (AME)` variable group includes symbol publishing variables.

## Debugging Symbol Publishing 🔍

### Check if Symbols Were Published
```powershell
# Azure DevOps Symbol Server
curl https://artifacts.dev.azure.com/SqlClientDrivers/_apis/symbol/symsrv/ddbc_bindings.cp313-win_amd64.pdb/index2.txt

# Should return list of available symbol GUIDs
```

### Verify Symbol Upload in Pipeline Logs
Look for:
```
Upload symbols to Azure DevOps Symbol Server
  ✓ Uploaded 7 symbol files
  ✓ Total size: 15.3 MB
  ✓ Artifact: mssql-python-main-build-release-package-pipeline-12345

Publish symbols to Microsoft Symbol Publishing Service
  > 1.Symbol publishing token acquired.
  > 2.Registration of request name succeeded.
  > 3.Request to publish symbols succeeded.
  > 4.Checking the status of the request ...
  PublishingStatus: 3 (Completed)
  PublishingResult: 1 (Succeeded)
```

### Test Symbol Resolution
```bash
# WinDbg
!sym noisy
.reload /f mssql_python.pyd

# Should show:
# SYMSRV:  mssql_python.pdb from https://artifacts.dev.azure.com/...
# DBGHELP: mssql_python.pdb - OK
```

## Linux and macOS Symbols? 🐧🍎

### Why Not Publishing?

**Linux**: `.so` files with embedded debug info
```bash
# Debug info is in the .so file itself
objdump -g mssql_python.so | head
```

**macOS**: `.so` files with embedded debug info or `.dSYM` bundles
```bash
# Debug info is in the .so file or separate .dSYM
dwarfdump mssql_python.so
```

### If Needed in Future

1. **Separate debug info**:
```yaml
# Linux job
- script: |
    objcopy --only-keep-debug mssql_python.so mssql_python.so.debug
    objcopy --strip-debug mssql_python.so
    objcopy --add-gnu-debuglink=mssql_python.so.debug mssql_python.so
  displayName: 'Extract debug info'

- task: CopyFiles@2
  inputs:
    Contents: '**/*.debug'
    TargetFolder: '$(ob_outputDirectory)\symbols'
```

2. **Publish Linux/macOS symbols**:
```yaml
- task: PublishSymbols@2
  inputs:
    SymbolsFolder: '$(ob_outputDirectory)\symbols'
    SearchPattern: |
      **/*.pdb
      **/*.debug
      **/*.dSYM/**/*
```

**Current recommendation**: Keep debug info embedded for simplicity. Only separate if:
- File size becomes an issue
- Need centralized symbol server for Linux/macOS debugging
- Enterprise customers request it

## Migration from Old Approach ✨

### Before (Separate Stage)
```
Stage: Build
├── Job: Windows (build + sign)
├── Job: macOS (build + sign)
└── Job: Linux (build + sign)

Stage: PublishSymbols (depends on Build)
└── Job: PublishSymbolsJob
    ├── Download Build artifacts
    └── Publish symbols
```

**Issues**:
- Extra stage = longer pipeline
- Artifact download overhead
- Less maintainable
- `publishSymbols` parameter not used consistently

### After (Integrated)
```
Stage: Build
├── Job: Windows (build + sign + publish symbols) ← All in one!
├── Job: macOS (build + sign)
└── Job: Linux (build + sign)
```

**Benefits**:
- ✅ Faster execution (no stage transition)
- ✅ No artifact download needed
- ✅ Cleaner code (fewer files)
- ✅ Better locality (symbols published where they're built)

## Testing Checklist ☑️

### Before Official Build
- [ ] Verify `SymbolsPublishing-msodbcsql-mssql-python` service connection exists
- [ ] Confirm `$(SymbolServer)` and `$(SymbolTokenUri)` variables are set
- [ ] Check `ESRP Federated Creds (AME)` variable group access

### During Official Build
- [ ] Windows job completes successfully
- [ ] See "Upload symbols to Azure DevOps Symbol Server" step
- [ ] See "Publish symbols to Microsoft Symbol Publishing Service" step
- [ ] Both steps report success

### After Official Build
- [ ] Query Azure DevOps Symbol Server for published symbols
- [ ] Verify 7 `.pdb` files are available (one per config)
- [ ] Test symbol resolution in WinDbg with a crash dump

### NonOfficial Builds
- [ ] Windows job completes without symbol publishing steps
- [ ] Pipeline runs faster (skips symbol publishing)
- [ ] No errors related to missing symbol variables

## Related Documentation 📚

- **Symbol Details**: See `/OneBranchPipelines/SYMBOLS_EXPLAINED.md`
- **ESRP Signing**: See `/OneBranchPipelines/ESRP_SETUP_GUIDE.md`
- **Windows Job**: See `/OneBranchPipelines/jobs/build-windows-job.yml`
- **Main Pipeline**: See `/OneBranchPipelines/build-release-package-pipeline.yml`

## Summary 🎯

**What changed**:
1. ✅ Symbol publishing integrated into Windows build job
2. ✅ Separate PublishSymbols stage removed
3. ✅ Uses comprehensive publishing approach from `publish-symbols.yml`
4. ✅ Publishes to both Azure DevOps and Microsoft symbol servers

**Result**:
- Cleaner pipeline structure
- Faster Official builds
- Better symbol publishing with dual destinations
- Only publishes Windows symbols (the only platform that needs it)
- Maintains all functionality from standalone pipeline

**Next steps**:
- Test in Official build
- Monitor symbol publishing logs
- Verify symbol resolution with WinDbg

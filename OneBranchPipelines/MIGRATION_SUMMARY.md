# OneBranch Migration - Implementation Summary

**Date:** October 1, 2025  
**Project:** mssql-python  
**Status:** ✅ **COMPLETED** - Ready for Testing

---

## 📋 What Was Delivered

### 1. Complete OneBranch Pipeline Structure
✅ **Main Pipeline**: `OneBranchPipelines/build-whl-pipeline.yml`
- Uses `extends` pattern with OneBranch.Official.CrossPlat.yml
- Implements proper `globalSdl` configuration with 12+ security tools
- Supports both Official and NonOfficial build types
- Includes parameters for flexible builds (signing, SDL, symbols)

### 2. Modular Job Templates (3 files)
✅ `jobs/build-windows-job.yml` - Windows x64/ARM64 builds
✅ `jobs/build-macos-job.yml` - macOS universal2 builds  
✅ `jobs/build-linux-job.yml` - Linux manylinux/musllinux builds

Each job template includes:
- Complete build matrix for Python 3.10-3.13
- Malware scanning integration
- Conditional ESRP code signing (Official builds only)
- OneBranch artifact publishing via `ob_outputDirectory`

### 3. Reusable Step Templates (2 files)
✅ `steps/malware-scanning-step.yml` - Component governance
✅ `steps/compound-esrp-code-signing-step.yml` - ESRP signing for DLLs and packages

### 4. Variable Templates (5 files)
✅ `variables/common-variables.yml` - Build configuration
✅ `variables/onebranch-variables.yml` - OneBranch settings
✅ `variables/build-variables.yml` - Build paths and tools
✅ `variables/signing-variables.yml` - ESRP signing config
✅ `variables/symbol-variables.yml` - Symbol publishing

### 5. SDL Configuration Files (3 files)
✅ `.config/CredScanSuppressions.json` - Credential scan suppressions
✅ `.config/PolicheckExclusions.xml` - PoliCheck exclusions
✅ `.config/tsaoptions.json` - TSA configuration

### 6. Documentation
✅ `OneBranchPipelines/README.md` - Comprehensive guide with:
- Quick start instructions
- Prerequisites and setup
- Troubleshooting guide
- Pre-production checklist

---

## 📊 File Count

| Category | Count | Details |
|----------|-------|---------|
| **Pipeline Files** | 1 | Main OneBranch pipeline |
| **Job Templates** | 3 | Windows, macOS, Linux |
| **Step Templates** | 2 | Malware scan, ESRP signing |
| **Variable Templates** | 5 | Common, OneBranch, Build, Signing, Symbols |
| **SDL Config** | 3 | CredScan, PoliCheck, TSA |
| **Documentation** | 1 | Comprehensive README |
| **TOTAL** | **15 files** | Complete OneBranch migration |

---

## 🔐 Security Features Implemented

### Global SDL Configuration
- ✅ **ApiScan** - API vulnerability scanning
- ✅ **Armory** - Binary security analysis (breaks build on issues)
- ✅ **BinSkim** - Binary security analyzer (breaks build)
- ✅ **CodeInspector** - Source code security
- ✅ **CodeQL** - Semantic Python analysis
- ✅ **CredScan** - Credential scanning with suppressions
- ✅ **PoliCheck** - Inappropriate term checking (breaks build)
- ✅ **SBOM** - Software Bill of Materials generation
- ✅ **TSA** - Threat and Security Assessment (Official only)

### Code Signing
- ✅ **Native binaries** (.pyd, .so, .dylib) - SigntoolSign
- ✅ **Package files** (.whl) - NuGetSign
- ✅ **Conditional** - Official builds only

---

## 🏗️ Build Coverage

### Platforms & Architectures
| Platform | Python Versions | Architectures | Total Builds |
|----------|-----------------|---------------|--------------|
| Windows | 3.10, 3.11, 3.12, 3.13 | x64, ARM64 | 7 |
| macOS | 3.10, 3.11, 3.12, 3.13 | universal2 | 4 |
| Linux | 3.10, 3.11, 3.12, 3.13 | x86_64, aarch64 (manylinux + musllinux) | 16 |
| **TOTAL** | | | **27 builds** |

---

## 🎯 Key Improvements Over Classic Pipeline

| Aspect | Classic Pipeline | OneBranch Pipeline |
|--------|------------------|-------------------|
| **Structure** | Monolithic (~900 lines) | Modular (15 files, avg ~150 lines each) |
| **Maintainability** | Hard to modify | Easy to update individual components |
| **Security** | Manual SDL tasks | Automatic via `globalSdl` |
| **Signing** | Manual ESRP calls | Integrated via templates |
| **Artifacts** | `PublishBuildArtifacts` tasks | Automatic via `ob_outputDirectory` |
| **Compliance** | Optional | Mandatory for Official builds |
| **Reusability** | None (copy-paste) | High (templates) |
| **Pool Config** | `vmImage: 'windows-latest'` | `type: windows` (OneBranch standard) |

---

## 🚀 Next Steps for Testing

### Phase 1: NonOfficial Build Test (Week 1)
1. **Setup Prerequisites**:
   - [ ] Create `build-secrets` variable group in Azure DevOps
   - [ ] Add `DB_PASSWORD` secret variable
   - [ ] Request access to OneBranch.Pipelines/GovernedTemplates repository

2. **Run Test Build**:
   ```yaml
   parameters:
     oneBranchType: 'NonOfficial'
     signingEnabled: false
     runSdlTasks: true
   ```

3. **Validation**:
   - [ ] Pipeline runs without errors
   - [ ] All 27 builds complete successfully
   - [ ] Artifacts are published to `ob_outputDirectory`
   - [ ] SDL scans complete (review warnings/errors)

### Phase 2: Configure Signing (Week 2)
1. **Setup ESRP**:
   - [ ] Create ESRP service connection
   - [ ] Add all signing variables to `build-secrets` group:
     - SigningAppRegistrationClientId
     - SigningAppRegistrationTenantId
     - SigningAuthAkvName
     - SigningAuthSignCertName
     - SigningEsrpClientId
     - SigningEsrpConnectedServiceName

2. **Test Signing** (NonOfficial):
   ```yaml
   parameters:
     oneBranchType: 'NonOfficial'
     signingEnabled: true
   ```

3. **Validation**:
   - [ ] ESRP signing completes successfully
   - [ ] Signed artifacts (.pyd, .so, .whl) are valid
   - [ ] Verify signatures on binaries

### Phase 3: Configure TSA & Symbol Publishing (Week 3)
1. **TSA Configuration**:
   - [ ] Update `.config/tsaoptions.json` with your org details
   - [ ] Configure notification aliases
   - [ ] Set up area/iteration paths

2. **Symbol Publishing**:
   - [ ] Add symbol publishing variables to `build-secrets`:
     - SymbolsAzureSubscription
     - SymbolsPublishServer
     - SymbolsPublishProjectName
     - SymbolsPublishTokenUri
     - SymbolsUploadAccount

3. **Validation**:
   - [ ] TSA creates work items for security issues
   - [ ] Symbols are published to symbol server
   - [ ] Debug symbols are accessible

### Phase 4: Official Build (Week 4)
1. **Run Official Build**:
   ```yaml
   parameters:
     oneBranchType: 'Official'
     signingEnabled: true
     publishSymbols: true
     runSdlTasks: true
   ```

2. **Validation**:
   - [ ] Build completes successfully
   - [ ] All SDL tools run without breaking build
   - [ ] TSA work items created
   - [ ] SBOM generated
   - [ ] Artifacts signed and published
   - [ ] Symbols published

### Phase 5: Production Rollout (Week 5-6)
1. **Monitoring**:
   - [ ] Monitor first 3-5 production builds
   - [ ] Review TSA dashboard regularly
   - [ ] Address SDL suppressions as needed

2. **Documentation**:
   - [ ] Update README with organization-specific details
   - [ ] Document any custom suppressions added
   - [ ] Train team on new pipeline structure

3. **Deprecation**:
   - [ ] Disable classic pipeline triggers
   - [ ] Archive `eng/pipelines/build-whl-pipeline.yml`
   - [ ] Update all documentation references

---

## ⚠️ Important Notes

### Variables That Need Your Configuration

**In `build-secrets` variable group:**
```yaml
# Database (for testing)
DB_PASSWORD: "YourSecurePassword"

# ESRP Signing
SigningAppRegistrationClientId: "your-app-id"
SigningAppRegistrationTenantId: "your-tenant-id"
SigningAuthAkvName: "your-keyvault-name"
SigningAuthSignCertName: "your-cert-name"
SigningEsrpClientId: "your-esrp-client-id"
SigningEsrpConnectedServiceName: "YourESRPConnection"

# Symbol Publishing
SymbolsAzureSubscription: "your-subscription"
SymbolsPublishServer: "https://your-symbol-server"
SymbolsPublishProjectName: "your-project"
SymbolsPublishTokenUri: "https://your-token-uri"
SymbolsUploadAccount: "your-upload-account"
```

**In `.config/tsaoptions.json`:**
```json
{
  "notificationAliases": ["your-security-team@microsoft.com"],
  "codebaseAdmins": ["your-team@microsoft.com"],
  "instanceUrl": "https://dev.azure.com/YOUR_ORG",
  "projectName": "YOUR_PROJECT",
  "areaPath": "YOUR_AREA",
  "iterationPath": "YOUR_ITERATION"
}
```

---

## 📞 Support & Resources

### If You Encounter Issues

1. **Variable Group Issues**:
   - Verify group exists: Azure DevOps → Pipelines → Library
   - Check all required variables are set
   - Ensure secrets are marked as "secret"

2. **Repository Access Issues**:
   - Contact your OneBranch team for template access
   - Verify repository name: `OneBranch.Pipelines/GovernedTemplates`

3. **ESRP Signing Issues**:
   - Verify service connection exists
   - Check Azure Key Vault permissions
   - For testing: set `signingEnabled: false`

4. **SDL Breaking Build**:
   - Review scan results in pipeline logs
   - Add suppressions to `.config/` files
   - For urgent testing: set `runSdlTasks: false`

### Internal Microsoft Resources
- [OneBranch Documentation](https://aka.ms/obpipelines)
- [OneBranch SDL Guide](https://aka.ms/obpipelines/sdl)
- [ESRP Documentation](https://aka.ms/esrp)
- [OneBranch Teams Channel](https://teams.microsoft.com/l/channel/...)

---

## ✅ Migration Complete!

**All files created and ready for testing.**  
**Original pipeline (`eng/pipelines/`) remains untouched for rollback if needed.**

Good luck with testing! I'm here to help with any issues or questions. 🚀

---

_Generated by: Senior DevOps Engineer (AI Assistant)_  
_Date: October 1, 2025_

# OneBranch Pipeline for mssql-python

This directory contains the **OneBranch-migrated** version of the classic `build-whl-pipeline` with full security compliance.

## 🎯 What is OneBranch?

OneBranch is Microsoft's **secure software supply chain framework** that provides:
- ✅ **Built-in SDL compliance** (Security Development Lifecycle)
- ✅ **Automated code signing** (ESRP)
- ✅ **Symbol publishing** for debugging
- ✅ **Threat and Security Assessment** (TSA)
- ✅ **Software Bill of Materials** (SBOM)
- ✅ **Governed templates** and security scanning

## 📁 Directory Structure

```
OneBranchPipelines/
├── build-whl-pipeline.yml              # 🚀 Main OneBranch pipeline (extends pattern)
├── jobs/                                # Job templates (extracted for modularity)
│   ├── build-windows-job.yml           # Windows x64/ARM64 builds
│   ├── build-macos-job.yml             # macOS universal2 builds
│   └── build-linux-job.yml             # Linux manylinux/musllinux builds
├── steps/                               # Reusable step templates
│   ├── malware-scanning-step.yml       # Component governance & malware scan
│   └── compound-esrp-code-signing-step.yml  # ESRP code signing
└── variables/                           # Variable templates
    ├── common-variables.yml             # Common build variables
    ├── onebranch-variables.yml          # OneBranch-specific config
    ├── build-variables.yml              # Build paths and tools
    ├── signing-variables.yml            # ESRP signing configuration
    └── symbol-variables.yml             # Symbol publishing config
```

## 🚀 Quick Start

### Prerequisites

1. **Azure DevOps Variable Groups** - Create `build-secrets` variable group with:
   - `DB_PASSWORD`
   - `SigningAppRegistrationClientId`, `SigningAppRegistrationTenantId`
   - `SigningAuthAkvName`, `SigningAuthSignCertName`
   - `SigningEsrpClientId`, `SigningEsrpConnectedServiceName`
   - `SymbolsAzureSubscription`, `SymbolsPublishServer`, etc.

2. **OneBranch Repository Access**:
   - Access to `OneBranch.Pipelines/GovernedTemplates`

3. **Service Connections**:
   - ESRP service connection for code signing

### Pipeline Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `oneBranchType` | string | NonOfficial | Official (production) or NonOfficial (testing) |
| `runSdlTasks` | boolean | true | Run SDL security scanning |
| `signingEnabled` | boolean | true | Enable ESRP code signing |
| `publishSymbols` | boolean | true | Publish debug symbols |

## 🔒 Security Features

### SDL Tools Enabled

- **CodeQL** (Python semantic analysis)
- **CredScan** (Credential scanning)  
- **BinSkim** (Binary security analysis)
- **PoliCheck** (Term checking)
- **Armory** (Security scanning)
- **SBOM** (Software Bill of Materials)
- **TSA** (Official builds only)

## 🏗️ Build Matrix

- **Windows**: Python 3.10-3.13, x64/ARM64 (7 builds)
- **macOS**: Python 3.10-3.13, universal2 (4 builds)
- **Linux**: Python 3.10-3.13, manylinux/musllinux, x86_64/aarch64 (16 builds)

## 🧪 Testing

1. **Test with NonOfficial first**:
   ```bash
   # Set oneBranchType=NonOfficial, signingEnabled=false
   ```

2. **Verify artifacts** in `ob_outputDirectory/`:
   - `wheels/` - Python packages
   - `bindings/` - Native binaries
   - `symbols/` - Debug symbols

3. **Run Official build** when ready for production

## 🚨 Troubleshooting

| Issue | Solution |
|-------|----------|
| "Repository not found" | Request OneBranch repo access |
| "Variable group not found" | Create `build-secrets` group |
| "ESRP signing failed" | Verify service connection, or set `signingEnabled: false` for testing |
| "SDL breaks build" | Add suppressions to `.config/` files |

## 📚 Resources

- [OneBranch Documentation](https://aka.ms/obpipelines)
- [OneBranch SDL Guide](https://aka.ms/obpipelines/sdl)
- [ESRP Code Signing](https://aka.ms/esrp)

## ✅ Pre-Production Checklist

- [ ] Create variable groups with secrets
- [ ] Configure ESRP service connection
- [ ] Update `.config/tsaoptions.json`
- [ ] Test with NonOfficial build
- [ ] Review SDL scan results
- [ ] Run Official build with signing

---

**Original:** `eng/pipelines/build-whl-pipeline.yml` (unchanged)  
**Migrated:** `OneBranchPipelines/build-whl-pipeline.yml` (this directory)

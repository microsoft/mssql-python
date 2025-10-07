# OneBranch Pipeline Summary for mssql-python

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Pipeline Structure](#pipeline-structure)
4. [Parameters](#parameters)
5. [Variables](#variables)
6. [Build Matrix](#build-matrix)
7. [SDL Integration](#sdl-integration)
8. [Job Templates](#job-templates)
9. [Step Templates](#step-templates)
10. [Signing and Publishing](#signing-and-publishing)
11. [Usage Guide](#usage-guide)
12. [Troubleshooting](#troubleshooting)

---

## Overview

This document provides a comprehensive reference for the OneBranch build pipeline implementation for the `mssql-python` project. The pipeline builds Python wheels for multiple Python versions (3.10-3.13) across three platforms (Windows, macOS, Linux) with full SDL compliance.

### Key Facts
- **Project**: mssql-python (Python bindings for Microsoft SQL Server)
- **Package Type**: PyPI distribution (Python wheels)
- **OneBranch Version**: v2 (CrossPlat)
- **Build Types**: Official (with signing/TSA) and NonOfficial (testing)
- **Total Builds**: 27 wheel builds per run
- **SDL Tools**: 10 integrated (ApiScan, Armory, BinSkim, CodeInspector, CodeQL, CredScan, PoliCheck, SBOM, TSA, PublishLogs)

---

## Architecture

### OneBranch Framework Pattern

```yaml
# Main Pipeline extends OneBranch template
extends:
  template: v2/OneBranch.${{ parameters.oneBranchType }}.CrossPlat.yml@templates
  parameters:
    globalSdl: { ... }
    stages: [ ... ]
```

**Template Resolution**:
- `oneBranchType: 'Official'` → `v2/OneBranch.Official.CrossPlat.yml@templates`
- `oneBranchType: 'NonOfficial'` → `v2/OneBranch.NonOfficial.CrossPlat.yml@templates`

### Path Conventions

**CRITICAL**: All template paths must use absolute paths with leading slash when using the `extends` pattern:

```yaml
# ✅ CORRECT - Absolute path
- template: /OneBranchPipelines/jobs/build-windows-job.yml@self

# ❌ WRONG - Relative path (will fail)
- template: jobs/build-windows-job.yml
```

### Variable Syntax

**Compile-Time Variables** (evaluated during pipeline compilation):
```yaml
${{ variables.packageVersion }}      # Used in globalSdl parameters
${{ parameters.buildConfiguration }} # Used in template parameters
```

**Runtime Variables** (evaluated during pipeline execution):
```yaml
$(PACKAGE_VERSION)        # Used in build steps
$(BUILD_CONFIGURATION)    # Used in scripts
```

---

## Pipeline Structure

### File Organization

```
OneBranchPipelines/
├── build-release-package-pipeline.yml    # Main pipeline (229 lines)
├── jobs/
│   ├── build-windows-job.yml            # Windows x64/ARM64 builds (200 lines)
│   ├── build-macos-job.yml              # macOS universal2 builds
│   └── build-linux-job.yml              # Linux manylinux/musllinux builds
├── steps/
│   ├── malware-scanning-step.yml        # Malware scanning step
│   └── compound-esrp-code-signing-step.yml # ESRP signing step
└── variables/
    ├── common-variables.yml             # Common build variables
    ├── build-variables.yml              # Build-specific variables
    ├── onebranch-variables.yml          # OneBranch framework variables
    ├── signing-variables.yml            # Code signing variables
    └── symbol-variables.yml             # Symbol publishing variables
```

### Main Pipeline Flow

```
build-release-package-pipeline.yml
  ↓
Loads 5 Variable Templates
  ↓
Configures globalSdl (10 SDL tools)
  ↓
Build Stage
  ├── build-windows-job.yml (7 builds: Python 3.10-3.13 x64/ARM64)
  ├── build-macos-job.yml (4 builds: Python 3.10-3.13 universal2)
  └── build-linux-job.yml (16 builds: Python 3.10-3.13 manylinux/musllinux x86_64/aarch64)
    ↓
Each job runs:
  1. Setup environment (Python, dependencies)
  2. Build wheel (setup.py bdist_wheel)
  3. Run tests (pytest)
  4. Malware scanning (malware-scanning-step.yml)
  5. Code signing (compound-esrp-code-signing-step.yml) [if signingEnabled]
  6. Publish artifacts to ob_outputDirectory
```

---

## Parameters

### Pipeline Parameters (User-Facing)

```yaml
parameters:
- name: oneBranchType
  displayName: 'OneBranch Build Type'
  type: string
  default: 'NonOfficial'
  values:
    - Official      # Production builds (requires signing, TSA)
    - NonOfficial   # Testing builds (no signing required)

- name: buildConfiguration
  displayName: 'Build Configuration'
  type: string
  default: 'Release'
  values:
    - Release
    - Debug

- name: publishSymbols
  displayName: 'Publish Symbols'
  type: boolean
  default: true

- name: runSdlTasks
  displayName: 'Run SDL Tasks'
  type: boolean
  default: true

- name: signingEnabled
  displayName: 'Enable Code Signing'
  type: boolean
  default: true

- name: packageVersion
  displayName: 'Package Version (e.g., 0.13.0)'
  type: string
  default: '0.13.0'
```

### Parameter Usage

**NonOfficial Build (Testing)**:
```bash
# Use defaults (NonOfficial, Release, version 0.13.0)
az pipelines run --name build-release-package-pipeline
```

**Official Build (Production)**:
```yaml
# Set via UI or YAML trigger
oneBranchType: 'Official'
buildConfiguration: 'Release'
packageVersion: '0.14.0'  # Match setup.py version!
```

**Custom Version Build**:
```bash
# Specify version via CLI
az pipelines run --name build-release-package-pipeline \
  --parameters packageVersion='0.15.0-rc1'
```

---

## Variables

### Variable Templates Hierarchy

```yaml
variables:
# 1. Common variables (paths, versions, package name)
- template: /OneBranchPipelines/variables/common-variables.yml@self

# 2. OneBranch framework variables (CDP, SBOM)
- template: /OneBranchPipelines/variables/onebranch-variables.yml@self

# 3. Build-specific variables (build tools, configurations)
- template: /OneBranchPipelines/variables/build-variables.yml@self

# 4. Code signing variables (ESRP config - maps from 'ESRP Federated Creds (AME)' group)
- template: /OneBranchPipelines/variables/signing-variables.yml@self

# 5. Symbol publishing variables (symbol server config)
- template: /OneBranchPipelines/variables/symbol-variables.yml@self

# 6. External variable group (ESRP signing credentials)
- group: 'ESRP Federated Creds (AME)'

# 7. Pipeline-level variables (from parameters)
- name: PACKAGE_VERSION
  value: ${{ parameters.packageVersion }}
  readonly: true

- name: packageVersion  # Alias for SDL tools
  value: ${{ parameters.packageVersion }}
  readonly: true
```

### Key Variables

#### common-variables.yml
```yaml
variables:
- name: REPO_ROOT
  value: $(Build.SourcesDirectory)
  readonly: true

- name: ARTIFACT_PATH
  value: $(REPO_ROOT)/dist
  readonly: true

- name: BUILD_CONFIGURATION
  value: Release

- name: PYTHON_VERSIONS
  value: '3.10,3.11,3.12,3.13'

- name: PACKAGE_NAME
  value: 'mssql-python'
  readonly: true

# From external variable group
- group: 'ESRP Federated Creds (AME)'  # Contains ESRP signing credentials
```

**Note**: `PACKAGE_VERSION` is NOT defined in common-variables.yml anymore. It comes from the pipeline `packageVersion` parameter.

#### Package Version Strategy

**Two Forms for Different Contexts**:

1. **PACKAGE_VERSION** (runtime variable):
   - Used in build steps: `$(PACKAGE_VERSION)`
   - Available during script execution
   - Example: `echo "Building version $(PACKAGE_VERSION)"`

2. **packageVersion** (compile-time variable):
   - Used in SDL tools: `${{ variables.packageVersion }}`
   - Evaluated during pipeline compilation
   - Example: `softwareVersionNum: ${{ variables.packageVersion }}`

**Important**: The `packageVersion` parameter provides version metadata for SDL tools only. The actual PyPI package version is defined in `setup.py` (line 88: `version='0.13.0'`). Keep both in sync manually.

---

## Build Matrix

### Total Builds: 27 Wheels

#### Windows (7 builds)
```yaml
strategy:
  matrix:
    py310_x64:  { PYTHON_VERSION: '3.10', ARCHITECTURE: 'x64' }
    py311_x64:  { PYTHON_VERSION: '3.11', ARCHITECTURE: 'x64' }
    py312_x64:  { PYTHON_VERSION: '3.12', ARCHITECTURE: 'x64' }
    py313_x64:  { PYTHON_VERSION: '3.13', ARCHITECTURE: 'x64' }
    py310_arm64: { PYTHON_VERSION: '3.10', ARCHITECTURE: 'ARM64' }
    py311_arm64: { PYTHON_VERSION: '3.11', ARCHITECTURE: 'ARM64' }
    py313_arm64: { PYTHON_VERSION: '3.13', ARCHITECTURE: 'ARM64' }
```

**Key Steps**:
1. Setup Python version
2. Setup LocalDB (for testing)
3. Download ARM64 libs (for ARM64 builds)
4. Build wheel: `python setup.py bdist_wheel`
5. Run tests: `pytest tests/`
6. Malware scanning
7. ESRP signing (if enabled)
8. Copy to `ob_outputDirectory`

#### macOS (4 builds)
```yaml
strategy:
  matrix:
    py310: { PYTHON_VERSION: '3.10' }
    py311: { PYTHON_VERSION: '3.11' }
    py312: { PYTHON_VERSION: '3.12' }
    py313: { PYTHON_VERSION: '3.13' }
```

**Platform Tag**: `macosx_10_9_universal2` (Intel + Apple Silicon)

**Key Steps**:
1. Setup Python version
2. Install dependencies
3. Build wheel: `python setup.py bdist_wheel`
4. Run tests: `pytest tests/`
5. Malware scanning
6. ESRP signing (if enabled)
7. Copy to `ob_outputDirectory`

#### Linux (16 builds)
```yaml
strategy:
  matrix:
    # manylinux x86_64 (4 builds)
    py310_manylinux_x86_64: { PYTHON_VERSION: '3.10', PLATFORM: 'manylinux', ARCH: 'x86_64' }
    py311_manylinux_x86_64: { PYTHON_VERSION: '3.11', PLATFORM: 'manylinux', ARCH: 'x86_64' }
    py312_manylinux_x86_64: { PYTHON_VERSION: '3.12', PLATFORM: 'manylinux', ARCH: 'x86_64' }
    py313_manylinux_x86_64: { PYTHON_VERSION: '3.13', PLATFORM: 'manylinux', ARCH: 'x86_64' }
    
    # manylinux aarch64 (4 builds)
    py310_manylinux_aarch64: { PYTHON_VERSION: '3.10', PLATFORM: 'manylinux', ARCH: 'aarch64' }
    py311_manylinux_aarch64: { PYTHON_VERSION: '3.11', PLATFORM: 'manylinux', ARCH: 'aarch64' }
    py312_manylinux_aarch64: { PYTHON_VERSION: '3.12', PLATFORM: 'manylinux', ARCH: 'aarch64' }
    py313_manylinux_aarch64: { PYTHON_VERSION: '3.13', PLATFORM: 'manylinux', ARCH: 'aarch64' }
    
    # musllinux x86_64 (4 builds)
    py310_musllinux_x86_64: { PYTHON_VERSION: '3.10', PLATFORM: 'musllinux', ARCH: 'x86_64' }
    py311_musllinux_x86_64: { PYTHON_VERSION: '3.11', PLATFORM: 'musllinux', ARCH: 'x86_64' }
    py312_musllinux_x86_64: { PYTHON_VERSION: '3.12', PLATFORM: 'musllinux', ARCH: 'x86_64' }
    py313_musllinux_x86_64: { PYTHON_VERSION: '3.13', PLATFORM: 'musllinux', ARCH: 'x86_64' }
    
    # musllinux aarch64 (4 builds)
    py310_musllinux_aarch64: { PYTHON_VERSION: '3.10', PLATFORM: 'musllinux', ARCH: 'aarch64' }
    py311_musllinux_aarch64: { PYTHON_VERSION: '3.11', PLATFORM: 'musllinux', ARCH: 'aarch64' }
    py312_musllinux_aarch64: { PYTHON_VERSION: '3.12', PLATFORM: 'musllinux', ARCH: 'aarch64' }
    py313_musllinux_aarch64: { PYTHON_VERSION: '3.13', PLATFORM: 'musllinux', ARCH: 'aarch64' }
```

**Platform Tags**:
- `manylinux_2_17_x86_64` / `manylinux_2_17_aarch64`
- `musllinux_1_1_x86_64` / `musllinux_1_1_aarch64`

**Key Steps**:
1. Setup Python version
2. Install build tools
3. Build wheel: `python setup.py bdist_wheel`
4. Run tests: `pytest tests/`
5. Malware scanning
6. ESRP signing (if enabled)
7. Copy to `ob_outputDirectory`

---

## SDL Integration

### globalSdl Configuration

The pipeline integrates 10 SDL tools through the `globalSdl` section:

```yaml
globalSdl:
  # 1. API Security Scanning
  apiscan:
    softwareName: 'mssql-python'
    softwareVersionNum: ${{ variables.packageVersion }}
    isDeployable: true

  # 2. Security Risk Detection
  armory:
    enabled: ${{ parameters.runSdlTasks }}

  # 3. Binary Security Analysis
  binskim:
    enabled: ${{ parameters.runSdlTasks }}
    scanOutputDirectoryOnly: true

  # 4. Code Quality and Security
  codeInspector:
    enabled: ${{ parameters.runSdlTasks }}

  # 5. Code Vulnerability Scanning (Python + C++)
  codeql:
    language: python,cpp  # C++ for pybind11 bindings
    runSourceLanguagesInSourceAnalysis: true

  # 6. Credential Scanning
  credscan:
    enabled: ${{ parameters.runSdlTasks }}
    suppressionsFile: $(Build.SourcesDirectory)/.config/CredScanSuppressions.json

  # 7. Language Compliance
  policheck:
    enabled: ${{ parameters.runSdlTasks }}

  # 8. Software Bill of Materials
  sbom:
    enabled: true
    packageName: 'mssql-python'
    packageVersion: ${{ variables.packageVersion }}

  # 9. Threat Security Analysis (Official builds only)
  tsa:
    enabled: true
    configFile: $(Build.SourcesDirectory)/.config/tsaoptions.json

  # 10. Logging
  publishLogs:
    enabled: true
```

### SDL Tool Details

#### CodeQL (Static Analysis)
- **Languages**: `python,cpp`
- **Why C++?**: The project uses pybind11 to create Python bindings from C++ code
- **Scope**: Scans both Python wrapper code and C++ native extensions
- **Output**: Security vulnerabilities, code quality issues

#### ApiScan (API Security)
- **Purpose**: Validates API endpoints and security patterns
- **Version**: Uses `${{ variables.packageVersion }}` for tracking
- **Deployment**: Marked as `isDeployable: true` for production APIs

#### SBOM (Software Bill of Materials)
- **Purpose**: Generates complete dependency manifest
- **Package Info**: Uses `mssql-python` name and `${{ variables.packageVersion }}`
- **Format**: SPDX or CycloneDX
- **Usage**: Supply chain security, vulnerability tracking

#### CredScan (Credential Scanning)
- **Purpose**: Detects hardcoded secrets, passwords, API keys
- **Suppressions**: `.config/CredScanSuppressions.json`
- **Scope**: Entire source tree

#### TSA (Threat Security Analysis)
- **Mode**: Official builds only
- **Config**: `.config/tsaoptions.json`
- **Purpose**: Enterprise-grade threat modeling

---

## Job Templates

### build-windows-job.yml

**Purpose**: Build Python wheels for Windows x64 and ARM64 architectures.

**Parameters**:
```yaml
parameters:
- name: oneBranchType
  type: string
- name: signingEnabled
  type: boolean
- name: buildConfiguration
  type: string
```

**Pool**: `windows-2022`

**Matrix**: 7 builds (4 x64, 3 ARM64)

**Key Features**:
- LocalDB setup for SQL Server testing
- ARM64 library download from CDN
- Platform-specific build scripts (`build.bat`)
- Pytest validation with SQL Server connection
- Malware scanning
- ESRP code signing (if enabled)

**Artifacts**:
- Output: `$(ob_outputDirectory)/wheels/windows/`
- Format: `mssql_python-0.13.0-cp310-cp310-win_amd64.whl`

**Step Templates Used**:
```yaml
- template: /OneBranchPipelines/steps/malware-scanning-step.yml@self
  parameters:
    scanPath: $(ARTIFACT_PATH)

- template: /OneBranchPipelines/steps/compound-esrp-code-signing-step.yml@self
  parameters:
    filesToSign: '$(ARTIFACT_PATH)/*.whl'
    signingEnabled: ${{ parameters.signingEnabled }}
```

### build-macos-job.yml

**Purpose**: Build universal2 wheels for macOS (Intel + Apple Silicon).

**Parameters**:
```yaml
parameters:
- name: oneBranchType
  type: string
- name: signingEnabled
  type: boolean
- name: buildConfiguration
  type: string
```

**Pool**: `macOS-12`

**Matrix**: 4 builds (Python 3.10-3.13)

**Key Features**:
- Universal2 binary support (x86_64 + arm64)
- Homebrew dependencies
- macOS-specific build scripts (`build.sh`)
- Platform tag: `macosx_10_9_universal2`

**Artifacts**:
- Output: `$(ob_outputDirectory)/wheels/macos/`
- Format: `mssql_python-0.13.0-cp310-cp310-macosx_10_9_universal2.whl`

### build-linux-job.yml

**Purpose**: Build wheels for Linux manylinux and musllinux distributions.

**Parameters**:
```yaml
parameters:
- name: oneBranchType
  type: string
- name: signingEnabled
  type: boolean
- name: buildConfiguration
  type: string
```

**Pool**: `ubuntu-20.04`

**Matrix**: 16 builds (4 Python versions × 2 platforms × 2 architectures)

**Key Features**:
- Docker-based builds for consistent environments
- manylinux2014 compatibility
- musllinux support (Alpine Linux)
- Multi-architecture (x86_64, aarch64)

**Artifacts**:
- Output: `$(ob_outputDirectory)/wheels/linux/`
- Format: `mssql_python-0.13.0-cp310-cp310-manylinux_2_17_x86_64.whl`

---

## Step Templates

### malware-scanning-step.yml

**Purpose**: Scan build artifacts for malware and security threats.

**Parameters**:
```yaml
parameters:
- name: scanPath
  type: string
  description: 'Absolute path to directory or files to scan'
```

**Tool**: Windows Defender / Antimalware service

**Process**:
1. Scan all files in `$(ARTIFACT_PATH)`
2. Generate scan report
3. Fail build if threats detected
4. Publish results to pipeline

**Usage**:
```yaml
- template: /OneBranchPipelines/steps/malware-scanning-step.yml@self
  parameters:
    scanPath: $(ARTIFACT_PATH)
```

### compound-esrp-code-signing-step.yml

**Purpose**: Sign build artifacts using Enterprise Secure Release Process (ESRP).

**Parameters**:
```yaml
parameters:
- name: filesToSign
  type: string
  description: 'Glob pattern for files to sign (e.g., *.whl, *.dll)'
- name: signingEnabled
  type: boolean
  description: 'Whether signing is enabled'
```

**Signing Operations**:
1. **Python Wheels** (.whl):
   - Operation: `EsrpSign`
   - Certificate: Python Package Signing

2. **Windows Binaries** (.dll, .pyd):
   - Operation: `EsrpSign`
   - Certificate: Windows Authenticode

**Process**:
1. Authenticate to ESRP service
2. Upload files to signing service
3. Apply digital signature
4. Download signed artifacts
5. Verify signatures
6. Replace unsigned files

**Usage**:
```yaml
- template: /OneBranchPipelines/steps/compound-esrp-code-signing-step.yml@self
  parameters:
    filesToSign: '$(ARTIFACT_PATH)/*.whl'
    signingEnabled: ${{ parameters.signingEnabled }}
```

**Requirements**:
- Azure AD service principal (for Official builds)
- ESRP tenant configuration
- Signing certificate access
- Variable group: `ESRP Federated Creds (AME)`

---

## Signing and Publishing

### ESRP Code Signing Overview

**What is ESRP Code Signing?**

ESRP (Enterprise Secure Release Process) Code Signing is Microsoft's internal service for digitally signing software artifacts. It provides:

1. **Digital Signatures**: Cryptographically signs files (`.whl`, `.dll`, `.pyd`) to verify publisher identity
2. **Integrity Verification**: Ensures files haven't been tampered with after signing
3. **Trust Chain**: Establishes trust through Microsoft's code signing certificates
4. **Compliance**: Meets security and compliance requirements for official releases

**Two Types of ESRP Operations**:

1. **ESRP Code Signing** (This pipeline):
   - Signs individual binary files during build
   - Used for: Python wheels (`.whl`), DLLs (`.dll`), Python extensions (`.pyd`)
   - When: During build process (before artifact publishing)
   - Task: `EsrpCodeSigning@2` or custom signing steps

2. **ESRP Release** (Your `official-release-pipeline.yml`):
   - Publishes signed packages to distribution channels
   - Used for: PyPI package distribution
   - When: After build process (separate release pipeline)
   - Task: `EsrpRelease@9`

**Variable Group: `ESRP Federated Creds (AME)`**

Your existing variable group contains all necessary credentials for both code signing and release:

| Variable in Group | Used By | Purpose |
|------------------|---------|---------|
| `ESRPConnectedServiceName` | Both | Azure service connection to ESRP |
| `AuthAKVName` | Both | Azure Key Vault containing certificates |
| `AuthSignCertName` | Both | Certificate name in Key Vault |
| `EsrpClientId` | Both | ESRP client application ID |
| `DomainTenantId` | Both | Azure AD tenant ID |
| `owner` | Release only | Release approval owner |
| `approver` | Release only | Release approval reviewer |

**Variable Mapping**:

The OneBranch pipeline maps your existing ESRP variables to OneBranch naming convention:

```yaml
# In signing-variables.yml
SigningEsrpConnectedServiceName → $(ESRPConnectedServiceName)
SigningAuthAkvName → $(AuthAKVName)
SigningAuthSignCertName → $(AuthSignCertName)
SigningEsrpClientId → $(EsrpClientId)
SigningAppRegistrationClientId → $(EsrpClientId)
SigningAppRegistrationTenantId → $(DomainTenantId)
```

This means you don't need to create a new variable group - your existing `ESRP Federated Creds (AME)` group works for both pipelines!

### Code Signing Flow

```
Build Artifact (unsigned)
  ↓
Malware Scanning
  ↓
[If signingEnabled = true]
  ↓
ESRP Code Signing
  - Authenticate to ESRP
  - Upload artifact
  - Sign with certificate
  - Download signed artifact
  - Verify signature
  ↓
Copy to ob_outputDirectory
  ↓
OneBranch Framework publishes to CDP
```

### Artifact Publishing

**OneBranch Automatic Publishing**:
- **Location**: `ob_outputDirectory` (special OneBranch directory)
- **Destination**: Component Detection Platform (CDP)
- **Access**: Via Azure DevOps artifact browser
- **Retention**: Based on OneBranch retention policies

**Artifact Structure**:
```
ob_outputDirectory/
├── wheels/
│   ├── windows/
│   │   ├── mssql_python-0.13.0-cp310-cp310-win_amd64.whl
│   │   ├── mssql_python-0.13.0-cp311-cp311-win_amd64.whl
│   │   ├── ...
│   ├── macos/
│   │   ├── mssql_python-0.13.0-cp310-cp310-macosx_10_9_universal2.whl
│   │   ├── ...
│   └── linux/
│       ├── mssql_python-0.13.0-cp310-cp310-manylinux_2_17_x86_64.whl
│       ├── mssql_python-0.13.0-cp310-cp310-musllinux_1_1_x86_64.whl
│       ├── ...
└── logs/
    └── build-logs/
```

### PyPI Publishing (Not Implemented)

**Current State**: This pipeline builds and signs wheels but does NOT publish to PyPI.

**To Add PyPI Publishing**:
1. Create `publish-pypi-job.yml`
2. Use `twine upload` with service connection
3. Add to pipeline as separate stage after Build
4. Require Official build + manual approval

---

## Usage Guide

### Running NonOfficial Build (Testing)

**Via Azure DevOps UI**:
1. Navigate to Pipelines → `build-release-package-pipeline`
2. Click "Run pipeline"
3. Use defaults:
   - OneBranch Build Type: `NonOfficial`
   - Build Configuration: `Release`
   - Package Version: `0.13.0`
4. Click "Run"

**Via Azure CLI**:
```bash
az pipelines run \
  --name build-release-package-pipeline \
  --branch sharmag/onebranch_setup \
  --org https://dev.azure.com/your-org \
  --project mssql-python
```

**Expected Duration**: ~45 minutes (27 builds in parallel)

**Output**: 27 unsigned wheels in CDP artifacts

### Running Official Build (Production)

**Requirements**:
1. ✅ ESRP service principal configured (You already have this!)
2. ✅ `ESRP Federated Creds (AME)` variable group with pipeline permissions
3. ✅ TSA configuration file (`.config/tsaoptions.json`)
4. ✅ CredScan suppressions (`.config/CredScanSuppressions.json`)
5. ✅ Manual approval policy on Official builds (optional)

**Via Azure DevOps UI**:
1. Navigate to Pipelines → `build-release-package-pipeline`
2. Click "Run pipeline"
3. Set parameters:
   - OneBranch Build Type: `Official`
   - Build Configuration: `Release`
   - Enable Code Signing: `true`
   - Package Version: `0.14.0` (match setup.py!)
4. Click "Run"
5. Approve pre-deployment check (if configured)

**Expected Duration**: ~60 minutes (includes signing, TSA)

**Output**: 27 signed wheels + SBOM in CDP artifacts

**Note**: Your existing `ESRP Federated Creds (AME)` group contains all necessary signing credentials. Just grant the pipeline access to use it!

### Custom Version Build

**Scenario**: Building release candidate or hotfix version.

**Steps**:
1. Update `setup.py` version:
   ```python
   # setup.py line 88
   version='0.14.0-rc1',
   ```

2. Run pipeline with matching version:
   ```bash
   az pipelines run \
     --name build-release-package-pipeline \
     --parameters packageVersion='0.14.0-rc1'
   ```

3. Verify artifacts have correct version:
   ```bash
   # Artifact name should match
   mssql_python-0.14.0rc1-cp310-cp310-win_amd64.whl
   ```

**Important**: Always keep `setup.py` version and `packageVersion` parameter in sync!

### Debugging Build Failures

**Common Issues**:

1. **Test Failures**:
   - **Symptom**: Pytest exits with non-zero code
   - **Check**: `build-secrets` variable group has `DB_PASSWORD`
   - **Fix**: Ensure LocalDB (Windows) or Docker SQL (Linux/macOS) is running

2. **Signing Failures**:
   - **Symptom**: ESRP task fails with auth error
   - **Check**: Official build requires service principal
   - **Fix**: Use NonOfficial for testing, configure ESRP for Official

3. **Template Not Found**:
   - **Symptom**: `Template not found: jobs/build-windows-job.yml`
   - **Check**: Template path must be absolute (`/OneBranchPipelines/...@self`)
   - **Fix**: Add leading slash to all template paths

4. **Variable Not Found**:
   - **Symptom**: `${{ variables.packageVersion }} could not be found`
   - **Check**: Variable defined with `readonly: true` flag
   - **Fix**: Ensure variable defined before use in globalSdl

---

## Troubleshooting

### Pipeline Validation Errors

**Error**: `Template not found: ../steps/malware-scanning-step.yml`

**Cause**: Relative paths don't work with `extends` pattern.

**Solution**: Change to absolute path:
```yaml
# ❌ WRONG
- template: ../steps/malware-scanning-step.yml

# ✅ CORRECT
- template: /OneBranchPipelines/steps/malware-scanning-step.yml@self
```

---

**Error**: `Variable packageVersion could not be found`

**Cause**: Variable used in compile-time context before definition.

**Solution**: Define variable in main pipeline before globalSdl:
```yaml
variables:
- name: packageVersion
  value: ${{ parameters.packageVersion }}
  readonly: true

extends:
  template: v2/OneBranch.Official.CrossPlat.yml@templates
  parameters:
    globalSdl:
      sbom:
        packageVersion: ${{ variables.packageVersion }}  # Now available
```

---

**Error**: `Build name contains invalid characters`

**Cause**: Incorrect date format in name.

**Solution**: Use OneBranch format `$(Year:YY)$(DayOfYear)$(Rev:.r)`:
```yaml
# ❌ WRONG
name: $(Date:yyyyMMdd).$(Rev:r)

# ✅ CORRECT
name: $(Year:YY)$(DayOfYear)$(Rev:.r)
```

---

### Build Failures

**Error**: `MSSQL-Python tests failed: Connection refused`

**Cause**: Database not accessible during tests.

**Solution**:
1. For quick testing: Skip tests or use mock database
2. For full testing: Add `DB_PASSWORD` to `ESRP Federated Creds (AME)` group
3. For Windows: Ensure LocalDB starts successfully
4. For Linux: Check Docker SQL container health

---

**Error**: `ARM64 libraries not found`

**Cause**: ARM64 builds on Windows need special library download.

**Solution**: Check step in `build-windows-job.yml`:
```yaml
- script: |
    curl -o arm64_libs.zip https://cdn.example.com/mssql-python/arm64_libs.zip
    unzip arm64_libs.zip -d $(Build.SourcesDirectory)/libs/windows/ARM64/
  condition: eq(variables['ARCHITECTURE'], 'ARM64')
```

---

**Error**: `No files matched the search pattern *.whl`

**Cause**: Build failed before wheel creation.

**Solution**:
1. Check Python setup step succeeded
2. Verify dependencies installed
3. Review `setup.py bdist_wheel` output
4. Check platform-specific build script errors

---

### SDL Failures

**Error**: `CodeQL analysis failed: No Python code found`

**Cause**: CodeQL looking in wrong directory.

**Solution**: Verify `REPO_ROOT` points to source:
```yaml
variables:
- name: REPO_ROOT
  value: $(Build.SourcesDirectory)
```

---

**Error**: `CredScan found hardcoded credentials`

**Cause**: Test files contain example connection strings.

**Solution**: Add suppressions to `.config/CredScanSuppressions.json`:
```json
{
  "tool": "Credential Scanner",
  "suppressions": [
    {
      "file": "tests/test_connection.py",
      "justification": "Test fixtures with example passwords"
    }
  ]
}
```

---

**Error**: `TSA configuration not found`

**Cause**: Official build requires TSA config file.

**Solution**: Create `.config/tsaoptions.json`:
```json
{
  "codebaseName": "mssql-python",
  "notificationAliases": ["your-team@microsoft.com"],
  "tsaVersion": "TsaV2",
  "tsaEnvironment": "PROD"
}
```

---

### Signing Failures

**Error**: `ESRP authentication failed`

**Cause**: Official build without service principal.

**Solution**:
1. For testing: Use `oneBranchType: 'NonOfficial'` and `signingEnabled: false`
2. For production: Configure Azure AD service principal with ESRP access

---

**Error**: `Signing operation timed out`

**Cause**: ESRP service temporarily unavailable.

**Solution**: Retry the build. ESRP has transient failures occasionally.

---

## Advanced Topics

### Adding New Python Version

**Example**: Adding Python 3.14 support

**Steps**:

1. Update `common-variables.yml`:
```yaml
- name: PYTHON_VERSIONS
  value: '3.10,3.11,3.12,3.13,3.14'
```

2. Update Windows job matrix:
```yaml
py314_x64:
  PYTHON_VERSION: '3.14'
  ARCHITECTURE: 'x64'
```

3. Update macOS job matrix:
```yaml
py314:
  PYTHON_VERSION: '3.14'
```

4. Update Linux job matrix (4 new entries):
```yaml
py314_manylinux_x86_64:
  PYTHON_VERSION: '3.14'
  PLATFORM: 'manylinux'
  ARCH: 'x86_64'
# ... 3 more
```

5. Test with NonOfficial build
6. Verify all 31 builds succeed

---

### Adding Custom SDL Tool

**Example**: Adding Bandit (Python security linter)

**Steps**:

1. Add to `globalSdl`:
```yaml
globalSdl:
  customTool:
    name: 'Bandit'
    enabled: ${{ parameters.runSdlTasks }}
    command: 'bandit -r $(Build.SourcesDirectory)/mssql_python -f json -o $(Build.ArtifactStagingDirectory)/bandit-results.json'
```

2. Add result publishing:
```yaml
- task: PublishBuildArtifacts@1
  inputs:
    pathToPublish: $(Build.ArtifactStagingDirectory)/bandit-results.json
    artifactName: 'bandit-results'
```

---

### Customizing Build Configuration

**Debug Builds**:

Change parameter:
```yaml
buildConfiguration: 'Debug'
```

This propagates to:
- All job templates via `${{ parameters.buildConfiguration }}`
- Build scripts via `$(BUILD_CONFIGURATION)`
- Artifact paths via `$(BUILD_CONFIGURATION)` folder

**Custom Configurations**:

Add to parameter values:
```yaml
parameters:
- name: buildConfiguration
  values:
    - Release
    - Debug
    - Profile  # New option
```

Update build scripts to handle `Profile` configuration.

---

## References

### Key Files

- **Main Pipeline**: `OneBranchPipelines/build-release-package-pipeline.yml`
- **Variables**: `OneBranchPipelines/variables/common-variables.yml`
- **Windows Jobs**: `OneBranchPipelines/jobs/build-windows-job.yml`
- **macOS Jobs**: `OneBranchPipelines/jobs/build-macos-job.yml`
- **Linux Jobs**: `OneBranchPipelines/jobs/build-linux-job.yml`
- **Package Version Docs**: `OneBranchPipelines/PACKAGE_VERSION_IMPLEMENTATION.md`
- **Migration Summary**: `OneBranchPipelines/MIGRATION_SUMMARY.md`

### Documentation

- **OneBranch Learnings**: `OneBranch_Learnings/` directory
- **SqlClient Reference**: `sqlclient_eng/pipelines/`
- **This Document**: `OneBranchPipelines/OneBranch-mssql-python-summary.md`

### External Resources

- [OneBranch Documentation](https://aka.ms/onebranch)
- [Azure Pipelines YAML Schema](https://learn.microsoft.com/en-us/azure/devops/pipelines/yaml-schema/)
- [ESRP Documentation](https://aka.ms/esrp)
- [PyPI Packaging Guide](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/)

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-10-07 | Initial comprehensive documentation |

---

## Contact

For questions or issues with this pipeline:
- **Team**: Azure SQL Python Team
- **Repository**: [microsoft/mssql-python](https://github.com/microsoft/mssql-python)
- **Branch**: `sharmag/onebranch_setup`

---

**Last Updated**: October 7, 2025  
**Pipeline Version**: v2 OneBranch CrossPlat  
**Document Owner**: Gaurav Sharma

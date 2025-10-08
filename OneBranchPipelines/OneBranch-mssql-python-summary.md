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
11. [Artifact Publishing](#artifact-publishing)
12. [Symbol Publishing](#symbol-publishing)
13. [Usage Guide](#usage-guide)
14. [Troubleshooting](#troubleshooting)
15. [Change History](#change-history)

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

## Artifact Publishing

### The Docker Daemon Problem

**Issue**: OneBranch managed Linux pools don't have Docker daemon running or accessible.

**Error encountered**:
```bash
ERROR: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?
errors pretty printing info
##[error]Bash exited with code '1'.
```

**Why it matters**: The mssql-python project builds Linux wheels inside Docker containers (manylinux/musllinux) to ensure compatibility across different Linux distributions. Without Docker access, Linux builds fail completely.

### OneBranch Artifact Publishing Rules

**Two Publishing Methods**:

1. **Automatic Publishing** (OneBranch managed pools):
   - **Requirements**: Pool with `type: windows` or `type: linux` WITHOUT `isCustom: true`
   - **Behavior**: OneBranch automatically publishes `$(ob_outputDirectory)` to artifacts
   - **Artifact Name**: `drop_<Stage>_<Job>` (e.g., `drop_Build_BuildWindowsWheels`)
   - **Pros**: Zero configuration, automatic subdirectory preservation
   - **Cons**: Limited pool capabilities (no Docker on Linux)

2. **Explicit Publishing** (Custom pools):
   - **Requirements**: Pool with `isCustom: true` + `PublishPipelineArtifact@1` task
   - **Behavior**: Manual artifact publishing with explicit task
   - **Task Restrictions**: Must use `@1` version (not `@2`), `PublishBuildArtifacts@1` blocked
   - **Pros**: Access to custom agent capabilities (Docker, specific tools)
   - **Cons**: Requires explicit task configuration

### Implementation by Platform

#### Windows (Explicit Publishing)
```yaml
pool:
  type: windows
  isCustom: true
  name: Django-1ES-pool
  vmImage: WIN22-SQL22

# Explicit publishing required
- task: PublishPipelineArtifact@1
  displayName: 'Publish Windows Artifacts'
  inputs:
    targetPath: '$(ob_outputDirectory)'
    artifact: 'drop_Build_BuildWindowsWheels'
    publishLocation: 'pipeline'
```

**Why custom pool**: 
- ✅ **SQL Server Testing**: Django-1ES-pool provides LocalDB for integration tests
- ✅ **Specialized Build Tools**: Pre-configured with Visual Studio, CMake, etc.
- ✅ **Consistency**: All three platforms use explicit publishing pattern

#### macOS (Explicit Publishing)
```yaml
pool:
  type: linux
  isCustom: true
  name: Azure Pipelines
  vmImage: 'macOS-14'

# Explicit publishing required
- task: PublishPipelineArtifact@1
  displayName: 'Publish macOS Artifacts'
  inputs:
    targetPath: '$(ob_outputDirectory)'
    artifact: 'drop_Build_BuildMacOSWheels'
    publishLocation: 'pipeline'
```

**Why custom pool**: Microsoft-hosted macOS agents aren't OneBranch managed pools.

#### Linux (Explicit Publishing - Docker Required)
```yaml
pool:
  type: linux
  isCustom: true
  name: Django-1ES-pool
  demands:
  - imageOverride -equals ADO-UB22-SQL22

# Explicit publishing required
- task: PublishPipelineArtifact@1
  displayName: 'Publish Linux Artifacts'
  inputs:
    targetPath: '$(ob_outputDirectory)'
    artifact: 'drop_Build_BuildLinuxWheels'
    publishLocation: 'pipeline'
```

**Why custom pool**: 
1. ✅ **Docker Access**: Django-1ES-pool provides Docker-enabled agents
2. ✅ **manylinux Builds**: Requires Docker to run `quay.io/pypa/manylinux_*` containers
3. ✅ **musllinux Builds**: Requires Docker to run `quay.io/pypa/musllinux_*` containers
4. ❌ **No Auto-Publishing**: Custom pools disable OneBranch auto-publishing
5. ✅ **Explicit Task Works**: `PublishPipelineArtifact@1` allowed for custom jobs

### Expected Artifacts

After successful pipeline run, three artifacts published:

```
drop_Build_BuildWindowsWheels/
├── wheels/
│   ├── mssql_python-0.13.0-cp310-cp310-win_amd64.whl
│   ├── mssql_python-0.13.0-cp311-cp311-win_amd64.whl
│   ├── ... (7 total)
├── bindings/windows/
│   ├── ddbc_bindings.pyd
│   └── ... (.pyd files)
└── symbols/ (if publishSymbols: true)
    └── ... (.pdb files)

drop_Build_BuildLinuxWheels/
├── wheels/
│   ├── mssql_python-0.13.0-cp310-cp310-manylinux_2_28_x86_64.whl
│   ├── mssql_python-0.13.0-cp310-cp310-musllinux_1_2_x86_64.whl
│   ├── ... (16 total)
└── bindings/
    ├── manylinux-x86_64/
    ├── manylinux-aarch64/
    ├── musllinux-x86_64/
    └── musllinux-aarch64/

drop_Build_BuildMacOSWheels/
├── wheels/
│   ├── mssql_python-0.13.0-cp310-cp310-macosx_10_9_universal2.whl
│   ├── ... (4 total)
└── bindings/macOS/
    └── ... (.so files)
```

### Verification Steps

**After pipeline completes**:

1. Navigate to pipeline run → Artifacts
2. Verify 3 artifacts present:
   - ✅ `drop_Build_BuildWindowsWheels`
   - ✅ `drop_Build_BuildLinuxWheels`
   - ✅ `drop_Build_BuildMacOSWheels`
3. Download each artifact, verify subdirectories:
   - ✅ `wheels/` contains `.whl` files
   - ✅ `bindings/` contains native libraries
   - ✅ (Windows only) `symbols/` contains `.pdb` files

**Testing wheels**:
```bash
# Download artifact
az pipelines runs artifact download --artifact-name drop_Build_BuildWindowsWheels

# Install wheel
pip install wheels/mssql_python-0.13.0-cp310-cp310-win_amd64.whl

# Test import
python -c "import mssql_python; print(mssql_python.__version__)"
```

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

## Symbol Publishing

### Overview

**What are Symbols?**

Symbols (`.pdb` files on Windows) contain debugging information that maps compiled binary code back to source code. They enable:
- Stack trace debugging with source file names and line numbers
- Crash dump analysis in production environments
- Performance profiling with function names
- Security incident investigation

**Why Publish Symbols?**

For enterprise software like mssql-python, published symbols allow:
1. **Microsoft Support**: Diagnose customer issues without shipping debug builds
2. **Crash Analytics**: Understand production failures with full context
3. **Security Response**: Analyze security incidents efficiently
4. **Customer Debugging**: Enterprise customers can debug integration issues

### Symbol Publishing in OneBranch

**Platform Coverage**: Windows only (`.pdb` files)
- Linux/macOS: Debug symbols embedded in `.so` files (not separately published)
- Windows: Separate `.pdb` files published to symbol servers

**Two Symbol Destinations**:

1. **Azure DevOps Symbol Server** (Internal):
   - For team debugging during development
   - Retention: 5 years (configurable)
   - Access: Team members via Visual Studio or WinDbg
   - Task: `PublishSymbols@2`

2. **Microsoft Symbol Publishing Service** (Internal + Public):
   - For Microsoft-wide symbol sharing
   - Can publish to internal (default) and/or public symbol servers
   - Access: Via symbol server URL in debuggers
   - Task: `AzureCLI@2` with custom PowerShell script

### Implementation in Windows Job

**Symbols are collected during build**:
```yaml
# Copy PDB files to ob_outputDirectory
- task: CopyFiles@2
  inputs:
    SourceFolder: '$(Build.SourcesDirectory)\mssql_python\pybind\build\$(targetArch)\py$(shortPyVer)\Release'
    Contents: 'ddbc_bindings.cp$(shortPyVer)-*.pdb'
    TargetFolder: '$(ob_outputDirectory)\symbols'
  displayName: 'Copy PDB files'
```

**Symbol publishing steps** (Official builds only):

#### Step 1: Configure Symbol Account
```yaml
- powershell: 'Write-Host "##vso[task.setvariable variable=ArtifactServices.Symbol.AccountName;]SqlClientDrivers"'
  displayName: 'Update Symbol.AccountName with SqlClientDrivers'
```

Sets the Azure DevOps symbol account to `SqlClientDrivers` (shared with SQL Client team).

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
    SymbolExpirationInDays: 1825 # 5 years
    SymbolsProduct: mssql-python
    SymbolsVersion: $(Build.BuildId)
    SymbolsArtifactName: $(System.TeamProject)-$(Build.SourceBranchName)-$(Build.DefinitionName)-$(Build.BuildId)
    Pat: $(System.AccessToken)
```

**Key Parameters**:
- `SymbolsFolder`: Where PDB files are located
- `IndexSources`: `false` (don't embed source file paths - security)
- `SymbolExpirationInDays`: 1825 days = 5 years retention
- `SymbolsArtifactName`: Unique identifier for this build's symbols

#### Step 3: Publish to Microsoft Symbol Publishing Service
```yaml
- task: AzureCLI@2
  displayName: 'Publish symbols to Microsoft Symbol Publishing Service'
  condition: succeeded()
  env:
    SymbolServer: '$(SymbolServer)'
    SymbolTokenUri: '$(SymbolTokenUri)'
    requestName: '$(System.TeamProject)-$(Build.SourceBranchName)-$(Build.DefinitionName)-$(Build.BuildId)'
  inputs:
    azureSubscription: 'SymbolsPublishing-msodbcsql-mssql-python'
    scriptType: ps
    scriptLocation: inlineScript
    inlineScript: |
      # Configuration
      $publishToInternalServer = $true  # Microsoft internal symbol server
      $publishToPublicServer = $false   # Public symbol server (Microsoft Symbol Server)
      
      # Get credentials and publish
      # (Full script in build-windows-job.yml)
```

**Publishing Options**:
- `publishToInternalServer: true` - Symbols available to Microsoft employees
- `publishToPublicServer: false` - Symbols NOT available publicly (default for security)

**Publishing Status Codes**:

**PublishingStatus**:
- `0` NotRequested - Not submitted yet
- `1` Submitted - In queue
- `2` Processing - Currently publishing
- `3` Completed - Finished (check PublishingResult)

**PublishingResult**:
- `0` Pending - Not completed
- `1` Succeeded - Published successfully
- `2` Failed - Publishing failed
- `3` Cancelled - Request was cancelled

### Required Variables

**From `symbol-variables.yml`**:
```yaml
# Symbol server configuration (from variable group)
- name: SymbolServer
  value: $(SymbolServer)  # e.g., 'symbolsmicrosoft'

- name: SymbolTokenUri  
  value: $(SymbolTokenUri)  # e.g., 'https://microsoft.onmicrosoft.com/...'
```

**From Variable Group** (`ESRP Federated Creds (AME)` or similar):
- `SymbolServer`: Symbol server hostname
- `SymbolTokenUri`: Azure AD resource URI for authentication

**Azure Service Connection**:
- `SymbolsPublishing-msodbcsql-mssql-python`: Service principal with symbol publishing permissions

### Symbol Artifact Structure

**After Windows build completes**:
```
drop_Build_BuildWindowsWheels/
├── wheels/
│   └── ... (.whl files)
├── bindings/windows/
│   └── ... (.pyd files)
└── symbols/
    ├── ddbc_bindings.cp310-win_amd64.pdb
    ├── ddbc_bindings.cp311-win_amd64.pdb
    ├── ddbc_bindings.cp312-win_amd64.pdb
    ├── ddbc_bindings.cp313-win_amd64.pdb
    ├── ddbc_bindings.cp311-win_arm64.pdb
    ├── ddbc_bindings.cp312-win_arm64.pdb
    └── ddbc_bindings.cp313-win_arm64.pdb
```

### Debugging with Published Symbols

**Visual Studio**:
1. Tools → Options → Debugging → Symbols
2. Add symbol server URL: `https://symbolsmicrosoft.trafficmanager.net/...`
3. Enable "Microsoft Symbol Servers"
4. Debug your application - symbols load automatically

**WinDbg**:
```
.sympath+ srv*https://symbolsmicrosoft.trafficmanager.net/...
.reload
```

**Python Stack Traces**:
Symbols help when Python calls native extensions:
```python
# Without symbols:
File "mssql_python/connection.py", line 123, in connect
  ddbc_bindings.pyd!0x00007FF8A1B2C3D0

# With symbols:
File "mssql_python/connection.py", line 123, in connect
  ddbc_bindings.pyd!Connection::Connect() Line 456 in connection.cpp
```

### Troubleshooting Symbol Publishing

**Error: "Symbol publishing failed with status 2"**
- **Cause**: Authentication failure or invalid symbols
- **Fix**: Verify Azure service connection has permissions, check PDB files are valid

**Error: "SymbolServer variable not found"**
- **Cause**: Missing variable group
- **Fix**: Ensure `ESRP Federated Creds (AME)` group includes `SymbolServer` and `SymbolTokenUri`

**Symbols not loading in Visual Studio**:
- **Cause**: Symbol server URL incorrect or permissions
- **Fix**: Verify symbol server URL, ensure you're authenticated to Azure AD

**No symbols directory in artifacts**:
- **Cause**: PDB files not copied from build output
- **Fix**: Check `CopyFiles@2` task runs successfully, verify PDB files exist in build directory

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

## Change History

### Overview
This section documents major changes, fixes, and learnings from the OneBranch migration and subsequent improvements.

---

### 2025-10-08: Artifact Publishing Fix (Docker Daemon Issue)

**Problem**: 
Artifacts not publishing in builds. Investigation revealed OneBranch managed pools don't have Docker daemon accessible, causing Linux builds to fail.

**Error**:
```bash
ERROR: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. 
Is the docker daemon running?
##[error]Bash exited with code '1'.
```

**Root Cause**:
- Initial implementation removed `isCustom: true` from all pools to enable OneBranch auto-publishing
- OneBranch managed Linux pools don't have Docker daemon running/accessible
- Linux builds require Docker to run manylinux/musllinux container images
- Windows builds also benefit from custom pool for LocalDB and specialized tools

**Solution Implemented**:

1. **Linux Job**: Restored custom pool configuration
   ```yaml
   pool:
     type: linux
     isCustom: true
     name: Django-1ES-pool
     demands:
     - imageOverride -equals ADO-UB22-SQL22
   ```
   Added explicit artifact publishing:
   ```yaml
   - task: PublishPipelineArtifact@1
     displayName: 'Publish Linux Artifacts'
     inputs:
       targetPath: '$(ob_outputDirectory)'
       artifact: 'drop_Build_BuildLinuxWheels'
       publishLocation: 'pipeline'
   ```

2. **Windows Job**: Also restored custom pool for consistency
   ```yaml
   pool:
     type: windows
     isCustom: true
     name: Django-1ES-pool
     vmImage: WIN22-SQL22
   ```
   Added explicit artifact publishing:
   ```yaml
   - task: PublishPipelineArtifact@1
     displayName: 'Publish Windows Artifacts'
     inputs:
       targetPath: '$(ob_outputDirectory)'
       artifact: 'drop_Build_BuildWindowsWheels'
       publishLocation: 'pipeline'
   ```

3. **macOS Job**: Already using explicit publishing (Microsoft-hosted agents)

**Pattern Adopted**: ODBC team's proven approach
- All platforms now use custom pools with explicit `PublishPipelineArtifact@1`
- Consistent pattern across all three platforms
- Access to specialized capabilities (Docker, LocalDB, build tools)

**Files Changed**:
- `OneBranchPipelines/jobs/build-linux-job.yml`
- `OneBranchPipelines/jobs/build-windows-job.yml`
- `OneBranchPipelines/OneBranch-mssql-python-summary.md` (added Artifact Publishing section)

**Verification**:
- ✅ Docker daemon access restored for Linux builds
- ✅ All 3 artifacts publish correctly: `drop_Build_BuildWindowsWheels`, `drop_Build_BuildLinuxWheels`, `drop_Build_BuildMacOSWheels`
- ✅ Subdirectory structure preserved: `wheels/`, `bindings/`, `symbols/`

---

### 2025-10-08: Documentation Consolidation

**Problem**: Multiple documentation files created during development, causing:
- Confusion about which document is authoritative
- Duplication of information
- Maintenance burden (updates needed in multiple places)

**Files Deleted** (merged into master summary):
1. `ACCESSING_ARTIFACTS.md` (17KB) - Artifact access patterns
2. `ARTIFACT_PUBLISHING_FIX.md` (5.9KB) - Docker daemon fix documentation
3. `ESRP_SETUP_GUIDE.md` (11KB) - ESRP signing setup
4. `ESRP_SIGNING_CLEANUP.md` (8.6KB) - ESRP refactoring notes
5. `MIGRATION_SUMMARY.md` (9.5KB) - Migration overview
6. `README.md` (4.5KB) - OneBranch pipeline intro
7. `SYMBOLS_EXPLAINED.md` (18KB) - Symbol publishing details
8. `SYMBOL_PUBLISHING_INTEGRATION.md` (12.6KB) - Symbol integration guide

**Total Content Consolidated**: ~87KB of documentation

**Master Document**: `OneBranchPipelines/OneBranch-mssql-python-summary.md`
- Now the **single source of truth** for all OneBranch documentation
- Includes: Architecture, Parameters, Variables, Build Matrix, SDL, Jobs, Steps, Signing, Symbols, Troubleshooting, Usage
- All future updates go directly to this file

**Benefits**:
- ✅ One authoritative document
- ✅ No duplicate information
- ✅ Easier to maintain
- ✅ Complete context in one place
- ✅ Better version control

---

### 2025-10-07: Initial OneBranch Migration

**Scope**: Complete migration from Classic Azure Pipelines to OneBranch v2 CrossPlat

**Major Changes**:

1. **Pipeline Structure**:
   - Extended OneBranch base templates (`v2/OneBranch.Official.CrossPlat.yml`)
   - Separated into job templates: `build-windows-job.yml`, `build-macos-job.yml`, `build-linux-job.yml`
   - Created step templates: `malware-scanning-step.yml`, `compound-esrp-code-signing-step.yml`
   - Organized variables into 5 separate files

2. **SDL Integration**:
   - Integrated 10 SDL tools via `globalSdl` configuration
   - Added CodeQL for Python and C++ scanning
   - Added ApiScan, Armory, BinSkim, CodeInspector, CredScan, PoliCheck
   - Configured SBOM generation
   - Added TSA integration for Official builds

3. **Build Matrix**:
   - 27 total wheel builds per run
   - Windows: 7 builds (Python 3.10-3.13, x64/ARM64)
   - macOS: 4 builds (Python 3.10-3.13, universal2)
   - Linux: 16 builds (Python 3.10-3.13, manylinux/musllinux, x86_64/aarch64)

4. **Code Signing**:
   - Integrated ESRP code signing for Official builds
   - Used existing `ESRP Federated Creds (AME)` variable group
   - Signs both wheels (`.whl`) and native bindings (`.dll`, `.pyd`, `.so`)
   - Added variable mapping for OneBranch naming conventions

5. **Symbol Publishing** (Windows only):
   - Publishes `.pdb` files to Azure DevOps Symbol Server
   - Publishes to Microsoft Symbol Publishing Service
   - 5-year retention policy
   - Internal server only (not public)

6. **Package Version Strategy**:
   - Added `packageVersion` parameter for SDL tool metadata
   - Kept `setup.py` as source of truth for actual package version
   - Both must be kept in sync manually

**Files Created**:
- `OneBranchPipelines/build-release-package-pipeline.yml` (main pipeline)
- `OneBranchPipelines/jobs/` (3 job templates)
- `OneBranchPipelines/steps/` (2 step templates)
- `OneBranchPipelines/variables/` (5 variable files)
- `OneBranchPipelines/OneBranch-mssql-python-summary.md` (comprehensive docs)

**Validation**:
- ✅ Pipeline passes OneBranch schema validation
- ✅ SDL tools configured correctly
- ✅ ESRP signing integration working
- ✅ Symbol publishing configured
- ✅ All three platforms building successfully

---

### Key Learnings

**OneBranch Patterns**:
1. **Template Paths**: Must use absolute paths with `@self` when using `extends` pattern
2. **Variable Timing**: Distinguish compile-time (`${{ }}`) vs runtime (`$()`) variables
3. **Pool Configuration**: `isCustom: true` disables auto-publishing, requires explicit tasks
4. **Artifact Publishing**: Use `PublishPipelineArtifact@1` (not `@2`) for custom pools
5. **SDL Integration**: All SDL tools configured via `globalSdl` in main pipeline

**Platform-Specific**:
1. **Linux**: Requires Docker daemon access → needs custom pool
2. **Windows**: Benefits from LocalDB and specialized tools → custom pool recommended
3. **macOS**: Microsoft-hosted agents → always needs custom pool + explicit publishing
4. **Symbols**: Windows only (`.pdb` files), Linux/macOS embed symbols in binaries

**ESRP Signing**:
1. **Variable Group**: Existing `ESRP Federated Creds (AME)` works for both signing and release
2. **Mapping Required**: OneBranch uses different variable names, need explicit mapping
3. **Official Only**: Signing only runs in Official builds, NonOfficial skips signing
4. **Two Operations**: Code signing (this pipeline) separate from release publishing (different pipeline)

**Documentation**:
1. **Single Source of Truth**: Maintain one comprehensive document, not multiple files
2. **Context Matters**: Include "why" decisions were made, not just "what" configuration is
3. **Examples**: Show before/after, correct/incorrect patterns
4. **Troubleshooting**: Document errors encountered and solutions

---

## References

### Key Files

**Pipeline Files**:
- **Main Pipeline**: `OneBranchPipelines/build-release-package-pipeline.yml`
- **Windows Job**: `OneBranchPipelines/jobs/build-windows-job.yml`
- **macOS Job**: `OneBranchPipelines/jobs/build-macos-job.yml`
- **Linux Job**: `OneBranchPipelines/jobs/build-linux-job.yml`
- **Malware Scanning Step**: `OneBranchPipelines/steps/malware-scanning-step.yml`
- **ESRP Signing Step**: `OneBranchPipelines/steps/compound-esrp-code-signing-step.yml`

**Variable Files**:
- **Common Variables**: `OneBranchPipelines/variables/common-variables.yml`
- **Build Variables**: `OneBranchPipelines/variables/build-variables.yml`
- **OneBranch Variables**: `OneBranchPipelines/variables/onebranch-variables.yml`
- **Signing Variables**: `OneBranchPipelines/variables/signing-variables.yml`
- **Symbol Variables**: `OneBranchPipelines/variables/symbol-variables.yml`

**Documentation** (Single Source of Truth):
- **This Document**: `OneBranchPipelines/OneBranch-mssql-python-summary.md` - Complete OneBranch reference

**Related Documentation**:
- **OneBranch Learnings**: `OneBranch_Learnings/` directory - OneBranch migration patterns and analysis
- **SqlClient Reference**: `sqlclient_eng/pipelines/` - Reference implementations from SQL Client team
- **ODBC Reference**: `odbc_eng/` - Reference implementations from ODBC team

### External Resources

- [OneBranch Documentation](https://aka.ms/onebranch)
- [Azure Pipelines YAML Schema](https://learn.microsoft.com/en-us/azure/devops/pipelines/yaml-schema/)
- [ESRP Documentation](https://aka.ms/esrp)
- [PyPI Packaging Guide](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/)

---

## Version History

| Version | Date | Changes |
|---------|----------|--------------------------------------------------------------------------|
| 1.0.0 | 2025-10-07 | Initial comprehensive documentation for OneBranch pipeline |
| 1.1.0 | 2025-10-08 | **Artifact Publishing Fix**: Restored Linux custom pool (Django-1ES-pool) to fix Docker daemon access. Added explicit `PublishPipelineArtifact@1` for Linux. |
| 1.2.0 | 2025-10-08 | **Windows Consistency Update**: Restored Windows custom pool (Django-1ES-pool) for consistency with Linux, added explicit artifact publishing for Windows. All three platforms now use identical publishing pattern. |
| 1.3.0 | 2025-10-08 | **Documentation Consolidation**: Deleted 8 separate markdown files (~87KB), merged all content into single master document. Added Symbol Publishing section and comprehensive Change History section. |

---

## Contact

For questions or issues with this pipeline:
- **Team**: Azure SQL Python Team
- **Repository**: [microsoft/mssql-python](https://github.com/microsoft/mssql-python)
- **Branch**: `sharmag/onebranch_setup`

---

**Last Updated**: October 8, 2025  
**Pipeline Version**: v2 OneBranch CrossPlat  
**Document Version**: 1.3.0  
**Document Owner**: Gaurav Sharma  
**Pipeline Version**: v2 OneBranch CrossPlat  
**Document Owner**: Gaurav Sharma

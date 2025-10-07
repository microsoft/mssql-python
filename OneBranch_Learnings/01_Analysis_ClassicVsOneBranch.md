# Analysis: Classic ADO Pipelines vs OneBranch Pipelines

## Date: October 1, 2025
## Project: mssql-python OneBranch Migration

---

## 1. Classic ADO Pipeline Analysis (Current State)

### Current Pipelines in `eng/pipelines/`:
1. **build-whl-pipeline.yml** - Main build pipeline for wheels
2. **pr-validation-pipeline.yml** - PR validation and testing
3. **dummy-release-pipeline.yml** - Dummy release pipeline
4. **official-release-pipeline.yml** - Official release pipeline
5. **pypi-package-smoketest.yml** - PyPI smoke tests

### Key Characteristics of Classic Pipelines:

#### Structure:
```yaml
name: pipeline-name
trigger:
  branches:
    include: [...]
jobs:
  - job: JobName
    pool:
      vmImage: 'ubuntu-latest'
    steps:
      - task: TaskName@version
      - script: |
          command
```

#### Common Patterns Found:
- **Direct pool specification**: `pool: { vmImage: 'ubuntu-latest' }`
- **Inline scripts**: Extensive use of `script:` tasks
- **Task-based approach**: Direct use of Azure DevOps tasks
  - `UsePythonVersion@0`
  - `PublishBuildArtifacts@1`
  - `PublishTestResults@2`
  - `CodeQL3000Init@0` / `CodeQL3000Finalize@0`
- **Matrix strategies**: For multi-platform/multi-version builds
- **Environment variables**: Set inline in tasks
- **Artifact publishing**: Using `PublishBuildArtifacts@1`

---

## 2. OneBranch Pipeline Analysis (Target State)

### Reference Implementation in `sqlclient_eng/pipelines/`:
1. **akv-official-pipeline.yml** - Main OneBranch pipeline
2. Supporting structure:
   - `jobs/` - Job templates
   - `stages/` - Stage templates  
   - `steps/` - Step templates
   - `variables/` - Variable templates
   - `libraries/` - Shared libraries

### Key Characteristics of OneBranch Pipelines:

#### Structure:
```yaml
name: $(Year:YY)$(DayOfYear)$(Rev:.r)

parameters:
  - name: paramName
    type: string
    values: [...]
    default: value

variables:
  - template: /path/to/variables.yml@self

resources:
  repositories:
    - repository: templates
      type: 'git'
      name: 'OneBranch.Pipelines/GovernedTemplates'
      ref: 'refs/heads/main'

extends:
  template: 'v2/OneBranch.Official.CrossPlat.yml@templates'
  parameters:
    featureFlags: {...}
    globalSdl: {...}
    stages: [...]
```

#### Key OneBranch Concepts:

##### 1. **Template Extension Pattern**
- Extends OneBranch governed templates
- Uses `v2/OneBranch.Official.CrossPlat.yml` or `v2/OneBranch.NonOfficial.CrossPlat.yml`
- Provides standardized security and compliance

##### 2. **Parameterization**
- Heavy use of parameters for flexibility
- Type-safe parameters with validation
- Default values provided

##### 3. **Variable Organization**
- Separated into template files
- **onebranch-variables.yml**: OneBranch-specific configs
- **common-variables.yml**: Project-wide variables
- **esrp-signing-variables.yml**: Code signing configs
- Variable groups for secrets

##### 4. **Security & Compliance (globalSdl)**
Built-in SDL tools:
- **ApiScan**: API security scanning
- **Armory**: Security testing
- **BinSkim**: Binary analysis
- **CodeInspector**: Code quality
- **CodeQL**: Security analysis
- **CredScan**: Credential scanning
- **PoliCheck**: Policy compliance
- **Roslyn**: Static analysis
- **SBOM**: Software Bill of Materials
- **TSA**: Security result publishing

##### 5. **Modular Structure**
- Jobs defined in separate templates (`jobs/*.yml`)
- Steps grouped into compound templates (`steps/*.yml`)
- Reusable across multiple pipelines

##### 6. **ESRP Code Signing**
- Enterprise-grade code signing
- Malware scanning before signing
- Uses Azure Key Vault for certificates
- Supports DLL and NuGet package signing

##### 7. **Pool Management**
- Uses OneBranch managed pools
- `pool: { type: windows }` instead of `vmImage`
- Better resource management and security

---

## 3. Key Transformation Patterns

### Pattern 1: Pipeline Structure
**Classic:**
```yaml
name: build-whl-pipeline
trigger:
  branches:
    include: [main]
jobs:
  - job: BuildJob
```

**OneBranch:**
```yaml
name: $(Year:YY)$(DayOfYear)$(Rev:.r)
# Triggers moved to OneBranch template or parameters
extends:
  template: 'v2/OneBranch.Official.CrossPlat.yml@templates'
  parameters:
    stages:
      - stage: Build
        jobs:
          - template: /jobs/build-job.yml@self
```

### Pattern 2: Pool/Agent Selection
**Classic:**
```yaml
pool:
  vmImage: 'ubuntu-latest'
```

**OneBranch:**
```yaml
pool:
  type: windows  # or linux
# Managed by OneBranch templates
```

### Pattern 3: Artifact Publishing
**Classic:**
```yaml
- task: PublishBuildArtifacts@1
  inputs:
    PathtoPublish: '$(Build.ArtifactStagingDirectory)/dist'
    ArtifactName: 'wheels'
```

**OneBranch:**
```yaml
variables:
  ob_outputDirectory: '$(ARTIFACT_PATH)'
# Artifacts automatically published from ob_outputDirectory
```

### Pattern 4: Security Tasks
**Classic:**
```yaml
- task: CodeQL3000Init@0
- script: build commands
- task: CodeQL3000Finalize@0
```

**OneBranch:**
```yaml
globalSdl:
  codeql:
    enabled: true
    sourceRoot: '$(REPO_ROOT)/src'
# Automatically injected by OneBranch
```

### Pattern 5: Variables
**Classic:**
```yaml
variables:
  pythonVersion: '3.13'
  buildConfig: 'Release'
```

**OneBranch:**
```yaml
variables:
  - template: /path/to/variables.yml@self
# Organized in separate files
parameters:
  - name: buildConfiguration
    type: string
    default: 'Release'
```

### Pattern 6: Code Signing
**Classic:**
```yaml
# Manual signing steps or external process
- script: sign-package.sh
```

**OneBranch:**
```yaml
- template: ../steps/compound-esrp-code-signing-step.yml@self
  parameters:
    artifactType: 'dll'
    authAkvName: '$(SigningAuthAkvName)'
# Standardized ESRP signing
```

---

## 4. Benefits of OneBranch Migration

### Security & Compliance
- ✅ Built-in SDL tools (CodeQL, BinSkim, CredScan, etc.)
- ✅ Automated security scanning
- ✅ TSA integration for vulnerability tracking
- ✅ SBOM generation
- ✅ Enterprise code signing (ESRP)

### Governance
- ✅ Standardized pipeline structure
- ✅ Managed pools with better isolation
- ✅ Centralized compliance controls
- ✅ Audit trails

### Maintainability
- ✅ Modular template structure
- ✅ Reusable components
- ✅ Centralized configuration
- ✅ Type-safe parameters

### Operational Excellence
- ✅ Better resource management
- ✅ Consistent build environments
- ✅ Reduced boilerplate code
- ✅ Easier pipeline updates

---

## 5. Migration Scope for mssql-python

### In Scope:
- ✅ **Build-Release-Package Pipeline**: Build Python packages for all platforms, publish symbols, CodeQL

### Out of Scope (Future):
- ⏸️ Dummy release pipeline
- ⏸️ Official release pipeline

### Current Focus:
Transform `build-whl-pipeline.yml` → OneBranch Build-Release-Package pipeline with:
1. Multi-platform wheel building (Windows, macOS, Linux)
2. Multi-architecture support (x64, ARM64, universal2)
3. Symbol publishing
4. CodeQL security analysis
5. Package validation and testing

---

## 6. Key Challenges & Considerations

### Python-Specific Challenges:
1. **Multi-platform builds**: Windows (x64, ARM64), macOS (universal2), Linux (x64, ARM64, multiple distros)
2. **Native extensions**: pybind11 bindings need compilation
3. **Wheel building**: Platform-specific wheels with native libraries
4. **Testing**: Need SQL Server for integration tests
5. **Docker usage**: Linux builds use Docker containers

### OneBranch Adaptations Needed:
1. **Container support**: OneBranch container integration for Linux builds
2. **Test infrastructure**: SQL Server setup in OneBranch environment
3. **Artifact management**: Wheel and binding artifact organization
4. **Symbol publishing**: Integration with Microsoft Symbol Server
5. **Matrix builds**: Support for multiple Python versions and platforms

---

## Next Steps
1. Design Bandish task specifications
2. Create comprehensive knowledge base
3. Map classic patterns to OneBranch equivalents
4. Document transformation rules

# OneBranch Pipeline Architecture

## Date: October 1, 2025
## Project: mssql-python OneBranch Migration

---

## 1. OneBranch File Structure

### Recommended Directory Layout:
```
project-root/
├── eng/
│   └── pipelines/
│       ├── onebranch/
│       │   ├── build-release-package.yml        # Main OneBranch pipeline
│       │   ├── jobs/
│       │   │   ├── build-windows-job.yml
│       │   │   ├── build-macos-job.yml
│       │   │   ├── build-linux-job.yml
│       │   │   └── test-packages-job.yml
│       │   ├── stages/
│       │   │   ├── build-stage.yml
│       │   │   └── test-stage.yml
│       │   ├── steps/
│       │   │   ├── setup-python-step.yml
│       │   │   ├── build-wheel-step.yml
│       │   │   ├── run-tests-step.yml
│       │   │   └── publish-symbols-step.yml
│       │   └── variables/
│       │       ├── onebranch-variables.yml
│       │       ├── common-variables.yml
│       │       ├── build-variables.yml
│       │       └── signing-variables.yml
│       └── classic/                            # Keep existing pipelines
│           ├── build-whl-pipeline.yml
│           └── pr-validation-pipeline.yml
```

---

## 2. Core OneBranch Components

### 2.1 Main Pipeline File Structure

```yaml
#################################################################################
# OneBranch Build-Release-Package Pipeline
# Builds Python wheels for all platforms and architectures
#################################################################################

name: $(Year:YY)$(DayOfYear)$(Rev:.r)

# Pipeline Parameters
parameters:
    - name: oneBranchType
      displayName: 'OneBranch Template Type'
      type: string
      values:
          - 'Official'
          - 'NonOfficial'
      default: 'Official'

    - name: buildConfiguration
      displayName: 'Build Configuration'
      type: string
      values:
          - 'Release'
          - 'Debug'
      default: 'Release'

    - name: publishSymbols
      displayName: 'Publish Symbols to Symbol Server'
      type: boolean
      default: true

    - name: runSdlTasks
      displayName: 'Run SDL Security Tasks'
      type: boolean
      default: true

# Variable Templates
variables:
    - template: /eng/pipelines/onebranch/variables/common-variables.yml@self
    - template: /eng/pipelines/onebranch/variables/onebranch-variables.yml@self
    - template: /eng/pipelines/onebranch/variables/build-variables.yml@self
    - template: /eng/pipelines/onebranch/variables/signing-variables.yml@self

# OneBranch Governed Templates Repository
resources:
    repositories:
        - repository: templates
          type: 'git'
          name: 'OneBranch.Pipelines/GovernedTemplates'
          ref: 'refs/heads/main'

# Extend OneBranch Template
extends:
    template: 'v2/OneBranch.${{ parameters.oneBranchType }}.CrossPlat.yml@templates'
    
    parameters:
        # Feature Flags
        featureFlags:
            WindowsHostVersion:
                Version: '2022'
            LinuxHostVersion:
                Version: 'ubuntu-22.04'

        # Global SDL Configuration
        globalSdl:
            # See detailed SDL config below
            
        # Pipeline Stages
        stages:
            - stage: BuildPackages
              displayName: 'Build Python Packages'
              jobs:
                  - template: /eng/pipelines/onebranch/jobs/build-windows-job.yml@self
                  - template: /eng/pipelines/onebranch/jobs/build-macos-job.yml@self
                  - template: /eng/pipelines/onebranch/jobs/build-linux-job.yml@self
                  
            - stage: TestPackages
              displayName: 'Test Python Packages'
              dependsOn: BuildPackages
              jobs:
                  - template: /eng/pipelines/onebranch/jobs/test-packages-job.yml@self
```

---

### 2.2 Global SDL Configuration

OneBranch provides integrated security and compliance tools:

```yaml
globalSdl:
    # API Security Scanning
    apiscan:
        enabled: ${{ parameters.runSdlTasks }}
        softwareFolder: '$(Build.SourcesDirectory)/mssql_python'
        softwareName: 'mssql-python'
        softwareVersionNum: '$(packageVersion)'
        symbolsFolder: '$(Build.SourcesDirectory)/symbols'

    # Armory Security Testing
    armory:
        enabled: ${{ parameters.runSdlTasks }}
        break: true  # Fail build on critical issues

    # Binary Analysis
    binskim:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        analyzeTargetGlob: '+:file|**/*.pyd;+:file|**/*.so;+:file|**/*.dylib'

    # Code Inspector
    codeinspector:
        enabled: ${{ parameters.runSdlTasks }}
        logLevel: Error

    # CodeQL Security Analysis
    codeql:
        enabled: ${{ parameters.runSdlTasks }}
        sourceRoot: '$(REPO_ROOT)'
        language: 'python,cpp'
        buildCommands: |
            cd $(REPO_ROOT)/mssql_python/pybind
            ./build.sh

    # Credential Scanning
    credscan:
        enabled: ${{ parameters.runSdlTasks }}
        suppressionsFile: '$(REPO_ROOT)/.config/CredScanSuppressions.json'

    # ESLint (Not needed for Python)
    eslint:
        enabled: false

    # Policy Check
    policheck:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        exclusionFile: '$(REPO_ROOT)/.config/PolicheckExclusions.xml'

    # Roslyn Analyzers
    roslyn:
        enabled: false  # Not applicable to Python

    # Publish SDL Results
    publishLogs:
        enabled: ${{ parameters.runSdlTasks }}

    # Software Bill of Materials
    sbom:
        enabled: ${{ parameters.runSdlTasks }}
        packageName: 'mssql-python'
        packageVersion: '$(packageVersion)'

    # Trust Services Automation
    tsa:
        enabled: ${{ eq(parameters.oneBranchType, 'Official') }}
        configFile: '$(REPO_ROOT)/.config/tsaoptions.json'
```

---

### 2.3 Variable Templates

#### common-variables.yml
```yaml
variables:
    # Repository Paths
    - name: REPO_ROOT
      value: '$(Build.SourcesDirectory)'
      readonly: true
      
    - name: BUILD_OUTPUT
      value: '$(Build.SourcesDirectory)/dist'
      readonly: true
      
    - name: ARTIFACT_PATH
      value: '$(Build.SourcesDirectory)/artifacts'
      readonly: true
      
    # Package Information
    - name: packageName
      value: 'mssql-python'
      
    - name: packageVersion
      value: '1.0.0'  # Should be dynamically set
```

#### onebranch-variables.yml
```yaml
variables:
    # OneBranch Configuration
    - name: Packaging.EnableSBOMSigning
      value: true
      
    - name: WindowsContainerImage
      value: 'onebranch.azurecr.io/windows/ltsc2022/vse2022:latest'
      
    - name: LinuxContainerImage
      value: 'onebranch.azurecr.io/linux/ubuntu-2204:latest'
```

#### build-variables.yml
```yaml
variables:
    # Python Versions to Build
    - name: pythonVersions
      value: ['3.10', '3.11', '3.12', '3.13']
      
    # Architectures
    - name: windowsArchitectures
      value: ['x64', 'arm64']
      
    - name: macosArchitectures
      value: ['universal2']
      
    - name: linuxArchitectures
      value: ['x86_64', 'aarch64']
      
    # Build Configuration
    - name: buildConfiguration
      value: 'Release'
```

---

### 2.4 Job Template Structure

#### Example: build-windows-job.yml
```yaml
parameters:
    - name: buildConfiguration
      type: string
      default: 'Release'

jobs:
    - job: BuildWindowsWheels
      displayName: 'Build Windows Wheels'
      pool:
          type: windows  # OneBranch managed pool
          
      variables:
          ob_outputDirectory: '$(ARTIFACT_PATH)'
          
      strategy:
          matrix:
              py310_x64:
                  pythonVersion: '3.10'
                  architecture: 'x64'
              py311_x64:
                  pythonVersion: '3.11'
                  architecture: 'x64'
              # ... more combinations
              
      steps:
          - template: ../steps/setup-python-step.yml@self
            parameters:
                pythonVersion: '$(pythonVersion)'
                architecture: '$(architecture)'
                
          - template: ../steps/build-wheel-step.yml@self
            parameters:
                platform: 'windows'
                architecture: '$(architecture)'
                buildConfiguration: '${{ parameters.buildConfiguration }}'
```

---

### 2.5 Step Template Structure

#### Example: build-wheel-step.yml
```yaml
parameters:
    - name: platform
      type: string
      
    - name: architecture
      type: string
      
    - name: buildConfiguration
      type: string

steps:
    - task: UsePythonVersion@0
      inputs:
          versionSpec: '$(pythonVersion)'
          architecture: '${{ parameters.architecture }}'
      displayName: 'Setup Python $(pythonVersion)'
      
    - script: |
          python -m pip install --upgrade pip wheel setuptools
          pip install -r requirements.txt
      displayName: 'Install Python Dependencies'
      
    - script: |
          cd mssql_python/pybind
          ${{ if eq(parameters.platform, 'windows') }}:
              call build.bat ${{ parameters.architecture }}
          ${{ else }}:
              ./build.sh
      displayName: 'Build Native Extension'
      
    - script: |
          python setup.py bdist_wheel
      displayName: 'Build Wheel Package'
      
    # Files in ob_outputDirectory are automatically published
    - task: CopyFiles@2
      inputs:
          SourceFolder: '$(Build.SourcesDirectory)/dist'
          Contents: '*.whl'
          TargetFolder: '$(ob_outputDirectory)'
      displayName: 'Stage Wheel for Publishing'
```

---

## 3. OneBranch-Specific Features

### 3.1 Automatic Artifact Publishing
```yaml
variables:
    ob_outputDirectory: '$(ARTIFACT_PATH)'
# All files in ob_outputDirectory are automatically published
```

### 3.2 Symbol Publishing Integration
```yaml
- template: ../steps/publish-symbols-step.yml@self
  parameters:
      symbolsAzureSubscription: '$(SymbolsAzureSubscription)'
      symbolsPublishServer: '$(SymbolsPublishServer)'
      searchPattern: '**/*.pdb;**/*.pyd'
```

### 3.3 Code Signing (ESRP)
```yaml
- template: ../steps/esrp-code-signing-step.yml@self
  parameters:
      artifactType: 'dll'  # or 'pkg' for packages
      folderPath: '$(BUILD_OUTPUT)'
      pattern: '*.pyd;*.so;*.dylib'
```

---

## 4. Migration Strategy

### Phase 1: Setup (Week 1)
1. Create OneBranch directory structure
2. Set up variable templates
3. Configure SDL suppressions and policies

### Phase 2: Core Build (Week 2-3)
1. Migrate Windows wheel building
2. Migrate macOS wheel building
3. Migrate Linux wheel building

### Phase 3: Testing & Validation (Week 4)
1. Set up test jobs
2. Validate wheel integrity
3. Run integration tests

### Phase 4: Publishing & Security (Week 5)
1. Configure symbol publishing
2. Set up ESRP code signing
3. Enable all SDL tasks

### Phase 5: Cutover (Week 6)
1. Final testing
2. Documentation
3. Enable OneBranch pipeline
4. Deprecate classic pipeline

---

## 5. Best Practices

### ✅ DO:
- Use parameter types and validation
- Organize variables in separate files
- Create reusable step templates
- Enable SDL tasks for security
- Use OneBranch managed pools
- Document all parameters

### ❌ DON'T:
- Hardcode values in main pipeline
- Mix classic and OneBranch patterns
- Disable SDL without justification
- Use public pools
- Expose secrets in logs
- Skip SBOM generation

---

## 6. Testing Strategy

### Local Testing:
- Test scripts independently
- Validate wheel building
- Run unit tests

### Pipeline Testing:
- Start with NonOfficial template
- Test incremental changes
- Validate artifacts
- Check SDL results

### Production Readiness:
- All SDL tasks passing
- Symbols published correctly
- Packages signed
- Documentation complete

---

## Next Steps:
1. Create Bandish knowledge base
2. Define transformation patterns
3. Generate migration rules

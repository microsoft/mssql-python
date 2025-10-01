# Transformation Patterns: Classic ADO to OneBranch

## Date: October 1, 2025
## Project: mssql-python OneBranch Migration

---

## Pattern Categories

This document catalogs all transformation patterns needed for migrating classic Azure DevOps pipelines to OneBranch. Each pattern includes:
- **Classic Pattern**: The original code structure
- **OneBranch Pattern**: The transformed code
- **Matcher Strategy**: How Bandish will identify this pattern
- **Transformation Rules**: Step-by-step transformation logic

---

## 1. Pipeline Root Structure

### Pattern 1.1: Pipeline Name Declaration

**Classic:**
```yaml
name: build-whl-pipeline
```

**OneBranch:**
```yaml
name: $(Year:YY)$(DayOfYear)$(Rev:.r)
```

**Matcher:**
- `$match:files="*.yml"`
- `$match:regex="^name:\s+[\w-]+$"`
- `$match:keyword="name:"`

**Transformation:**
- Replace static name with OneBranch versioning format
- Use `$(Year:YY)$(DayOfYear)$(Rev:.r)` for build numbering

---

### Pattern 1.2: Trigger Configuration

**Classic:**
```yaml
trigger:
  branches:
    include:
      - main
pr:
  branches:
    include:
      - main
```

**OneBranch:**
```yaml
# Triggers managed by OneBranch template or can be added as comments
# trigger:
#   branches:
#     include:
#       - main
```

**Matcher:**
- `$match:keyword="trigger:"`
- `$match:keyword="pr:"`

**Transformation:**
- Comment out or remove trigger configuration
- Add note that triggers are managed by OneBranch
- Can be configured at repository settings or kept for documentation

---

### Pattern 1.3: Schedules

**Classic:**
```yaml
schedules:
  - cron: "30 1 * * *"
    displayName: Daily run
    branches:
      include:
        - main
```

**OneBranch:**
```yaml
# Schedules managed by OneBranch or repository settings
# schedules:
#   - cron: "30 1 * * *"
```

**Matcher:**
- `$match:keyword="schedules:"`
- `$match:regex="cron:"`

**Transformation:**
- Document schedule requirements
- Configure in repository pipeline settings
- Comment out inline schedules

---

## 2. Parameters

### Pattern 2.1: Adding Parameters Section

**Classic:**
```yaml
# No parameters section
jobs:
  - job: Build
```

**OneBranch:**
```yaml
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
      displayName: 'Publish Symbols'
      type: boolean
      default: true

    - name: runSdlTasks
      displayName: 'Run SDL Tasks'
      type: boolean
      default: true
```

**Matcher:**
- `$match:files="*.yml"`
- `$match:keyword="jobs:"`
- NOT `$match:keyword="parameters:"`

**Transformation:**
- Add standard OneBranch parameters at top of file
- Include oneBranchType, buildConfiguration, publishSymbols, runSdlTasks

---

## 3. Variables

### Pattern 3.1: Inline Variables to Template References

**Classic:**
```yaml
variables:
  pythonVersion: '3.13'
  buildConfig: 'Release'
  REPO_ROOT: '$(Build.SourcesDirectory)'
```

**OneBranch:**
```yaml
variables:
    - template: /eng/pipelines/onebranch/variables/common-variables.yml@self
    - template: /eng/pipelines/onebranch/variables/onebranch-variables.yml@self
    - template: /eng/pipelines/onebranch/variables/build-variables.yml@self
```

**Matcher:**
- `$match:keyword="variables:"`
- `$match:regex="^\s+[\w_]+:\s+['\"].*['\"]"`

**Transformation:**
- Extract variables to separate template files
- Replace inline variables with template references
- Create common-variables.yml, onebranch-variables.yml, build-variables.yml

---

## 4. Resources

### Pattern 4.1: Adding OneBranch Template Repository

**Classic:**
```yaml
# No resources section
```

**OneBranch:**
```yaml
resources:
    repositories:
        - repository: templates
          type: 'git'
          name: 'OneBranch.Pipelines/GovernedTemplates'
          ref: 'refs/heads/main'
```

**Matcher:**
- `$match:files="*.yml"`
- NOT `$match:keyword="resources:"`

**Transformation:**
- Add resources section after variables
- Include OneBranch.Pipelines/GovernedTemplates repository reference

---

## 5. Pipeline Extension

### Pattern 5.1: Jobs to Extends Structure

**Classic:**
```yaml
jobs:
  - job: BuildJob
    pool:
      vmImage: 'ubuntu-latest'
    steps:
      - task: Something@1
```

**OneBranch:**
```yaml
extends:
    template: 'v2/OneBranch.${{ parameters.oneBranchType }}.CrossPlat.yml@templates'
    
    parameters:
        featureFlags:
            WindowsHostVersion:
                Version: '2022'
                
        globalSdl:
            # SDL configuration
            
        stages:
            - stage: BuildStage
              displayName: 'Build Packages'
              jobs:
                  - template: /eng/pipelines/onebranch/jobs/build-job.yml@self
```

**Matcher:**
- `$match:keyword="jobs:"`
- NOT `$match:keyword="extends:"`
- `$match:files="*.yml"`

**Transformation:**
- Replace top-level jobs with extends structure
- Reference OneBranch.Official or OneBranch.NonOfficial template
- Wrap jobs in stages
- Extract jobs to separate template files

---

## 6. Pool Configuration

### Pattern 6.1: vmImage to OneBranch Pool Type

**Classic:**
```yaml
pool:
  vmImage: 'ubuntu-latest'
```

**OneBranch:**
```yaml
pool:
  type: linux
```

**Classic:**
```yaml
pool:
  vmImage: 'windows-latest'
```

**OneBranch:**
```yaml
pool:
  type: windows
```

**Classic:**
```yaml
pool:
  vmImage: 'macos-latest'
```

**OneBranch:**
```yaml
pool:
  type: linux  # or windows, macOS builds often use Linux with cross-compilation
```

**Matcher:**
- `$match:keyword="pool:"`
- `$match:keyword="vmImage:"`
- `$match:regex="vmImage:\s+['\"].*['\"]"`

**Transformation:**
- Replace `vmImage: 'ubuntu-latest'` with `type: linux`
- Replace `vmImage: 'windows-latest'` with `type: windows`
- Replace `vmImage: 'macos-latest'` with appropriate OneBranch pool type

---

## 7. Artifact Publishing

### Pattern 7.1: PublishBuildArtifacts to ob_outputDirectory

**Classic:**
```yaml
- task: PublishBuildArtifacts@1
  inputs:
    PathtoPublish: '$(Build.ArtifactStagingDirectory)/dist'
    ArtifactName: 'wheels'
    publishLocation: 'Container'
```

**OneBranch:**
```yaml
variables:
    ob_outputDirectory: '$(ARTIFACT_PATH)'

steps:
    - task: CopyFiles@2
      inputs:
          SourceFolder: '$(Build.SourcesDirectory)/dist'
          Contents: '*.whl'
          TargetFolder: '$(ob_outputDirectory)'
      displayName: 'Stage artifacts for automatic publishing'
```

**Matcher:**
- `$match:keyword="PublishBuildArtifacts@"`
- `$match:regex="PublishBuildArtifacts@\d+"`

**Transformation:**
- Remove PublishBuildArtifacts tasks
- Add ob_outputDirectory variable to job
- Add CopyFiles task to copy artifacts to ob_outputDirectory
- OneBranch automatically publishes from ob_outputDirectory

---

## 8. Security Tasks

### Pattern 8.1: CodeQL Tasks to globalSdl

**Classic:**
```yaml
steps:
  - task: CodeQL3000Init@0
    inputs:
      Enabled: true
      
  - script: |
      cd mssql_python/pybind
      ./build.sh
      
  - task: CodeQL3000Finalize@0
    condition: always()
```

**OneBranch:**
```yaml
# In extends parameters
globalSdl:
    codeql:
        enabled: ${{ parameters.runSdlTasks }}
        sourceRoot: '$(REPO_ROOT)'
        language: 'python,cpp'
        buildCommands: |
            cd $(REPO_ROOT)/mssql_python/pybind
            ./build.sh
```

**Matcher:**
- `$match:keyword="CodeQL3000Init@"`
- `$match:keyword="CodeQL3000Finalize@"`
- `$match:regex="CodeQL\d+.*@\d+"`

**Transformation:**
- Remove CodeQL task steps
- Add codeql configuration to globalSdl
- Extract build commands from between Init and Finalize

---

### Pattern 8.2: Adding Comprehensive SDL Configuration

**Classic:**
```yaml
# No SDL configuration
```

**OneBranch:**
```yaml
globalSdl:
    apiscan:
        enabled: ${{ parameters.runSdlTasks }}
        softwareFolder: '$(apiScanDllPath)'
        softwareName: 'mssql-python'
        softwareVersionNum: '$(packageVersion)'
        
    binskim:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        analyzeTargetGlob: '+:file|**/*.pyd;+:file|**/*.so;+:file|**/*.dylib'
        
    credscan:
        enabled: ${{ parameters.runSdlTasks }}
        suppressionsFile: '$(REPO_ROOT)/.config/CredScanSuppressions.json'
        
    policheck:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        exclusionFile: '$(REPO_ROOT)/.config/PolicheckExclusions.xml'
        
    sbom:
        enabled: ${{ parameters.runSdlTasks }}
        packageName: 'mssql-python'
        packageVersion: '$(packageVersion)'
        
    tsa:
        enabled: ${{ eq(parameters.oneBranchType, 'Official') }}
        configFile: '$(REPO_ROOT)/.config/tsaoptions.json'
```

**Matcher:**
- `$match:files="*.yml"`
- NOT `$match:keyword="globalSdl:"`

**Transformation:**
- Add complete globalSdl section with all security tools
- Enable based on parameters.runSdlTasks
- Create suppression files in .config/ directory

---

## 9. Job Organization

### Pattern 9.1: Inline Job to Template Reference

**Classic:**
```yaml
jobs:
  - job: BuildWindowsWheels
    pool:
      vmImage: 'windows-latest'
    strategy:
      matrix:
        py310_x64:
          pythonVersion: '3.10'
          architecture: 'x64'
    steps:
      - task: UsePythonVersion@0
      - script: build commands
```

**OneBranch:**
```yaml
# In main pipeline
stages:
    - stage: BuildStage
      jobs:
          - template: /eng/pipelines/onebranch/jobs/build-windows-job.yml@self
            parameters:
                buildConfiguration: '${{ parameters.buildConfiguration }}'

# In jobs/build-windows-job.yml
jobs:
    - job: BuildWindowsWheels
      pool:
          type: windows
      variables:
          ob_outputDirectory: '$(ARTIFACT_PATH)'
      strategy:
          matrix:
              py310_x64:
                  pythonVersion: '3.10'
                  architecture: 'x64'
      steps:
          - template: ../steps/build-wheel-step.yml@self
```

**Matcher:**
- `$match:keyword="- job:"`
- `$match:regex="^\s+- job:\s+\w+"`

**Transformation:**
- Extract job definition to separate file in jobs/ directory
- Replace with template reference
- Add parameters for configuration values
- Update pool to OneBranch type

---

## 10. Step Organization

### Pattern 10.1: Inline Steps to Template Reference

**Classic:**
```yaml
steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '$(pythonVersion)'
      architecture: '$(architecture)'
      
  - script: |
      python -m pip install --upgrade pip
      pip install -r requirements.txt
    displayName: 'Install dependencies'
    
  - script: |
      python setup.py bdist_wheel
    displayName: 'Build wheel'
```

**OneBranch:**
```yaml
steps:
    - template: ../steps/setup-python-step.yml@self
      parameters:
          pythonVersion: '$(pythonVersion)'
          architecture: '$(architecture)'
          
    - template: ../steps/install-dependencies-step.yml@self
    
    - template: ../steps/build-wheel-step.yml@self
      parameters:
          platform: 'windows'
```

**Matcher:**
- `$match:keyword="steps:"`
- Multiple consecutive script or task definitions

**Transformation:**
- Group related steps into compound templates
- Extract to steps/ directory
- Parameterize platform-specific logic
- Create reusable step templates

---

## 11. Matrix Strategies

### Pattern 11.1: Matrix Strategy Preservation

**Classic:**
```yaml
strategy:
  matrix:
    py310_x64:
      pythonVersion: '3.10'
      architecture: 'x64'
    py311_arm64:
      pythonVersion: '3.11'
      architecture: 'arm64'
```

**OneBranch:**
```yaml
strategy:
    matrix:
        py310_x64:
            pythonVersion: '3.10'
            architecture: 'x64'
        py311_arm64:
            pythonVersion: '3.11'
            architecture: 'arm64'
```

**Matcher:**
- `$match:keyword="strategy:"`
- `$match:keyword="matrix:"`

**Transformation:**
- Preserve matrix strategies (indentation may change)
- Keep matrix variable definitions
- Can be extracted to variable templates if complex

---

## 12. Environment Variables

### Pattern 12.1: Secret Variables

**Classic:**
```yaml
env:
  DB_PASSWORD: $(DB_PASSWORD)
```

**OneBranch:**
```yaml
# Use variable groups for secrets
variables:
    - group: 'build-secrets'
      # DB_PASSWORD defined in variable group
      
# Or keep inline for non-secrets
env:
    DB_CONNECTION_STRING: 'Server=...;Pwd=$(DB_PASSWORD)'
```

**Matcher:**
- `$match:keyword="env:"`
- `$match:regex="\$\([A-Z_]+\)"`

**Transformation:**
- Move secrets to variable groups
- Reference variable groups in variables section
- Keep inline for constructed values

---

## 13. Test Result Publishing

### Pattern 13.1: Test Results Publishing

**Classic:**
```yaml
- task: PublishTestResults@2
  condition: succeededOrFailed()
  inputs:
    testResultsFiles: '**/test-results.xml'
    testRunTitle: 'Test Results'
```

**OneBranch:**
```yaml
- task: PublishTestResults@2
  condition: succeededOrFailed()
  inputs:
      testResultsFiles: '**/test-results.xml'
      testRunTitle: 'Test Results'
```

**Matcher:**
- `$match:keyword="PublishTestResults@"`

**Transformation:**
- Preserve PublishTestResults tasks
- Adjust indentation to OneBranch style
- Can be moved to test step templates

---

## 14. Docker Usage

### Pattern 14.1: Docker Commands in Scripts

**Classic:**
```yaml
- script: |
    docker run -d --name sqlserver \
      -e ACCEPT_EULA=Y \
      -e MSSQL_SA_PASSWORD="$(DB_PASSWORD)" \
      -p 1433:1433 \
      mcr.microsoft.com/mssql/server:2022-latest
```

**OneBranch:**
```yaml
# Docker usage should be reviewed for OneBranch compliance
# May need to use OneBranch container integration
- script: |
    docker run -d --name sqlserver \
      -e ACCEPT_EULA=Y \
      -e MSSQL_SA_PASSWORD="$(DB_PASSWORD)" \
      -p 1433:1433 \
      mcr.microsoft.com/mssql/server:2022-latest
  displayName: 'Start SQL Server Container'
```

**Matcher:**
- `$match:keyword="docker"`
- `$match:regex="docker\s+(run|build|exec)"`

**Transformation:**
- Preserve Docker commands (may need compliance review)
- Ensure container images are from approved registries
- Document container usage in OneBranch context

---

## 15. Symbol Publishing

### Pattern 15.1: Adding Symbol Publishing

**Classic:**
```yaml
# No symbol publishing
```

**OneBranch:**
```yaml
- ${{ if parameters.publishSymbols }}:
    - template: ../steps/compound-publish-symbols-step.yml@self
      parameters:
          artifactName: 'symbols_$(System.TeamProject)_$(Build.SourceBranchName)'
          azureSubscription: '$(SymbolsAzureSubscription)'
          publishServer: '$(SymbolsPublishServer)'
          searchPattern: |
              **/*.pdb
              **/*.pyd
          uploadAccount: '$(SymbolsUploadAccount)'
```

**Matcher:**
- `$match:files="*.yml"`
- `$match:keyword=".pdb"`
- NOT `$match:keyword="publishSymbols"`

**Transformation:**
- Add symbol publishing step template
- Conditional on parameters.publishSymbols
- Configure symbol server variables

---

## 16. File Organization

### Pattern 16.1: Monolithic to Modular Structure

**Classic:**
```yaml
# All in one file: pipeline.yml (500+ lines)
name: pipeline
jobs:
  - job: Job1
    steps: [...]
  - job: Job2
    steps: [...]
```

**OneBranch:**
```
pipeline.yml (main file, ~100 lines)
├── jobs/
│   ├── job1.yml
│   └── job2.yml
├── steps/
│   ├── step-template1.yml
│   └── step-template2.yml
└── variables/
    └── variables.yml
```

**Matcher:**
- `$match:files="*.yml"`
- File size > 300 lines

**Transformation:**
- Split into modular structure
- Create directory hierarchy
- Extract jobs, steps, variables to separate files
- Keep main pipeline file concise

---

## Summary

These transformation patterns provide the foundation for the Bandish knowledge base. Each pattern has been identified from analyzing both classic and OneBranch pipelines, and includes:

1. **Clear before/after examples**
2. **Specific matchers** for Bandish to identify patterns
3. **Transformation rules** for code generation
4. **Context** about why the transformation is needed

The next step is to encode these patterns into Bandish task specifications with appropriate matchers.

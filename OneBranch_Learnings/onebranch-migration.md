# Transform Pipeline Name to OneBranch Versioning
`$match:files="*.yml"`
`$match:keyword="name:"`
`$match:regex="^name:\s+[\w-]+"`

Replace static pipeline names with OneBranch dynamic versioning format:
- Change `name: build-whl-pipeline` to `name: $(Year:YY)$(DayOfYear)$(Rev:.r))`
- Change `name: pr-validation-pipeline` to `name: $(Year:YY)$(DayOfYear)$(Rev:.r))`
- OneBranch uses dynamic build numbering: Year(2-digit) + DayOfYear + Revision
- This ensures unique, chronological build identification
- Format: `YYDDD.r` where YY=year, DDD=day of year, r=revision

Example:
```yaml
# Classic
name: build-whl-pipeline

# OneBranch
name: $(Year:YY)$(DayOfYear)$(Rev:.r))
```

---

# Add OneBranch Parameters Section
`$match:files="*.yml"`
`$match:keyword="jobs:"`
`$match:regex="^jobs:\s*$"`

Add a parameters section before variables for OneBranch configurability:
- Add `oneBranchType` parameter with values ['Official', 'NonOfficial']
- Add `buildConfiguration` parameter with values ['Release', 'Debug']
- Add `publishSymbols` boolean parameter (default: true)
- Add `runSdlTasks` boolean parameter (default: true)
- Parameters must be defined before variables section
- Use proper YAML indentation (4 spaces for nested items)
- All output files will be in OneBranchPipelines/ folder

Example:
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
      displayName: 'Publish Symbols to Symbol Server'
      type: boolean
      default: true

    - name: runSdlTasks
      displayName: 'Run SDL Security Tasks'
      type: boolean
      default: true
```

---

# Transform Inline Variables to Template References
`$match:files="*.yml"`
`$match:keyword="variables:"`
`$match:regex="^\s+[\w_]+:\s+"`

Replace inline variable definitions with template references:
- Change inline `variables:` section to template-based structure
- Add `- template: /OneBranchPipelines/variables/common-variables.yml@self`
- Add `- template: /OneBranchPipelines/variables/onebranch-variables.yml@self`
- Add `- template: /OneBranchPipelines/variables/build-variables.yml@self`
- Extract existing variables to appropriate template files
- Keep variable organization modular and maintainable

Example:
```yaml
# Classic
variables:
  pythonVersion: '3.13'
  buildConfig: 'Release'
  REPO_ROOT: '$(Build.SourcesDirectory)'

# OneBranch
variables:
    - template: /OneBranchPipelines/variables/common-variables.yml@self
    - template: /OneBranchPipelines/variables/onebranch-variables.yml@self
    - template: /OneBranchPipelines/variables/build-variables.yml@self
```

---

# Add OneBranch Resources Section
`$match:files="*.yml"`
`$match:regex="^variables:\s*$"`

Add OneBranch resources section after variables:
- Add `resources:` section with OneBranch.Pipelines/GovernedTemplates repository
- Reference `OneBranch.Pipelines/GovernedTemplates` from ADO
- Use `refs/heads/main` as the ref
- Repository type should be 'git'
- This provides access to OneBranch security templates

Example:
```yaml
resources:
    repositories:
        - repository: templates
          type: 'git'
          name: 'OneBranch.Pipelines/GovernedTemplates'
          ref: 'refs/heads/main'
```

---

# Transform Jobs Section to Extends Pattern
`$match:files="*.yml"`
`$match:keyword="jobs:"`
`$match:regex="^jobs:\s*$"`

Replace top-level `jobs:` section with OneBranch `extends:` pattern:
- Replace `jobs:` with `extends:` block
- Template path: `v2/OneBranch.${{ parameters.oneBranchType }}.CrossPlat.yml@templates`
- Wrap existing jobs in `stages:` hierarchy
- Add `featureFlags:` section with Windows and Linux host versions
- Add comprehensive `globalSdl:` configuration
- Extract jobs to separate template files in OneBranchPipelines/jobs/ directory

Example:
```yaml
# Classic
jobs:
  - job: BuildJob
    pool:
      vmImage: 'ubuntu-latest'

# OneBranch
extends:
    template: 'v2/OneBranch.${{ parameters.oneBranchType }}.CrossPlat.yml@templates'
    
    parameters:
        featureFlags:
            WindowsHostVersion:
                Version: '2022'
            LinuxHostVersion:
                Version: 'ubuntu-22.04'
                
        globalSdl:
            # SDL configuration here
            
        stages:
            - stage: BuildStage
              displayName: 'Build Packages'
              jobs:
                  - template: /OneBranchPipelines/jobs/build-job.yml@self
```

---

# Add Comprehensive SDL Configuration
`$match:files="*.yml"`
`$match:keyword="extends:"`
`$match:keyword="parameters:"`

Add complete globalSdl section within extends parameters:
- Enable ApiScan for API security scanning
- Enable Armory for security testing with break:true
- Enable BinSkim for binary analysis (break:true)
- Enable CodeInspector for code quality
- Enable CodeQL for security analysis (python,cpp)
- Enable CredScan with suppressions file
- Disable ESLint (not needed for Python)
- Enable PoliCheck with exclusions file (break:true)
- Disable Roslyn (not applicable to Python)
- Enable publishLogs for SDL results
- Enable SBOM generation
- Enable TSA for Official builds only
- All SDL tasks controlled by ${{ parameters.runSdlTasks }}

Example:
```yaml
globalSdl:
    apiscan:
        enabled: ${{ parameters.runSdlTasks }}
        softwareFolder: '$(Build.SourcesDirectory)/mssql_python'
        softwareName: 'mssql-python'
        softwareVersionNum: '$(packageVersion)'
        
    armory:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        
    binskim:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        analyzeTargetGlob: '+:file|**/*.pyd;+:file|**/*.so;+:file|**/*.dylib'
        
    codeinspector:
        enabled: ${{ parameters.runSdlTasks }}
        logLevel: Error
        
    codeql:
        enabled: ${{ parameters.runSdlTasks }}
        sourceRoot: '$(REPO_ROOT)'
        language: 'python,cpp'
        buildCommands: |
            cd $(REPO_ROOT)/mssql_python/pybind
            ./build.sh
            
    credscan:
        enabled: ${{ parameters.runSdlTasks }}
        suppressionsFile: '$(REPO_ROOT)/.config/CredScanSuppressions.json'
        
    eslint:
        enabled: false
        
    policheck:
        enabled: ${{ parameters.runSdlTasks }}
        break: true
        exclusionFile: '$(REPO_ROOT)/.config/PolicheckExclusions.xml'
        
    roslyn:
        enabled: false
        
    publishLogs:
        enabled: ${{ parameters.runSdlTasks }}
        
    sbom:
        enabled: ${{ parameters.runSdlTasks }}
        packageName: 'mssql-python'
        packageVersion: '$(packageVersion)'
        
    tsa:
        enabled: ${{ eq(parameters.oneBranchType, 'Official') }}
        configFile: '$(REPO_ROOT)/.config/tsaoptions.json'
```

---

# Transform CodeQL Tasks to Global SDL
`$match:keyword="CodeQL3000Init@"`
`$match:keyword="CodeQL3000Finalize@"`
`$match:regex="CodeQL\d+"`

Remove manual CodeQL tasks and use OneBranch globalSdl instead:
- Remove `CodeQL3000Init@0` task
- Remove `CodeQL3000Finalize@0` task
- Extract build commands between Init and Finalize
- Add build commands to globalSdl.codeql.buildCommands
- CodeQL is automatically managed by OneBranch
- Specify source languages: 'python,cpp'
- Set sourceRoot to repository root

Example:
```yaml
# Classic
- task: CodeQL3000Init@0
  inputs:
    Enabled: true
- script: |
    cd mssql_python/pybind
    ./build.sh
- task: CodeQL3000Finalize@0
  condition: always()

# OneBranch (in globalSdl)
codeql:
    enabled: ${{ parameters.runSdlTasks }}
    sourceRoot: '$(REPO_ROOT)'
    language: 'python,cpp'
    buildCommands: |
        cd $(REPO_ROOT)/mssql_python/pybind
        ./build.sh
```

---

# Transform Pool vmImage to OneBranch Type
`$match:keyword="pool:"`
`$match:keyword="vmImage:"`
`$match:regex="vmImage:\s+['\"].*['\"]"`

Replace vmImage pool specification with OneBranch pool type:
- Replace `vmImage: 'ubuntu-latest'` with `type: linux`
- Replace `vmImage: 'windows-latest'` with `type: windows`
- Replace `vmImage: 'macos-latest'` with appropriate type (usually linux)
- Remove vmImage key entirely
- OneBranch manages pool resources and security
- Pool type determines OneBranch managed agent pool

Example:
```yaml
# Classic
pool:
  vmImage: 'ubuntu-latest'

# OneBranch
pool:
  type: linux
```

```yaml
# Classic
pool:
  vmImage: 'windows-latest'

# OneBranch
pool:
  type: windows
```

---

# Transform Artifact Publishing to ob_outputDirectory
`$match:keyword="PublishBuildArtifacts@"`
`$match:regex="PublishBuildArtifacts@\d+"`

Replace PublishBuildArtifacts tasks with OneBranch automatic publishing:
- Remove all `PublishBuildArtifacts@1` or `PublishBuildArtifacts@2` tasks
- Add `ob_outputDirectory` variable to job variables section
- Set `ob_outputDirectory: '$(ARTIFACT_PATH)'`
- Add CopyFiles@2 task to copy artifacts to ob_outputDirectory
- OneBranch automatically publishes everything in ob_outputDirectory
- No need for explicit artifact publishing tasks

Example:
```yaml
# Classic
- task: PublishBuildArtifacts@1
  inputs:
    PathtoPublish: '$(Build.ArtifactStagingDirectory)/dist'
    ArtifactName: 'wheels'
    publishLocation: 'Container'

# OneBranch
# In job variables:
variables:
    ob_outputDirectory: '$(ARTIFACT_PATH)'

# In steps:
- task: CopyFiles@2
  inputs:
      SourceFolder: '$(Build.SourcesDirectory)/dist'
      Contents: '**/*.whl'
      TargetFolder: '$(ob_outputDirectory)'
  displayName: 'Stage artifacts for automatic publishing'
```

---

# Comment Out or Remove Trigger Configuration
`$match:keyword="trigger:"`
`$match:regex="^trigger:\s*$"`

Handle trigger configuration for OneBranch:
- Comment out existing `trigger:` section with explanation
- Add note that triggers are managed by OneBranch or repository settings
- Keep trigger configuration as comments for documentation
- Can be configured in Azure DevOps pipeline settings
- OneBranch templates may have their own trigger logic

Example:
```yaml
# Classic
trigger:
  branches:
    include:
      - main

# OneBranch
# Triggers managed by OneBranch template or repository pipeline settings
# trigger:
#   branches:
#     include:
#       - main
```

---

# Comment Out or Remove PR Triggers
`$match:keyword="pr:"`
`$match:regex="^pr:\s*$"`

Handle PR trigger configuration for OneBranch:
- Comment out existing `pr:` section
- Add note about OneBranch PR trigger management
- Keep as documentation comments
- Configure in pipeline settings if needed

Example:
```yaml
# Classic
pr:
  branches:
    include:
      - main

# OneBranch
# PR triggers managed by repository pipeline settings
# pr:
#   branches:
#     include:
#       - main
```

---

# Comment Out or Remove Schedule Configuration
`$match:keyword="schedules:"`
`$match:keyword="cron:"`

Handle schedule configuration for OneBranch:
- Comment out `schedules:` section
- Document cron expression for reference
- Note that schedules are configured in pipeline settings
- Keep original schedule information as comments

Example:
```yaml
# Classic
schedules:
  - cron: "30 1 * * *"
    displayName: Daily run at 07:00 AM IST
    branches:
      include:
        - main

# OneBranch
# Schedules managed by repository pipeline settings
# schedules:
#   - cron: "30 1 * * *"  # Daily at 07:00 AM IST
```

---

# Extract Job to Separate Template File
`$match:keyword="- job:"`
`$match:regex="^\s+- job:\s+\w+"`

Modularize pipeline by extracting jobs to separate files:
- Create jobs/ directory if it doesn't exist
- Extract job definition to `/OneBranchPipelines/jobs/{job-name}.yml`
- Replace inline job with template reference
- Add parameters for configuration values
- Update pool to OneBranch pool type
- Add ob_outputDirectory variable if artifacts are published

Example:
```yaml
# Classic (inline)
jobs:
  - job: BuildWindowsWheels
    pool:
      vmImage: 'windows-latest'
    steps:
      - task: UsePythonVersion@0
      - script: build commands

# OneBranch (main pipeline)
stages:
    - stage: BuildStage
      jobs:
          - template: /OneBranchPipelines/jobs/build-windows-job.yml@self
            parameters:
                buildConfiguration: '${{ parameters.buildConfiguration }}'

# OneBranch (jobs/build-windows-job.yml)
jobs:
    - job: BuildWindowsWheels
      displayName: 'Build Windows Wheels'
      pool:
          type: windows
      variables:
          ob_outputDirectory: '$(ARTIFACT_PATH)'
      steps:
          - template: ../steps/setup-python-step.yml@self
```

---

# Extract Steps to Template Files
`$match:keyword="steps:"`
`$match:regex="^\s+steps:\s*$"`

Create reusable step templates for common operations:
- Create steps/ directory if it doesn't exist
- Group related steps into compound templates
- Extract to `/OneBranchPipelines/steps/{step-name}.yml`
- Parameterize platform-specific logic
- Create templates for: setup, build, test, publish operations
- Reduce duplication across jobs

Example:
```yaml
# Classic (inline)
steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '$(pythonVersion)'
  - script: |
      pip install -r requirements.txt
  - script: |
      python setup.py bdist_wheel

# OneBranch (main job)
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

---

# Preserve Matrix Strategies
`$match:keyword="strategy:"`
`$match:keyword="matrix:"`

Keep matrix strategies but adjust formatting:
- Preserve matrix strategy structure
- Update indentation to match OneBranch style (4 spaces)
- Keep matrix variable definitions
- Matrix strategies work the same in OneBranch
- Can reference matrix variables in steps

Example:
```yaml
# Both Classic and OneBranch use same structure
strategy:
    matrix:
        py310_x64:
            pythonVersion: '3.10'
            architecture: 'x64'
        py311_x64:
            pythonVersion: '3.11'
            architecture: 'x64'
        py312_arm64:
            pythonVersion: '3.12'
            architecture: 'arm64'
```

---

# Preserve Test Results Publishing
`$match:keyword="PublishTestResults@"`
`$match:regex="PublishTestResults@\d+"`

Keep test result publishing tasks:
- Preserve `PublishTestResults@2` tasks
- Adjust indentation to OneBranch style
- Keep condition: succeededOrFailed()
- Test results are compatible with OneBranch
- Can be moved to test step templates for reusability

Example:
```yaml
# Both Classic and OneBranch
- task: PublishTestResults@2
  condition: succeededOrFailed()
  inputs:
      testResultsFiles: '**/test-results.xml'
      testRunTitle: 'Publish test results'
```

---

# Add Symbol Publishing for Release Builds
`$match:files="*.yml"`
`$match:keyword=".pdb"`

Add symbol publishing capability for debugging support:
- Add conditional symbol publishing step template
- Condition on ${{ parameters.publishSymbols }}
- Reference symbol server variables from variable groups
- Search for .pdb, .pyd files
- Upload to Microsoft Symbol Server
- Required for debugging production binaries

Example:
```yaml
- ${{ if parameters.publishSymbols }}:
    - template: ../steps/compound-publish-symbols-step.yml@self
      parameters:
          artifactName: 'symbols_$(System.TeamProject)_$(Build.SourceBranchName)_$(packageVersion)'
          azureSubscription: '$(SymbolsAzureSubscription)'
          publishServer: '$(SymbolsPublishServer)'
          searchPattern: |
              **/*.pdb
              **/*.pyd
          uploadAccount: '$(SymbolsUploadAccount)'
          version: '$(packageVersion)'
```

---

# Add ESRP Code Signing Step
`$match:files="*.yml"`
`$match:keyword="Build"`
`$match:regex="build.*wheel|bdist_wheel"`

Add enterprise code signing after build steps:
- Add ESRP code signing step template after build
- Sign .pyd, .so, .dylib files (artifactType: 'dll')
- Sign .nupkg files (artifactType: 'pkg')
- Use Azure Key Vault for certificates
- Include malware scanning before signing
- Reference signing variables from variable groups

Example:
```yaml
- template: ../steps/compound-esrp-code-signing-step.yml@self
  parameters:
      appRegistrationClientId: '$(SigningAppRegistrationClientId)'
      appRegistrationTenantId: '$(SigningAppRegistrationTenantId)'
      artifactType: 'dll'
      authAkvName: '$(SigningAuthAkvName)'
      authSignCertName: '$(SigningAuthSignCertName)'
      esrpClientId: '$(SigningEsrpClientId)'
      esrpConnectedServiceName: '$(SigningEsrpConnectedServiceName)'
```

---

# Create Common Variables Template
`$match:files="*.yml"`
`$match:any`

Create common-variables.yml for shared variables:
- Define REPO_ROOT as readonly
- Define BUILD_OUTPUT directory
- Define ARTIFACT_PATH for staging
- Define package name and version
- Use $(Build.SourcesDirectory) for repo root
- Mark path variables as readonly when appropriate

Create file: `/OneBranchPipelines/variables/common-variables.yml`
```yaml
variables:
    - name: REPO_ROOT
      value: '$(Build.SourcesDirectory)'
      readonly: true
      
    - name: BUILD_OUTPUT
      value: '$(Build.SourcesDirectory)/dist'
      readonly: true
      
    - name: ARTIFACT_PATH
      value: '$(Build.SourcesDirectory)/artifacts'
      readonly: true
      
    - name: packageName
      value: 'mssql-python'
      
    - name: packageVersion
      value: '1.0.0'
```

---

# Create OneBranch Variables Template
`$match:files="*.yml"`
`$match:any`

Create onebranch-variables.yml for OneBranch-specific configuration:
- Enable SBOM signing
- Define Windows container image
- Define Linux container image
- Use OneBranch provided container images
- Reference OneBranch.azurecr.io registry

Create file: `/OneBranchPipelines/variables/onebranch-variables.yml`
```yaml
variables:
    - name: Packaging.EnableSBOMSigning
      value: true
      
    - name: WindowsContainerImage
      value: 'onebranch.azurecr.io/windows/ltsc2022/vse2022:latest'
      
    - name: LinuxContainerImage
      value: 'onebranch.azurecr.io/linux/ubuntu-2204:latest'
```

---

# Create Build Variables Template
`$match:files="*.yml"`
`$match:keyword="pythonVersion"`
`$match:keyword="architecture"`

Create build-variables.yml for build-specific configuration:
- Define Python versions to build (3.10, 3.11, 3.12, 3.13)
- Define target architectures by platform
- Windows: x64, arm64
- macOS: universal2
- Linux: x86_64, aarch64
- Define build configuration
- These can be referenced in matrix strategies

Create file: `/OneBranchPipelines/variables/build-variables.yml`
```yaml
variables:
    - name: pythonVersions
      value: ['3.10', '3.11', '3.12', '3.13']
      
    - name: windowsArchitectures
      value: ['x64', 'arm64']
      
    - name: macosArchitectures
      value: ['universal2']
      
    - name: linuxArchitectures
      value: ['x86_64', 'aarch64']
      
    - name: linuxDistros
      value: ['manylinux', 'musllinux']
```

---

# Create Signing Variables Template
`$match:files="*.yml"`
`$match:any`

Create signing-variables.yml for ESRP code signing:
- Reference variable group 'esrp-variables-v2'
- Contains: SigningAppRegistrationClientId
- Contains: SigningAppRegistrationTenantId
- Contains: SigningAuthAkvName
- Contains: SigningAuthSignCertName
- Contains: SigningEsrpClientId
- Contains: SigningEsrpConnectedServiceName
- Variable group must be created in Azure DevOps Library

Create file: `/OneBranchPipelines/variables/signing-variables.yml`
```yaml
variables:
    - group: 'esrp-variables-v2'
      # SigningAppRegistrationClientId
      # SigningAppRegistrationTenantId
      # SigningAuthAkvName
      # SigningAuthSignCertName
      # SigningEsrpClientId
      # SigningEsrpConnectedServiceName
```

---

# Create Symbol Publishing Variables Template
`$match:files="*.yml"`
`$match:any`

Create symbol-variables.yml for symbol server configuration:
- Reference variable group 'symbol-publishing-variables'
- Contains: SymbolsAzureSubscription
- Contains: SymbolsPublishServer
- Contains: SymbolsPublishTokenUri
- Contains: SymbolsUploadAccount
- Contains: SymbolsPublishProjectName
- Variable group must be configured in Azure DevOps

Create file: `/OneBranchPipelines/variables/symbol-variables.yml`
```yaml
variables:
    - group: 'symbol-publishing-variables'
      # SymbolsAzureSubscription
      # SymbolsPublishServer
      # SymbolsPublishTokenUri
      # SymbolsUploadAccount
      # SymbolsPublishProjectName
```

---

# Create SDL Suppression Configuration Files
`$match:files="*.yml"`
`$match:keyword="globalSdl"`

Create SDL configuration files for suppressions:
- Create `.config/` directory in repository root
- Create `CredScanSuppressions.json` for credential scan suppressions
- Create `PolicheckExclusions.xml` for policy check exclusions
- Create `tsaoptions.json` for TSA configuration
- These files control SDL tool behavior
- Empty files are valid for initial setup

Create files:
1. `.config/CredScanSuppressions.json`
```json
{
  "tool": "Credential Scanner",
  "suppressions": []
}
```

2. `.config/PolicheckExclusions.xml`
```xml
<?xml version="1.0" encoding="utf-8"?>
<PoliCheckExclusions>
  <Exclusion Type="Folder">
    <Name>node_modules</Name>
  </Exclusion>
</PoliCheckExclusions>
```

3. `.config/tsaoptions.json`
```json
{
  "codebaseName": "mssql-python",
  "notificationAliases": [],
  "codebaseAdmins": [],
  "instanceUrl": "https://devdiv.visualstudio.com/DefaultCollection",
  "projectName": "DevDiv",
  "areaPath": "DevDiv\\Data and AI\\SQL Connectivity",
  "iterationPath": "DevDiv"
}
```

---

# Handle Docker Container Usage
`$match:keyword="docker"`
`$match:regex="docker\s+(run|build|exec|pull)"`

Document Docker usage for OneBranch compliance:
- Preserve Docker commands (may need compliance review)
- Ensure container images are from approved registries
- Document which containers are used and why
- Consider OneBranch container integration
- Add comments about container security scanning
- Microsoft container images (mcr.microsoft.com) are pre-approved

Example:
```yaml
# Docker usage preserved but documented for OneBranch
- script: |
    # Using Microsoft-provided SQL Server container (pre-approved)
    docker run -d --name sqlserver \
      -e ACCEPT_EULA=Y \
      -e MSSQL_SA_PASSWORD="$(DB_PASSWORD)" \
      -p 1433:1433 \
      mcr.microsoft.com/mssql/server:2022-latest
  displayName: 'Start SQL Server Container (OneBranch Approved)'
```

---

# Reorganize Pipeline File Structure
`$match:files="*.yml"`
`$match:any`

Create modular directory structure for OneBranch pipeline:
- Create `/OneBranchPipelines/` main directory
- Create `/OneBranchPipelines/jobs/` for job templates
- Create `/OneBranchPipelines/steps/` for step templates
- Create `/OneBranchPipelines/stages/` for stage templates
- Create `/OneBranchPipelines/variables/` for variable templates
- Keep eng/ folder untouched with original classic pipelines
- Main pipeline file: `build-whl-pipeline.yml`
- Keep directory structure organized and consistent

Directory structure:
```
OneBranchPipelines/
├── build-whl-pipeline.yml
├── jobs/
│   ├── build-windows-job.yml
│   ├── build-macos-job.yml
│   └── build-linux-job.yml
├── steps/
│   ├── setup-python-step.yml
│   ├── build-wheel-step.yml
│   └── run-tests-step.yml
└── variables/
    ├── common-variables.yml
    ├── onebranch-variables.yml
    ├── build-variables.yml
    └── signing-variables.yml

eng/pipelines/  (unchanged)
├── build-whl-pipeline.yml
└── pr-validation-pipeline.yml
```

---

# Add Pipeline Documentation Header
`$match:files="*.yml"`
`$match:any`

Add comprehensive documentation header to pipeline files:
- Include copyright and license information
- Add pipeline purpose and description
- Document parameters and their usage
- List prerequisites and dependencies
- Include OneBranch migration date
- Reference related documentation

Example:
```yaml
#################################################################################
# OneBranch Build-Release-Package Pipeline
# Purpose: Build Python wheels for all platforms and architectures
# 
# Platforms: Windows (x64, ARM64), macOS (universal2), Linux (x64, ARM64)
# Python Versions: 3.10, 3.11, 3.12, 3.13
# 
# Features:
# - Multi-platform wheel building
# - Native extension compilation
# - ESRP code signing
# - Symbol publishing
# - Comprehensive SDL security scanning
# 
# Migrated to OneBranch: October 2025
#################################################################################
```

---

# Add Stage Organization
`$match:keyword="extends:"`
`$match:keyword="stages:"`

Organize pipeline into logical stages:
- Create BuildPackages stage for compilation
- Create TestPackages stage for validation
- Create PublishPackages stage for distribution (optional)
- Use dependsOn for stage dependencies
- Each stage groups related jobs
- Clear separation of concerns

Example:
```yaml
stages:
    - stage: BuildPackages
      displayName: 'Build Python Packages'
      jobs:
          - template: /OneBranchPipelines/jobs/build-windows-job.yml@self
          - template: /OneBranchPipelines/jobs/build-macos-job.yml@self
          - template: /OneBranchPipelines/jobs/build-linux-job.yml@self
          
    - stage: TestPackages
      displayName: 'Test Python Packages'
      dependsOn: BuildPackages
      jobs:
          - template: /OneBranchPipelines/jobs/test-packages-job.yml@self
```

---

# Handle Environment Variables and Secrets
`$match:keyword="env:"`
`$match:regex="\$\([A-Z_]+\)"`

Properly handle environment variables and secrets:
- Move secrets to variable groups
- Reference variable groups in variables section
- Keep constructed connection strings inline
- Document which variables are secrets
- Use variable group references for sensitive data
- Secrets are managed separately from pipeline code

Example:
```yaml
# Classic
env:
  DB_PASSWORD: $(DB_PASSWORD)

# OneBranch
# In variables section:
variables:
    - group: 'build-secrets'
      # DB_PASSWORD defined in variable group

# In step env:
env:
    DB_CONNECTION_STRING: 'Server=...;Pwd=$(DB_PASSWORD);'
```

---

# Add Feature Flags Configuration
`$match:keyword="extends:"`
`$match:keyword="parameters:"`

Configure OneBranch feature flags:
- Set WindowsHostVersion to '2022' for Windows Server 2022
- Set LinuxHostVersion to 'ubuntu-22.04' for Ubuntu 22.04
- Feature flags control OneBranch infrastructure
- These determine agent capabilities
- Required for OneBranch template execution

Example:
```yaml
extends:
    template: 'v2/OneBranch.${{ parameters.oneBranchType }}.CrossPlat.yml@templates'
    
    parameters:
        featureFlags:
            WindowsHostVersion:
                Version: '2022'
            LinuxHostVersion:
                Version: 'ubuntu-22.04'
```

---

# Python Multi-Platform Build Strategy
`$match:files="*.yml"`
`$match:keyword="python"`
`$match:keyword="bdist_wheel"`

Implement Python-specific build patterns for OneBranch:
- Support Windows x64 and ARM64 architectures
- Support macOS universal2 binaries
- Support Linux manylinux and musllinux wheels
- Support multiple Python versions (3.10-3.13)
- Build native extensions (pybind11)
- Package platform-specific wheels
- Maintain ABI compatibility

Strategy considerations:
- Windows: Use build.bat for native compilation
- macOS: Use build.sh with universal2 flags
- Linux: Use manylinux/musllinux containers
- Matrix strategies for version/platform combinations
- Separate jobs per platform for parallel execution

---

# Native Extension Build Handling
`$match:keyword="pybind"`
`$match:keyword="cmake"`
`$match:regex="build\.(bat|sh)"`

Handle native C++ extension builds for OneBranch:
- Native extensions require compilation
- Use CMake for cross-platform builds
- pybind11 for Python-C++ bindings
- Platform-specific build scripts (build.bat, build.sh)
- Architecture-specific compilation
- Debug/Release configurations
- Preserve build artifacts (.pyd, .so, .dylib)

Ensure:
- Build environment has compilers (MSVC, GCC, Clang)
- CMake and pybind11 are installed
- Build scripts are executable
- Artifacts are properly staged

---

# Test Infrastructure Setup
`$match:keyword="pytest"`
`$match:keyword="sqlserver"`
`$match:keyword="test"`

Configure test infrastructure for OneBranch:
- Start SQL Server containers for integration tests
- Install test dependencies (pytest, coverage)
- Run tests against built wheels
- Publish test results
- Generate code coverage reports
- Support multiple platforms and Python versions

Test considerations:
- Use docker for SQL Server on Linux/macOS
- Use LocalDB on Windows
- Wait for SQL Server readiness
- Clean up containers after tests
- Continue on test failures for better diagnostics

---

# Cross-Platform Compatibility
`$match:files="*.yml"`
`$match:regex="(windows|linux|macos|darwin)"`

Ensure cross-platform script compatibility:
- Use platform-specific script syntax
- PowerShell for Windows scripts
- Bash for Linux/macOS scripts
- Handle path separators correctly (\ vs /)
- Use appropriate line endings
- Platform-conditional steps with ${{ if eq(...) }}
- Test scripts on target platforms

Example:
```yaml
- script: |
    ${{ if eq(parameters.platform, 'windows') }}:
        call build.bat $(architecture)
    ${{ else }}:
        ./build.sh
  displayName: 'Build Native Extension'
```

---

# Artifact Management and Organization
`$match:keyword="artifact"`
`$match:keyword="dist"`
`$match:regex="(\.whl|\.pyd|\.so|\.dylib)"`

Organize build artifacts for OneBranch automatic publishing:
- Copy wheels to $(ob_outputDirectory)/wheels
- Copy bindings to $(ob_outputDirectory)/bindings
- Copy symbols to $(ob_outputDirectory)/symbols
- Maintain platform-specific subdirectories
- Clear naming conventions for artifacts
- Preserve debug symbols (.pdb files)

Artifact structure:
```
$(ob_outputDirectory)/
├── wheels/
│   ├── windows/
│   ├── macos/
│   └── linux/
├── bindings/
│   ├── windows/
│   ├── macos/
│   └── linux/
└── symbols/
```

---

# Version Management and Tagging
`$match:files="*.yml"`
`$match:keyword="version"`
`$match:regex="[\d]+\.[\d]+\.[\d]+"`

Implement version management for packages:
- Extract version from source code or file
- Use semantic versioning (MAJOR.MINOR.PATCH)
- Include version in package names
- Tag symbols with version
- Include version in SDL SBOM
- Dynamic version calculation from git tags
- Consistent versioning across artifacts

Consider:
- Read version from setup.py or __init__.py
- Pass version as pipeline parameter
- Store in packageVersion variable
- Use in artifact names and SDL configuration

---

# Conditional Logic for Official vs NonOfficial
`$match:keyword="parameters.oneBranchType"`
`$match:regex="\$\{\{\s*eq\(parameters\.oneBranchType"`

Implement different behavior for Official vs NonOfficial builds:
- TSA (security reporting) only for Official builds
- Code signing only for Official builds
- Symbol publishing only for Official builds
- Full SDL scanning always enabled
- Use conditional syntax: ${{ eq(parameters.oneBranchType, 'Official') }}
- NonOfficial for testing and validation

Example:
```yaml
- ${{ if eq(parameters.oneBranchType, 'Official') }}:
    - template: ../steps/compound-esrp-code-signing-step.yml@self
```

---

# Add Compliance and License Headers
`$match:files="*.yml"`
`$match:any`

Add Microsoft compliance headers to all pipeline files:
- Include .NET Foundation license header
- Reference MIT license
- Add copyright information
- Required for Microsoft projects
- Consistent across all template files
- Placed at top of each YAML file

Example:
```yaml
#################################################################################
# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.
#################################################################################
```

---

# Final Migration Checklist
`$match:any`

Complete these steps for successful OneBranch migration:

**Pre-Migration:**
- ✅ Review current pipeline functionality
- ✅ Document all custom scripts and tools
- ✅ Identify secrets and variable groups
- ✅ Plan rollout strategy

**Migration:**
- ✅ Create OneBranch directory structure
- ✅ Transform pipeline to extends pattern
- ✅ Extract jobs to templates
- ✅ Extract steps to templates
- ✅ Create variable templates
- ✅ Add globalSdl configuration
- ✅ Configure ESRP code signing
- ✅ Set up symbol publishing
- ✅ Create SDL suppression files
- ✅ Add compliance headers

**Testing:**
- ✅ Test with NonOfficial template first
- ✅ Validate all build artifacts
- ✅ Verify security scanning results
- ✅ Test symbol publishing
- ✅ Validate code signing
- ✅ Run full integration tests

**Post-Migration:**
- ✅ Switch to Official template
- ✅ Monitor first production run
- ✅ Update documentation
- ✅ Train team on new structure
- ✅ Archive classic pipelines
- ✅ Celebrate success! 🎉

---

# End of Knowledge Base

This comprehensive knowledge base provides all transformation patterns needed to migrate classic Azure DevOps pipelines to OneBranch security framework using Bandish code generation tool.

**Usage with Bandish:**
```bash
bandish run --kb onebranch-migration.md -i OneBranchPipelines/build-whl-pipeline.yml
```

**Key Features:**
- 40+ task specifications covering all transformation patterns
- Comprehensive matchers (files, keyword, regex, content)
- Step-by-step transformation instructions
- Real-world examples from mssql-python project
- Security-first approach with SDL integration
- Modular, maintainable pipeline structure
- Cross-platform support (Windows, macOS, Linux)
- Multi-architecture support (x64, ARM64)
- Python-specific patterns and best practices

**Target Pipeline:** Build-Release-Package
**Functions:** Build Python packages, publish symbols, CodeQL scanning

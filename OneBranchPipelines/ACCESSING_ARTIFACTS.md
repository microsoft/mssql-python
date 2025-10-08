# How to Access OneBranch Artifacts

## Overview 📦

OneBranch automatically publishes `$(ob_outputDirectory)` as pipeline artifacts with a **standardized naming convention**:

```
drop_<StageName>_<JobName>
```

For our pipeline:
- Windows: `drop_Build_BuildWindowsWheels`
- macOS: `drop_Build_BuildMacOSWheels`
- Linux: `drop_Build_BuildLinuxWheels` (varies by matrix configuration)

## Artifact Structure 🗂️

### Windows Artifacts
```
drop_Build_BuildWindowsWheels/
├── wheels/
│   ├── mssql_python-2.0.0-cp310-cp310-win_amd64.whl
│   ├── mssql_python-2.0.0-cp311-cp311-win_amd64.whl
│   ├── mssql_python-2.0.0-cp311-cp311-win_arm64.whl
│   ├── mssql_python-2.0.0-cp312-cp312-win_amd64.whl
│   ├── mssql_python-2.0.0-cp312-cp312-win_arm64.whl
│   ├── mssql_python-2.0.0-cp313-cp313-win_amd64.whl
│   └── mssql_python-2.0.0-cp313-cp313-win_arm64.whl
├── bindings/
│   └── windows/
│       ├── ddbc_bindings.cp310-win_amd64.pyd
│       ├── ddbc_bindings.cp311-win_amd64.pyd
│       ├── ddbc_bindings.cp311-win_arm64.pyd
│       ├── ddbc_bindings.cp312-win_amd64.pyd
│       ├── ddbc_bindings.cp312-win_arm64.pyd
│       ├── ddbc_bindings.cp313-win_amd64.pyd
│       └── ddbc_bindings.cp313-win_arm64.pyd
└── symbols/
    ├── ddbc_bindings.cp310-win_amd64.pdb
    ├── ddbc_bindings.cp311-win_amd64.pdb
    ├── ddbc_bindings.cp311-win_arm64.pdb
    ├── ddbc_bindings.cp312-win_amd64.pdb
    ├── ddbc_bindings.cp312-win_arm64.pdb
    ├── ddbc_bindings.cp313-win_amd64.pdb
    └── ddbc_bindings.cp313-win_arm64.pdb
```

### macOS Artifacts
```
drop_Build_BuildMacOSWheels/
├── wheels/
│   ├── mssql_python-2.0.0-cp310-cp310-macosx_11_0_universal2.whl
│   ├── mssql_python-2.0.0-cp311-cp311-macosx_11_0_universal2.whl
│   ├── mssql_python-2.0.0-cp312-cp312-macosx_11_0_universal2.whl
│   └── mssql_python-2.0.0-cp313-cp313-macosx_11_0_universal2.whl
└── bindings/
    └── macOS/
        ├── ddbc_bindings.cp310-darwin.so
        ├── ddbc_bindings.cp311-darwin.so
        ├── ddbc_bindings.cp312-darwin.so
        └── ddbc_bindings.cp313-darwin.so
```

### Linux Artifacts
```
drop_Build_BuildLinuxWheels/
├── wheels/
│   ├── mssql_python-2.0.0-cp310-cp310-manylinux_2_28_x86_64.whl
│   ├── mssql_python-2.0.0-cp310-cp310-manylinux_2_28_aarch64.whl
│   ├── mssql_python-2.0.0-cp310-cp310-musllinux_1_2_x86_64.whl
│   ├── mssql_python-2.0.0-cp310-cp310-musllinux_1_2_aarch64.whl
│   └── ... (similar for cp311, cp312, cp313)
└── bindings/
    ├── manylinux-x86_64/
    │   ├── ddbc_bindings.cp310-linux-x86_64.so
    │   └── ...
    ├── manylinux-aarch64/
    │   ├── ddbc_bindings.cp310-linux-aarch64.so
    │   └── ...
    ├── musllinux-x86_64/
    │   ├── ddbc_bindings.cp310-linux-x86_64.so
    │   └── ...
    └── musllinux-aarch64/
        ├── ddbc_bindings.cp310-linux-aarch64.so
        └── ...
```

## Accessing Artifacts 🔍

### 1. From Azure DevOps Web UI 🌐

#### Method A: Pipeline Run Page
1. Navigate to your pipeline run
2. Click on the **Build** stage
3. Click on the job (e.g., `BuildWindowsWheels`)
4. Click the **"N published"** link at the top of the job summary
5. You'll see artifacts like `drop_Build_BuildWindowsWheels`
6. Click to browse or download

**Direct URL Pattern**:
```
https://dev.azure.com/<organization>/<project>/_build/results?buildId=<buildId>&view=artifacts
```

#### Method B: Artifacts Tab
1. Go to your pipeline run
2. Click the **"Artifacts"** button in the top-right corner
3. Browse all artifacts from the run
4. Download entire artifact or specific files

#### Method C: Download Artifact
```
Right-click on artifact → "Download artifacts"
```

Downloads a ZIP file with the artifact contents.

### 2. From Azure CLI 💻

#### Install Azure CLI
```bash
# macOS
brew install azure-cli

# Windows
winget install Microsoft.AzureCLI

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

#### Login
```bash
az login
az devops configure --defaults organization=https://dev.azure.com/<organization> project=<project>
```

#### Download Artifacts
```bash
# List artifacts for a build
az pipelines runs artifact list --run-id <buildId>

# Download specific artifact
az pipelines runs artifact download \
  --run-id <buildId> \
  --artifact-name drop_Build_BuildWindowsWheels \
  --path ./artifacts

# Example: Download all wheels
az pipelines runs artifact download \
  --run-id 12345 \
  --artifact-name drop_Build_BuildWindowsWheels \
  --path ./downloads

# Now you have:
# ./downloads/drop_Build_BuildWindowsWheels/wheels/*.whl
# ./downloads/drop_Build_BuildWindowsWheels/bindings/windows/*.pyd
```

### 3. From Another Pipeline Stage/Job 🔄

#### Download in Subsequent Stage
```yaml
stages:
  - stage: Build
    jobs:
      - template: /OneBranchPipelines/jobs/build-windows-job.yml
        # Automatically publishes drop_Build_BuildWindowsWheels

  - stage: Test
    dependsOn: Build
    jobs:
      - job: TestWindows
        steps:
          - task: DownloadPipelineArtifact@2
            inputs:
              artifact: 'drop_Build_BuildWindowsWheels'
              path: '$(Pipeline.Workspace)/artifacts'
            displayName: 'Download Windows build artifacts'
          
          # Now you can access:
          # $(Pipeline.Workspace)/artifacts/wheels/*.whl
          # $(Pipeline.Workspace)/artifacts/bindings/windows/*.pyd
          
          - script: |
              ls -la $(Pipeline.Workspace)/artifacts/wheels/
              ls -la $(Pipeline.Workspace)/artifacts/bindings/windows/
            displayName: 'List downloaded artifacts'
```

#### Download Specific Subdirectory
```yaml
- task: DownloadPipelineArtifact@2
  inputs:
    artifact: 'drop_Build_BuildWindowsWheels'
    path: '$(Pipeline.Workspace)/wheels'
    patterns: 'wheels/**'
  displayName: 'Download only wheels'

# Or download just bindings
- task: DownloadPipelineArtifact@2
  inputs:
    artifact: 'drop_Build_BuildWindowsWheels'
    path: '$(Pipeline.Workspace)/bindings'
    patterns: 'bindings/**'
  displayName: 'Download only bindings'
```

#### Download Multiple Platform Artifacts
```yaml
- task: DownloadPipelineArtifact@2
  inputs:
    artifact: 'drop_Build_BuildWindowsWheels'
    path: '$(Pipeline.Workspace)/windows'

- task: DownloadPipelineArtifact@2
  inputs:
    artifact: 'drop_Build_BuildMacOSWheels'
    path: '$(Pipeline.Workspace)/macos'

- task: DownloadPipelineArtifact@2
  inputs:
    artifact: 'drop_Build_BuildLinuxWheels'
    path: '$(Pipeline.Workspace)/linux'

# Result:
# $(Pipeline.Workspace)/
# ├── windows/
# │   ├── wheels/
# │   └── bindings/
# ├── macos/
# │   ├── wheels/
# │   └── bindings/
# └── linux/
#     ├── wheels/
#     └── bindings/
```

### 4. From PowerShell/Bash Script 🔧

#### PowerShell Script
```powershell
# Set variables
$organization = "your-org"
$project = "your-project"
$buildId = "12345"
$artifactName = "drop_Build_BuildWindowsWheels"
$downloadPath = "./artifacts"

# Get PAT token (set as environment variable)
$token = $env:AZURE_DEVOPS_PAT
$base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(":$($token)"))

# Download artifact
$url = "https://dev.azure.com/$organization/$project/_apis/build/builds/$buildId/artifacts?artifactName=$artifactName&api-version=7.1"

Invoke-RestMethod -Uri $url -Method Get -Headers @{Authorization=("Basic {0}" -f $base64AuthInfo)} | 
  ForEach-Object {
    Invoke-WebRequest -Uri $_.resource.downloadUrl -Headers @{Authorization=("Basic {0}" -f $base64AuthInfo)} -OutFile "$downloadPath/$artifactName.zip"
  }

# Extract
Expand-Archive -Path "$downloadPath/$artifactName.zip" -DestinationPath $downloadPath
```

#### Bash Script
```bash
#!/bin/bash

ORGANIZATION="your-org"
PROJECT="your-project"
BUILD_ID="12345"
ARTIFACT_NAME="drop_Build_BuildWindowsWheels"
DOWNLOAD_PATH="./artifacts"
PAT_TOKEN="${AZURE_DEVOPS_PAT}"

# Create download directory
mkdir -p "$DOWNLOAD_PATH"

# Get artifact download URL
ARTIFACT_URL="https://dev.azure.com/$ORGANIZATION/$PROJECT/_apis/build/builds/$BUILD_ID/artifacts?artifactName=$ARTIFACT_NAME&api-version=7.1"

DOWNLOAD_URL=$(curl -s -u ":$PAT_TOKEN" "$ARTIFACT_URL" | jq -r '.resource.downloadUrl')

# Download artifact
curl -u ":$PAT_TOKEN" -o "$DOWNLOAD_PATH/$ARTIFACT_NAME.zip" "$DOWNLOAD_URL"

# Extract
unzip "$DOWNLOAD_PATH/$ARTIFACT_NAME.zip" -d "$DOWNLOAD_PATH"

echo "Artifacts downloaded to $DOWNLOAD_PATH"
ls -la "$DOWNLOAD_PATH"
```

### 5. From Python Script 🐍

```python
import os
import requests
import zipfile
from pathlib import Path

# Configuration
ORGANIZATION = "your-org"
PROJECT = "your-project"
BUILD_ID = "12345"
ARTIFACT_NAME = "drop_Build_BuildWindowsWheels"
DOWNLOAD_PATH = Path("./artifacts")
PAT_TOKEN = os.environ.get("AZURE_DEVOPS_PAT")

# Create download directory
DOWNLOAD_PATH.mkdir(exist_ok=True)

# Get artifact info
artifact_url = (
    f"https://dev.azure.com/{ORGANIZATION}/{PROJECT}/_apis/build/builds/"
    f"{BUILD_ID}/artifacts?artifactName={ARTIFACT_NAME}&api-version=7.1"
)

response = requests.get(
    artifact_url,
    auth=("", PAT_TOKEN),
    headers={"Content-Type": "application/json"}
)
artifact_info = response.json()

# Download artifact
download_url = artifact_info["resource"]["downloadUrl"]
artifact_zip = DOWNLOAD_PATH / f"{ARTIFACT_NAME}.zip"

print(f"Downloading {ARTIFACT_NAME}...")
response = requests.get(download_url, auth=("", PAT_TOKEN))
artifact_zip.write_bytes(response.content)

# Extract
print(f"Extracting to {DOWNLOAD_PATH}...")
with zipfile.ZipFile(artifact_zip, 'r') as zip_ref:
    zip_ref.extractall(DOWNLOAD_PATH)

print("Done!")
print(f"Wheels: {list((DOWNLOAD_PATH / 'wheels').glob('*.whl'))}")
print(f"Bindings: {list((DOWNLOAD_PATH / 'bindings').glob('**/*.pyd'))}")
```

### 6. Publish to PyPI or Artifact Feed 📤

#### Publish to Azure Artifacts Feed
```yaml
- stage: Publish
  dependsOn: Build
  jobs:
    - job: PublishToFeed
      steps:
        - task: DownloadPipelineArtifact@2
          inputs:
            artifact: 'drop_Build_BuildWindowsWheels'
            patterns: 'wheels/**'
            path: '$(Pipeline.Workspace)/wheels'
        
        - task: TwineAuthenticate@1
          inputs:
            artifactFeed: 'your-feed-name'
        
        - script: |
            python -m pip install twine
            python -m twine upload -r your-feed-name --config-file $(PYPIRC_PATH) $(Pipeline.Workspace)/wheels/wheels/*.whl
          displayName: 'Upload wheels to Azure Artifacts'
```

#### Publish to PyPI
```yaml
- stage: PublishToPyPI
  dependsOn: Build
  condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
  jobs:
    - job: UploadToPyPI
      steps:
        # Download all platform wheels
        - task: DownloadPipelineArtifact@2
          inputs:
            artifact: 'drop_Build_BuildWindowsWheels'
            patterns: 'wheels/**'
            path: '$(Pipeline.Workspace)/dist'
        
        - task: DownloadPipelineArtifact@2
          inputs:
            artifact: 'drop_Build_BuildMacOSWheels'
            patterns: 'wheels/**'
            path: '$(Pipeline.Workspace)/dist'
        
        - task: DownloadPipelineArtifact@2
          inputs:
            artifact: 'drop_Build_BuildLinuxWheels'
            patterns: 'wheels/**'
            path: '$(Pipeline.Workspace)/dist'
        
        - task: TwineAuthenticate@1
          inputs:
            pythonUploadServiceConnection: 'PyPI-Connection'
        
        - script: |
            python -m pip install twine
            python -m twine upload --config-file $(PYPIRC_PATH) $(Pipeline.Workspace)/dist/**/*.whl
          displayName: 'Upload to PyPI'
          env:
            TWINE_USERNAME: __token__
            TWINE_PASSWORD: $(PYPI_TOKEN)
```

## Finding Artifact Names 🔎

### In Pipeline YAML
Artifact names follow the pattern: `drop_<StageName>_<JobName>`

Look at your pipeline structure:
```yaml
stages:
  - stage: Build              # ← Stage name
    jobs:
      - job: BuildWindowsWheels  # ← Job name
        # Artifact: drop_Build_BuildWindowsWheels
```

### Using Azure CLI
```bash
# List all artifacts from a build
az pipelines runs artifact list --run-id <buildId> --output table

# Output:
# Name                              Type
# --------------------------------  --------
# drop_Build_BuildWindowsWheels     pipeline
# drop_Build_BuildMacOSWheels       pipeline
# drop_Build_BuildLinuxWheels       pipeline
```

### In Build Logs
Look for OneBranch's automatic artifact publishing step:
```
OneBranch Artifact Publishing
Publishing artifact: drop_Build_BuildWindowsWheels
Source: D:\a\1\a
Published 127 files
```

## Common Use Cases 💼

### Use Case 1: Manual Testing
**Goal**: Download wheels to test locally

**Steps**:
1. Go to Azure DevOps → Pipeline run → Artifacts
2. Click `drop_Build_BuildWindowsWheels`
3. Navigate to `wheels/`
4. Download specific wheel
5. Install locally: `pip install mssql_python-2.0.0-cp310-cp310-win_amd64.whl`

### Use Case 2: Release Pipeline
**Goal**: Publish all platform wheels to PyPI

**Solution**: See "Publish to PyPI" section above

### Use Case 3: Integration Testing
**Goal**: Test wheels in multiple environments

```yaml
- stage: IntegrationTest
  dependsOn: Build
  jobs:
    - job: TestOnWindows
      pool: { vmImage: 'windows-latest' }
      steps:
        - task: DownloadPipelineArtifact@2
          inputs:
            artifact: 'drop_Build_BuildWindowsWheels'
            patterns: 'wheels/*.whl'
        - script: |
            pip install wheels/*.whl
            python -m pytest tests/
    
    - job: TestOnMacOS
      pool: { vmImage: 'macos-latest' }
      steps:
        - task: DownloadPipelineArtifact@2
          inputs:
            artifact: 'drop_Build_BuildMacOSWheels'
            patterns: 'wheels/*.whl'
        - script: |
            pip install wheels/*.whl
            python -m pytest tests/
```

### Use Case 4: Customer Distribution
**Goal**: Provide pre-built wheels to customers

**Options**:
1. **GitHub Release**: Download artifacts, attach to GitHub release
2. **Azure Blob Storage**: Upload artifacts to public blob
3. **CDN**: Distribute via CDN for fast downloads
4. **Private Feed**: Use Azure Artifacts for enterprise customers

## Retention Policy 📅

OneBranch artifacts follow Azure DevOps retention policies:

- **Default**: 30 days for pipeline runs
- **Custom**: Can be configured in project settings
- **Official Builds**: Often retained longer (90+ days)
- **Release Artifacts**: Can be retained indefinitely

**To extend retention**:
```yaml
- stage: Build
  jobs:
    - job: BuildWindowsWheels
      # Set longer retention for this build
      variables:
        Build.RetentionDays: 90
```

## Troubleshooting 🔧

### Artifact Not Found
**Symptom**: "Artifact 'drop_Build_BuildWindowsWheels' not found"

**Solutions**:
1. Check the exact artifact name (case-sensitive)
2. Verify the build completed successfully
3. Check if the job actually ran (matrix jobs may be skipped)
4. Ensure the build hasn't exceeded retention period

### Empty Artifact
**Symptom**: Artifact exists but has no files

**Solutions**:
1. Check `ob_outputDirectory` is set correctly
2. Verify `CopyFiles@2` tasks are copying to correct paths
3. Check build logs for copy errors
4. Ensure files were actually built (check build steps)

### Cannot Download
**Symptom**: Download fails or requires authentication

**Solutions**:
1. Ensure you have read permissions on the project
2. For CLI/scripts, verify PAT token has `Build (Read)` permission
3. Check organization allows external access if accessing from outside network
4. Verify artifact hasn't been deleted or expired

## Summary 📋

**Artifact Naming**:
- Windows: `drop_Build_BuildWindowsWheels`
- macOS: `drop_Build_BuildMacOSWheels`
- Linux: `drop_Build_BuildLinuxWheels`

**Structure**:
```
drop_Build_<JobName>/
├── wheels/          ← All .whl files
├── bindings/        ← All native bindings (.pyd, .so)
│   ├── windows/
│   ├── macOS/
│   └── (linux variants)/
└── symbols/         ← Windows only (.pdb files)
```

**Access Methods**:
1. ✅ Azure DevOps UI (easiest)
2. ✅ Azure CLI (automation)
3. ✅ Pipeline stages (CI/CD)
4. ✅ REST API (custom tools)
5. ✅ Scripts (Python, PowerShell, Bash)

**Key Insight**: OneBranch enforces standardized artifact naming for security, compliance, and automation. While you can't customize artifact names, the subdirectory structure (`wheels/`, `bindings/`) provides clear organization! 🎯

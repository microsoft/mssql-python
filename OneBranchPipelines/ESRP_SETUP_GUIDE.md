# ESRP Setup Guide for OneBranch Pipeline

## Quick Answer: You're Already Set Up! ✅

Your existing `ESRP Federated Creds (AME)` variable group works for BOTH:
- ✅ OneBranch code signing (this new pipeline)
- ✅ PyPI release publishing (your existing `official-release-pipeline.yml`)

**Action Required**: Just grant your new `build-release-package-pipeline` access to the variable group.

---

## What is ESRP Code Signing?

### Overview

**ESRP** (Enterprise Secure Release Process) is Microsoft's internal service for securely signing and releasing software. It provides two main operations:

### 1. ESRP Code Signing (OneBranch Pipeline)
- **Purpose**: Digitally sign individual files during build
- **When**: DURING the build process
- **Signs**: `.whl` files, `.dll` files, `.pyd` files
- **Task**: `EsrpCodeSigning@2` or custom signing steps
- **Output**: Signed binaries with Microsoft digital signature

### 2. ESRP Release (Your Existing Pipeline)
- **Purpose**: Publish signed packages to distribution channels
- **When**: AFTER the build process (separate pipeline)
- **Publishes to**: PyPI (Python Package Index)
- **Task**: `EsrpRelease@9`
- **Output**: Package available on PyPI

---

## Why Code Signing Matters

### Security Benefits
1. **Publisher Verification**: Proves the file came from Microsoft
2. **Integrity Protection**: Ensures files haven't been tampered with
3. **Trust Chain**: Establishes trust through Microsoft's certificates
4. **Compliance**: Meets enterprise security requirements

### User Benefits
1. **No Security Warnings**: Windows/macOS don't show "untrusted publisher" warnings
2. **Verified Downloads**: Users can verify the package authenticity
3. **Supply Chain Security**: Protects against malicious modifications

---

## ESRP Variable Group: `ESRP Federated Creds (AME)`

### Variables You Already Have

| Variable Name | Purpose | Used By |
|--------------|---------|---------|
| `ESRPConnectedServiceName` | Azure service connection to ESRP | Both pipelines |
| `AuthAKVName` | Azure Key Vault containing certificates | Both pipelines |
| `AuthSignCertName` | Certificate name in Key Vault | Both pipelines |
| `EsrpClientId` | ESRP client application ID | Both pipelines |
| `DomainTenantId` | Azure AD tenant ID | Both pipelines |
| `owner` | Release approval owner | Release only |
| `approver` | Release approval reviewer | Release only |

### How Variables Are Mapped

Your OneBranch pipeline automatically maps your existing variables to OneBranch naming convention:

```yaml
# In OneBranchPipelines/variables/signing-variables.yml
# Maps from ESRP Federated Creds (AME) → OneBranch variable names

SigningEsrpConnectedServiceName ← $(ESRPConnectedServiceName)
SigningAuthAkvName ← $(AuthAKVName)
SigningAuthSignCertName ← $(AuthSignCertName)
SigningEsrpClientId ← $(EsrpClientId)
SigningAppRegistrationClientId ← $(EsrpClientId)
SigningAppRegistrationTenantId ← $(DomainTenantId)
```

**Result**: You don't need to create or modify any variables!

---

## Setup Instructions

### Step 1: Grant Pipeline Access to Variable Group

1. Navigate to **Pipelines → Library** in Azure DevOps
2. Click on **ESRP Federated Creds (AME)**
3. Click **Pipeline permissions** tab
4. Click **+** button
5. Search for and select: `build-release-package-pipeline`
6. Click **Save**

**That's it!** Your OneBranch pipeline can now use ESRP signing.

### Step 2: Verify Service Connection

Ensure your ESRP service connection is configured:

1. Navigate to **Project Settings → Service connections**
2. Find the connection named in `$(ESRPConnectedServiceName)`
3. Verify it has:
   - ✅ Connection type: Azure Resource Manager or Generic
   - ✅ Federated credentials configured
   - ✅ Access to ESRP service endpoints

### Step 3: Test NonOfficial Build (No Signing)

Before enabling signing, test the pipeline without it:

```yaml
# Run parameters
oneBranchType: 'NonOfficial'
signingEnabled: false  # Disable signing for testing
buildConfiguration: 'Release'
```

**Expected**: Pipeline completes successfully, wheels are unsigned.

### Step 4: Test Official Build (With Signing)

Once NonOfficial builds work, test signing:

```yaml
# Run parameters
oneBranchType: 'Official'
signingEnabled: true  # Enable signing
buildConfiguration: 'Release'
```

**Expected**: Pipeline completes successfully, wheels are digitally signed.

---

## How Code Signing Works in Your Pipeline

### Signing Flow

```
1. Build Python Wheel
   └─ python setup.py bdist_wheel
   └─ Output: mssql_python-0.13.0-cp310-cp310-win_amd64.whl (unsigned)

2. Malware Scanning
   └─ Scan for viruses/malware
   └─ Fail build if threats detected

3. ESRP Code Signing (if signingEnabled=true and oneBranchType='Official')
   ├─ Authenticate to ESRP service
   │  └─ Use credentials from ESRP Federated Creds (AME)
   ├─ Upload wheel to ESRP
   ├─ ESRP signs the wheel with Microsoft certificate
   ├─ Download signed wheel
   └─ Verify signature

4. Publish to ob_outputDirectory
   └─ OneBranch framework uploads to CDP
   └─ Artifacts available in Azure DevOps

5. (Later) ESRP Release to PyPI
   └─ Separate pipeline: official-release-pipeline.yml
   └─ Downloads signed wheels from CDP
   └─ Publishes to PyPI using EsrpRelease@9
```

### Conditional Signing Logic

Signing only happens when:
```yaml
${{ if and(eq(parameters.signingEnabled, true), eq(parameters.oneBranchType, 'Official')) }}:
  - template: /OneBranchPipelines/steps/compound-esrp-code-signing-step.yml@self
```

**Translation**:
- `signingEnabled = true` → User wants signing
- `oneBranchType = 'Official'` → Official build (has ESRP access)
- Both conditions met → Run ESRP signing step

**NonOfficial builds**: Signing step is skipped (saves time, doesn't require ESRP access)

---

## Pipeline Parameters

### signingEnabled Parameter

```yaml
- name: signingEnabled
  displayName: 'Enable Code Signing (ESRP)'
  type: boolean
  default: true
```

**When to use `true`**:
- Official builds destined for production
- Need signed artifacts for distribution
- Publishing to PyPI

**When to use `false`**:
- Testing/development builds
- Debugging build process
- NonOfficial builds (saves ~10 minutes)

### oneBranchType Parameter

```yaml
- name: oneBranchType
  displayName: 'OneBranch Build Type'
  type: string
  default: 'NonOfficial'
  values:
    - Official
    - NonOfficial
```

**NonOfficial**:
- For testing and validation
- Faster builds (no TSA, optional signing)
- Doesn't require all SDL tools
- Can run more frequently

**Official**:
- For production releases
- Full SDL compliance (TSA, signing)
- Requires ESRP access
- Generates signed, release-ready artifacts

---

## Comparing Your Two Pipelines

### OneBranch Pipeline (build-release-package-pipeline.yml)

**Purpose**: Build and sign Python wheels

**Steps**:
1. Build 27 wheels (Windows/macOS/Linux)
2. Run tests (pytest)
3. Scan for malware
4. **Code sign** wheels (EsrpCodeSigning)
5. Run SDL tools (CodeQL, CredScan, SBOM, etc.)
6. Publish artifacts to CDP

**Output**: Signed `.whl` files in Azure DevOps artifacts

**Frequency**: Every PR, nightly, or on-demand

---

### Release Pipeline (official-release-pipeline.yml)

**Purpose**: Publish signed wheels to PyPI

**Steps**:
1. Download signed wheels from OneBranch pipeline
2. **Release** to PyPI (EsrpRelease@9)
3. Wait for release completion

**Output**: Package available on pypi.org

**Frequency**: Only for official releases (e.g., 0.13.0, 0.14.0)

---

### Workflow

```
Developer pushes code
  ↓
OneBranch Pipeline runs
  ├─ Builds 27 wheels
  ├─ Signs with ESRP Code Signing
  └─ Publishes to CDP
  
(Time passes, testing happens)
  
Release approved
  ↓
Release Pipeline runs
  ├─ Downloads signed wheels from CDP
  └─ Publishes to PyPI with ESRP Release
  
Package available on PyPI
```

---

## Troubleshooting

### Error: "Pipeline does not have permission to access variable group"

**Solution**: Grant pipeline access (see Step 1 above)

---

### Error: "Service connection not found: $(ESRPConnectedServiceName)"

**Cause**: Variable group variable not populated or service connection doesn't exist

**Solution**:
1. Check variable group has `ESRPConnectedServiceName` defined
2. Verify service connection exists in Project Settings
3. Ensure service connection name matches variable value

---

### Error: "ESRP authentication failed"

**Cause**: Federated credentials or permissions issue

**Solution**:
1. Verify service principal has ESRP access
2. Check Azure Key Vault permissions
3. Ensure certificate exists in Key Vault
4. Contact ESRP team for access verification

---

### Error: "Signing operation timed out"

**Cause**: ESRP service temporarily unavailable

**Solution**: Retry the build. ESRP can have transient failures.

---

### Testing Without ESRP Access

If you don't have ESRP access yet:

```yaml
# Use these parameters
oneBranchType: 'NonOfficial'
signingEnabled: false
```

This will:
- ✅ Build all 27 wheels
- ✅ Run tests
- ✅ Run malware scanning
- ✅ Skip ESRP signing
- ✅ Publish unsigned artifacts

Once you get ESRP access, switch to:
```yaml
oneBranchType: 'Official'
signingEnabled: true
```

---

## Verification

### How to Verify Signed Wheels

After Official build completes:

1. Download wheel from artifacts
2. Extract wheel contents:
   ```bash
   unzip mssql_python-0.13.0-cp310-cp310-win_amd64.whl
   ```
3. Check for signature metadata:
   ```bash
   # Look for signature info in wheel metadata
   cat mssql_python-*.dist-info/RECORD
   ```

For Windows DLLs:
```powershell
# Check digital signature
Get-AuthenticodeSignature .\mssql_python\libs\windows\x64\*.dll
```

**Signed file will show**:
- Status: Valid
- SignerCertificate: Microsoft Corporation
- TimeStamperCertificate: Present

---

## Summary Checklist

Before running Official builds with signing:

- [ ] `ESRP Federated Creds (AME)` variable group exists
- [ ] Variable group contains: ESRPConnectedServiceName, AuthAKVName, AuthSignCertName, EsrpClientId, DomainTenantId
- [ ] `build-release-package-pipeline` has permission to access variable group
- [ ] ESRP service connection is configured in Project Settings
- [ ] Service principal has access to ESRP service
- [ ] Azure Key Vault contains signing certificate
- [ ] NonOfficial build tested successfully (signingEnabled=false)
- [ ] Ready to test Official build with signing

---

## Additional Resources

- [ESRP Documentation (Microsoft Internal)](https://aka.ms/esrp)
- [OneBranch Security & Compliance](https://aka.ms/onebranch/sdl)
- [Code Signing Best Practices](https://aka.ms/codesigning)

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-10-07 | Initial ESRP setup guide |

---

**Document Owner**: Gaurav Sharma  
**Pipeline**: build-release-package-pipeline.yml  
**Variable Group**: ESRP Federated Creds (AME)

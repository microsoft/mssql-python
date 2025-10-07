# SDL Configuration - mssql-python

**Organization:** Microsoft ADO.Net Team  
**Project:** mssql-python  
**Instance:** https://sqlclientdrivers.visualstudio.com/  
**Date:** October 1, 2025

---

## 📋 Configuration Files Overview

### 1. TSA Options (`tsaoptions.json`)

**Purpose:** Configures Threat and Security Assessment (TSA) for automated security issue tracking.

**Configuration:**
```json
{
  "instanceUrl": "https://sqlclientdrivers.visualstudio.com/",
  "projectName": "ADO.Net",
  "areaPath": "ADO.Net",
  "iterationPath": "ADO.Net\\TSA\\mssql-python",
  "notificationAliases": ["SqlClient@microsoft.com"],
  "repositoryName": "mssql-python",
  "codebaseName": "mssql-python",
  "allTools": true,
  "template": "MSDATA_RevolutionR_Overloaded0",
  "language": "python",
  "includePathPatterns": "mssql_python/*, setup.py, requirements.txt",
  "excludePathPatterns": "tests/*, benchmarks/*, examples/*, docs/*"
}
```

**Key Points:**
- ✅ Uses same ADO.Net project as SqlClient
- ✅ Notifications go to SqlClient@microsoft.com
- ✅ Scans only production code (excludes tests/docs/examples)
- ✅ Python-specific language configuration
- ✅ Uses MSDATA template for consistency

---

### 2. CredScan Suppressions (`CredScanSuppressions.json`)

**Purpose:** Suppresses false positives from credential scanning in non-production code.

**Excluded Paths:**
- ✅ `tests/*` - Test code with sample credentials
- ✅ `examples/*` - Example code with demo connection strings
- ✅ `docs/*` - Documentation with sample data
- ✅ `benchmarks/*` - Benchmark code with test credentials

**Why These Exclusions:**
- Test and example code intentionally contains non-production credentials
- Documentation shows sample connection strings for educational purposes
- These paths don't contain actual secrets used in production

---

### 3. PoliCheck Exclusions (`PolicheckExclusions.xml`)

**Purpose:** Excludes specific directories and file types from politically incorrect term scanning.

**Exclusions:**

**Folders:**
- `TESTS` - Test code
- `BENCHMARKS` - Performance benchmarks
- `EXAMPLES` - Example code
- `DOCS` - Documentation
- `BUILD-ARTIFACTS` - Build outputs
- `DIST` - Distribution packages
- `__PYCACHE__` - Python cache
- `MYVENV` / `TESTENV` - Virtual environments

**File Types:**
- `.YML` - Pipeline configuration files
- `.MD` - Markdown documentation
- `.SQL` - SQL scripts (may contain diverse terminology)
- `.JSON` - Configuration files
- `.TXT` - Text files
- `.LOG` - Log files

**Specific Files:**
- `CHANGELOG.MD` - Change log
- `README.MD` - Repository documentation
- `LICENSE` - License file
- `NOTICE.TXT` - Legal notices
- `ROADMAP.MD` - Project roadmap

**Why These Exclusions:**
- Configuration and documentation files may reference diverse terminology
- Test/example code may contain varied sample data
- Build artifacts are auto-generated and temporary
- Virtual environments contain third-party code

---

## 🔒 How SDL Tools Use These Configurations

### During OneBranch Pipeline Execution

1. **TSA (Threat and Security Assessment)**
   - Runs on **Official builds only**
   - Creates work items in ADO.Net project
   - Sends notifications to SqlClient@microsoft.com
   - Tracks security issues in iteration: `ADO.Net\TSA\mssql-python`

2. **CredScan (Credential Scanner)**
   - Scans all files except those in suppressions list
   - Flags potential credentials/secrets
   - Breaks build if credentials found (except suppressed paths)
   - Safe to have test credentials in excluded paths

3. **PoliCheck (Politically Incorrect Term Check)**
   - Scans source code for inappropriate terminology
   - Skips excluded folders and file types
   - Breaks build if violations found (except suppressed)
   - Focuses on production code only

---

## 📝 When to Update These Files

### Add to CredScan Suppressions When:
- Adding new test directories with sample credentials
- Creating examples with demo connection strings
- Documentation includes sample authentication data

### Add to PoliCheck Exclusions When:
- New documentation files reference technical terms flagged incorrectly
- New test data files contain diverse terminology
- Third-party code added that shouldn't be scanned

### Update TSA Options When:
- Project organization changes (e.g., moved to different ADO project)
- Notification distribution list changes
- Area/iteration paths change in Azure DevOps
- Want to include/exclude different paths from scanning

---

## ✅ Validation Checklist

Before running Official builds:

- [x] TSA configuration points to correct ADO.Net project
- [x] Notification email (SqlClient@microsoft.com) is correct
- [x] CredScan suppressions cover all test/example paths
- [x] PoliCheck exclusions include all documentation/config files
- [x] Include patterns focus on production code only
- [x] Exclude patterns cover tests, docs, examples, benchmarks

---

## 🚨 Monitoring & Maintenance

### Weekly Tasks
- Check TSA dashboard for new security issues
- Review CredScan/PoliCheck warnings in pipeline logs
- Address any legitimate security findings

### When Builds Break
1. **CredScan breaks build:**
   - Check if it's a false positive (test/example code)
   - If yes: Add to CredScanSuppressions.json
   - If no: Remove the credential and use proper secrets management

2. **PoliCheck breaks build:**
   - Review the flagged term
   - If it's technical terminology: Add to PolicheckExclusions.xml
   - If it's in documentation/tests: Verify those paths are excluded
   - If legitimate issue: Update the code

3. **TSA creates work items:**
   - Review the security finding
   - Follow your team's security issue resolution process
   - Update suppressions only if truly false positive

---

## 📚 Related Documentation

- **OneBranch SDL Guide:** https://aka.ms/obpipelines/sdl
- **CredScan Documentation:** Internal Microsoft docs
- **PoliCheck Guidelines:** Internal Microsoft docs
- **TSA Dashboard:** https://sqlclientdrivers.visualstudio.com/ → TSA work items

---

## 🎯 Key Differences from SqlClient Configuration

| Aspect | SqlClient | mssql-python |
|--------|-----------|--------------|
| **Language** | C# | Python |
| **Repository** | SqlClient | mssql-python |
| **Include Patterns** | src/Microsoft.Data.SqlClient/* | mssql_python/* |
| **Exclude Patterns** | src/.../tests/* | tests/*, examples/*, benchmarks/* |
| **TSA Iteration** | ADO.Net\TSA\SqlClient | ADO.Net\TSA\mssql-python |

**Note:** Both projects share:
- ✅ Same ADO.Net project
- ✅ Same notification alias (SqlClient@microsoft.com)
- ✅ Same TSA template (MSDATA_RevolutionR_Overloaded0)
- ✅ Similar exclusion philosophy (exclude tests/docs/examples)

---

_Last Updated: October 1, 2025_  
_Maintained by: mssql-python team_

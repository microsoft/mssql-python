# OneBranch Migration Project - Summary & Guide

**Project:** mssql-python OneBranch Migration  
**Date:** October 1, 2025  
**Status:** ✅ Knowledge Base Complete

---

## 📋 Project Overview

This project provides a comprehensive Bandish knowledge base for migrating classic Azure DevOps (ADO) pipelines to Microsoft's OneBranch security framework for the mssql-python project.

### Goals
- ✅ Generate a production-ready Bandish knowledge base
- ✅ Cover all transformation patterns for OneBranch migration
- ✅ Document architecture and design decisions
- ✅ Provide clear examples and usage guidelines

---

## 📁 Deliverables

### 1. Analysis Documentation (`01_Analysis_ClassicVsOneBranch.md`)
**Purpose:** Comprehensive comparison between Classic ADO and OneBranch pipelines

**Contents:**
- Current pipeline structure analysis (5 existing pipelines)
- OneBranch reference implementation analysis (sqlclient_eng)
- 16 key transformation patterns identified
- Benefits of OneBranch migration
- Migration scope and challenges
- Python-specific considerations

**Key Insights:**
- OneBranch provides built-in SDL tools (CodeQL, BinSkim, CredScan, etc.)
- Modular template structure improves maintainability
- Automatic artifact publishing via `ob_outputDirectory`
- Integrated ESRP code signing
- Better security, governance, and compliance

---

### 2. Architecture Guide (`02_OneBranch_Architecture.md`)
**Purpose:** Detailed OneBranch pipeline architecture and best practices

**Contents:**
- Recommended directory structure
- Core OneBranch components breakdown
- Global SDL configuration reference
- Variable template organization
- Job and step template patterns
- Migration strategy (6-week plan)
- Best practices and anti-patterns
- Testing strategy

**Key Components:**
- **Main Pipeline:** Extends OneBranch governed templates
- **Parameters:** Type-safe, validated configuration
- **Variables:** Modular templates (common, onebranch, build, signing)
- **Jobs:** Platform-specific build jobs (Windows, macOS, Linux)
- **Steps:** Reusable compound templates
- **Security:** Comprehensive globalSdl configuration

---

### 3. Transformation Patterns (`03_Transformation_Patterns.md`)
**Purpose:** Catalog of all transformation patterns with examples

**Contents:**
- **16 Pattern Categories:**
  1. Pipeline Root Structure (3 patterns)
  2. Parameters (1 pattern)
  3. Variables (1 pattern)
  4. Resources (1 pattern)
  5. Pipeline Extension (1 pattern)
  6. Pool Configuration (1 pattern)
  7. Artifact Publishing (1 pattern)
  8. Security Tasks (2 patterns)
  9. Job Organization (1 pattern)
  10. Step Organization (1 pattern)
  11. Matrix Strategies (1 pattern)
  12. Environment Variables (1 pattern)
  13. Test Results Publishing (1 pattern)
  14. Docker Usage (1 pattern)
  15. Symbol Publishing (1 pattern)
  16. File Organization (1 pattern)

**Each Pattern Includes:**
- Classic ADO example
- OneBranch equivalent
- Matcher strategy for Bandish
- Transformation rules
- Context and rationale

---

### 4. Bandish Knowledge Base (`onebranch-migration.md`) ⭐
**Purpose:** Production-ready Bandish knowledge base for code generation

**Contents:**
- **40+ Task Specifications** covering all transformation scenarios
- **Multiple Matcher Types:**
  - `$match:files="*.yml"` - File pattern matching
  - `$match:keyword="keyword"` - Keyword detection
  - `$match:regex="pattern"` - Regular expression matching
  - `$match:content="text"` - Content search
  - `$match:any` - Universal matchers

**Major Task Categories:**
1. **Pipeline Structure** (5 tasks)
   - Name transformation
   - Parameter addition
   - Variable reorganization
   - Resource configuration
   - Extends pattern

2. **Security & Compliance** (8 tasks)
   - Global SDL configuration
   - CodeQL integration
   - ESRP code signing
   - SDL suppression files
   - Compliance headers

3. **Build Organization** (7 tasks)
   - Pool transformation
   - Artifact publishing
   - Job extraction
   - Step templates
   - Matrix strategies

4. **Variables & Configuration** (6 tasks)
   - Common variables
   - OneBranch variables
   - Build variables
   - Signing variables
   - Symbol variables
   - Environment variables

5. **Platform-Specific** (6 tasks)
   - Python multi-platform builds
   - Native extension handling
   - Cross-platform compatibility
   - Test infrastructure
   - Docker usage
   - Artifact management

6. **Advanced Features** (8 tasks)
   - Symbol publishing
   - Version management
   - Conditional logic
   - Stage organization
   - Feature flags
   - Final checklist

---

## 🚀 Using the Knowledge Base

### With Bandish CLI

```bash
# Transform a single pipeline
bandish transform \
  --kb OneBranch_Learnings/onebranch-migration.md \
  --input eng/pipelines/build-whl-pipeline.yml \
  --output eng/pipelines/onebranch/build-release-package.yml

# Transform multiple files
bandish transform \
  --kb OneBranch_Learnings/onebranch-migration.md \
  --input eng/pipelines/*.yml \
  --output eng/pipelines/onebranch/
```

### Expected Transformations

**Input:** `eng/pipelines/build-whl-pipeline.yml` (Classic ADO)
- ~500 lines
- Monolithic structure
- Manual security tasks
- Inline variables
- Direct job definitions

**Output:** OneBranch Pipeline Structure
- `eng/pipelines/onebranch/build-release-package.yml` (~150 lines)
- `eng/pipelines/onebranch/jobs/` (3 job templates)
- `eng/pipelines/onebranch/steps/` (6+ step templates)
- `eng/pipelines/onebranch/variables/` (4 variable templates)
- `.config/` (SDL configuration files)

---

## 📊 Migration Statistics

### Classic Pipeline Analysis
- **Total Pipelines:** 5
- **Primary Pipeline:** build-whl-pipeline.yml (900+ lines)
- **Platforms:** Windows, macOS, Linux (multiple distros)
- **Architectures:** x64, ARM64, universal2
- **Python Versions:** 3.10, 3.11, 3.12, 3.13
- **Jobs:** 7+ (Windows, macOS, Linux variants, tests)
- **Security Tasks:** 1 (CodeQL only)

### OneBranch Target
- **Total Files:** 15+
- **Main Pipeline:** ~150 lines (modular)
- **Job Templates:** 3+ files
- **Step Templates:** 6+ files
- **Variable Templates:** 5 files
- **Security Tasks:** 12+ (comprehensive SDL)
- **Code Signing:** ESRP integrated
- **Symbol Publishing:** Automated

---

## 🎯 Key Features of the Knowledge Base

### 1. Comprehensive Coverage
- **40+ task specifications** cover every aspect of migration
- **Real-world patterns** from actual mssql-python pipelines
- **Reference implementations** from sqlclient_eng OneBranch pipeline

### 2. Smart Matching
- **Multi-strategy matching:** Files, keywords, regex, content
- **Precise targeting:** Specific patterns matched accurately
- **Flexible logic:** AND/OR combinations for complex scenarios

### 3. Clear Instructions
- **Step-by-step transformations** with examples
- **Before/after comparisons** for every pattern
- **Context and rationale** explaining why changes are needed

### 4. Python-Specific
- **Multi-platform wheel building** (Windows, macOS, Linux)
- **Native extension compilation** (pybind11, CMake)
- **Cross-architecture support** (x64, ARM64, universal2)
- **Python version matrix** (3.10-3.13)
- **Package testing and validation**

### 5. Security-First
- **Comprehensive SDL integration:** 12+ security tools
- **ESRP code signing:** Enterprise-grade signing
- **Symbol publishing:** Debugging support
- **SBOM generation:** Software bill of materials
- **TSA integration:** Vulnerability tracking

---

## 🗺️ Migration Roadmap

### Phase 1: Preparation (Week 1)
- ✅ Analyze existing pipelines
- ✅ Study OneBranch reference implementation
- ✅ Create knowledge base
- ⬜ Set up OneBranch repository permissions
- ⬜ Configure variable groups (secrets, signing, symbols)

### Phase 2: Structure Setup (Week 2)
- ⬜ Create OneBranch directory structure
- ⬜ Create variable templates
- ⬜ Create SDL suppression files
- ⬜ Set up tsaoptions.json

### Phase 3: Pipeline Transformation (Week 3)
- ⬜ Run Bandish transformation
- ⬜ Review and refine generated code
- ⬜ Extract jobs to templates
- ⬜ Extract steps to templates
- ⬜ Configure globalSdl

### Phase 4: Testing (Week 4)
- ⬜ Test with NonOfficial template
- ⬜ Validate Windows builds
- ⬜ Validate macOS builds
- ⬜ Validate Linux builds
- ⬜ Run integration tests
- ⬜ Review SDL results

### Phase 5: Security & Signing (Week 5)
- ⬜ Configure ESRP code signing
- ⬜ Set up symbol publishing
- ⬜ Enable all SDL tasks
- ⬜ Fix security findings
- ⬜ Test Official template

### Phase 6: Production Rollout (Week 6)
- ⬜ Final testing and validation
- ⬜ Update documentation
- ⬜ Enable OneBranch pipeline
- ⬜ Monitor first production runs
- ⬜ Archive classic pipelines
- ⬜ Team training

---

## 📚 Documentation Index

### Quick Reference
1. **Start Here:** `README.md` (this file)
2. **Analysis:** `01_Analysis_ClassicVsOneBranch.md`
3. **Architecture:** `02_OneBranch_Architecture.md`
4. **Patterns:** `03_Transformation_Patterns.md`
5. **Knowledge Base:** `onebranch-migration.md` ⭐

### File Purposes

| File | Purpose | Audience |
|------|---------|----------|
| `README.md` | Project overview and guide | All stakeholders |
| `01_Analysis_ClassicVsOneBranch.md` | Understanding the migration | Engineers, architects |
| `02_OneBranch_Architecture.md` | Implementation guide | Engineers |
| `03_Transformation_Patterns.md` | Detailed pattern reference | Engineers, Bandish users |
| `onebranch-migration.md` | Bandish knowledge base | Bandish tool (code generation) |

---

## 🎓 Key Learnings

### What Makes OneBranch Different?

1. **Template Extension Pattern**
   - Extends governed templates instead of defining pipelines directly
   - Provides standardization and security out-of-the-box

2. **Automatic Artifact Publishing**
   - `ob_outputDirectory` variable replaces PublishBuildArtifacts tasks
   - Simpler, more reliable artifact management

3. **Integrated Security (globalSdl)**
   - 12+ security tools configured in one place
   - Automatic injection into build process
   - TSA integration for vulnerability tracking

4. **Modular Structure**
   - Jobs, steps, variables in separate template files
   - Reusability and maintainability
   - Clear separation of concerns

5. **Enterprise Code Signing**
   - ESRP integration with Azure Key Vault
   - Malware scanning before signing
   - Supports DLLs and packages

### Common Pitfalls to Avoid

❌ **Don't:**
- Hardcode values in main pipeline
- Mix classic and OneBranch patterns
- Disable SDL without justification
- Use public/unmanaged pools
- Expose secrets in logs

✅ **Do:**
- Use parameters for configuration
- Organize in modular templates
- Enable all SDL tasks
- Use OneBranch managed pools
- Store secrets in variable groups

---

## 🔧 Tools and Resources

### Bandish
- **Tool:** Code transformation and generation
- **Knowledge Base Format:** Markdown with task specifications
- **Matchers:** files, keyword, regex, content, symbol, any
- **Usage:** `bandish transform --kb <kb-file> --input <source>`

### OneBranch Resources
- **Templates Repository:** OneBranch.Pipelines/GovernedTemplates
- **Template Version:** v2/OneBranch.{Official|NonOfficial}.CrossPlat.yml
- **Documentation:** https://aka.ms/obpipelines
- **SDL Tools:** https://aka.ms/obpipelines/sdl

### Variable Groups to Create
1. **esrp-variables-v2:** Code signing secrets
2. **symbol-publishing-variables:** Symbol server configuration
3. **build-secrets:** Database passwords and other secrets

---

## ✅ Success Criteria

### Pipeline Functionality
- ✅ All platforms build successfully (Windows, macOS, Linux)
- ✅ All architectures supported (x64, ARM64, universal2)
- ✅ All Python versions built (3.10-3.13)
- ✅ Tests pass on all platforms
- ✅ Artifacts published correctly

### Security & Compliance
- ✅ All SDL tasks enabled and passing
- ✅ CodeQL security analysis passing
- ✅ BinSkim binary analysis passing
- ✅ CredScan credential scanning passing
- ✅ ESRP code signing successful
- ✅ SBOM generated
- ✅ Symbols published to Microsoft Symbol Server

### Code Quality
- ✅ Modular, maintainable structure
- ✅ Comprehensive documentation
- ✅ Type-safe parameters
- ✅ Reusable templates
- ✅ Clear naming conventions

---

## 📞 Support & Questions

### Common Questions

**Q: Can I test OneBranch without affecting production?**  
A: Yes! Use `oneBranchType: 'NonOfficial'` parameter for testing. This disables TSA and some official-only features.

**Q: Do I need to migrate all pipelines at once?**  
A: No. Migrate one at a time, starting with build-whl-pipeline. Keep classic pipelines for reference.

**Q: What if Bandish doesn't generate perfect code?**  
A: The knowledge base provides guidance. Review and refine generated code. Some manual adjustments are expected.

**Q: How do I handle custom scripts?**  
A: Extract to step templates. Most scripts can be preserved with minor adjustments for OneBranch.

**Q: What about Docker usage?**  
A: Docker is supported. Ensure images are from approved registries (e.g., mcr.microsoft.com).

---

## 🎉 Conclusion

This knowledge base provides everything needed to migrate mssql-python pipelines to OneBranch using Bandish. The migration will result in:

- **Better Security:** 12+ integrated SDL tools
- **Better Governance:** Standardized, compliant pipelines
- **Better Maintainability:** Modular, template-based structure
- **Better Reliability:** Managed infrastructure and automatic publishing

The knowledge base is production-ready and covers all transformation patterns identified in the mssql-python project. It can be used directly with Bandish to generate OneBranch pipelines.

**Target Pipeline:** Build-Release-Package  
**Functions:** Build Python packages, publish symbols, CodeQL scanning

---

**Next Steps:**
1. Review this documentation
2. Set up OneBranch prerequisites (variable groups, permissions)
3. Run Bandish transformation
4. Test with NonOfficial template
5. Refine and deploy to production

**Happy Migrating! 🚀**

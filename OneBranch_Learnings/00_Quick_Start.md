# OneBranch Migration - Quick Start Guide

## 🎯 Purpose
Migrate mssql-python from Classic Azure DevOps pipelines to OneBranch security framework.

## 📦 What's in This Folder?

```
OneBranch_Learnings/
├── README.md                              ⭐ START HERE - Complete guide
├── 00_Quick_Start.md                      📖 This file - Quick reference
├── 01_Analysis_ClassicVsOneBranch.md      📊 Pipeline comparison & analysis
├── 02_OneBranch_Architecture.md           🏗️  Architecture & implementation guide
├── 03_Transformation_Patterns.md          🔄 Detailed transformation patterns
└── onebranch-migration.md                 🤖 Bandish knowledge base (40+ tasks)
```

## ⚡ Quick Start (5 Minutes)

### 1. Understand the Goal
Transform this:
```yaml
# Classic ADO Pipeline (900+ lines, monolithic)
name: build-whl-pipeline
jobs:
  - job: BuildJob
    pool: { vmImage: 'ubuntu-latest' }
    steps: [...]
```

Into this:
```yaml
# OneBranch Pipeline (150 lines, modular)
name: $(Year:YY)$(DayOfYear)$(Rev:.r)
extends:
  template: 'v2/OneBranch.Official.CrossPlat.yml@templates'
  parameters:
    globalSdl: { ... 12+ security tools ... }
    stages: [...]
```

### 2. Use the Knowledge Base
```bash
# Transform with Bandish
bandish transform \
  --kb OneBranch_Learnings/onebranch-migration.md \
  --input eng/pipelines/build-whl-pipeline.yml
```

### 3. Review Generated Code
- Main pipeline: ~150 lines
- Job templates: 3+ files
- Step templates: 6+ files
- Variables: 5 files
- SDL config: Comprehensive

## 📚 Reading Order

| Step | File | Time | Purpose |
|------|------|------|---------|
| 1️⃣ | `README.md` | 15 min | Overview & guide |
| 2️⃣ | `01_Analysis_ClassicVsOneBranch.md` | 20 min | Understand differences |
| 3️⃣ | `02_OneBranch_Architecture.md` | 30 min | Learn OneBranch structure |
| 4️⃣ | `onebranch-migration.md` | 60 min | Study knowledge base |
| 5️⃣ | `03_Transformation_Patterns.md` | 30 min | Reference as needed |

**Total: ~2.5 hours to full understanding**

## 🎓 Key Concepts (1 Minute)

### OneBranch = Security + Compliance + Standardization
- ✅ **Extends governed templates** (not direct pipeline definitions)
- ✅ **globalSdl:** 12+ integrated security tools
- ✅ **ob_outputDirectory:** Automatic artifact publishing
- ✅ **ESRP:** Enterprise code signing
- ✅ **Modular:** Jobs, steps, variables in separate templates

### What Changes?
| Classic | OneBranch |
|---------|-----------|
| `name: build-pipeline` | `name: $(Year:YY)$(DayOfYear)$(Rev:.r)` |
| `jobs:` | `extends: OneBranch.template` |
| `pool: { vmImage: ... }` | `pool: { type: windows/linux }` |
| `PublishBuildArtifacts` | `ob_outputDirectory` |
| Manual CodeQL | `globalSdl.codeql` |
| 1 file | 15+ modular files |

## 🚀 Migration Checklist

### Pre-Migration
- [ ] Read `README.md`
- [ ] Review `01_Analysis_ClassicVsOneBranch.md`
- [ ] Set up variable groups (esrp-variables-v2, etc.)
- [ ] Get OneBranch repository permissions

### Migration
- [ ] Run Bandish transformation
- [ ] Review generated code
- [ ] Create directory structure
- [ ] Configure SDL suppressions
- [ ] Test with NonOfficial template

### Post-Migration
- [ ] Validate all builds
- [ ] Review security scan results
- [ ] Enable code signing
- [ ] Switch to Official template
- [ ] Update documentation

## 🎯 Target Pipeline

**Name:** Build-Release-Package  
**Purpose:** Build Python packages for all platforms, publish symbols, run CodeQL

**Platforms:** Windows (x64, ARM64), macOS (universal2), Linux (x64, ARM64)  
**Python:** 3.10, 3.11, 3.12, 3.13  
**Security:** CodeQL, BinSkim, CredScan, ESRP signing, Symbol publishing

## 🔧 Using Bandish

### Basic Transform
```bash
bandish transform \
  --kb onebranch-migration.md \
  --input eng/pipelines/build-whl-pipeline.yml \
  --output eng/pipelines/onebranch/build-release-package.yml
```

### Knowledge Base Has:
- **40+ task specifications**
- **5 matcher types** (files, keyword, regex, content, any)
- **Step-by-step instructions**
- **Real examples** from mssql-python

## 📊 Migration Impact

### Before (Classic)
- 1 monolithic file: 900+ lines
- 1 security tool: CodeQL
- Manual artifact publishing
- No code signing
- Inline everything

### After (OneBranch)
- 15+ modular files: ~150 lines main
- 12+ security tools: Complete SDL
- Automatic artifact publishing
- ESRP code signing
- Template-based organization

### Benefits
✅ Better security & compliance  
✅ Better maintainability  
✅ Better governance  
✅ Better reliability  

## 💡 Pro Tips

1. **Start with NonOfficial:** Test everything before going Official
2. **One pipeline at a time:** Don't migrate all at once
3. **Review SDL results:** Fix security findings before Official
4. **Use reference:** sqlclient_eng is your OneBranch example
5. **Document changes:** Keep notes for future migrations

## ❓ Quick FAQ

**Q: Will this break my existing pipeline?**  
A: No. OneBranch pipeline is separate. Classic pipeline stays until you're ready.

**Q: How long does migration take?**  
A: Knowledge base generation: Complete ✅  
Full migration: ~6 weeks (per roadmap)

**Q: Do I need OneBranch expertise?**  
A: The knowledge base guides you. Basic YAML knowledge is enough.

**Q: What if something doesn't work?**  
A: Test with NonOfficial first. Review `03_Transformation_Patterns.md` for examples.

## 🎉 You're Ready!

You now have:
- ✅ Comprehensive knowledge base (40+ tasks)
- ✅ Complete documentation (4 guides)
- ✅ Real-world patterns (from mssql-python)
- ✅ Reference implementation (sqlclient_eng)
- ✅ Step-by-step instructions

**Next: Read `README.md` for full details**

---

**Created:** October 1, 2025  
**Status:** Production Ready  
**Tool:** Bandish  
**Target:** Build-Release-Package Pipeline

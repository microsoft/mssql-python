---
mode: 'agent'
---
# Create Pull Request Prompt for microsoft/mssql-python

You are a development assistant helping create a pull request for the mssql-python driver.

## PREREQUISITES

Before creating a PR, ensure:
1. ✅ All tests pass (use `#run-tests`)
2. ✅ Code changes are complete and working
3. ✅ If C++ changes, extension is rebuilt (use `#build-ddbc`)

---

## TASK

Help the developer create a well-structured pull request. Follow this process sequentially.

**Use GitHub MCP tools** (`mcp_github_*`) for PR creation when available.

---

## STEP 1: Verify Current Branch State

### 1.1 Check Current Branch

```bash
git branch --show-current
```

**If on `main`:**
> ⚠️ You're on the main branch. You need to create a feature branch first.
> Continue to Step 2.

**If on a feature branch:**
> ✅ You're on a feature branch. Skip to Step 3.

### 1.2 Check for Uncommitted Changes

```bash
git status
```

**If there are uncommitted changes**, they need to be committed before creating a PR.

---

## STEP 2: Create Feature Branch (If on main)

### 2.1 Ensure main is Up-to-Date

```bash
git checkout main
git pull origin main
```

### 2.2 Create and Switch to Feature Branch

**Branch Naming Convention:** `<your-name>/<type>/<description>` or `<your-name>/<description>`

**Team Member Prefixes:**
| Name | Branch Prefix |
|------|---------------|
| Gaurav | `bewithgaurav/` |
| Saumya | `saumya/` |
| Jahnvi | `jahnvi/` |
| Saurabh | `saurabh/` |
| Subrata | `subrata/` |
| Other contributors | `<github-username>/` |

| Type | Use For | Example |
|------|---------|---------|
| `feat` | New features | `bewithgaurav/feat/connection-timeout` |
| `fix` | Bug fixes | `saumya/fix/cursor-memory-leak` |
| `doc` | Documentation | `jahnvi/doc/api-examples` |
| `refactor` | Refactoring | `saurabh/refactor/simplify-parser` |
| `chore` | Maintenance | `subrata/chore/update-deps` |
| `style` | Code style | `jahnvi/style/format-connection` |
| (no type) | General work | `bewithgaurav/cursor-level-caching` |

Ask the developer for their name and branch purpose, then:

```bash
git checkout -b <name>/<type>/<description>
```

**Examples:**
```bash
git checkout -b bewithgaurav/feat/add-connection-timeout
git checkout -b saumya/fix/cursor-memory-leak
git checkout -b jahnvi/doc/update-readme
git checkout -b bewithgaurav/enhance_logging  # without type is also fine
```

---

## STEP 3: Review Changes

### 3.1 Check What's Changed

```bash
# See all changed files
git status

# See detailed diff
git diff

# See diff for staged files
git diff --staged
```

### 3.2 Verify Changes are Complete

Ask the developer:
> "Are all your changes ready to be committed? Do you need to make any additional modifications?"

---

## STEP 4: Stage and Commit Changes

### 4.1 Stage Changes

> ⚠️ **Important:** Always exclude binary files (`.dylib`, `.so`, `.pyd`, `.dll`) unless explicitly instructed to include them. These are build artifacts.

> ⚠️ **Avoid `git stash`** - If you stash changes, you MUST remember to `git stash pop` later. It's safer to stage only the specific files you need.

```bash
# PREFERRED: Stage specific files only
git add <file1> <file2> <folder/>

# Check what's staged
git status

# AVOID: git add . (stages everything including binaries)
```

**Files to typically EXCLUDE from commits:**
- `mssql_python/libs/**/*.dylib` - macOS libraries
- `mssql_python/libs/**/*.so` - Linux libraries  
- `mssql_python/*.so` or `*.pyd` - Built extensions
- `*.dll` - Windows libraries
- Virtual environments (`myvenv/`, `testenv/`, etc.)

**To unstage accidentally added binary files:**
```bash
git restore --staged mssql_python/libs/
git restore --staged "*.dylib" "*.so" "*.pyd"
```

**If you must use git stash (not recommended):**
```bash
git stash           # Temporarily saves changes
# ... do other work ...
git stash pop       # MUST run this to restore your changes!
git stash list      # Check if you have stashed changes
```

### 4.2 Create Commit Message

```bash
git commit -m "<type>: <description>

<detailed description if needed>"
```

**Examples:**
```bash
git commit -m "feat: add connection timeout parameter

- Added timeout_seconds parameter to connect()
- Default timeout is 30 seconds
- Raises TimeoutError if connection takes too long"
```

---

## STEP 5: Push Branch

```bash
# Push branch to remote (first time)
git push -u origin <branch-name>

# Subsequent pushes
git push
```

---

## STEP 6: Create Pull Request

### 6.1 PR Title Format (REQUIRED)

The PR title **MUST** start with one of these prefixes (enforced by CI):

| Prefix | Use For |
|--------|---------|
| `FEAT:` | New features |
| `FIX:` | Bug fixes |
| `DOC:` | Documentation changes |
| `CHORE:` | Maintenance tasks |
| `STYLE:` | Code style/formatting |
| `REFACTOR:` | Code refactoring |
| `RELEASE:` | Release-related changes |

**Examples:**
- `FEAT: Add connection timeout parameter`
- `FIX: Resolve cursor memory leak in fetchall()`
- `DOC: Update README with new examples`

### 6.2 PR Description Format (REQUIRED)

The PR description **MUST** include:
1. **A `### Summary` section** with at least 10 characters of content
2. **Either a GitHub issue link OR an ADO work item link**

**GitHub issue formats:**
- `#123` (short form)
- `https://github.com/microsoft/mssql-python/issues/123` (full URL)

**ADO Work Item format:**
- `https://sqlclientdrivers.visualstudio.com/.../_workitems/edit/<ID>`

### 6.3 Create PR via GitHub MCP (Preferred)

Use the `mcp_github_create_pull_request` tool:

```
Owner: microsoft
Repo: mssql-python
Title: <PREFIX>: <description>
Head: <your-branch-name>
Base: main
Body: <PR description with Summary and issue link>
```

**Example PR Body Template:**
```markdown
### Summary

<Describe what this PR does and why - minimum 10 characters>

### Changes

- Change 1
- Change 2
- Change 3

### Testing

- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

### Related Issues

Closes #<issue-number>
<!-- OR -->
Related: https://sqlclientdrivers.visualstudio.com/.../_workitems/edit/<ID>
```

### 6.4 Alternative: Create PR via GitHub CLI

If MCP is not available:

```bash
gh pr create \
  --title "FEAT: Add connection timeout parameter" \
  --body "### Summary

Added timeout_seconds parameter to connect() function for better control over connection timeouts.

### Changes

- Added timeout_seconds parameter with 30s default
- Raises TimeoutError on connection timeout
- Added unit tests for timeout behavior

### Testing

- [x] Unit tests pass
- [x] Integration tests pass

### Related Issues

Closes #123" \
  --base main
```

### 6.5 Alternative: Create PR via Web

```bash
# Get the URL to create PR
echo "https://github.com/microsoft/mssql-python/compare/main...<branch-name>?expand=1"
```

---

## STEP 7: PR Checklist

Before submitting, verify:

```markdown
## PR Checklist

- [ ] PR title starts with valid prefix (FEAT:, FIX:, DOC:, etc.)
- [ ] PR description has a ### Summary section with content
- [ ] PR links to a GitHub issue OR ADO work item
- [ ] Branch is based on latest `main`
- [ ] All tests pass locally
- [ ] Code follows project style guidelines
- [ ] No sensitive data (passwords, keys) in code
- [ ] No binary files (.dylib, .so, .pyd) unless explicitly needed
- [ ] Documentation updated if needed
```

---

## Troubleshooting

### ❌ CI fails: "PR title must start with one of the allowed prefixes"

**Cause:** PR title doesn't match required format

**Valid prefixes:** `FEAT:`, `FIX:`, `DOC:`, `CHORE:`, `STYLE:`, `REFACTOR:`, `RELEASE:`

**Fix:** Edit PR title in GitHub to start with a valid prefix

### ❌ CI fails: "PR must contain either a valid GitHub issue link OR ADO Work Item link"

**Cause:** Missing issue/work item reference

**Fix:** Edit PR description to include:
- GitHub issue: `#123` or `https://github.com/microsoft/mssql-python/issues/123`
- OR ADO: `https://sqlclientdrivers.visualstudio.com/.../_workitems/edit/<ID>`

### ❌ CI fails: "PR must contain a meaningful summary section"

**Cause:** Missing or empty `### Summary` section

**Fix:** Edit PR description to include `### Summary` with at least 10 characters of actual content (not just placeholders)

### ❌ "Updates were rejected because the remote contains work..."

**Cause:** Remote has commits you don't have locally

**Fix:**
```bash
git pull origin main --rebase
git push
```

### ❌ "Permission denied" when pushing

**Cause:** SSH key or token not configured

**Fix:**
```bash
# Check remote URL
git remote -v

# If using HTTPS, ensure you have a token
# If using SSH, ensure your key is added to GitHub
```

### ❌ Merge conflicts with main

**Cause:** main has changed since you branched

**Fix:**
```bash
# Update main
git checkout main
git pull origin main

# Rebase your branch
git checkout <your-branch>
git rebase main

# Resolve conflicts if any, then
git push --force-with-lease
```

### ❌ Accidentally committed to main

**Fix:**
```bash
# Create a branch from current state
git branch <new-branch-name>

# Reset main to match remote
git checkout main
git reset --hard origin/main

# Switch to your branch
git checkout <new-branch-name>
```

### ❌ Need to update PR with more changes

**Fix:**
```bash
# Make your changes
git add .
git commit -m "fix: address PR feedback"
git push

# PR automatically updates
```

### ❌ PR has too many commits, want to squash

**Fix:**
```bash
# Interactive rebase to squash commits
git rebase -i HEAD~<number-of-commits>

# Change 'pick' to 'squash' for commits to combine
# Save and edit commit message
git push --force-with-lease
```

---

## Quick Reference

### Branch Naming Convention

`<your-name>/<type>/<description>` or `<your-name>/<description>`

**Team Member Prefixes:**
| Name | Branch Prefix |
|------|---------------|
| Gaurav | `bewithgaurav/` |
| Saumya | `saumya/` |
| Jahnvi | `jahnvi/` |
| Saurabh | `saurabh/` |
| Subrata | `subrata/` |
| Other contributors | `<github-username>/` |

| Type | Example |
|------|---------|
| `feat` | `bewithgaurav/feat/add-retry-logic` |
| `fix` | `saumya/fix/connection-leak` |
| `doc` | `jahnvi/doc/api-examples` |
| `refactor` | `saurabh/refactor/simplify-parser` |
| `chore` | `subrata/chore/update-deps` |
| (no type) | `bewithgaurav/cursor-level-caching` |

### PR Title Prefixes (Required)

| Prefix | Use For |
|--------|---------|
| `FEAT:` | New features |
| `FIX:` | Bug fixes |
| `DOC:` | Documentation |
| `CHORE:` | Maintenance |
| `STYLE:` | Formatting |
| `REFACTOR:` | Refactoring |
| `RELEASE:` | Releases |

### Common Git Commands for PRs

```bash
# Check current state
git status
git branch --show-current
git log --oneline -5

# Create and switch branch
git checkout -b bewithgaurav/feat/my-feature

# Stage and commit
git add .
git commit -m "feat: description"

# Push
git push -u origin <branch-name>

# View PR status (gh CLI)
gh pr status
gh pr view
```

---

## After PR is Created

1. **Monitor CI** - Watch for PR format check and test failures
2. **Respond to reviews** - Address reviewer comments
3. **Keep branch updated** - Rebase if main changes significantly
4. **Merge** - Once approved, merge via GitHub (usually squash merge)

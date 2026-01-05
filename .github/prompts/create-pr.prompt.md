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

> ⚠️ **Prefer staging over stashing** - It's safer to stage specific files than to use `git stash`, which can lead to forgotten changes.

```bash
# PREFERRED: Stage specific files only
git add <file1> <file2> <folder/>

# Check what's staged
git status
```

> ⚠️ **AVOID:** `git add .` stages everything including binary files. Always stage specific files.

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

**If you use `git stash`, do so carefully and restore your changes:**
```bash
git stash           # Temporarily saves changes (use only if you understand stashing)
# ... do other work ...
git stash pop       # MUST run this to restore your changes (otherwise they stay hidden)!
git stash list      # Check if you still have stashed changes to restore
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

> ⚠️ **MANDATORY:** Before creating a PR, you MUST confirm **3 things** with the developer:
> 1. **PR Title** - Suggest options, get approval
> 2. **Work Item/Issue Link** - Search and suggest, get explicit confirmation (NEVER auto-add)
> 3. **PR Description** - Show full description, get approval

---

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

> ⚠️ **CONFIRM #1 - PR Title:** Suggest 3-5 title options to the developer and ask them to pick or modify one.

**Example:**
```
Here are some title options for your PR:

1. FEAT: Add connection timeout parameter
2. FEAT: Introduce configurable connection timeout
3. FEAT: Add timeout support for database connections

Which one do you prefer, or would you like to modify one?
```

---

### 6.2 Work Item / Issue Link (REQUIRED)

> ⚠️ **CONFIRM #2 - Work Item/Issue:** You MUST explicitly ask the developer which issue or work item this PR is linked to. 
> 
> **NEVER auto-add an issue number.** Even if you find a similar issue, ask the user to confirm.

**Process:**
1. Search GitHub issues for potentially related issues
2. If found similar ones, list them as **suggestions only**
3. Ask: "Which issue or ADO work item should this PR be linked to?"
4. User can provide: GitHub issue, ADO work item, both, or none (if creating new issue)
5. **Ask if they want "Closes" prefix** (only for GitHub issues) - default is NO

**Example prompt to user:**
```
Which work item or issue should this PR be linked to?

I found these potentially related GitHub issues:
- #123: Add developer documentation
- #145: Improve onboarding experience

Options:
- Enter a GitHub issue number (e.g., 123)
- Enter an ADO work item ID (e.g., 41340)
- Enter both
- Say "none" if you'll create an issue separately

For GitHub issues: Should this PR close the issue? (default: no)
```

**Format in PR description (simple hashtag format):**
- ADO Work Item: `#41340` (ADO plugin auto-links)
- GitHub Issue: `#123` (GitHub auto-links)
- GitHub Issue with close: `Closes #123` (only if user confirms)

> 💡 **Note:** No need for full URLs. Just use `#<ID>` - plugins handle the linking automatically.

---

### 6.3 PR Description (REQUIRED)

> ⚠️ **CONFIRM #3 - PR Description:** Show the full PR description to the developer and get approval before creating the PR.

**Use EXACTLY this format (from `.github/PULL_REQUEST_TEMPLATE.MD`):**

```markdown
### Work Item / Issue Reference  

<!-- mssql-python maintainers: ADO Work Item -->
> AB#<WORK_ITEM_ID>

<!-- External contributors: GitHub Issue -->
> GitHub Issue: #<ISSUE_NUMBER>

-------------------------------------------------------------------
### Summary   

<Your summary here - minimum 10 characters>
```

> 💡 **Notes:**
> - For team members: Use `AB#<ID>` format for ADO work items
> - For external contributors: Use `GitHub Issue: #<ID>` format
> - Only one reference is required (either ADO or GitHub)
> - Keep the exact format including the dashed line separator

**Example prompt to user:**
```
Here's the PR description I'll use:

---
### Work Item / Issue Reference  

> AB#41340

-------------------------------------------------------------------
### Summary   

Added VS Code Copilot prompts for developer workflow...
---

Does this look good? Should I modify anything before creating the PR?
```

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
### Work Item / Issue Reference  

<!-- mssql-python maintainers: ADO Work Item -->
> AB#<WORK_ITEM_ID>

<!-- External contributors: GitHub Issue -->
> GitHub Issue: #<ISSUE_NUMBER>

-------------------------------------------------------------------
### Summary   

<Describe what this PR does and why - minimum 10 characters>
```

> 💡 Use EXACTLY this format from `.github/PULL_REQUEST_TEMPLATE.MD`. Use `AB#ID` for ADO, `GitHub Issue: #ID` for GitHub issues.

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
git add <files>
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

> See "Team Member Prefixes" table in Step 2.2 above for the current list of prefixes.

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
git add <files>
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

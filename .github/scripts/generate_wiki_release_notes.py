"""
Smart Wiki Updater — Analyzes merged PRs, matches changes to existing wiki
pages, and generates proposed wiki updates for review in the main repo.

Flow:
  1. Read existing wiki pages (from cloned .wiki.git)
  2. Fetch merged PRs since last release with diffs
  3. Use LLM to analyze which wiki pages need updates and generate new content
  4. Write updated pages to wiki/ folder in the main repo
  5. Open a PR in the main repo for review; a separate sync job pushes to the
     wiki repo after the PR is merged

Zero pip dependencies — uses gh CLI + Python stdlib + optional LLM API.
Drop into any GitHub repo (mssql-python, mssql-jdbc, msoledbsql, etc.)

Environment variables:
  GH_TOKEN     — GitHub token (auto-provided in Actions, used for gh CLI AND GitHub Models LLM)
  SINCE_TAG    — Release tag to diff from (default: latest release)
  WIKI_DIR     — Path to cloned wiki repo (default: "_wiki")
  DRY_RUN      — "true" to preview without pushing
  AI_API_KEY   — Optional override: external API key (default: uses GH_TOKEN with GitHub Models)
  AI_API_BASE  — Optional override: API base URL (default: https://models.inference.ai.azure.com)
  AI_MODEL     — Model name (default: gpt-4.1)
"""

import json
import os
import re
import subprocess
import sys
import textwrap
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WIKI_DIR = Path(os.environ.get("WIKI_DIR", "_wiki"))
SINCE_TAG = os.environ.get("SINCE_TAG", "").strip()
NEW_VERSION = os.environ.get("NEW_VERSION", "").strip()
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

# LLM config: defaults to GitHub Models via GH_TOKEN (zero-config in Actions).
# Override with AI_API_KEY / AI_API_BASE for external providers.
_gh_token = os.environ.get("GH_TOKEN", "").strip()
AI_API_KEY = os.environ.get("AI_API_KEY", "").strip() or _gh_token
AI_API_BASE = os.environ.get("AI_API_BASE", "https://models.inference.ai.azure.com").strip().rstrip("/")
AI_MODEL = os.environ.get("AI_MODEL", "gpt-4.1").strip()

MAX_DIFF_CHARS = 8000
MAX_WIKI_CHARS = 12000
MAX_PRS_PER_BATCH = 15

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gh(*args: str) -> str:
    """Run a gh CLI command and return stdout."""
    result = subprocess.run(["gh", *args], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"gh {' '.join(args)} failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def git(*args: str, cwd: str | None = None) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        print(f"git {' '.join(args)} failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... [truncated]"


def llm_call(
    system_prompt: str,
    user_prompt: str,
    response_format: dict[str, str] | None = None,
) -> str | None:
    """Call GitHub Models (or any OpenAI-compatible API). Returns None on failure."""
    if not AI_API_KEY:
        return None

    url = f"{AI_API_BASE}/chat/completions"
    payload: dict = {
        "model": AI_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    if response_format is not None:
        payload["response_format"] = response_format
    body = json.dumps(payload).encode()

    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {AI_API_KEY}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
            return data["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()[:500]
        print(f"LLM API error {e.code}: {body_text}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"LLM API error: {e}", file=sys.stderr)
        return None


def get_page_summary(content: str, max_lines: int = 15) -> str:
    """Extract the first meaningful lines as a page summary."""
    lines = [l.strip() for l in content.splitlines() if l.strip()][:max_lines]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Heuristic matching (fallback when no LLM is available)
# ---------------------------------------------------------------------------

# Generic file-path/keyword → wiki page mapping. Works across drivers.
HEURISTIC_MAP = {
    "connection":  ["Connection.md", "Connection-to-SQL-Database.md"],
    "cursor":      ["Cursor.md"],
    "row":         ["Row.md"],
    "auth":        ["Microsoft-Entra-ID-support.md", "Connection-to-SQL-Database.md"],
    "entra":       ["Microsoft-Entra-ID-support.md"],
    "bulk":        ["Bulk-Copy-(BCP)-API-Reference.md"],
    "bcp":         ["Bulk-Copy-(BCP)-API-Reference.md"],
    "type":        ["Data-Type-Conversion.md"],
    "spatial":     ["Data-Type-Conversion.md"],
    "geography":   ["Data-Type-Conversion.md"],
    "geometry":    ["Data-Type-Conversion.md"],
    "log":         ["Logging.md"],
    "exception":   ["Exception.md"],
    "error":       ["Exception.md"],
    "install":     ["Installation.md", "Installation-Guide.md"],
    "build":       ["Build-From-Source.md"],
    "transaction": ["Database-Transaction-Management.md"],
    "stored_proc": ["Calling-Stored-Procedures.md"],
    "procedure":   ["Calling-Stored-Procedures.md"],
}

SKIP_PREFIXES = ["release:", "chore:", "ci:", "test:", "ci/cd"]


def heuristic_match(prs: list[dict], pages: dict[str, str]) -> list[dict]:
    """Match PRs to wiki pages using keywords and file paths."""
    matches = []
    for pr in prs:
        title_lower = pr["title"].lower()

        # Skip internal-only changes
        if any(title_lower.startswith(p) for p in SKIP_PREFIXES):
            if not any(kw in title_lower for kw in ["feature", "feat", "api", "support", "public"]):
                continue

        matched_pages: set[str] = set()

        # Match by file paths
        for f in pr["files"]:
            f_lower = f.lower()
            for keyword, page_list in HEURISTIC_MAP.items():
                if keyword in f_lower:
                    matched_pages.update(p for p in page_list if p in pages)

        # Match by title keywords
        for keyword, page_list in HEURISTIC_MAP.items():
            if keyword in title_lower:
                matched_pages.update(p for p in page_list if p in pages)

        for page in matched_pages:
            matches.append({
                "pr_number": pr["number"],
                "pr_title": pr["title"],
                "wiki_page": page,
                "reason": f"PR modifies code related to {page.replace('.md', '').replace('-', ' ')}",
                "change_type": "new_feature" if "feat" in title_lower else "bug_fix" if "fix" in title_lower else "update",
            })
    return matches


# ---------------------------------------------------------------------------
# Summary-only page update (fallback when no LLM is available)
# ---------------------------------------------------------------------------

def generate_summary_update(
    original: str,
    updates: list[dict],
    all_prs: list[dict],
    tag: str,
) -> str:
    """Append a structured 'Recent Changes' section to the page."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    section = [
        "",
        f"## Recent Changes (since {tag}) <!-- Auto-generated {today} -->",
        "",
        "The following merged PRs introduced changes relevant to this page.",
        "Please review and update the documentation above accordingly.",
        "",
    ]
    for u in updates:
        pr = next((p for p in all_prs if p["number"] == u["pr_number"]), None)
        title = pr["title"] if pr else u["pr_title"]
        section.append(f"- **#{u['pr_number']}** — {title}")
        section.append(f"  - _{u['reason']}_")
        if pr and pr.get("body"):
            first_line = pr["body"].split("\n")[0].strip()[:200]
            if first_line:
                section.append(f"  - {first_line}")
    section.append("")
    return original.rstrip() + "\n" + "\n".join(section)


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    # ------------------------------------------------------------------
    # 1. Read existing wiki pages
    # ------------------------------------------------------------------
    print("=" * 60)
    print("  Smart Wiki Updater")
    print("=" * 60)

    if not WIKI_DIR.is_dir():
        print(f"Wiki directory '{WIKI_DIR}' not found. Nothing to update.")
        sys.exit(0)

    wiki_pages: dict[str, str] = {}
    for md_file in sorted(WIKI_DIR.glob("*.md")):
        wiki_pages[md_file.name] = md_file.read_text(encoding="utf-8", errors="replace")

    if not wiki_pages:
        print("No wiki pages found. Nothing to update.")
        sys.exit(0)

    print(f"\nFound {len(wiki_pages)} wiki page(s):")
    for name, content in wiki_pages.items():
        print(f"  {name} ({len(content.splitlines())} lines)")

    # ------------------------------------------------------------------
    # 2. Determine release tag and fetch merged PRs
    # ------------------------------------------------------------------
    tag_name = SINCE_TAG
    if not tag_name:
        release_json = gh("release", "view", "--json", "tagName")
        tag_name = json.loads(release_json)["tagName"]

    print(f"\nAnalyzing PRs merged since: {tag_name}")

    release_json = gh("release", "view", tag_name, "--json", "publishedAt")
    since_date = json.loads(release_json)["publishedAt"][:10]

    prs_json = gh(
        "pr", "list",
        "--state", "merged",
        "--search", f"merged:>{since_date}",
        "--limit", "200",
        "--json", "number,title,body,labels,author,files,mergedAt,url",
    )
    prs = json.loads(prs_json)

    if not prs:
        print("No merged PRs found since last release. Nothing to do.")
        sys.exit(0)

    prs.sort(key=lambda p: p["number"])
    print(f"Found {len(prs)} merged PR(s)\n")

    # ------------------------------------------------------------------
    # 3. Fetch diffs for each PR
    # ------------------------------------------------------------------
    print("Fetching PR diffs...")
    pr_details: list[dict] = []

    for pr in prs:
        num = pr["number"]
        diff = gh("pr", "diff", str(num))
        files = [f.get("path", "") for f in pr.get("files", [])]
        pr_details.append({
            "number": num,
            "title": pr["title"],
            "body": (pr.get("body") or "")[:2000],
            "labels": [l.get("name", "") for l in pr.get("labels", [])],
            "author": pr.get("author", {}).get("login", "unknown"),
            "files": files,
            "diff": truncate(diff, MAX_DIFF_CHARS),
        })
        print(f"  #{num}: {pr['title'][:60]} ({len(files)} files)")

    # ------------------------------------------------------------------
    # 4. Analyze: Match PRs → wiki pages
    # ------------------------------------------------------------------
    print("\n--- Analysis ---")

    if AI_API_KEY:
        all_matches = llm_analyze(pr_details, wiki_pages)
    else:
        print("No AI_API_KEY — using heuristic matching.\n"
              "  (Set AI_API_KEY for smarter, LLM-powered analysis)\n")
        all_matches = heuristic_match(pr_details, wiki_pages)

    if not all_matches:
        print("\nNo wiki pages need updating. Done!")
        sys.exit(0)

    # Group by wiki page
    page_updates: dict[str, list[dict]] = {}
    for m in all_matches:
        page_updates.setdefault(m["wiki_page"], []).append(m)

    print(f"\n{len(all_matches)} update(s) across {len(page_updates)} wiki page(s):")
    for page, updates in page_updates.items():
        pr_nums = ", ".join(f"#{u['pr_number']}" for u in updates)
        print(f"  {page} ← {pr_nums}")

    # ------------------------------------------------------------------
    # 5. Generate updated wiki content
    # ------------------------------------------------------------------
    print("\n--- Generating updates ---")
    updated_pages: dict[str, str] = {}

    for page_name, updates in page_updates.items():
        original = wiki_pages.get(page_name, "")

        if AI_API_KEY:
            new_content = llm_update_page(page_name, original, updates, pr_details)
            if new_content:
                updated_pages[page_name] = new_content
            else:
                updated_pages[page_name] = generate_summary_update(original, updates, pr_details, tag_name)
        else:
            updated_pages[page_name] = generate_summary_update(original, updates, pr_details, tag_name)

    # Diff summary
    print(f"\n--- Summary ---")
    for page_name, new_content in updated_pages.items():
        orig_lines = len(wiki_pages.get(page_name, "").splitlines())
        new_lines = len(new_content.splitlines())
        diff = new_lines - orig_lines
        print(f"  {page_name}: {orig_lines} → {new_lines} lines ({'+' if diff >= 0 else ''}{diff})")

    # ------------------------------------------------------------------
    # 6. Generate release artifacts (if new_version is provided)
    # ------------------------------------------------------------------
    release_notes_md = ""
    pypi_description_md = ""

    if NEW_VERSION:
        print(f"\n--- Release Prep for v{NEW_VERSION} ---")

        # Generate release notes (will be used for draft GitHub Release)
        release_notes_md = generate_release_notes(pr_details, NEW_VERSION, tag_name)
        print(f"  Release notes: {len(release_notes_md.splitlines())} lines")

        # Generate PyPI description
        pypi_path = Path("PyPI_Description.md")
        existing_pypi = pypi_path.read_text(encoding="utf-8") if pypi_path.exists() else ""
        pypi_description_md = generate_pypi_description(pr_details, NEW_VERSION, existing_pypi)
        print(f"  PyPI description: {len(pypi_description_md.splitlines())} lines")

    # ------------------------------------------------------------------
    # 7. Write changes and create a PR in the main repo
    # ------------------------------------------------------------------
    if DRY_RUN:
        print("\n[DRY RUN] — No changes pushed.")
        for page_name, new_content in updated_pages.items():
            print(f"\n{'=' * 60}\n  {page_name}\n{'=' * 60}")
            print(new_content[-600:])
        if release_notes_md:
            print(f"\n{'=' * 60}\n  Draft GitHub Release (v{NEW_VERSION})\n{'=' * 60}")
            print(release_notes_md[:2000])
        if pypi_description_md:
            print(f"\n{'=' * 60}\n  PyPI_Description.md (updated)\n{'=' * 60}")
            print(pypi_description_md[-800:])
        return

    # Write updated wiki pages into a wiki/ folder in the main repo
    wiki_pr_dir = Path("wiki")
    wiki_pr_dir.mkdir(exist_ok=True)

    # Copy ALL wiki pages (so the folder is a complete mirror), then
    # overwrite the ones that were updated so the diff only shows changes.
    for page_name, content in wiki_pages.items():
        (wiki_pr_dir / page_name).write_text(content, encoding="utf-8")
    for page_name, new_content in updated_pages.items():
        (wiki_pr_dir / page_name).write_text(new_content, encoding="utf-8")

    # Write PyPI description if new_version was provided
    if NEW_VERSION and pypi_description_md:
        Path("PyPI_Description.md").write_text(pypi_description_md, encoding="utf-8")
        print(f"  Wrote PyPI_Description.md")

    # Branch, commit, push in the main repo
    # Include run ID to avoid collisions on re-runs for the same tag
    run_id = os.environ.get("GITHUB_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
    if NEW_VERSION:
        branch = f"release/prep-v{NEW_VERSION}-{run_id}"
    else:
        branch = f"wiki/updates-since-{tag_name}-{run_id}"

    git("config", "user.name", "github-actions[bot]")
    git("config", "user.email", "github-actions[bot]@users.noreply.github.com")
    git("checkout", "-b", branch)
    git("add", "wiki/")
    if NEW_VERSION and pypi_description_md:
        git("add", "PyPI_Description.md")

    commit_msg = f"wiki: update {len(updated_pages)} page(s) for changes since {tag_name}"
    if NEW_VERSION:
        commit_msg = f"release: prep v{NEW_VERSION} — wiki + PyPI description"
    git("commit", "-m", commit_msg)
    git("push", "--set-upstream", "origin", branch)

    # Create draft GitHub Release directly (if new_version provided)
    draft_release_url = ""
    if NEW_VERSION and release_notes_md:
        print(f"\n  Creating draft GitHub Release for v{NEW_VERSION}...")
        draft_release_url = gh(
            "release", "create",
            f"v{NEW_VERSION}",
            "--draft",
            "--title", f"v{NEW_VERSION}",
            "--notes", release_notes_md,
            "--target", "main",
        )
        print(f"  Draft release created: {draft_release_url}")

    # Build PR body
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    page_list = "\n".join(f"- [`{p}`](wiki/{p})" for p in updated_pages)
    pr_refs = "\n".join(
        f"- #{u['pr_number']} — {u['pr_title']} (`{u['change_type']}`)"
        for ups in page_updates.values() for u in ups
    )

    release_section = ""
    if NEW_VERSION:
        draft_link = f"\n[Review the draft release]({draft_release_url})" if draft_release_url else ""
        release_section = textwrap.dedent(f"""\

            ### Release artifacts (v{NEW_VERSION})
            - **Draft GitHub Release** — created directly in [Releases](https://github.com/{repo}/releases){draft_link}
            - [`PyPI_Description.md`](PyPI_Description.md) — updated "What's new" section

            **Review carefully** — these are drafts generated by LLM from merged PR data.
            Edit the draft release and PyPI description before publishing.
        """)

    if NEW_VERSION:
        pr_title = f"RELEASE: v{NEW_VERSION} — wiki updates + PyPI description"
    else:
        pr_title = f"Wiki: Update pages for changes since {tag_name}"

    pr_body = textwrap.dedent(f"""\
        ## {'Release Prep v' + NEW_VERSION if NEW_VERSION else 'Wiki Update'} — changes since {tag_name}

        This PR contains **proposed updates** generated by analyzing
        {len(pr_details)} merged PRs since `{tag_name}`.

        ### Wiki pages updated ({len(updated_pages)})
        {page_list}

        ### Based on these PRs ({len(all_matches)} changes)
        {pr_refs}
        {release_section}
        ### How to review
        - Review the file diffs in this PR — each `wiki/*.md` file maps 1:1 to a wiki page.
        - Sections marked with `<!-- Updated for {tag_name} -->` are new/changed.
        - {'Review `PyPI_Description.md` and the [draft release](https://github.com/' + repo + '/releases) for accuracy.' if NEW_VERSION else ''}
        - Edit directly in this PR if adjustments are needed.

        ### What happens on merge
        A workflow automatically syncs the `wiki/` folder to the GitHub wiki.
        {'Publish the draft release from the [Releases page](https://github.com/' + repo + '/releases) when ready.' if NEW_VERSION else ''}

        ---
        _Auto-generated by [Smart Wiki Updater](.github/scripts/generate_wiki_release_notes.py)_
    """)

    pr_url = gh(
        "pr", "create",
        "--title", pr_title,
        "--body", pr_body,
        "--head", branch,
        "--label", "documentation",
    )
    print(f"\nPR created: {pr_url}")
    print(f"Review the diffs at: {pr_url}/files")


# ---------------------------------------------------------------------------
# LLM-powered analysis
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_ANALYZE = textwrap.dedent("""\
    You are a technical documentation expert. You analyze code changes (PRs)
    and determine which wiki documentation pages need to be updated.

    Rules:
    - Only flag a page if the PR introduces a USER-VISIBLE change relevant
      to that page (new feature, API change, behavior change, new parameter,
      deprecation, bug fix that changes documented behavior, etc.)
    - Do NOT flag for: internal refactors, CI changes, test-only changes,
      dependency bumps, or release bookkeeping — unless they affect users.
    - Be conservative — only flag when you are confident.

    Respond ONLY with valid JSON (no markdown fences):
    {
      "matches": [
        {
          "pr_number": 123,
          "pr_title": "...",
          "wiki_page": "Page-Name.md",
          "reason": "Brief explanation of what needs updating",
          "change_type": "new_feature|behavior_change|api_change|deprecation|bug_doc"
        }
      ]
    }
    Return {"matches": []} if nothing needs updating.
""")

SYSTEM_PROMPT_UPDATE = textwrap.dedent("""\
    You are a technical documentation writer updating a wiki page based on
    code changes from merged pull requests.

    Rules:
    - Preserve the existing structure, tone, and formatting.
    - Add new sections or update existing ones as needed.
    - Include code examples where appropriate.
    - Mark new/changed sections with <!-- Updated for vX.Y.Z --> comments.
    - Do NOT remove existing content unless it is now incorrect.
    - Do NOT add marketing language or opinions.
    - Return ONLY the full updated markdown page content.
""")

SYSTEM_PROMPT_RELEASE_NOTES = textwrap.dedent("""\
    You are a release engineer writing GitHub Release Notes for a software driver.
    Use EXACTLY this format — group PRs into sections, each with a narrative description:

    # Release Notes - Version X.Y.Z

    ## Enhancements

    • Enhancement Name (PR links)

    What changed: <Detailed description of the change>
    Who benefits: <Target audience>
    Impact: <User impact>

    > Implements #PR1, #PR2

    ## Bug Fixes

    • Bug Name (PR link)

    What changed: <Root cause and fix>
    Who benefits: <Affected users>
    Impact: <Reliability/correctness improvement>

    > Fixes #PR

    ## Developer Experience  (if applicable)

    • DX Improvement Name (PR link)

    Rules:
    - Group related PRs under a single heading (e.g., multiple bulk copy PRs → one "Bulk Copy" section)
    - Skip CI-only, CHORE, test-only, release bookkeeping, and internal-only PRs
    - Use the > blockquote format for "Implements" / "Fixes" references
    - Include the PR number links like [#123](PR_URL)
    - Be factual — no marketing language
    - Return ONLY the markdown content
""")

SYSTEM_PROMPT_PYPI = textwrap.dedent("""\
    You are writing the PyPI project description for a Python driver package.
    The description has a FIXED top section (about the architecture) that must NOT be changed,
    followed by a "What's new in vX.Y.Z" section that YOU generate.

    For the "What's new" section, use this format:

    ## What's new in vX.Y.Z

    ### Features

    - **Feature Name** - One-sentence description with `code formatting` for APIs.

    ### Bug Fixes

    - **Fix Name** - One-sentence description of what was fixed.

    Rules:
    - Keep descriptions concise (one sentence each)
    - Use **bold** for the item name, then a dash, then the description
    - Use `backticks` for code/API references
    - Skip internal/CI changes
    - Return ONLY the "What's new" section markdown (NOT the full file)
""")


def llm_analyze(pr_details: list[dict], wiki_pages: dict[str, str]) -> list[dict]:
    """Use LLM to match PRs to wiki pages."""
    page_summaries = {
        name: get_page_summary(content) for name, content in wiki_pages.items()
    }

    all_matches: list[dict] = []
    for i in range(0, len(pr_details), MAX_PRS_PER_BATCH):
        batch = pr_details[i:i + MAX_PRS_PER_BATCH]
        print(f"Analyzing PRs #{batch[0]['number']}–#{batch[-1]['number']} with LLM...")

        # Build prompt
        parts = ["# Existing Wiki Pages\n"]
        for name, summary in page_summaries.items():
            parts.append(f"## {name}\n{summary}\n")
        parts.append("\n# Merged Pull Requests\n")
        for pr in batch:
            parts.append(f"## PR #{pr['number']}: {pr['title']}")
            parts.append(f"Labels: {', '.join(pr['labels']) or 'none'}")
            parts.append(f"Files changed: {', '.join(pr['files'][:20])}")
            if pr["body"]:
                parts.append(f"Description:\n{pr['body'][:1000]}")
            parts.append(f"Diff (excerpt):\n{pr['diff'][:3000]}")
            parts.append("")

        response = llm_call(
            SYSTEM_PROMPT_ANALYZE,
            "\n".join(parts),
            response_format={"type": "json_object"},
        )
        if response:
            cleaned = re.sub(r'^```(?:json)?\s*', '', response.strip())
            cleaned = re.sub(r'\s*```$', '', cleaned)
            try:
                matches = json.loads(cleaned).get("matches", [])
                all_matches.extend(matches)
                print(f"  → {len(matches)} page update(s) identified")
            except json.JSONDecodeError:
                print("  → Failed to parse LLM response, trying heuristic for this batch")
                all_matches.extend(heuristic_match(batch, wiki_pages))

    return all_matches


def llm_update_page(
    page_name: str,
    original: str,
    updates: list[dict],
    all_prs: list[dict],
) -> str | None:
    """Use LLM to generate an updated wiki page."""
    pr_context_parts = []
    for u in updates:
        pr = next((p for p in all_prs if p["number"] == u["pr_number"]), None)
        if not pr:
            continue
        pr_context_parts.append(
            f"### PR #{pr['number']}: {pr['title']}\n"
            f"Change type: {u['change_type']}\n"
            f"Reason for update: {u['reason']}\n"
            f"Files: {', '.join(pr['files'][:15])}\n"
            f"Description:\n{pr['body'][:1500]}\n"
            f"Diff:\n{pr['diff'][:4000]}\n"
        )

    user_prompt = (
        f"# Current Wiki Page: {page_name}\n\n"
        f"{truncate(original, MAX_WIKI_CHARS)}\n\n"
        f"# PRs That Require Updates to This Page\n\n"
        f"{''.join(pr_context_parts)}\n\n"
        f"Return the complete updated wiki page."
    )

    print(f"  Updating {page_name} via LLM...")
    result = llm_call(SYSTEM_PROMPT_UPDATE, user_prompt)
    if not result:
        return None

    # Strip wrapping markdown fences if present
    cleaned = result.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:markdown|md)?\s*\n?', '', cleaned)
        cleaned = re.sub(r'\n?```\s*$', '', cleaned)
    return cleaned


# ---------------------------------------------------------------------------
# Release artifact generation
# ---------------------------------------------------------------------------

def generate_release_notes(pr_details: list[dict], version: str, tag_name: str) -> str:
    """Generate GitHub Release Notes markdown."""
    pr_context = "\n".join(
        f"- PR #{p['number']}: {p['title']}\n"
        f"  Labels: {', '.join(p['labels']) or 'none'}\n"
        f"  Files: {', '.join(p['files'][:10])}\n"
        f"  Body: {p['body'][:500]}\n"
        for p in pr_details
    )

    user_prompt = (
        f"Generate release notes for version {version}. "
        f"These PRs were merged since {tag_name}:\n\n{pr_context}"
    )

    if AI_API_KEY:
        print("  Generating release notes via LLM...")
        result = llm_call(SYSTEM_PROMPT_RELEASE_NOTES, user_prompt)
        if result:
            cleaned = result.strip()
            if cleaned.startswith("```"):
                cleaned = re.sub(r'^```(?:markdown|md)?\s*\n?', '', cleaned)
                cleaned = re.sub(r'\n?```\s*$', '', cleaned)
            return cleaned

    # Fallback: structured release notes without LLM
    return _fallback_release_notes(pr_details, version)


def _fallback_release_notes(pr_details: list[dict], version: str) -> str:
    """Generate release notes without LLM using PR metadata."""
    features, fixes, other = [], [], []
    for pr in pr_details:
        title_lower = pr["title"].lower()
        if any(title_lower.startswith(p) for p in ["chore:", "ci:", "release:", "test:"]):
            continue
        entry = f"- {pr['title']} ([#{pr['number']}]({pr.get('url', '')}))"
        labels = [l.lower() for l in pr["labels"]]
        if "bug" in labels or title_lower.startswith("fix:"):
            fixes.append(entry)
        elif "feature" in labels or title_lower.startswith("feat:"):
            features.append(entry)
        else:
            other.append(entry)

    lines = [f"# Release Notes - Version {version}", ""]
    if features:
        lines += ["## Enhancements", ""] + features + [""]
    if fixes:
        lines += ["## Bug Fixes", ""] + fixes + [""]
    if other:
        lines += ["## Other Changes", ""] + other + [""]
    lines.append(f"\n> _Draft generated from {len(pr_details)} merged PRs. Please review and edit before publishing._")
    return "\n".join(lines)


def generate_pypi_description(pr_details: list[dict], version: str, existing_pypi: str) -> str:
    """Generate updated PyPI_Description.md content."""
    # The top section (architecture description) is fixed — only update "What's new"
    pr_context = "\n".join(
        f"- PR #{p['number']}: {p['title']} (labels: {', '.join(p['labels']) or 'none'})\n"
        f"  Body: {p['body'][:300]}\n"
        for p in pr_details
    )

    if AI_API_KEY:
        print("  Generating PyPI description via LLM...")
        user_prompt = (
            f"Generate the 'What's new in v{version}' section for these PRs:\n\n{pr_context}"
        )
        result = llm_call(SYSTEM_PROMPT_PYPI, user_prompt)
        if result:
            whats_new = result.strip()
            if whats_new.startswith("```"):
                whats_new = re.sub(r'^```(?:markdown|md)?\s*\n?', '', whats_new)
                whats_new = re.sub(r'\n?```\s*$', '', whats_new)
            return _splice_pypi_description(existing_pypi, whats_new, version)

    # Fallback
    return _splice_pypi_description(existing_pypi, _fallback_pypi_whats_new(pr_details, version), version)


def _fallback_pypi_whats_new(pr_details: list[dict], version: str) -> str:
    """Generate the What's new section without LLM."""
    features, fixes = [], []
    for pr in pr_details:
        title_lower = pr["title"].lower()
        if any(title_lower.startswith(p) for p in ["chore:", "ci:", "release:", "test:"]):
            continue
        labels = [l.lower() for l in pr["labels"]]
        # Extract a clean name from the title (strip prefix)
        name = re.sub(r'^(feat|fix|feature|bug):\s*', '', pr["title"], flags=re.IGNORECASE).strip()
        if "bug" in labels or title_lower.startswith("fix:"):
            first_line = (pr["body"] or name).split("\n")[0].strip()[:150]
            fixes.append(f"- **{name}** - {first_line}")
        elif "feature" in labels or title_lower.startswith("feat:"):
            first_line = (pr["body"] or name).split("\n")[0].strip()[:150]
            features.append(f"- **{name}** - {first_line}")

    lines = [f"## What's new in v{version}", ""]
    if features:
        lines += ["### Features", ""] + features + [""]
    if fixes:
        lines += ["### Bug Fixes", ""] + fixes + [""]
    return "\n".join(lines)


def _splice_pypi_description(existing: str, whats_new_section: str, version: str) -> str:
    """Replace the What's new section in PyPI_Description.md, keeping everything else."""
    # Find the old "What's new" section and replace it
    pattern = r"(## What's new in v[\d.]+.*?)(\n## What's Next)"
    match = re.search(pattern, existing, re.DOTALL)
    if match:
        # Replace old "What's new" with new content
        return existing[:match.start(1)] + whats_new_section.rstrip() + "\n" + existing[match.start(2):]

    # If no "What's new" section found, insert before "What's Next"
    next_marker = existing.find("## What's Next")
    if next_marker != -1:
        return existing[:next_marker] + whats_new_section.rstrip() + "\n\n" + existing[next_marker:]

    # Last resort: append
    return existing.rstrip() + "\n\n" + whats_new_section + "\n"


if __name__ == "__main__":
    main()

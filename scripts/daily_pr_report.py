"""Daily open pull request report for microsoft/mssql-python.

Queries the GitHub REST API for every open pull request in the target
repository, builds a markdown summary and posts it to a Microsoft Teams
incoming webhook.

Environment variables:
    TARGET_REPO       - "owner/repo" to inspect (default: microsoft/mssql-python)
    GITHUB_TOKEN      - token used for authenticated GitHub API calls (required)
    TEAMS_WEBHOOK_URL - Teams incoming webhook URL (required)

The webhook URL is never logged, only the resulting HTTP status is.
"""

import logging
import os
import sys
from typing import Any, Dict, List

import requests

LOGGER = logging.getLogger("daily_pr_report")

GITHUB_API = "https://api.github.com"
DEFAULT_REPO = "microsoft/mssql-python"
REQUEST_TIMEOUT = 30
PER_PAGE = 100
MAX_PAGES = 100


def configure_logging() -> None:
    """Configure console logging for the workflow run."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def fetch_open_pull_requests(repo: str, token: str) -> List[Dict[str, Any]]:
    """Return every open pull request for *repo*, following pagination."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": "Bearer " + token,
    }
    url = f"{GITHUB_API}/repos/{repo}/pulls"

    pulls: List[Dict[str, Any]] = []
    for page in range(1, MAX_PAGES + 1):
        params = {"state": "open", "per_page": PER_PAGE, "page": page}
        LOGGER.info("Fetching open pull requests page %d for %s", page, repo)
        response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        batch = response.json()
        if not isinstance(batch, list):
            raise ValueError(f"Unexpected GitHub API response type: {type(batch).__name__}")
        pulls.extend(batch)
        if len(batch) < PER_PAGE:
            break
    else:
        raise ValueError(
            f"Stopped after {MAX_PAGES} pages; refusing to send a truncated report."
        )

    LOGGER.info("Found %d open pull request(s) in %s", len(pulls), repo)
    return pulls


def build_report(repo: str, pulls: List[Dict[str, Any]]) -> str:
    """Build the markdown report body for *pulls*."""
    repo_name = repo.split("/")[-1]
    if not pulls:
        return f"Daily {repo_name} report: No open pull requests found."

    lines = [
        f"## Daily {repo_name} Open PR Report",
        "",
        f"**Total Open PRs: {len(pulls)}**",
        "",
    ]
    for pull in pulls:
        author = (pull.get("user") or {}).get("login") or "unknown"
        created = (pull.get("created_at") or "")[:10]
        lines.append(f"- **PR #{pull.get('number')}: {pull.get('title') or '(no title)'}**")
        lines.append(f"  - Author: {author}")
        lines.append(f"  - Created: {created}")
        lines.append(f"  - URL: {pull.get('html_url') or ''}")
    return "\n".join(lines)


def send_to_teams(webhook_url: str, report: str) -> None:
    """Post *report* to the Teams incoming webhook."""
    LOGGER.info("Posting report to the Teams webhook")
    response = requests.post(webhook_url, json={"text": report}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    LOGGER.info("Teams webhook responded with status %s", response.status_code)


def main() -> int:
    """Generate the daily report and deliver it to Teams."""
    configure_logging()

    repo = os.environ.get("TARGET_REPO") or DEFAULT_REPO
    token = os.environ.get("GITHUB_TOKEN", "")
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "")

    if not token:
        LOGGER.error("GITHUB_TOKEN is not set; cannot query the GitHub API.")
        return 1
    if not webhook_url:
        LOGGER.error("TEAMS_WEBHOOK_URL is not set; cannot deliver the report.")
        return 1

    try:
        pulls = fetch_open_pull_requests(repo, token)
    except (requests.RequestException, ValueError) as exc:
        LOGGER.error("Failed to query the GitHub API: %s", exc)
        return 1

    report = build_report(repo, pulls)
    LOGGER.info("Report generated:\n%s", report)

    try:
        send_to_teams(webhook_url, report)
    except requests.RequestException as exc:
        LOGGER.error("Failed to post the report to Teams: %s", exc)
        return 1

    LOGGER.info("Daily PR report completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

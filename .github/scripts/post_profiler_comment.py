"""Read public ADO artifacts as data and update a SHA-bound PR performance comment."""

import argparse
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time
from urllib.error import URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from eng.profiler_benchmarks.report import (
    LEGS,
    MARKER,
    MAX_BYTES,
    render,
    suite_hash,
    suite_paths,
    validate,
)

ROOT = Path(__file__).resolve().parents[2]
ADO = "https://dev.azure.com/sqlclientdrivers/public/_apis/build"
REPOSITORY = "microsoft/mssql-python"
HEADER = f"{MARKER}\n## PR Performance Report\n\n"
# Allow a 160-minute ADO job plus queueing; the workflow reserves publication time.
WAIT_MINUTES = 220


def allowed_url(url):
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
    ):
        return False
    return host in ("api.github.com", "dev.azure.com", "sqlclientdrivers.visualstudio.com") or (
        host.endswith(".vsblob.vsassets.io")
        or host.endswith(".blob.core.windows.net")
        or host.endswith(".artifacts.visualstudio.com")
    )


class SafeRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not allowed_url(newurl):
            raise ValueError("Artifact redirect outside permitted hosts")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if urlparse(req.full_url).hostname != urlparse(newurl).hostname:
            redirected.remove_header("Authorization")
        return redirected


def fetch(url, token=None, method=None, data=None, limit=4 * 1024 * 1024):
    if not allowed_url(url):
        raise ValueError("URL outside permitted hosts")
    headers = {"Accept": "application/json", "User-Agent": "mssql-python-profiler-ci"}
    if token:
        if urlparse(url).hostname != "api.github.com":
            raise ValueError("GitHub credentials must not be sent to artifact hosts")
        headers["Authorization"] = "Bearer " + token
    payload = None if data is None else json.dumps(data).encode()
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = Request(url, headers=headers, method=method, data=payload)
    with build_opener(SafeRedirect()).open(request, timeout=30) as response:
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError("Response exceeds size limit")
    return body


def api(url, **kwargs):
    return json.loads(fetch(url, **kwargs).decode("utf-8-sig"))


def github(path, **kwargs):
    return api(
        f"https://api.github.com/repos/{REPOSITORY}/{path}", token=os.environ["GH_TOKEN"], **kwargs
    )


def artifact_report(raw):
    """Read exactly one bounded JSON member; never extract or execute artifact files."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if len(members) > 200 or sum(member.file_size for member in members) > 64 * 1024 * 1024:
            raise ValueError("Oversized artifact")
        reports = [
            member for member in members if PurePosixPath(member.filename).name == "report.json"
        ]
        if len(reports) != 1:
            raise ValueError("Expected exactly one report.json")
        member = reports[0]
        path = PurePosixPath(member.filename)
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in member.filename
            or stat.S_ISLNK(member.external_attr >> 16)
            or member.file_size > MAX_BYTES
        ):
            raise ValueError("Invalid report member")
        if member.flag_bits & 1:
            raise ValueError("Encrypted performance artifacts are unsupported")
        return json.loads(archive.read(member).decode("utf-8"))


def publish(pr_number, head, body, base=None):
    def current():
        pr = github(f"pulls/{pr_number}")
        return (
            pr["state"] == "open"
            and pr["head"]["sha"] == head
            and (base is None or pr["base"]["sha"] == base)
        )

    if not current():
        print("Not publishing stale performance results")
        return
    page = 1
    comment = None
    while True:
        comments = github(f"issues/{pr_number}/comments?per_page=100&page={page}")
        comment = next(
            (
                c
                for c in comments
                if c["user"]["login"] == "github-actions[bot]" and c["body"].startswith(MARKER)
            ),
            comment,
        )
        if len(comments) < 100:
            break
        page += 1
    if comment:
        if not current():
            return
        github(f"issues/comments/{comment['id']}", method="PATCH", data={"body": body})
    else:
        if not current():
            return
        github(f"issues/{pr_number}/comments", method="POST", data={"body": body})


def find_build(builds, number, head):
    return next(
        (
            build
            for build in builds
            if isinstance(build, dict)
            and isinstance(build.get("definition"), dict)
            and isinstance(build.get("repository"), dict)
            and isinstance(build.get("triggerInfo"), dict)
            and build.get("definition", {}).get("id") == 2128
            and build.get("repository", {}).get("id", "").lower() == REPOSITORY
            and build.get("sourceBranch") == f"refs/pull/{number}/merge"
            and build.get("triggerInfo", {}).get("pr.sourceSha") == head
            and build.get("triggerInfo", {}).get("pr.number") == str(number)
        ),
        None,
    )


def build_items(response):
    items = response.get("value") if isinstance(response, dict) else None
    if not isinstance(items, list) or not all(
        isinstance(build, dict)
        and isinstance(build.get("id"), int)
        and isinstance(build.get("status"), str)
        and isinstance(build.get("definition"), dict)
        and isinstance(build.get("repository"), dict)
        and isinstance(build.get("triggerInfo"), dict)
        and isinstance(build.get("sourceBranch"), str)
        for build in items
    ):
        raise ValueError("Invalid build list")
    return items


def suite_blobs(commit):
    tree_sha = commit.get("tree", {}).get("sha")
    if not re.fullmatch(r"[0-9a-f]{40}", tree_sha or ""):
        raise ValueError("Invalid commit tree")
    tree = github(f"git/trees/{tree_sha}?recursive=1")
    if tree.get("truncated") is not False or not isinstance(tree.get("tree"), list):
        raise ValueError("Incomplete commit tree")
    expected = {path.relative_to(ROOT).as_posix() for path in suite_paths(ROOT)}
    blobs = {
        entry.get("path"): entry.get("sha")
        for entry in tree["tree"]
        if entry.get("type") == "blob" and entry.get("path") in expected
    }
    if set(blobs) != expected or any(
        not re.fullmatch(r"[0-9a-f]{40}", sha or "") for sha in blobs.values()
    ):
        raise ValueError("Benchmark suite missing from commit tree")
    return blobs


def artifact_items(response):
    items = response.get("value") if isinstance(response, dict) else None
    if not isinstance(items, list) or not all(
        isinstance(item, dict)
        and isinstance(item.get("name"), str)
        and isinstance(item.get("resource"), dict)
        for item in items
    ):
        raise ValueError("Invalid artifact list")
    return items


def unavailable(number, head, reason, base=None):
    publish(
        number,
        head,
        HEADER + "**Performance could not be assessed.**\n\n" + reason + " No result is available.",
        base,
    )


def run(number, head, wait_minutes):
    publish(
        number,
        head,
        HEADER
        + "**Performance assessment pending.**\n\n"
        + f"Waiting for the matching performance run for head `{head}`.",
    )
    deadline = time.monotonic() + wait_minutes * 60
    build = None
    pr_base = None
    failures = 0
    while time.monotonic() < deadline:
        try:
            pr = github(f"pulls/{number}")
            if (
                not isinstance(pr, dict)
                or not isinstance(pr.get("state"), str)
                or not isinstance(pr.get("head"), dict)
                or not isinstance(pr.get("base"), dict)
            ):
                raise ValueError
            current_head = pr["head"].get("sha")
            current_base = pr["base"].get("sha")
            pr_base = current_base
            query = urlencode(
                {
                    "definitions": 2128,
                    "branchName": f"refs/pull/{number}/merge",
                    "queryOrder": "queueTimeDescending",
                    "$top": 50,
                    "api-version": "7.1",
                }
            )
            build = find_build(build_items(api(f"{ADO}/builds?{query}")), number, head)
            if pr["state"] != "open" or current_head != head:
                return
            complete = (
                build is not None
                and build.get("status") == "completed"
                and build.get("result") != "canceled"
            )
        except (ValueError, KeyError, TypeError, URLError, TimeoutError):
            failures += 1
            if failures >= 5:
                unavailable(number, head, "Performance data services failed repeatedly.", pr_base)
                return
            time.sleep(30)
            continue
        failures = 0
        if complete:
            break
        time.sleep(30)
    if build is None or build.get("status") != "completed" or build.get("result") == "canceled":
        unavailable(
            number,
            head,
            f"No matching performance run completed within the {wait_minutes}-minute wait "
            f"for `{head}`.",
            pr_base,
        )
        return
    build_id = build["id"]
    source = build.get("sourceVersion")
    try:
        if (
            type(build_id) is not int
            or build_id <= 0
            or not re.fullmatch(r"[0-9a-f]{40}", source)
            or not re.fullmatch(r"[0-9a-f]{40}", pr_base or "")
        ):
            raise ValueError
        # Authenticate both sides of the merge through the current GitHub PR.
        commit = github(f"git/commits/{source}")
        if len(commit["parents"]) != 2 or [parent["sha"] for parent in commit["parents"]] != [
            pr_base,
            head,
        ]:
            raise ValueError
        base = pr_base
    except (ValueError, KeyError, TypeError, URLError, TimeoutError):
        unavailable(number, head, "Build provenance validation failed.", pr_base)
        return
    try:
        suite_unchanged = suite_blobs(commit) == suite_blobs(github(f"git/commits/{base}"))
    except (ValueError, KeyError, TypeError, URLError, TimeoutError):
        unavailable(
            number,
            head,
            "Benchmark suite validation failed because a required file changed.",
            pr_base,
        )
        return
    artifact_deadline = time.monotonic() + 120
    artifacts = None
    failures = 0
    while time.monotonic() < artifact_deadline:
        try:
            artifacts = artifact_items(api(f"{ADO}/builds/{build_id}/artifacts?api-version=7.1"))
            failures = 0
            if any(item.get("name", "").startswith("profiler-") for item in artifacts):
                break
        except (ValueError, KeyError, TypeError, URLError, TimeoutError):
            failures += 1
            if failures >= 5:
                artifacts = None
                break
        time.sleep(30)
    if artifacts is None:
        unavailable(number, head, "Performance artifacts remained unavailable.", pr_base)
        return
    reports, issues = [], []
    for leg in LEGS:
        matching = [item for item in artifacts if item["name"] == "profiler-" + leg]
        if len(matching) != 1:
            issues.append(leg + " (missing)")
            continue
        try:
            raw = fetch(matching[0]["resource"]["downloadUrl"], limit=32 * 1024 * 1024)
            report = validate(artifact_report(raw), build_id, head, source, base)
            if report["leg"] != leg:
                raise ValueError("Artifact leg mismatch")
            reports.append(report)
        except (ValueError, KeyError, TypeError, RecursionError, URLError, zipfile.BadZipFile):
            # Invalid data is visibly incomplete, never converted to a success verdict.
            issues.append(leg + " (invalid artifact)")
    if not suite_unchanged or any(report["suite_hash"] != suite_hash(ROOT) for report in reports):
        reports = []
        issues.append("workload version differs from trusted base")
    publish(number, head, render(reports, head, build_id, issues), base)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr", type=int, required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--wait-minutes", type=int, default=WAIT_MINUTES)
    args = parser.parse_args()
    if (
        args.pr <= 0
        or not re.fullmatch(r"[0-9a-f]{40}", args.head)
        or not 1 <= args.wait_minutes <= WAIT_MINUTES
    ):
        parser.error("Invalid PR, head SHA or wait limit")
    run(args.pr, args.head, args.wait_minutes)

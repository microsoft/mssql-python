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

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks"))
from profiler_report import LEGS, MARKER, MAX_BYTES, render, validate

ADO = "https://dev.azure.com/sqlclientdrivers/public/_apis/build"
REPOSITORY = "microsoft/mssql-python"


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


def publish(pr_number, head, body):
    pr = github(f"pulls/{pr_number}")
    if pr["state"] != "open" or pr["head"]["sha"] != head:
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
        if github(f"pulls/{pr_number}")["head"]["sha"] != head:
            return
        github(f"issues/comments/{comment['id']}", method="PATCH", data={"body": body})
    else:
        if github(f"pulls/{pr_number}")["head"]["sha"] != head:
            return
        github(f"issues/{pr_number}/comments", method="POST", data={"body": body})


def find_build(builds, number, head):
    return next(
        (
            build
            for build in builds
            if build.get("definition", {}).get("id") == 2128
            and build.get("repository", {}).get("id", "").lower() == REPOSITORY
            and build.get("sourceBranch") == f"refs/pull/{number}/merge"
            and build.get("triggerInfo", {}).get("pr.sourceSha") == head
            and build.get("triggerInfo", {}).get("pr.number") == str(number)
        ),
        None,
    )


def run(number, head, wait_minutes):
    publish(
        number,
        head,
        f"{MARKER}\n## Profiler performance report\n"
        f"Awaiting paired ADO measurements for head `{head}`. No regression verdict yet.",
    )
    deadline = time.monotonic() + wait_minutes * 60
    build = None
    while time.monotonic() < deadline:
        pr = github(f"pulls/{number}")
        if pr["state"] != "open" or pr["head"]["sha"] != head:
            return
        query = urlencode(
            {
                "definitions": 2128,
                "branchName": f"refs/pull/{number}/merge",
                "queryOrder": "queueTimeDescending",
                "$top": 50,
                "api-version": "7.1",
            }
        )
        build = find_build(api(f"{ADO}/builds?{query}")["value"], number, head)
        if build and build["status"] == "completed":
            break
        time.sleep(30)
    if build is None:
        publish(
            number,
            head,
            f"{MARKER}\n## Profiler performance report\n"
            f"No matching ADO run became available for `{head}`. Results are incomplete.",
        )
        return
    build_id = build["id"]
    source = build["sourceVersion"]
    if type(build_id) is not int or build_id <= 0 or not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("Invalid ADO build identity")
    # Authenticate the merge topology through GitHub, not the artifact's claims.
    commit = github(f"git/commits/{source}")
    if len(commit["parents"]) != 2 or commit["parents"][1]["sha"] != head:
        raise ValueError("ADO merge does not match current PR head")
    base = commit["parents"][0]["sha"]
    artifacts = api(f"{ADO}/builds/{build_id}/artifacts?api-version=7.1")["value"]
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
        except (ValueError, KeyError, TypeError, URLError, zipfile.BadZipFile):
            # Invalid data is visibly incomplete, never converted to a success verdict.
            issues.append(leg + " (invalid artifact)")
    if len({r["suite_hash"] for r in reports}) > 1:
        reports = []
        issues.append("workload versions differ across legs")
    publish(number, head, render(reports, head, build_id, issues))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr", type=int, required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--wait-minutes", type=int, default=95)
    args = parser.parse_args()
    if (
        args.pr <= 0
        or not re.fullmatch(r"[0-9a-f]{40}", args.head)
        or not 1 <= args.wait_minutes <= 95
    ):
        parser.error("Invalid PR, head SHA or wait limit")
    run(args.pr, args.head, args.wait_minutes)

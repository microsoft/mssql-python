"""Read public ADO artifacts as data and update a SHA-bound PR performance comment."""

import argparse
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from eng.profiler_benchmarks import report as reporting

ROOT = Path(__file__).resolve().parents[2]
ADO = "https://dev.azure.com/sqlclientdrivers/public/_apis/build"
REPOSITORY = "microsoft/mssql-python"
HEADER = f"{reporting.MARKER}\n## PR Performance Report\n\n"
# Allow a 160-minute ADO job plus queueing; the workflow reserves publication time.
WAIT_MINUTES = 220
COMPLETED_RESULTS = {"succeeded", "partiallySucceeded", "failed"}


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
    try:
        with build_opener(SafeRedirect()).open(request, timeout=30) as response:
            body = response.read(limit + 1)
    except (HTTPException, ConnectionError) as error:
        raise URLError("Incomplete HTTP response") from error
    if len(body) > limit:
        raise ValueError("Response exceeds size limit")
    return body


def api(url, **kwargs):
    return json.loads(fetch(url, **kwargs).decode("utf-8-sig"))


def github(path, **kwargs):
    return api(
        f"https://api.github.com/repos/{REPOSITORY}/{path}", token=os.environ["GH_TOKEN"], **kwargs
    )


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
                if c["user"]["login"] == "github-actions[bot]"
                and c["body"].startswith(reporting.MARKER)
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


def publish_with_retry(pr_number, head, body, base=None, attempts=3):
    for attempt in range(attempts):
        try:
            publish(pr_number, head, body, base)
            return
        except (KeyError, TypeError, ValueError, TimeoutError, URLError):
            if attempt + 1 == attempts:
                raise
            time.sleep(5)


def find_build(builds, number, head):
    return next(
        (
            build
            for build in builds
            if isinstance(build, dict)
            and isinstance(build.get("definition"), dict)
            and isinstance(build.get("repository"), dict)
            and isinstance(build["repository"].get("id"), str)
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
        and isinstance(build["repository"].get("id"), str)
        and isinstance(build.get("triggerInfo"), dict)
        and isinstance(build.get("sourceBranch"), str)
        for build in items
    ):
        raise ValueError("Invalid build list")
    return items


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
    publish_with_retry(
        number,
        head,
        HEADER + "**Performance could not be assessed.**\n\n" + reason + " No result is available.",
        base,
    )


def run(number, head, wait_minutes):
    publish_with_retry(
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
            if (
                build is not None
                and build.get("status") == "completed"
                and build.get("result") not in COMPLETED_RESULTS | {"canceled"}
            ):
                unavailable(
                    number,
                    head,
                    "Performance run completed with an unsupported result.",
                    pr_base,
                )
                return
            complete = (
                build is not None
                and build.get("status") == "completed"
                and build.get("result") in COMPLETED_RESULTS
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
    if (
        build is None
        or build.get("status") != "completed"
        or build.get("result") not in COMPLETED_RESULTS
    ):
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
        commit = github(f"git/commits/{source}")
        base = pr_base
        base_commit = github(f"git/commits/{base}")
        if not isinstance(commit, dict) or not isinstance(base_commit, dict):
            raise ValueError
        source_tree_info = commit.get("tree")
        base_tree_info = base_commit.get("tree")
        if not isinstance(source_tree_info, dict) or not isinstance(base_tree_info, dict):
            raise ValueError
        source_tree_sha = source_tree_info.get("sha")
        base_tree_sha = base_tree_info.get("sha")
        if not re.fullmatch(r"[0-9a-f]{40}", source_tree_sha or "") or not re.fullmatch(
            r"[0-9a-f]{40}", base_tree_sha or ""
        ):
            raise ValueError
        source_tree = github(f"git/trees/{source_tree_sha}?recursive=1")
        base_tree = github(f"git/trees/{base_tree_sha}?recursive=1")
    except (ValueError, KeyError, TypeError, URLError, TimeoutError):
        unavailable(number, head, "Build provenance validation failed.", pr_base)
        return
    artifacts = None
    failures = 0
    while time.monotonic() < deadline:
        try:
            artifacts = artifact_items(api(f"{ADO}/builds/{build_id}/artifacts?api-version=7.1"))
            failures = 0
            if {"profiler-" + leg for leg in reporting.LEGS} <= {
                item["name"] for item in artifacts
            }:
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
    artifact_urls, issues = {}, []
    for leg in reporting.LEGS:
        matching = [item for item in artifacts if item["name"] == "profiler-" + leg]
        if len(matching) != 1:
            issues.append(leg + " (missing)")
            continue
        url = matching[0]["resource"].get("downloadUrl")
        if not isinstance(url, str):
            issues.append(leg + " (invalid artifact)")
            continue
        artifact_urls[leg] = url

    def load_artifact(url):
        try:
            return fetch(url, limit=32 * 1024 * 1024)
        except (TimeoutError, URLError) as error:
            raise ValueError("Artifact download failed") from error

    evidence = reporting.AssessmentEvidence(
        build=build,
        head=head,
        base=base,
        merge_commit=commit,
        base_commit=base_commit,
        source_tree=source_tree,
        base_tree=base_tree,
        trusted_root=ROOT,
    )
    publish_with_retry(
        number, head, reporting.assess(evidence, artifact_urls, load_artifact, issues), base
    )


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

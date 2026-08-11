#!/usr/bin/env python3

import argparse
import html
import json
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

COMMENT_MARKER = "<!-- mssql-python-code-coverage -->"
EXPECTED_FIELDS = {
    "coverage_percentage",
    "covered_lines",
    "total_lines",
    "patch_coverage_pct",
    "low_coverage_files",
    "ado_url",
}
MAX_JSON_BYTES = 64 * 1024
MAX_LOW_COVERAGE_BYTES = 8 * 1024
PERCENTAGE_PATTERN = re.compile(r"^(?:0|[1-9][0-9]{0,2})(?:\.[0-9]+)?%$")
COUNT_PATTERN = re.compile(r"^(?:0|[1-9][0-9]{0,9}|N/A)$")
PATCH_COVERAGE_STATUSES = {"Could not parse", "Report not generated", "N/A%"}
ADO_PROJECT_ID = "904996cc-6198-4d39-8540-eca72bdf0b7b"
ADO_BUILD_PATHS = {
    "/sqlclientdrivers/public/_build/results",
    f"/sqlclientdrivers/{ADO_PROJECT_ID}/_build/results",
}


class ValidationError(ValueError):
    pass


def _load_json(path: Path, maximum_size: int = MAX_JSON_BYTES):
    if not path.is_file() or path.is_symlink():
        raise ValidationError(f"{path.name} must be a regular file")
    if path.stat().st_size > maximum_size:
        raise ValidationError(f"{path.name} exceeds the {maximum_size}-byte limit")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"{path.name} is not valid UTF-8 JSON") from exc


def _validate_single_line(name: str, value, maximum_length: int) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{name} must be a string")
    if not value or len(value) > maximum_length:
        raise ValidationError(f"{name} has an invalid length")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ValidationError(f"{name} contains control characters")
    return value


def _validate_percentage(name: str, value, allowed_statuses=frozenset()) -> str:
    value = _validate_single_line(name, value, 32)
    if value in allowed_statuses:
        return value
    if not PERCENTAGE_PATTERN.fullmatch(value):
        raise ValidationError(f"{name} must be a percentage")
    try:
        percentage = Decimal(value[:-1])
    except InvalidOperation as exc:
        raise ValidationError(f"{name} must be a percentage") from exc
    if percentage > 100:
        raise ValidationError(f"{name} cannot exceed 100%")
    return value


def _validate_count(name: str, value) -> str:
    value = _validate_single_line(name, value, 16)
    if not COUNT_PATTERN.fullmatch(value):
        raise ValidationError(f"{name} must be a non-negative integer or N/A")
    return value


def _validate_low_coverage_files(value) -> str:
    if not isinstance(value, str):
        raise ValidationError("low_coverage_files must be a string")
    if not value or len(value.encode("utf-8")) > MAX_LOW_COVERAGE_BYTES:
        raise ValidationError("low_coverage_files has an invalid length")
    if "\r" in value:
        raise ValidationError("low_coverage_files contains carriage returns")
    if len(value.splitlines()) > 10:
        raise ValidationError("low_coverage_files contains more than 10 lines")
    if any(
        character != "\n" and unicodedata.category(character).startswith("C") for character in value
    ):
        raise ValidationError("low_coverage_files contains control characters")
    return value


def _validate_ado_url(value) -> str:
    value = _validate_single_line("ado_url", value, 500)
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.hostname.lower() != "dev.azure.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.path.lower() not in ADO_BUILD_PATHS
        or parsed.fragment
    ):
        raise ValidationError("ado_url must reference the public SqlClientDrivers build")

    query = parse_qs(parsed.query, keep_blank_values=True)
    if set(query) != {"buildId"}:
        raise ValidationError("ado_url contains unexpected query parameters")
    build_ids = query.get("buildId", [])
    if len(build_ids) != 1 or not build_ids[0].isdigit():
        raise ValidationError("ado_url must contain one numeric buildId")
    return (
        f"https://dev.azure.com/SqlClientDrivers/{ADO_PROJECT_ID}/_build/results"
        f"?buildId={build_ids[0]}"
    )


def validate_artifact(artifact_directory: Path) -> dict:
    if not artifact_directory.is_dir() or artifact_directory.is_symlink():
        raise ValidationError("artifact path must be a directory")

    files = []
    for path in artifact_directory.rglob("*"):
        if path.is_symlink():
            raise ValidationError("artifact must not contain symbolic links")
        if path.is_file():
            files.append(path.relative_to(artifact_directory).as_posix())
    if files != ["pr-info.json"]:
        raise ValidationError("artifact must contain only pr-info.json")

    data = _load_json(artifact_directory / "pr-info.json")
    if not isinstance(data, dict) or set(data) != EXPECTED_FIELDS:
        raise ValidationError("pr-info.json does not match the expected schema")

    return {
        "coverage_percentage": _validate_percentage(
            "coverage_percentage", data["coverage_percentage"]
        ),
        "covered_lines": _validate_count("covered_lines", data["covered_lines"]),
        "total_lines": _validate_count("total_lines", data["total_lines"]),
        "patch_coverage_pct": _validate_percentage(
            "patch_coverage_pct",
            data["patch_coverage_pct"],
            PATCH_COVERAGE_STATUSES,
        ),
        "low_coverage_files": _validate_low_coverage_files(data["low_coverage_files"]),
        "ado_url": _validate_ado_url(data["ado_url"]),
    }


def resolve_pr_number(event: dict, associated_pulls: list) -> int:
    repository = event.get("repository", {})
    workflow_run = event.get("workflow_run", {})
    repository_name = repository.get("full_name")
    default_branch = repository.get("default_branch")
    head_sha = workflow_run.get("head_sha")
    head_repository = workflow_run.get("head_repository") or {}

    if not isinstance(repository_name, str) or not repository_name:
        raise ValidationError("event is missing the repository name")
    if not isinstance(default_branch, str) or not default_branch:
        raise ValidationError("event is missing the default branch")
    if not isinstance(head_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", head_sha):
        raise ValidationError("workflow run has an invalid head SHA")
    if head_repository.get("full_name") == repository_name:
        raise ValidationError("workflow run did not originate from a fork")

    event_pulls = workflow_run.get("pull_requests") or []
    if len(event_pulls) == 1:
        number = event_pulls[0].get("number")
        if isinstance(number, int) and number > 0:
            return number

    if not isinstance(associated_pulls, list):
        raise ValidationError("associated pull request response must be a list")
    matching_pulls = [
        pull
        for pull in associated_pulls
        if pull.get("head", {}).get("sha") == head_sha
        and pull.get("base", {}).get("ref") == default_branch
        and pull.get("base", {}).get("repo", {}).get("full_name") == repository_name
        and isinstance(pull.get("number"), int)
        and pull["number"] > 0
    ]
    if len(matching_pulls) != 1:
        raise ValidationError("workflow run must resolve to exactly one pull request")
    return matching_pulls[0]["number"]


def build_comment(data: dict) -> str:
    low_coverage_files = html.escape(data["low_coverage_files"])
    ado_url = data["ado_url"]
    return f"""\
{COMMENT_MARKER}
# Code Coverage Report

| Diff coverage | Overall coverage | Lines covered |
| --- | --- | --- |
| **{data["patch_coverage_pct"]}** | **{data["coverage_percentage"]}** | **{data["covered_lines"]}** of **{data["total_lines"]}** |

### Files needing attention

<pre>{low_coverage_files}</pre>

[View Azure DevOps build]({ado_url})
"""


def prepare_comment(artifact_directory: Path, event_path: Path, pulls_path: Path):
    data = validate_artifact(artifact_directory)
    event = _load_json(event_path, 1024 * 1024)
    associated_pulls = _load_json(pulls_path, 1024 * 1024)
    pr_number = resolve_pr_number(event, associated_pulls)
    return pr_number, build_comment(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-directory", required=True, type=Path)
    parser.add_argument("--event", required=True, type=Path)
    parser.add_argument("--associated-pulls", required=True, type=Path)
    parser.add_argument("--comment-output", required=True, type=Path)
    parser.add_argument("--pr-number-output", required=True, type=Path)
    args = parser.parse_args()

    try:
        pr_number, comment = prepare_comment(
            args.artifact_directory, args.event, args.associated_pulls
        )
    except ValidationError as exc:
        parser.error(str(exc))

    args.comment_output.write_text(json.dumps({"body": comment}), encoding="utf-8")
    args.pr_number_output.write_text(str(pr_number), encoding="ascii")


if __name__ == "__main__":
    main()

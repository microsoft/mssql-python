"""Read-only verification of the recorded Conda producer and its upstream wheel run."""

from __future__ import annotations

import json
import os
import re
from typing import Callable
from urllib.request import HTTPRedirectHandler, Request, build_opener

_REPOSITORY_ID = "eec96f30-ec96-4910-abd6-c45a99a5c29f"
_PROJECT_ID = "c6d89619-62de-46a0-8b46-70b92a84d85e"
_CONDA_PIPELINE_ID = 2318
_WHEEL_PIPELINE_ID = 2199
_MAIN = "refs/heads/main"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Unexpected Azure DevOps API redirect; credentials were not forwarded.")


def _positive_id(value: object) -> int:
    if not re.fullmatch(r"[1-9][0-9]*", str(value)):
        raise ValueError(f"Missing or invalid pipeline/run ID: {value!r}")
    return int(str(value))


def _verify_run(build: dict, run: dict, pipeline_id: int, run_id: int) -> dict:
    if build.get("id") != run_id or build.get("definition", {}).get("id") != pipeline_id:
        raise ValueError(f"Build API identity mismatch for pipeline {pipeline_id}, run {run_id}.")
    if run.get("id") != run_id or run.get("pipeline", {}).get("id") != pipeline_id:
        raise ValueError(f"Runs API identity mismatch for pipeline {pipeline_id}, run {run_id}.")
    if build.get("status") != "completed" or build.get("result") != "succeeded":
        raise ValueError(f"Build {run_id} must be completed/succeeded.")
    if run.get("state") != "completed" or run.get("result") != "succeeded":
        raise ValueError(f"Pipeline run {run_id} must be completed/succeeded.")
    if not build.get("buildNumber") or run.get("name") != build["buildNumber"]:
        raise ValueError(f"Build/run number mismatch for {run_id}.")

    branch, commit = build.get("sourceBranch"), build.get("sourceVersion")
    if not isinstance(branch, str) or not branch.startswith("refs/heads/"):
        raise ValueError(f"Build {run_id} has no valid source branch.")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", commit):
        raise ValueError(f"Build {run_id} has no valid source commit.")
    source = run.get("resources", {}).get("repositories", {}).get("self", {})
    if (
        source.get("refName") != branch
        or source.get("version") != commit
        or source.get("repository", {}).get("id") != _REPOSITORY_ID
        or build.get("repository", {}).get("id") != _REPOSITORY_ID
        or build.get("project", {}).get("id") != _PROJECT_ID
    ):
        raise ValueError(f"Build/run source repository, branch or commit mismatch for {run_id}.")
    return {"pipeline": pipeline_id, "run": run_id, "branch": branch, "commit": commit}


def verify_provenance(
    get_json: Callable[[str], dict],
    *,
    producer_pipeline_id: int,
    producer_run_id: int,
    producer_branch: str,
    producer_commit: str,
    release_branch: str,
    publish: bool,
) -> dict:
    if _positive_id(producer_pipeline_id) != _CONDA_PIPELINE_ID:
        raise ValueError(f"Conda producer must be pipeline {_CONDA_PIPELINE_ID}.")
    producer_run_id = _positive_id(producer_run_id)
    build = get_json(f"build/builds/{producer_run_id}?api-version=7.1")
    run = get_json(f"pipelines/{_CONDA_PIPELINE_ID}/runs/{producer_run_id}?api-version=7.1")
    producer = _verify_run(build, run, _CONDA_PIPELINE_ID, producer_run_id)
    if producer["branch"] != producer_branch or producer["commit"] != producer_commit:
        raise ValueError(
            "Selected Conda resource branch/commit differs from the recorded producer."
        )

    # ADO records the selected run ID in the nested pipeline.id and its build number
    # in version. The authoritative Build response supplies the actual definition ID.
    wheel_resource = run.get("resources", {}).get("pipelines", {}).get("buildPipeline", {})
    wheel_run_id = _positive_id(wheel_resource.get("pipeline", {}).get("id"))
    wheel_build = get_json(f"build/builds/{wheel_run_id}?api-version=7.1")
    if not wheel_resource.get("version") or wheel_resource["version"] != wheel_build.get(
        "buildNumber"
    ):
        raise ValueError("Recorded upstream wheel version differs from its Build API build number.")
    wheel = _verify_run(
        wheel_build,
        get_json(f"pipelines/{_WHEEL_PIPELINE_ID}/runs/{wheel_run_id}?api-version=7.1"),
        _WHEEL_PIPELINE_ID,
        wheel_run_id,
    )
    reasons = [
        f"{name} source is {branch}, not {_MAIN}"
        for name, branch in (
            ("release", release_branch),
            ("Conda producer", producer["branch"]),
            ("wheel producer", wheel["branch"]),
        )
        if branch != _MAIN
    ]
    if publish and reasons:
        raise ValueError("Production publication is ineligible: " + "; ".join(reasons))
    return {
        "producer": producer,
        "wheel": wheel,
        "productionEligible": not reasons,
        "productionIneligibilityReasons": reasons,
    }


def ado_get_json(path: str) -> dict:
    """Read release evidence only from the configured SqlClientDrivers collection."""
    token = os.environ["SYSTEM_ACCESSTOKEN"]
    if not token or token.startswith("$("):
        raise ValueError("System.AccessToken is unavailable; provenance cannot be verified.")
    collection = os.environ["SYSTEM_COLLECTIONURI"].rstrip("/")
    if collection.lower() not in {
        "https://dev.azure.com/sqlclientdrivers",
        "https://sqlclientdrivers.visualstudio.com",
    }:
        raise ValueError("Unexpected Azure DevOps collection; refusing to send credentials.")
    project = os.environ["SYSTEM_TEAMPROJECTID"]
    if project.lower() != _PROJECT_ID:
        raise ValueError("System.TeamProjectId must identify the mssql-python release project.")

    request = Request(
        f"{collection}/{project}/_apis/{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with build_opener(_NoRedirect()).open(request, timeout=60) as response:
        return json.load(response)


def main() -> None:
    publish = os.environ["PUBLISH_TO_CONDA"].lower()
    if publish not in {"true", "false"}:
        raise ValueError("PUBLISH_TO_CONDA must be true or false.")
    result = verify_provenance(
        ado_get_json,
        producer_pipeline_id=_positive_id(os.environ["CONDA_BUILD_PIPELINE_ID"]),
        producer_run_id=_positive_id(os.environ["CONDA_BUILD_RUN_ID"]),
        producer_branch=os.environ["CONDA_BUILD_SOURCE_BRANCH"],
        producer_commit=os.environ["CONDA_BUILD_SOURCE_COMMIT"],
        release_branch=os.environ["RELEASE_SOURCE_BRANCH"],
        publish=publish == "true",
    )
    print("VERIFIED_RECORDED_PROVENANCE: " + json.dumps(result, sort_keys=True))
    if publish == "false":
        print("Validate-only: provenance verified; package readiness is gated separately.")


if __name__ == "__main__":
    main()

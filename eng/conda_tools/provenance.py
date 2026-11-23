"""Read-only verification of the recorded Conda producer and its upstream wheel run."""

from __future__ import annotations

import json
import os
import re
import sys
from http.client import HTTPException
from typing import Callable
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .inputs import read_release_versions

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
        raise ValueError("Missing or invalid pipeline/run ID; expected a positive decimal integer.")
    return int(str(value))


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable {name}.")
    return value


def _producer_inputs(get_json: Callable[[str], dict], commit: str) -> dict:
    def read_source(path: str) -> str:
        query = urlencode(
            {
                "path": "/" + path,
                "includeContent": "true",
                "versionDescriptor.versionType": "commit",
                "versionDescriptor.version": commit,
                "$format": "json",
                "api-version": "7.1",
            }
        )
        item = get_json(f"git/repositories/{_REPOSITORY_ID}/items?{query}")
        if (
            item.get("path") != "/" + path
            or item.get("commitId") != commit
            or item.get("gitObjectType") != "blob"
            or not isinstance(item.get("content"), str)
        ):
            raise ValueError(f"Missing or mismatched wheel producer source for {path} at {commit}.")
        return item["content"]

    return {"versions": read_release_versions(read_source)}


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


def _wheel_security_reasons(get_json: Callable[[str], dict], build: dict, run: dict) -> list[str]:
    parameters = run.get("templateParameters")
    if not isinstance(parameters, dict):
        return ["wheel producer security parameters are missing"]
    reasons = []
    # Scheduled producers select Official even when the parameter defaults to NonOfficial.
    if build.get("reason") != "schedule" and parameters.get("oneBranchType") != "Official":
        reasons.append("wheel producer did not use Official mode")
    for name in ("runSdlTasks", "signWindowsBinaries"):
        if parameters.get(name) is not True:
            reasons.append(f"wheel producer {name} was not enabled")
    if reasons:
        return reasons

    expected_jobs = {"ODBC_BuildAll.BuildWheel.__default"}
    for parameter, prefix, fields, job in (
        ("windowsConfigs", "Win_py", ("pyVer", "arch"), "BuildWheel"),
        ("macosConfigs", "MacOS_py", ("pyVer",), "BuildWheel"),
        ("linuxConfigs", "Linux_", ("tag", "arch"), "BuildWheels"),
    ):
        configs = parameters.get(parameter)
        if not isinstance(configs, list) or not configs:
            return [f"wheel producer {parameter} evidence is missing"]
        for config in configs:
            if not isinstance(config, dict):
                return [f"wheel producer {parameter} evidence is invalid"]
            identity_fields = (
                (*fields, "shard")
                if parameter == "linuxConfigs" and "shard" in config
                else fields
            )
            if any(
                not isinstance(config.get(field), str)
                or not re.fullmatch(r"[a-zA-Z0-9_]+", config[field])
                for field in identity_fields
            ):
                return [f"wheel producer {parameter} evidence is invalid"]
            identifier = (
                prefix + "_".join(config[field] for field in identity_fields) + f".{job}.__default"
            )
            if identifier in expected_jobs:
                return [f"wheel producer {parameter} contains duplicate jobs"]
            expected_jobs.add(identifier)

    timeline = get_json(f"build/builds/{build['id']}/timeline?api-version=7.1").get("records")
    if not isinstance(timeline, list) or not all(isinstance(record, dict) for record in timeline):
        return ["wheel producer security timeline is missing or invalid"]
    for identifier in sorted(expected_jobs):
        jobs = [
            record
            for record in timeline
            if record.get("type") == "Job" and record.get("identifier") == identifier
        ]
        if (
            len(jobs) != 1
            or not isinstance(jobs[0].get("id"), str)
            or not jobs[0]["id"]
            or jobs[0].get("state") != "completed"
            or jobs[0].get("result") != "succeeded"
            or type(jobs[0].get("attempt")) is not int
            or jobs[0]["attempt"] < 1
        ):
            reasons.append(f"wheel producer job {identifier} is missing or did not succeed")
            continue
        job_record = jobs[0]
        wheel_kind = "ODBC" if identifier.startswith("ODBC_") else "Python"
        required_tasks = [
            (f"ESRP MalwareScanning - {wheel_kind} Wheels (Official)", "EsrpMalwareScanning"),
            ("Component Governance Detection", "ComponentGovernanceComponentDetection"),
        ]
        if identifier.startswith("Win_"):
            required_tasks.extend(
                [
                    ("ESRP MalwareScanning - Native extension", "EsrpMalwareScanning"),
                    ("ESRP CodeSigning - Native extension (.pyd)", "EsrpCodeSigning"),
                    ("Verify Authenticode signature (native extension)", "PowerShell"),
                    ("Verify signed .pyd inside built wheel", "PowerShell"),
                ]
            )
        for name, task_type in required_tasks:
            tasks = [
                record
                for record in timeline
                if record.get("type") == "Task"
                and record.get("parentId") == job_record["id"]
                and record.get("name") == name
                and record.get("attempt") == job_record["attempt"]
            ]
            if (
                len(tasks) != 1
                or tasks[0].get("state") != "completed"
                or tasks[0].get("result") != "succeeded"
                or not isinstance(tasks[0].get("task"), dict)
                or tasks[0]["task"].get("name") != task_type
            ):
                reasons.append(f"wheel producer {identifier} lacks completed/succeeded {name}")
    return reasons


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
    wheel_run = get_json(f"pipelines/{_WHEEL_PIPELINE_ID}/runs/{wheel_run_id}?api-version=7.1")
    wheel = _verify_run(wheel_build, wheel_run, _WHEEL_PIPELINE_ID, wheel_run_id)
    reasons = [
        f"{name} source is {branch}, not {_MAIN}"
        for name, branch in (
            ("release", release_branch),
            ("Conda producer", producer["branch"]),
            ("wheel producer", wheel["branch"]),
        )
        if branch != _MAIN
    ]
    reasons.extend(_wheel_security_reasons(get_json, wheel_build, wheel_run))
    if publish and reasons:
        raise ValueError("Production publication is ineligible: " + "; ".join(reasons))
    return {
        "producer": producer,
        "wheel": wheel,
        **_producer_inputs(get_json, wheel["commit"]),
        "productionEligible": not reasons,
        "productionIneligibilityReasons": reasons,
    }


def ado_get_json(path: str) -> dict:
    """Read release evidence only from the configured SqlClientDrivers collection."""
    token = _required_env("SYSTEM_ACCESSTOKEN")
    if not token or token.startswith("$("):
        raise ValueError("System.AccessToken is unavailable; provenance cannot be verified.")
    collection = _required_env("SYSTEM_COLLECTIONURI").rstrip("/")
    if collection.lower() not in {
        "https://dev.azure.com/sqlclientdrivers",
        "https://sqlclientdrivers.visualstudio.com",
    }:
        raise ValueError("Unexpected Azure DevOps collection; refusing to send credentials.")
    project = _required_env("SYSTEM_TEAMPROJECTID")
    if project.lower() != _PROJECT_ID:
        raise ValueError("System.TeamProjectId must identify the mssql-python release project.")

    request = Request(
        f"{collection}/{project}/_apis/{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with build_opener(_NoRedirect()).open(request, timeout=60) as response:
        return json.load(response)


def execute() -> None:
    publish = _required_env("PUBLISH_TO_CONDA").lower()
    if publish not in {"true", "false"}:
        raise ValueError("PUBLISH_TO_CONDA must be true or false.")
    result = verify_provenance(
        ado_get_json,
        producer_pipeline_id=_positive_id(_required_env("CONDA_BUILD_PIPELINE_ID")),
        producer_run_id=_positive_id(_required_env("CONDA_BUILD_RUN_ID")),
        producer_branch=_required_env("CONDA_BUILD_SOURCE_BRANCH"),
        producer_commit=_required_env("CONDA_BUILD_SOURCE_COMMIT"),
        release_branch=_required_env("RELEASE_SOURCE_BRANCH"),
        publish=publish == "true",
    )
    version = result["versions"]["mssql-python"]
    expected = os.environ.get("MSSQL_PYTHON_VERSION", "")
    if expected and expected != version:
        raise ValueError(
            "mssqlPythonVersion differs from the recorded wheel producer release version."
        )
    print("VERIFIED_RECORDED_PROVENANCE: " + json.dumps(result, sort_keys=True))
    print(f"##vso[task.setvariable variable=mssqlPythonVersion;isOutput=true]{version}")
    versions = json.dumps(result["versions"], sort_keys=True)
    print(f"##vso[task.setvariable variable=releaseVersions;isOutput=true]{versions}")
    if publish == "false":
        print("Non-publishing run: provenance verified; publisher rehearsal is gated separately.")


def cli() -> int:
    """Keep expected CLI failures concise; imported callers still receive exceptions."""
    expected_errors = (ValueError, RuntimeError, ImportError, OSError, HTTPException)
    try:
        execute()
    except expected_errors as error:
        cause = error.__cause__
        while cause is not None:
            if not isinstance(cause, expected_errors):
                raise
            cause = cause.__cause__
        if isinstance(error, ValueError) and not isinstance(
            error, (json.JSONDecodeError, UnicodeError)
        ):
            message = str(error)
        else:
            message = "Check provenance configuration, Azure DevOps access and response format."
        secret = os.environ.get("SYSTEM_ACCESSTOKEN")
        if secret:
            message = message.replace(secret, "[REDACTED]")
        print(f"ERROR: {type(error).__name__}: " + " ".join(message.split()), file=sys.stderr)
        return 1
    return 0

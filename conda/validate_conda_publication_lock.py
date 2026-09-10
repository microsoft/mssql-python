"""GET-only publication guard; native ADO checks, NOT this code, enforce the lock.

Admin prerequisite: group117 needs enabled Exclusive lock + Approval checks, sole
pipeline2322 authorization, and their actual CONDA_PUBLICATION_{LOCK,APPROVAL}_CHECK_ID
values. Call inside CondaRelease consuming that group with lockBehavior: sequential,
before snapshot; keep promotion/rollback/cleanup inside the SAME stage lock lifetime.
Never call during dry-run. Producer/wheel provenance remains a separate caller guard.

Positive lock-specific API evidence is UNVERIFIED in this project. Positive tests
describe an expected contract, not proven production enforcement. Empty/unknown
states reject publication pending administrator setup and live verification.
"""

from __future__ import annotations

import os
import re
import sys

from validate_conda_provenance import ado_get_json

PROJECT_ID = "c6d89619-62de-46a0-8b46-70b92a84d85e"
REPOSITORY_ID = "eec96f30-ec96-4910-abd6-c45a99a5c29f"
GROUP_ID = 117
PIPELINE_ID = 2322
STAGE = "CondaRelease"
MAIN = "refs/heads/main"
LOCK_TYPE = "2ef31ad6-baa0-403a-8b45-2cbc9b4e5563"
APPROVAL_TYPE = "8c6f20a7-a545-4486-9777-f762fafe0d4d"
_UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")


def _require(condition: bool, message: str) -> None:
    if not condition:
        # Never interpolate response bodies, variable values, tokens or API exceptions.
        raise ValueError("Publication lock rejected: " + message)


def _at(record: object, *keys: str) -> object:
    for key in keys:
        if not isinstance(record, dict):
            return None
        record = record.get(key)
    return record


def _id(value: object) -> int:
    _require(
        isinstance(value, (int, str))
        and not isinstance(value, bool)
        and re.fullmatch(r"[1-9][0-9]*", str(value)) is not None,
        "missing or invalid positive ID/attempt.",
    )
    return int(value)


def _items(value: object) -> list:
    _require(isinstance(value, list), "missing or malformed evidence list.")
    _require(all(isinstance(item, dict) for item in value), "malformed evidence record.")
    return value


def _one(records: list, predicate, message: str) -> dict:
    matches = [record for record in records if predicate(record)]
    _require(len(matches) == 1, message)
    return matches[0]


def _resource(value: object) -> bool:
    return _at(value, "type") == "variablegroup" and _at(value, "id") == str(GROUP_ID)


def _checkpoint(timeline: dict, stage_attempt: int) -> tuple[dict, dict]:
    records = _items(_at(timeline, "records"))
    stage = _one(
        records,
        lambda r: r.get("type") == "Stage"
        and r.get("identifier") == STAGE
        and r.get("attempt") == stage_attempt
        and not isinstance(r.get("attempt"), bool),
        "current CondaRelease stage/attempt is missing or ambiguous.",
    )
    _require(stage.get("state") == "inProgress", "stage is not running.")
    _require(stage.get("result") is None, "stage already has a result.")
    _require(bool(_UUID.fullmatch(str(stage.get("id", "")))), "invalid stage identity.")
    checkpoint = _one(
        records,
        lambda r: r.get("type") == "Checkpoint"
        and r.get("parentId") == stage["id"]
        and r.get("attempt") == stage_attempt
        and not isinstance(r.get("attempt"), bool),
        "current stage checkpoint is missing or ambiguous.",
    )
    _require(
        checkpoint.get("state") == "completed" and checkpoint.get("result") == "succeeded",
        "stage checkpoint has not succeeded.",
    )
    _require(bool(_UUID.fullmatch(str(checkpoint.get("id", "")))), "invalid checkpoint ID.")
    return stage, checkpoint


def validate_publication_lock(
    evidence: dict,
    *,
    run_id: int,
    source_commit: str,
    stage_attempt: int,
    stage_id: str,
    lock_check_id: int,
    approval_check_id: int,
    owner: str = "microsoft",
    package: str = "mssql-python",
    target_label: str = "main",
) -> dict:
    """Pure validator of supplied API records; no I/O and no lock acquisition."""
    _require(
        (owner, package, target_label) == ("microsoft", "mssql-python", "main"),
        "target is outside the protected publication contract.",
    )
    run_id, stage_attempt = _id(run_id), _id(stage_attempt)
    lock_check_id, approval_check_id = _id(lock_check_id), _id(approval_check_id)
    _require(lock_check_id != approval_check_id, "lock and approval IDs must differ.")
    _require(
        isinstance(source_commit, str)
        and re.fullmatch(r"[0-9a-fA-F]{40}", source_commit) is not None,
        "missing or invalid current source commit.",
    )
    _require(bool(_UUID.fullmatch(str(stage_id))), "invalid current stage ID.")
    group = _at(evidence, "group")
    _require(_at(group, "id") == GROUP_ID, "wrong group ID.")
    _require(_at(group, "name") == "Anaconda Publishing", "wrong group name.")
    secret = _at(group, "variables", "ANACONDA_API_TOKEN", "isSecret")
    _require(secret is True, "publishing token secret metadata is missing.")
    permissions = _at(evidence, "permissions")
    _require(_resource(_at(permissions, "resource")), "authorization resource mismatch.")
    _require("allPipelines" in permissions, "all-pipeline authorization is unknown.")
    ap = permissions["allPipelines"]
    _require(ap is None or _at(ap, "authorized") is False, "all-pipeline access is not denied.")
    pipelines = _items(_at(permissions, "pipelines"))
    _require(
        all(isinstance(pipeline.get("authorized"), bool) for pipeline in pipelines),
        "pipeline authorization state is unknown.",
    )
    authorized = [pipeline for pipeline in pipelines if pipeline["authorized"]]
    _require(len(authorized) == 1, "expected sole pipeline authorization.")
    _require(authorized[0].get("id") == PIPELINE_ID, "wrong authorized pipeline.")
    _require(authorized[0].get("authorized") is True, "pipeline is not authorized.")
    build = _at(evidence, "build")
    _require(_at(build, "id") == run_id, "wrong current build.")
    _require(_at(build, "definition", "id") == PIPELINE_ID, "wrong current pipeline.")
    _require(_at(build, "project", "id") == PROJECT_ID, "wrong current project.")
    _require(_at(build, "repository", "id") == REPOSITORY_ID, "wrong current repository.")
    _require(_at(build, "sourceBranch") == MAIN, "current build is not main.")
    _require(_at(build, "sourceVersion") == source_commit, "wrong current commit.")
    _require(_at(build, "status") == "inProgress", "current build is not running.")
    _require(_at(build, "result") is None, "current build already has a result.")
    plan_id = _at(build, "orchestrationPlan", "planId")
    _require(bool(_UUID.fullmatch(str(plan_id))), "missing or invalid build plan ID.")
    stage, checkpoint = _checkpoint(_at(evidence, "timeline"), stage_attempt)
    _require(stage["id"] == stage_id, "current stage ID differs from timeline stage.")
    suite = _at(evidence, "suite")
    context = _at(suite, "context")
    _require(
        _at(suite, "id") == checkpoint["id"]
        and _at(context, "Id") == checkpoint["id"]
        and _at(context, "PlanId") == plan_id
        and _at(context, "Project", "Id") == PROJECT_ID
        and _id(_at(context, "Pipeline", "Id")) == PIPELINE_ID
        and _at(context, "Pipeline", "Owner", "Id") == run_id
        and _at(context, "Branch") == MAIN
        and _at(context, "HubName") == "Build"
        and _at(context, "GraphNode", "Id") == stage["id"]
        and _at(context, "GraphNode", "Name") == STAGE
        and _id(_at(context, "GraphNode", "Attempt")) == stage_attempt,
        "check suite does not belong to this build/plan/stage/attempt.",
    )
    _require(_at(suite, "status") == "approved", "stage check suite is not approved.")
    configurations = _items(_at(evidence, "configurations", "value"))
    _require(
        _at(evidence, "configurations", "count") == len(configurations),
        "incomplete check configuration response.",
    )
    check_runs = _items(_at(suite, "checkRuns"))
    for check_id, check_type in ((lock_check_id, LOCK_TYPE), (approval_check_id, APPROVAL_TYPE)):
        config = _one(
            configurations, lambda c: c.get("id") == check_id, "missing/duplicate config."
        )
        _require(_resource(config.get("resource")), "wrong configured resource.")
        _require(_at(config, "type", "id") == check_type, "wrong configured native type.")
        _require(config.get("isDisabled") is False, "check disabled or enabled state unknown.")
        _require(not config.get("issue"), "invalid check configuration.")
        check = _one(
            check_runs,
            lambda c: _at(c, "checkConfigurationRef", "id") == check_id,
            "expected native check was not evaluated exactly once for this stage.",
        )
        reference = check.get("checkConfigurationRef")
        _require(_resource(_at(reference, "resource")), "wrong evaluated resource.")
        _require(_at(reference, "type", "id") == check_type, "wrong evaluated native type.")
        _require(check.get("status") == "approved", "lock state is unverified.")
        # If a configuration revision is exposed, never accept a stale evaluation.
        if "version" in config:
            _require(
                _id(config["version"]) == _id(_at(reference, "version")),
                "check configuration changed since stage evaluation.",
            )
    return {
        "buildId": run_id,
        "resourceId": GROUP_ID,
        "stage": STAGE,
        "stageAttempt": stage_attempt,
        "lockCheckId": lock_check_id,
        "approvalCheckId": approval_check_id,
    }


def require_publication_lock(
    owner: str = "microsoft", package: str = "mssql-python", target_label: str = "main"
) -> dict:
    """Publisher callback. Missing deployment metadata/prerequisites block publication."""
    _require(
        (owner, package, target_label) == ("microsoft", "mssql-python", "main"),
        "target is outside the protected publication contract.",
    )
    _require(os.environ.get("SYSTEM_TEAMPROJECTID") == PROJECT_ID, "unexpected project.")
    _require(os.environ.get("SYSTEM_STAGENAME") == STAGE, "caller is not in CondaRelease.")
    _require(os.environ.get("BUILD_SOURCEBRANCH") == MAIN, "caller is not using main.")
    stage_id = os.environ.get("SYSTEM_STAGEID", "")
    _require(bool(_UUID.fullmatch(stage_id)), "System.StageId is unavailable or invalid.")
    arguments = {
        "run_id": _id(os.environ.get("BUILD_BUILDID")),
        "source_commit": os.environ.get("BUILD_SOURCEVERSION", ""),
        "stage_attempt": _id(os.environ.get("SYSTEM_STAGEATTEMPT")),
        "stage_id": stage_id,
        "lock_check_id": _id(os.environ.get("CONDA_PUBLICATION_LOCK_CHECK_ID")),
        "approval_check_id": _id(os.environ.get("CONDA_PUBLICATION_APPROVAL_CHECK_ID")),
        "owner": owner,
        "package": package,
        "target_label": target_label,
    }
    run_id = arguments["run_id"]
    try:
        evidence = {
            "group": ado_get_json(f"distributedtask/variablegroups/{GROUP_ID}?api-version=7.1"),
            "configurations": ado_get_json(
                "pipelines/checks/configurations?resourceType=variablegroup"
                f"&resourceId={GROUP_ID}&$expand=settings&api-version=7.1-preview.1"
            ),
            "permissions": ado_get_json(
                f"pipelines/pipelinepermissions/variablegroup/{GROUP_ID}?api-version=7.1-preview.1"
            ),
            "build": ado_get_json(f"build/builds/{run_id}?api-version=7.1"),
            "timeline": ado_get_json(f"build/builds/{run_id}/timeline?api-version=7.1"),
        }
        _, checkpoint = _checkpoint(evidence["timeline"], arguments["stage_attempt"])
        evidence["suite"] = ado_get_json(
            f"pipelines/checks/runs/{checkpoint['id']}?$expand=resources&api-version=7.1-preview.1"
        )
    except (OSError, ValueError):
        raise ValueError("Publication evidence unavailable; publication remains blocked.") from None
    return validate_publication_lock(evidence, **arguments)


def main(argv: list[str] | None = None) -> None:
    """No CLI overrides or dry-run bypass; use only in the publishing task."""
    _require(not (sys.argv[1:] if argv is None else argv), "CLI accepts no arguments.")
    require_publication_lock()
    print("CONDA_PUBLICATION_CHECK_CONTRACT_VERIFIED")


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc)) from None

"""Synthetic policy tests, NOT live lock acquisition or revision-API qualification."""

import copy
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parent.parent / "conda" / "validate_conda_publication_lock.py"
if not _PATH.is_file():
    pytest.skip("Conda release sources are not shipped in wheels.", allow_module_level=True)
_SPEC = importlib.util.spec_from_file_location("conda_publication_lock", _PATH)
guard = importlib.util.module_from_spec(_SPEC)
sys.path.insert(0, str(_PATH.parent))
try:
    _SPEC.loader.exec_module(guard)
finally:
    sys.path.pop(0)

RUN = 174300  # Synthetic publication run, not a claim that a real run was queued.
PLAN = "ea0e4c86-98f9-4a6c-8338-beb1f44e861c"
STAGE_ID = "91e40e28-d0dd-5294-89cc-d0fc47500c9a"
PHASE_ID = "2eb4672b-d4ee-5173-5915-e9448b452ff6"
JOB_ID = "434a55e9-bbcf-549f-282d-77286bd65e3c"
CHECKPOINT = "08890543-3c78-57a1-f278-a9d77e3a93d7"
LOCK_ID, APPROVAL_ID = 901, 902  # Synthetic; actual group117 has no installed checks.
RESOURCE = {"type": "variablegroup", "id": "117", "name": "Anaconda Publishing"}


@pytest.fixture
def evidence():
    configurations, check_runs = [], []
    for check_id, check_type in ((LOCK_ID, guard.LOCK_TYPE), (APPROVAL_ID, guard.APPROVAL_TYPE)):
        configuration = {
            "id": check_id,
            "version": 2,
            "type": {"id": check_type},
            "resource": copy.deepcopy(RESOURCE),
            "isDisabled": False,
            "settings": {},
        }
        configurations.append(configuration)
        check_runs.append(
            {
                "status": "approved",
                "checkConfigurationRef": {
                    k: copy.deepcopy(configuration[k])
                    for k in ("id", "version", "type", "resource")
                },
            }
        )
    return {
        "group": {
            "id": 117,
            "name": "Anaconda Publishing",
            "variables": {"ANACONDA_API_TOKEN": {"isSecret": True, "value": None}},
        },
        "configurations": {"count": 2, "value": configurations},
        "permissions": {
            "resource": copy.deepcopy(RESOURCE),
            "allPipelines": None,
            "pipelines": [{"id": 2322, "authorized": True}],
        },
        "build": {
            "id": RUN,
            "definition": {"id": 2322},
            "project": {"id": guard.PROJECT_ID},
            "repository": {"id": guard.REPOSITORY_ID},
            "sourceBranch": "refs/heads/main",
            "sourceVersion": "a" * 40,
            "status": "inProgress",
            "result": None,
            "orchestrationPlan": {"planId": PLAN},
        },
        "timeline": {
            "records": [
                {
                    "id": STAGE_ID,
                    "type": "Stage",
                    "identifier": "CondaRelease",
                    "name": "Validate & Publish Conda Release",
                    "attempt": 1,
                    "state": "inProgress",
                    "result": None,
                },
                {
                    "id": CHECKPOINT,
                    "parentId": STAGE_ID,
                    "type": "Checkpoint",
                    "attempt": 1,
                    "state": "completed",
                    "result": "succeeded",
                },
                {
                    "id": PHASE_ID,
                    "parentId": STAGE_ID,
                    "type": "Phase",
                    "attempt": 1,
                    "state": "inProgress",
                    "result": None,
                },
                {
                    "id": JOB_ID,
                    "parentId": PHASE_ID,
                    "type": "Job",
                    "attempt": 1,
                    "state": "inProgress",
                    "result": None,
                },
            ]
        },
        "suite": {
            "id": CHECKPOINT,
            "status": "approved",
            "context": {
                "Id": CHECKPOINT,
                "PlanId": PLAN,
                "HubName": "Build",
                "Project": {"Id": guard.PROJECT_ID},
                "Pipeline": {"Id": "2322", "Owner": {"Id": RUN}},
                "Branch": "refs/heads/main",
                "GraphNode": {"Id": STAGE_ID, "Name": "CondaRelease", "Attempt": 1},
            },
            "checkRuns": check_runs,
        },
    }


def verify(evidence, **overrides):
    arguments = {
        "run_id": RUN,
        "source_commit": "a" * 40,
        "stage_attempt": 1,
        "stage_id": STAGE_ID,
        "lock_check_id": LOCK_ID,
        "approval_check_id": APPROVAL_ID,
    }
    arguments.update(overrides)
    return guard.validate_publication_lock(evidence, **arguments)


def test_expected_positive_api_contract_only_not_live_lock_qualification(evidence):
    original = copy.deepcopy(evidence)
    result = verify(evidence)
    assert result == {
        "buildId": RUN,
        "resourceId": 117,
        "stage": "CondaRelease",
        "stageAttempt": 1,
        "lockCheckId": LOCK_ID,
        "approvalCheckId": APPROVAL_ID,
    }
    assert evidence == original


def test_explicitly_revoked_other_pipeline_permissions_do_not_grant_publication(evidence):
    evidence["permissions"]["pipelines"].append({"id": 2318, "authorized": False})
    assert verify(evidence)["buildId"] == RUN


@pytest.mark.parametrize(
    "path,value",
    [
        (("group", "id"), 118),
        (("group", "name"), "Other Publishing"),
        (("group", "variables"), {}),
        (("group", "variables", "ANACONDA_API_TOKEN", "isSecret"), False),
        (("permissions", "resource", "id"), "118"),
        (("permissions", "allPipelines"), {"authorized": True}),
        (("permissions", "allPipelines"), {}),
        (("permissions", "pipelines"), []),
        (("permissions", "pipelines"), [{"id": 2322, "authorized": False}]),
        (("permissions", "pipelines"), [{"id": 2318, "authorized": True}]),
        (
            ("permissions", "pipelines"),
            [{"id": 2322, "authorized": True}, {"id": 2318, "authorized": True}],
        ),
        (("build", "id"), RUN - 1),
        (("build", "definition", "id"), 2318),
        (("build", "project", "id"), "904996cc-6198-4d39-8540-eca72bdf0b7b"),
        (("build", "repository", "id"), "other"),
        (("build", "sourceBranch"), "refs/heads/feature"),
        (("build", "sourceVersion"), "b" * 40),
        (("build", "status"), "completed"),
        (("build", "result"), "succeeded"),
        (("build", "orchestrationPlan", "planId"), None),
        (("suite", "id"), STAGE_ID),
        (("suite", "status"), "running"),
        (("suite", "context", "Id"), STAGE_ID),
        (("suite", "context", "PlanId"), STAGE_ID),
        (("suite", "context", "Project", "Id"), "other"),
        (("suite", "context", "Pipeline", "Id"), "2318"),
        (("suite", "context", "Pipeline", "Owner", "Id"), RUN - 1),
        (("suite", "context", "Branch"), "refs/heads/feature"),
        (("suite", "context", "HubName"), "Release"),
        (("suite", "context", "GraphNode", "Id"), CHECKPOINT),
        (("suite", "context", "GraphNode", "Name"), "sdl_sources"),
        (("suite", "context", "GraphNode", "Attempt"), 2),
        (("suite", "context", "GraphNode", "Attempt"), True),
        (("suite", "checkRuns"), []),
        (("suite", "checkRuns"), None),
        (("configurations", "value"), []),
        (("configurations", "count"), 3),
        (("timeline", "records"), []),
    ],
)
def test_wrong_missing_or_unknown_evidence_blocks(evidence, path, value):
    target = evidence
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 999),
        ("isDisabled", True),
        ("isDisabled", None),
        ("isDisabled", "false"),
        ("resource", {"type": "environment", "id": "117"}),
        ("resource", {"type": "variablegroup", "id": "118"}),
        ("type", {"id": "4020e66e-b0f3-47e1-bc88-48f3cc59b5f3"}),
        ("issue", {"message": "configuration error"}),
        ("version", 3),
    ],
)
def test_expected_enabled_native_configurations_are_mandatory(evidence, index, field, value):
    evidence["configurations"]["value"][index][field] = value
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize(
    "status",
    [
        None,
        "",
        "queued",
        "running",
        "completed",
        "rejected",
        "canceled",
        "timedOut",
        "failed",
        "held",
    ],
)
def test_unknown_or_unapproved_check_state_never_proves_lock(evidence, index, status):
    evidence["suite"]["checkRuns"][index]["status"] = status
    with pytest.raises(ValueError, match="lock state is unverified"):
        verify(evidence)


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 999),
        ("type", {"id": "not-the-native-check"}),
        ("resource", {"type": "variablegroup", "id": "118"}),
        ("version", 1),
    ],
)
def test_evaluated_configuration_must_match_current_resource_and_revision(
    evidence, index, field, value
):
    evidence["suite"]["checkRuns"][index]["checkConfigurationRef"][field] = value
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize(
    "current,evaluated",
    [
        (None, 2),
        (2, None),
        (None, None),
        (2, 1),
        (0, 0),
        (-1, -1),
        (True, True),
        (2.0, 2.0),
        ("invalid", "invalid"),
    ],
)
def test_revision_evidence_is_mandatory_not_a_live_api_contract(
    evidence, index, current, evaluated
):
    for record, revision in (
        (evidence["configurations"]["value"][index], current),
        (evidence["suite"]["checkRuns"][index]["checkConfigurationRef"], evaluated),
    ):
        if revision is None:
            record.pop("version")
        else:
            record["version"] = revision
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize("which", ["configuration", "check", "stage", "checkpoint"])
def test_duplicate_evidence_is_not_accepted(evidence, which):
    if which == "configuration":
        records = evidence["configurations"]["value"]
        records.append(copy.deepcopy(records[0]))
        evidence["configurations"]["count"] += 1
    elif which == "check":
        records = evidence["suite"]["checkRuns"]
        records.append(copy.deepcopy(records[0]))
    else:
        records = evidence["timeline"]["records"]
        records.append(copy.deepcopy(records[which == "checkpoint"]))
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("field,value", [("attempt", 2), ("attempt", True), ("state", "pending")])
def test_current_stage_and_completed_checkpoint_required(evidence, index, field, value):
    evidence["timeline"]["records"][index][field] = value
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize(
    "overrides",
    [
        {"owner": "other"},
        {"package": "mssql-python-odbc"},
        {"target_label": "dev"},
        {"lock_check_id": APPROVAL_ID},
        {"lock_check_id": None},
        {"approval_check_id": True},
        {"stage_attempt": 0},
        {"stage_id": CHECKPOINT},
        {"stage_id": ""},
        {"source_commit": ""},
    ],
)
def test_invalid_requested_contract_fails(evidence, overrides):
    with pytest.raises(ValueError):
        verify(evidence, **overrides)


def test_real_observed_approved_empty_suite_does_not_prove_acquisition(evidence):
    evidence["suite"]["checkRuns"] = []
    evidence["suite"]["context"]["GraphNode"]["PersistedStageLockBehavior"] = 3
    with pytest.raises(ValueError, match="not evaluated"):
        verify(evidence)


def test_secret_value_is_neither_required_nor_returned(evidence):
    evidence["group"]["variables"]["ANACONDA_API_TOKEN"]["value"] = object()
    result = verify(evidence)
    assert "token" not in json.dumps(result).lower()


@pytest.fixture
def pipeline_env(monkeypatch):
    values = {
        "SYSTEM_TEAMPROJECTID": guard.PROJECT_ID,
        "SYSTEM_STAGENAME": "CondaRelease",
        "SYSTEM_JOBID": JOB_ID,
        "SYSTEM_JOBATTEMPT": "1",
        "SYSTEM_PHASEATTEMPT": "1",
        "SYSTEM_PLANID": PLAN,
        "SYSTEM_STAGEATTEMPT": "1",
        "BUILD_BUILDID": str(RUN),
        "BUILD_SOURCEBRANCH": "refs/heads/main",
        "BUILD_SOURCEVERSION": "a" * 40,
        "CONDA_PUBLICATION_LOCK_CHECK_ID": str(LOCK_ID),
        "CONDA_PUBLICATION_APPROVAL_CHECK_ID": str(APPROVAL_ID),
        "SYSTEM_COLLECTIONURI": "https://dev.azure.com/SqlClientDrivers/",
        "SYSTEM_ACCESSTOKEN": "synthetic-secret-not-for-output",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


@pytest.fixture
def recorded_api(monkeypatch, evidence):
    calls = []
    paths = [
        ("distributedtask/variablegroups/117?api-version=7.1", "group"),
        (
            "pipelines/checks/configurations?resourceType=variablegroup"
            "&resourceId=117&$expand=settings&api-version=7.1-preview.1",
            "configurations",
        ),
        (
            "pipelines/pipelinepermissions/variablegroup/117?api-version=7.1-preview.1",
            "permissions",
        ),
        (f"build/builds/{RUN}?api-version=7.1", "build"),
        (f"build/builds/{RUN}/timeline?api-version=7.1", "timeline"),
        (
            f"pipelines/checks/runs/{CHECKPOINT}?$expand=resources&api-version=7.1-preview.1",
            "suite",
        ),
    ]
    responses = dict(paths)

    def get_json(path):
        calls.append(path)
        return evidence[responses[path]]

    monkeypatch.setattr(guard, "ado_get_json", get_json)
    return calls, [path for path, _ in paths]


def test_callback_fetches_only_expected_current_run_resources(
    monkeypatch, pipeline_env, recorded_api
):
    monkeypatch.delenv("SYSTEM_STAGEID", raising=False)
    calls, expected_paths = recorded_api
    assert guard.require_publication_lock("microsoft", "mssql-python", "main")["buildId"] == RUN
    assert calls == expected_paths


@pytest.mark.parametrize(
    "index,field,value",
    [
        (3, "id", CHECKPOINT),
        (3, "parentId", STAGE_ID),
        (3, "type", "Task"),
        (3, "attempt", 2),
        (3, "state", "completed"),
        (3, "result", "succeeded"),
        (2, "id", CHECKPOINT),
        (2, "parentId", CHECKPOINT),
        (2, "type", "Job"),
        (2, "attempt", 2),
        (2, "state", "completed"),
        (2, "result", "succeeded"),
    ],
)
def test_current_job_ancestry_and_attempts_are_required(
    pipeline_env, recorded_api, evidence, index, field, value
):
    evidence["timeline"]["records"][index][field] = value
    with pytest.raises(ValueError):
        guard.require_publication_lock()
    assert len(recorded_api[0]) == 5


@pytest.mark.parametrize("index", [2, 3])
def test_ambiguous_current_job_or_phase_rejects(pipeline_env, recorded_api, evidence, index):
    evidence["timeline"]["records"].append(copy.deepcopy(evidence["timeline"]["records"][index]))
    with pytest.raises(ValueError):
        guard.require_publication_lock()
    assert len(recorded_api[0]) == 5


def test_current_plan_must_match_authenticated_build(monkeypatch, pipeline_env, recorded_api):
    monkeypatch.setenv("SYSTEM_PLANID", STAGE_ID)
    with pytest.raises(ValueError):
        guard.require_publication_lock()
    assert len(recorded_api[0]) == 5


@pytest.mark.parametrize(
    "name",
    [
        "SYSTEM_TEAMPROJECTID",
        "SYSTEM_STAGENAME",
        "SYSTEM_JOBID",
        "SYSTEM_JOBATTEMPT",
        "SYSTEM_PHASEATTEMPT",
        "SYSTEM_PLANID",
        "SYSTEM_STAGEATTEMPT",
        "BUILD_BUILDID",
        "BUILD_SOURCEBRANCH",
        "CONDA_PUBLICATION_LOCK_CHECK_ID",
        "CONDA_PUBLICATION_APPROVAL_CHECK_ID",
    ],
)
def test_missing_caller_metadata_blocks_before_network(monkeypatch, pipeline_env, name):
    monkeypatch.delenv(name)
    monkeypatch.setattr(guard, "ado_get_json", lambda _: pytest.fail("Unexpected network call"))
    with pytest.raises(ValueError):
        guard.require_publication_lock()


def test_shared_exported_provenance_client_is_reused():
    from validate_conda_provenance import ado_get_json

    assert guard.ado_get_json is ado_get_json


def test_callback_sanitizes_shared_client_failure(monkeypatch, pipeline_env):
    def unavailable(_path):
        raise OSError("synthetic-secret-not-for-output")

    monkeypatch.setattr(guard, "ado_get_json", unavailable)
    with pytest.raises(ValueError, match="evidence unavailable") as exc:
        guard.require_publication_lock()
    assert "synthetic-secret" not in str(exc.value)
    assert exc.value.__suppress_context__


def test_no_argument_cli_actually_invokes_fixed_scope_guard(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(guard, "require_publication_lock", lambda: calls.append("checked"))
    guard.main([])
    assert calls == ["checked"]
    assert capsys.readouterr().out.strip() == "CONDA_PUBLICATION_CHECK_CONTRACT_VERIFIED"


def test_cli_cannot_override_scope_or_request_bypass(monkeypatch):
    monkeypatch.setattr(
        guard, "require_publication_lock", lambda: pytest.fail("Guard called with unexpected args")
    )
    with pytest.raises(ValueError, match="no arguments"):
        guard.main(["--dry-run"])


def test_actual_script_entrypoint_fails_closed_without_pipeline_context():
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    env.pop("SYSTEM_TEAMPROJECTID", None)  # Reject before any GET; no token is used.
    result = subprocess.run(
        [sys.executable, str(_PATH)], env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode != 0
    assert "Publication lock rejected: unexpected project." in result.stderr
    assert not result.stdout

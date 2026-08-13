import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / ".github" / "scripts" / "prepare_fork_coverage_comment.py"
SPEC = importlib.util.spec_from_file_location("prepare_fork_coverage_comment", SCRIPT_PATH)
coverage_comment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(coverage_comment)
REPOSITORY_ROOT = Path(__file__).parents[1]


@pytest.fixture
def valid_artifact(tmp_path):
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    data = {
        "coverage_percentage": "81.5%",
        "covered_lines": "815",
        "total_lines": "1000",
        "patch_coverage_pct": "92%",
        "low_coverage_files": "mssql_python/example.py: 55.0%",
        "ado_url": (
            "https://dev.azure.com/SqlClientDrivers/"
            "904996cc-6198-4d39-8540-eca72bdf0b7b/_build/results?buildId=46466"
        ),
    }
    (artifact / "pr-info.json").write_text(json.dumps(data), encoding="utf-8")
    return artifact


def _event(pull_requests=None):
    return {
        "repository": {
            "full_name": "microsoft/mssql-python",
            "default_branch": "main",
        },
        "workflow_run": {
            "head_sha": "a" * 40,
            "head_repository": {"full_name": "contributor/mssql-python"},
            "pull_requests": pull_requests or [],
        },
    }


def test_prepares_safe_comment_for_forked_pr(valid_artifact, tmp_path):
    event_path = tmp_path / "event.json"
    pulls_path = tmp_path / "pulls.json"
    event_path.write_text(json.dumps(_event()), encoding="utf-8")
    pulls_path.write_text(
        json.dumps(
            [
                {
                    "number": 123,
                    "head": {"sha": "a" * 40},
                    "base": {
                        "ref": "main",
                        "repo": {"full_name": "microsoft/mssql-python"},
                    },
                }
            ]
        ),
        encoding="utf-8",
    )

    pr_number, body = coverage_comment.prepare_comment(valid_artifact, event_path, pulls_path)

    assert pr_number == 123
    assert coverage_comment.COMMENT_MARKER in body
    assert "buildId=46466" in body
    assert coverage_comment.ADO_PROJECT_ID in body


def test_ignores_event_supplied_pull_requests():
    associated_pulls = [
        {
            "number": 456,
            "head": {"sha": "a" * 40},
            "base": {
                "ref": "main",
                "repo": {"full_name": "microsoft/mssql-python"},
            },
        }
    ]

    # An attacker-controlled workflow_run.pull_requests entry must never
    # short-circuit resolution; only the trusted head SHA match is honored.
    resolved = coverage_comment.resolve_pr_number(_event([{"number": 999}]), associated_pulls)

    assert resolved == 456


def test_rejects_artifact_supplied_pr_number(valid_artifact):
    path = valid_artifact / "pr-info.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["pr_number"] = "999"
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(coverage_comment.ValidationError, match="expected schema"):
        coverage_comment.validate_artifact(valid_artifact)


def test_rejects_unexpected_artifact_payload(valid_artifact):
    (valid_artifact / "payload.so").write_bytes(b"not executable")

    with pytest.raises(coverage_comment.ValidationError, match="only pr-info.json"):
        coverage_comment.validate_artifact(valid_artifact)


def test_rejects_unexpected_artifact_directory(valid_artifact):
    (valid_artifact / "nested" / "empty").mkdir(parents=True)

    with pytest.raises(coverage_comment.ValidationError, match="only pr-info.json"):
        coverage_comment.validate_artifact(valid_artifact)


def test_escapes_multiline_file_data_in_comment(valid_artifact):
    path = valid_artifact / "pr-info.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["low_coverage_files"] = "safe.py: 50%\n</pre><a href='https://evil.example'>"
    path.write_text(json.dumps(data), encoding="utf-8")

    validated = coverage_comment.validate_artifact(valid_artifact)
    body = coverage_comment.build_comment(validated)

    assert "</pre><a " not in body
    assert "&lt;/pre&gt;&lt;a " in body


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/_build/results?buildId=46466",
        "https://dev.azure.com/OtherOrg/public/_build/results?buildId=46466",
        "https://dev.azure.com/SqlClientDrivers/public/_build/results?buildId=x",
        "https://dev.azure.com/SqlClientDrivers/public/_build/results?buildId=1&next=evil",
        (
            "https://dev.azure.com/SqlClientDrivers/public/_build/results"
            "?buildId=1&view=x)%20[Sign%20in](https://evil.example"
        ),
    ],
)
def test_rejects_untrusted_ado_urls(valid_artifact, url):
    path = valid_artifact / "pr-info.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["ado_url"] = url
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(coverage_comment.ValidationError, match="ado_url"):
        coverage_comment.validate_artifact(valid_artifact)


def test_resolves_pr_by_workflow_head_sha_when_event_list_is_empty():
    associated_pulls = [
        {
            "number": 456,
            "head": {"sha": "a" * 40},
            "base": {
                "ref": "main",
                "repo": {"full_name": "microsoft/mssql-python"},
            },
        }
    ]

    assert coverage_comment.resolve_pr_number(_event(), associated_pulls) == 456


def test_rejects_ambiguous_pr_resolution():
    pull = {
        "number": 456,
        "head": {"sha": "a" * 40},
        "base": {
            "ref": "main",
            "repo": {"full_name": "microsoft/mssql-python"},
        },
    }

    with pytest.raises(coverage_comment.ValidationError, match="exactly one"):
        coverage_comment.resolve_pr_number(_event(), [pull, {**pull, "number": 789}])


def test_workflows_do_not_use_injectable_environment_records():
    privileged_workflow = (
        REPOSITORY_ROOT / ".github" / "workflows" / "forked-pr-coverage.yml"
    ).read_text(encoding="utf-8")
    producer_workflow = (
        REPOSITORY_ROOT / ".github" / "workflows" / "pr-code-coverage.yml"
    ).read_text(encoding="utf-8")

    assert "GITHUB_ENV" not in privileged_workflow
    assert "LOW_COVERAGE_FILES<<EOF" not in producer_workflow
    assert "PATCH_COVERAGE_SUMMARY<<EOF" not in producer_workflow

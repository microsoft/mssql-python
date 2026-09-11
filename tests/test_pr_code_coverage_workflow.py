"""Run the workflow's polling shell with local curl fixtures and an accelerated clock."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap

import pytest

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/pr-code-coverage.yml"
pytestmark = pytest.mark.skipif(
    not WORKFLOW.is_file()
    or sys.platform == "win32"
    or not shutil.which("bash")
    or not shutil.which("jq"),
    reason="requires a source checkout, Bash and jq (as on the coverage runner)",
)
SHA = "a" * 40
ARTIFACT_URL = "https://dev.azure.com/SqlClientDrivers/public/coverage.zip"
EMPTY = {"value": []}
ARTIFACT = {
    "value": [{"name": "Code Coverage Report_1", "resource": {"downloadUrl": ARTIFACT_URL}}]
}


def _build(build_id=174262, sha=SHA, pr="779", branch="refs/pull/779/merge", definition=2128):
    return {
        "id": build_id,
        "definition": {"id": definition},
        "sourceBranch": branch,
        "triggerInfo": {"pr.number": pr, "pr.sourceSha": sha},
        "status": "inProgress",
        "_links": {
            "web": {
                "href": (
                    "https://dev.azure.com/SqlClientDrivers/public/_build/results"
                    f"?buildId={build_id}"
                )
            }
        },
    }


def _script(step):
    section = WORKFLOW.read_text(encoding="utf-8").split(f"      - name: {step}\n", 1)[1]
    section = section.split("\n      - name:", 1)[0]
    return textwrap.dedent(section.split("        run: |\n", 1)[1])


def _run(tmp_path, script, fixtures):
    for kind, responses in fixtures.items():
        (tmp_path / f"{kind}.count").write_text(str(len(responses)), encoding="utf-8")
        (tmp_path / f"{kind}.next").write_text("0", encoding="utf-8")
        for index, response in enumerate(responses):
            code, body = response if isinstance(response, tuple) else (0, response)
            body = body if isinstance(body, str) else json.dumps(body)
            (tmp_path / f"{kind}.{index}.body").write_text(body, encoding="utf-8")
            (tmp_path / f"{kind}.{index}.code").write_text(str(code), encoding="utf-8")

    prefix = r"""
SECONDS=0
trap 'printf "%s\n" "$SECONDS" > "$FIXTURE_DIR/elapsed"' EXIT
sleep() {
  printf "%s\n" "$1" >> "$FIXTURE_DIR/sleeps"
  SECONDS=$((SECONDS + $1))
}
curl() {
  local url="${@: -1}" kind index count code
  printf "%s\n" "$*" >> "$FIXTURE_DIR/requests"
  case "$url" in
    *"/artifacts?"*) kind=artifacts ;;
    *"/builds?"*) kind=builds ;;
    *"/builds/174262?"*) kind=build ;;
    *) echo "Unexpected URL: $url" >&2; return 99 ;;
  esac
  if [[ ! -f "$FIXTURE_DIR/$kind.count" ]]; then
    echo "Unexpected request: $kind" >&2
    return 99
  fi
  index=$(< "$FIXTURE_DIR/$kind.next")
  count=$(< "$FIXTURE_DIR/$kind.count")
  printf "%s\n" "$((index + 1))" > "$FIXTURE_DIR/$kind.next"
  if (( index >= count )); then index=$((count - 1)); fi
  code=$(< "$FIXTURE_DIR/$kind.$index.code")
  cat "$FIXTURE_DIR/$kind.$index.body"
  return "$code"
}
"""
    env = {
        **os.environ,
        "FIXTURE_DIR": str(tmp_path),
        "GITHUB_ENV": str(tmp_path / "github-env"),
        "PR_NUMBER": "779",
        "PR_HEAD_SHA": SHA,
        "BUILD_ID": "174262",
    }
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", prefix + script],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=45,
    )
    requests = (tmp_path / "requests").read_text(encoding="utf-8").splitlines()
    for request in requests:
        assert "--fail" in request
        assert "--connect-timeout 10" in request
        assert "--max-time " in request
        timeout = int(request.split("--max-time ", 1)[1].split()[0])
        assert 0 < timeout <= 30
    assert "Unexpected request" not in result.stderr
    assert "Unexpected URL" not in result.stderr
    return result


def _poll(tmp_path, artifacts, builds):
    script = _script("Download and parse coverage report")
    # Only execute discovery; downloaded report contents are never executed by these tests.
    script = script.split('\nif [[ -n "$COVERAGE_ARTIFACT" &&', 1)[0]
    script += '\nprintf "COVERAGE_ARTIFACT=%s\\n" "$COVERAGE_ARTIFACT"\n'
    return _run(tmp_path, script, {"artifacts": artifacts, "build": builds})


def test_selects_exact_head_pr_branch_and_definition_even_when_build_failed(tmp_path):
    matching = {**_build(), "status": "completed", "result": "failed"}
    builds = [
        _build(174267, sha="b" * 40),
        _build(174266, pr="780"),
        _build(174265, branch="refs/heads/main"),
        _build(174264, definition=9999),
        {**_build(174263, sha=None), "sourceVersion": SHA},
        matching,
        _build(174261),
    ]
    result = _run(tmp_path, _script("Wait for ADO build to start"), {"builds": [{"value": builds}]})
    assert result.returncode == 0, result.stdout + result.stderr
    exported = (tmp_path / "github-env").read_text(encoding="utf-8")
    assert "BUILD_ID=174262\n" in exported
    assert f"ADO_URL={matching['_links']['web']['href']}\n" in exported
    request = (tmp_path / "requests").read_text(encoding="utf-8")
    assert "definitions=2128&branchName=refs%2Fpull%2F779%2Fmerge" in request
    assert "queryOrder=queueTimeDescending" in request


def test_ignores_old_head_until_exact_build_appears_and_retries_bad_responses(tmp_path):
    result = _run(
        tmp_path,
        _script("Wait for ADO build to start"),
        {
            "builds": [
                {"value": [_build(174261, sha="b" * 40)]},
                (22, "HTTP 503"),
                "<html>gateway error</html>",
                {"value": {}},
                {"value": [_build()]},
            ]
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "BUILD_ID=174262\n" in (tmp_path / "github-env").read_text(encoding="utf-8")


def test_accepts_late_artifact_after_failed_aggregate_completes(tmp_path):
    result = _poll(
        tmp_path,
        [EMPTY] * 92 + [ARTIFACT],
        [_build()] * 91 + [{**_build(), "status": "completed", "result": "failed"}],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"COVERAGE_ARTIFACT={ARTIFACT_URL}" in result.stdout
    assert "Build completed (failed)" in result.stdout
    assert int((tmp_path / "elapsed").read_text()) > 45 * 60


@pytest.mark.parametrize("result", ["succeeded", "failed", "canceled"])
def test_completed_without_artifact_stops_after_short_grace(tmp_path, result):
    completed = {**_build(), "status": "completed", "result": result}
    run = _poll(tmp_path, [EMPTY], [completed])
    assert run.returncode != 0
    assert "after propagation grace" in run.stdout
    assert 120 <= int((tmp_path / "elapsed").read_text()) < 180


def test_immediately_available_artifact_needs_no_lifecycle_request(tmp_path):
    result = _poll(tmp_path, [ARTIFACT], [])
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"COVERAGE_ARTIFACT={ARTIFACT_URL}" in result.stdout
    assert (tmp_path / "build.next").read_text() == "0"


def test_artifact_and_lifecycle_http_json_errors_are_retried(tmp_path):
    result = _poll(
        tmp_path,
        [(22, "HTTP 502"), "{invalid", {"value": None}, EMPTY, ARTIFACT],
        [(28, ""), "not json", {"status": "completed"}, _build()],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"COVERAGE_ARTIFACT={ARTIFACT_URL}" in result.stdout


def test_xml_artifact_refresh_retries_http_and_json_errors(tmp_path):
    script = _script("Download coverage XML from ADO")
    script = script.replace("BUILD_ID=${{ env.BUILD_ID }}\n", "")
    script = script.split('\necho "🔍 Available artifacts:"', 1)[0]
    result = _run(
        tmp_path,
        script,
        {"artifacts": [(22, "HTTP 503"), "invalid JSON", ARTIFACT]},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "artifacts.next").read_text().strip() == "3"


@pytest.mark.parametrize("failing_api", ["builds", "artifacts", "build"])
def test_persistent_api_errors_have_finite_retries(tmp_path, failing_api):
    if failing_api == "builds":
        result = _run(tmp_path, _script("Wait for ADO build to start"), {"builds": ["not JSON"]})
    else:
        result = _poll(
            tmp_path,
            ["not JSON"] if failing_api == "artifacts" else [EMPTY],
            ["not JSON"] if failing_api == "build" else [_build()],
        )
    assert result.returncode != 0
    assert "5 consecutive failures" in result.stdout
    assert int((tmp_path / f"{failing_api}.next").read_text()) == 5
    assert int((tmp_path / "elapsed").read_text()) < 180


@pytest.mark.parametrize("step,budget", [("build", 15 * 60), ("artifact", 120 * 60)])
def test_missing_build_or_queued_coverage_obeys_wall_clock_budget(tmp_path, step, budget):
    if step == "build":
        result = _run(
            tmp_path,
            _script("Wait for ADO build to start"),
            {"builds": [{"value": [_build(174261, sha="b" * 40)]}]},
        )
    else:
        result = _poll(tmp_path, [EMPTY], [{**_build(), "status": "notStarted"}])
    assert result.returncode != 0
    assert "Timeout:" in result.stdout
    assert budget <= int((tmp_path / "elapsed").read_text()) < budget + 30


def test_job_budget_leaves_time_for_downloads_and_publishing():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "    timeout-minutes: 145\n" in workflow
    assert "PR_HEAD_SHA: ${{ github.event.pull_request.head.sha }}" in workflow

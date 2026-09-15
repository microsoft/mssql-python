"""Source-only tests of the authoritative Azure DevOps release provenance chain."""

import importlib.util
import io
import json
import types
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode

import pytest

_PATH = Path(__file__).resolve().parent.parent / "conda" / "validate_conda_provenance.py"
if not _PATH.is_file():
    pytest.skip("Conda release sources are not shipped in wheels.", allow_module_level=True)
_SPEC = importlib.util.spec_from_file_location("conda_provenance", _PATH)
provenance = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(provenance)


def _records(pipeline, run, commit, branch="refs/heads/main"):
    repository = {"id": "eec96f30-ec96-4910-abd6-c45a99a5c29f", "type": "azureReposGit"}
    return (
        {
            "id": run,
            "definition": {"id": pipeline},
            "buildNumber": "26250.2" if pipeline == 2199 else "26253.2-CondaBuild",
            "status": "completed",
            "result": "succeeded",
            "sourceBranch": branch,
            "sourceVersion": commit,
            "repository": repository,
            "project": {"id": "c6d89619-62de-46a0-8b46-70b92a84d85e"},
        },
        {
            "id": run,
            "pipeline": {"id": pipeline},
            "name": "26250.2" if pipeline == 2199 else "26253.2-CondaBuild",
            "state": "completed",
            "result": "succeeded",
            "resources": {
                "repositories": {
                    "self": {"repository": repository, "refName": branch, "version": commit}
                }
            },
        },
    )


@pytest.fixture
def release_sources():
    return {
        "setup.py": "setup(\n    version='1.15.0',\n)\n",
        "mssql_python/__init__.py": '__version__ = "1.15.0"\n',
        "mssql_python_odbc/__init__.py": '__version__ = "18.6.2.1"\n',
    }


@pytest.fixture
def chain(release_sources):
    producer, producer_run = _records(2318, 174195, "a" * 40)
    wheel, wheel_run = _records(2199, 173176, "b" * 40)
    producer_run["resources"]["pipelines"] = {
        "buildPipeline": {"pipeline": {"id": 173176}, "version": "26250.2"}
    }
    records = {
        "build/builds/174195?api-version=7.1": producer,
        "pipelines/2318/runs/174195?api-version=7.1": producer_run,
        "build/builds/173176?api-version=7.1": wheel,
        "pipelines/2199/runs/173176?api-version=7.1": wheel_run,
    }
    for path, content in release_sources.items():
        query = urlencode(
            {
                "path": "/" + path,
                "includeContent": "true",
                "versionDescriptor.versionType": "commit",
                "versionDescriptor.version": "b" * 40,
                "$format": "json",
                "api-version": "7.1",
            }
        )
        records[f"git/repositories/{provenance._REPOSITORY_ID}/items?{query}"] = {
            "path": "/" + path,
            "commitId": "b" * 40,
            "gitObjectType": "blob",
            "content": content,
        }
    return records


def _verify(chain, **kwargs):
    return provenance.verify_provenance(
        chain.__getitem__,
        producer_pipeline_id=2318,
        producer_run_id=174195,
        producer_branch=kwargs.pop("producer_branch", "refs/heads/main"),
        producer_commit=kwargs.pop("producer_commit", "a" * 40),
        release_branch=kwargs.pop("release_branch", "refs/heads/main"),
        publish=kwargs.pop("publish", True),
        **kwargs,
    )


def test_exact_recorded_wheel_run_is_verified_not_packaging_mode_or_build_number(chain):
    result = _verify(chain)
    assert result["productionEligible"] is True
    assert result["wheel"] == {
        "pipeline": 2199,
        "run": 173176,
        "branch": "refs/heads/main",
        "commit": "b" * 40,
    }
    assert result["producer"]["commit"] == "a" * 40  # Recipe and wheel commits can differ.
    assert result["versions"] == {"mssql-python": "1.15.0", "mssql-python-odbc": "18.6.2.1"}


@pytest.mark.parametrize(
    "content",
    [
        "",
        "version=get_version()",
        "version='1.15.0'\nversion='1.15.0'",
        "version='1.14.0'",
        "version='$(unresolved)'",
        "version='1.15.0' + '.dev1'",
    ],
)
def test_missing_ambiguous_or_different_release_literal_fails(release_sources, content):
    release_sources["setup.py"] = content
    with pytest.raises(ValueError, match="release version|release versions"):
        provenance.read_release_versions(release_sources.__getitem__)


def test_release_source_is_read_not_executed(release_sources):
    release_sources["setup.py"] += "raise AssertionError('must not execute setup.py')\n"
    assert provenance.read_release_versions(release_sources.__getitem__)["mssql-python"] == "1.15.0"


@pytest.mark.parametrize(
    "field,value",
    [
        ("path", "/other.py"),
        ("commitId", "a" * 40),
        ("commitId", None),
        ("gitObjectType", "tree"),
        ("content", None),
    ],
)
def test_version_source_response_must_match_exact_wheel_commit(chain, field, value):
    item = next(value for key, value in chain.items() if key.startswith("git/repositories/"))
    item[field] = value
    with pytest.raises(ValueError, match="wheel producer source"):
        _verify(chain, publish=False)


def test_main_conda_producer_cannot_publish_feature_branch_wheels(chain):
    chain["build/builds/173176?api-version=7.1"]["sourceBranch"] = "refs/heads/feature"
    wheel_run = chain["pipelines/2199/runs/173176?api-version=7.1"]
    wheel_run["resources"]["repositories"]["self"]["refName"] = "refs/heads/feature"
    with pytest.raises(ValueError, match="wheel producer source"):
        _verify(chain)
    result = _verify(chain, publish=False)
    assert result["productionEligible"] is False
    assert len(result["productionIneligibilityReasons"]) == 1


def test_validate_only_can_inspect_feature_conda_artifacts_without_production_eligibility(chain):
    chain["build/builds/174195?api-version=7.1"]["sourceBranch"] = "refs/heads/feature"
    producer_run = chain["pipelines/2318/runs/174195?api-version=7.1"]
    producer_run["resources"]["repositories"]["self"]["refName"] = "refs/heads/feature"
    result = _verify(
        chain,
        producer_branch="refs/heads/feature",
        release_branch="refs/heads/feature",
        publish=False,
    )
    assert result["productionEligible"] is False
    assert len(result["productionIneligibilityReasons"]) == 2
    with pytest.raises(ValueError, match="Conda producer source"):
        _verify(chain, producer_branch="refs/heads/feature")


@pytest.mark.parametrize("publish", [False, True])
@pytest.mark.parametrize("run", [174195, 173176])
@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "inProgress"),
        ("result", "partiallySucceeded"),
        ("result", "failed"),
        ("result", None),
        ("sourceVersion", ""),
        ("sourceBranch", ""),
        ("id", 999),
        ("definition", {"id": 999}),
        ("buildNumber", "mismatched-number"),
        ("repository", {"id": "wrong-repository"}),
        ("project", {"id": "wrong-project"}),
    ],
)
def test_invalid_or_incomplete_build_never_passes_even_in_dry_run(
    chain, publish, run, field, value
):
    chain[f"build/builds/{run}?api-version=7.1"][field] = value
    with pytest.raises(ValueError):
        _verify(chain, publish=publish)


@pytest.mark.parametrize("pipeline,run", [(2318, 174195), (2199, 173176)])
@pytest.mark.parametrize(
    "field,value",
    [
        ("state", "inProgress"),
        ("result", "failed"),
        ("id", 999),
        ("pipeline", {"id": 999}),
        ("name", "mismatched-number"),
        ("resources", {}),
    ],
)
def test_invalid_authoritative_run_records_fail(chain, pipeline, run, field, value):
    chain[f"pipelines/{pipeline}/runs/{run}?api-version=7.1"][field] = value
    with pytest.raises(ValueError):
        _verify(chain, publish=False)


@pytest.mark.parametrize(
    "resource",
    [
        {},
        {"pipeline": {"id": None}, "version": "26250.2"},
        {"pipeline": {"id": 173176}},
        {"pipeline": {"id": 173176}, "version": "173176"},
        {"pipeline": {"id": 173176}, "version": None},
    ],
)
def test_missing_or_wrong_wheel_resource_fails(chain, resource):
    chain["pipelines/2318/runs/174195?api-version=7.1"]["resources"]["pipelines"] = {
        "buildPipeline": resource
    }
    with pytest.raises(ValueError):
        _verify(chain, publish=False)


@pytest.mark.parametrize("pipeline,run", [(2318, 174195), (2199, 173176)])
@pytest.mark.parametrize("field,value", [("refName", "refs/heads/mismatch"), ("version", "c" * 40)])
def test_recorded_source_branch_and_commit_must_match(chain, pipeline, run, field, value):
    source = chain[f"pipelines/{pipeline}/runs/{run}?api-version=7.1"]["resources"]["repositories"]
    source["self"][field] = value
    with pytest.raises(ValueError, match="source repository, branch or commit mismatch"):
        _verify(chain, publish=False)


@pytest.mark.parametrize(
    "selected", [{"producer_commit": "c" * 40}, {"producer_branch": "refs/heads/mismatch"}]
)
def test_selected_producer_source_cannot_mismatch_api(chain, selected):
    with pytest.raises(ValueError, match="Selected Conda resource"):
        _verify(chain, **selected)


def test_feature_release_yaml_cannot_publish(chain):
    with pytest.raises(ValueError, match="release source"):
        _verify(chain, release_branch="refs/heads/feature")


def test_provenance_api_failure_is_not_treated_as_validate_only_success(chain):
    del chain["build/builds/173176?api-version=7.1"]
    with pytest.raises(KeyError):
        _verify(chain, publish=False)


def test_api_cannot_substitute_another_run_for_recorded_upstream_version(chain):
    resource = chain["pipelines/2318/runs/174195?api-version=7.1"]["resources"]["pipelines"]
    resource["buildPipeline"]["pipeline"]["id"] = 173177
    chain["build/builds/173177?api-version=7.1"] = chain["build/builds/173176?api-version=7.1"]
    chain["pipelines/2199/runs/173177?api-version=7.1"] = chain[
        "pipelines/2199/runs/173176?api-version=7.1"
    ]
    with pytest.raises(ValueError, match="Build API identity mismatch"):
        _verify(chain, publish=False)


@pytest.fixture
def ado_env(monkeypatch):
    for key, value in {
        "SYSTEM_ACCESSTOKEN": "synthetic-token",
        "SYSTEM_COLLECTIONURI": "https://dev.azure.com/SqlClientDrivers/",
        "SYSTEM_TEAMPROJECTID": "c6d89619-62de-46a0-8b46-70b92a84d85e",
    }.items():
        monkeypatch.setenv(key, value)


@pytest.mark.parametrize(
    "key,value",
    [
        ("SYSTEM_ACCESSTOKEN", ""),
        ("SYSTEM_ACCESSTOKEN", "$(System.AccessToken)"),
        ("SYSTEM_COLLECTIONURI", "https://untrusted.invalid/SqlClientDrivers"),
        ("SYSTEM_TEAMPROJECTID", "904996cc-6198-4d39-8540-eca72bdf0b7b"),
    ],
)
def test_http_client_rejects_untrusted_context_before_network(monkeypatch, ado_env, key, value):
    monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        provenance, "build_opener", lambda *_args: pytest.fail("Unexpected network call")
    )
    with pytest.raises(ValueError):
        provenance.ado_get_json("build/builds/174195?api-version=7.1")


def test_http_client_uses_only_get_bounded_timeout_and_trusted_endpoint(monkeypatch, ado_env):
    calls = []

    def open_request(request, **kwargs):
        calls.append((request, kwargs))
        return io.BytesIO(b'{"id":174195}')

    monkeypatch.setattr(
        provenance, "build_opener", lambda *_args: types.SimpleNamespace(open=open_request)
    )
    assert provenance.ado_get_json("build/builds/174195?api-version=7.1") == {"id": 174195}
    request, kwargs = calls[0]
    assert request.get_method() == "GET"
    assert request.full_url == (
        "https://dev.azure.com/SqlClientDrivers/c6d89619-62de-46a0-8b46-70b92a84d85e"
        "/_apis/build/builds/174195?api-version=7.1"
    )
    assert request.get_header("Authorization") == "Bearer synthetic-token"
    assert kwargs == {"timeout": 60}


def test_api_redirects_cannot_forward_release_credentials():
    with pytest.raises(ValueError, match="credentials were not forwarded"):
        provenance._NoRedirect().redirect_request(
            None, None, 302, "redirect", {}, "https://untrusted.invalid"
        )


@pytest.mark.parametrize("publish", ["true", "false"])
def test_provenance_cli_executes_same_chain_in_both_modes(monkeypatch, chain, capsys, publish):
    for key, value in {
        "PUBLISH_TO_CONDA": publish,
        "CONDA_BUILD_PIPELINE_ID": "2318",
        "CONDA_BUILD_RUN_ID": "174195",
        "CONDA_BUILD_SOURCE_BRANCH": "refs/heads/main",
        "CONDA_BUILD_SOURCE_COMMIT": "a" * 40,
        "RELEASE_SOURCE_BRANCH": "refs/heads/main",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(provenance, "ado_get_json", chain.__getitem__)
    provenance.main()
    output = capsys.readouterr().out
    result = json.loads(output.splitlines()[0].split(": ", 1)[1])
    assert result["wheel"]["run"] == 173176
    assert ("Validate-only:" in output) == (publish == "false")


def test_invalid_publish_flag_is_not_treated_as_dry_run(monkeypatch):
    monkeypatch.setenv("PUBLISH_TO_CONDA", "perhaps")
    with pytest.raises(ValueError, match="true or false"):
        provenance.main()


def test_alternative_producer_definition_is_rejected(chain):
    with pytest.raises(ValueError, match="must be pipeline 2318"):
        provenance.verify_provenance(
            chain.__getitem__,
            producer_pipeline_id=2199,
            producer_run_id=174195,
            producer_branch="refs/heads/main",
            producer_commit="a" * 40,
            release_branch="refs/heads/main",
            publish=False,
        )


@pytest.fixture
def provenance_cli_env(monkeypatch, ado_env):
    values = {
        "PUBLISH_TO_CONDA": "true",
        "CONDA_BUILD_PIPELINE_ID": "2318",
        "CONDA_BUILD_RUN_ID": "174195",
        "CONDA_BUILD_SOURCE_BRANCH": "refs/heads/main",
        "CONDA_BUILD_SOURCE_COMMIT": "a" * 40,
        "RELEASE_SOURCE_BRANCH": "refs/heads/main",
        "MSSQL_PYTHON_VERSION": "",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


@pytest.mark.parametrize("expected", ["", "1.15.0", "1.14.0"])
@pytest.mark.parametrize("publish", ["true", "false"])
def test_cli_auto_version_and_override_are_producer_bound(
    monkeypatch, provenance_cli_env, chain, capsys, expected, publish
):
    monkeypatch.setenv("MSSQL_PYTHON_VERSION", expected)
    monkeypatch.setenv("PUBLISH_TO_CONDA", publish)
    monkeypatch.setattr(provenance, "ado_get_json", chain.__getitem__)
    assert provenance.cli() == (1 if expected == "1.14.0" else 0)
    output = capsys.readouterr()
    if expected == "1.14.0":
        assert output.out == "" and "differs from the recorded wheel producer" in output.err
    else:
        assert (
            "##vso[task.setvariable variable=mssqlPythonVersion;isOutput=true]1.15.0" in output.out
        )
        assert output.err == ""


@pytest.mark.parametrize(
    "key",
    [
        "PUBLISH_TO_CONDA",
        "CONDA_BUILD_PIPELINE_ID",
        "CONDA_BUILD_RUN_ID",
        "CONDA_BUILD_SOURCE_BRANCH",
        "CONDA_BUILD_SOURCE_COMMIT",
        "RELEASE_SOURCE_BRANCH",
        "SYSTEM_ACCESSTOKEN",
        "SYSTEM_COLLECTIONURI",
        "SYSTEM_TEAMPROJECTID",
    ],
)
def test_provenance_cli_missing_configuration(monkeypatch, provenance_cli_env, capsys, key):
    monkeypatch.delenv(key)
    monkeypatch.setattr(provenance, "build_opener", lambda *_: pytest.fail("Unexpected network"))
    with pytest.raises(ValueError, match=key):
        provenance.main()
    assert provenance.cli() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == f"ERROR: ValueError: Missing required environment variable {key}.\n"


@pytest.mark.parametrize("problem", ["success", "dry", "flag", "policy", "http", "json", "bug"])
def test_provenance_cli_boundary(monkeypatch, provenance_cli_env, chain, capsys, problem):
    errors = {
        "http": HTTPError("https://private.invalid", 403, "private server body", {}, None),
        "json": json.JSONDecodeError("private server body", "synthetic-token", 0),
        "bug": TypeError("unexpected implementation bug"),
    }

    def get_json(path):
        if problem in errors:
            raise errors[problem]
        return chain[path]

    monkeypatch.setattr(provenance, "ado_get_json", get_json)
    if problem in {"dry", "flag"}:
        monkeypatch.setenv("PUBLISH_TO_CONDA", "false" if problem == "dry" else "invalid")
    elif problem == "policy":
        monkeypatch.setenv("RELEASE_SOURCE_BRANCH", "refs/heads/feature")
    if problem == "bug":
        with pytest.raises(TypeError):
            provenance.cli()
        return
    success = problem in {"success", "dry"}
    if not success:
        with pytest.raises((ValueError, OSError)):
            provenance.main()
    assert provenance.cli() == (0 if success else 1)
    output = capsys.readouterr()
    if success:
        assert output.out.startswith("VERIFIED_RECORDED_PROVENANCE:") and output.err == ""
    else:
        assert output.out == ""
        assert output.err.startswith("ERROR:") and len(output.err.splitlines()) == 1
        assert all(
            value not in output.err
            for value in ("private server body", "synthetic-token", "private.invalid")
        )

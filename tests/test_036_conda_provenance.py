"""Source-only tests of the authoritative Azure DevOps release provenance chain."""

import io
import json
import hashlib
import tarfile
import types
import zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode

import pytest

_PATH = Path(__file__).resolve().parent.parent / "eng" / "conda_tools" / "provenance.py"
if not _PATH.is_file():
    pytest.skip("Conda release sources are not shipped in wheels.", allow_module_level=True)

from eng.conda_tools import __main__ as cli
from eng.conda_tools import contracts, inputs, provenance


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
        "setup.py": "setup(\n    version='1.15.0',\n    install_requires=[],\n)\n",
        "mssql_python/__init__.py": '__version__ = "1.15.0"\n',
        "mssql_python_odbc/__init__.py": '__version__ = "18.6.2.1"\n',
    }


def _component_files(name, version, rs=None):
    prefix = f"{name.replace('-', '_')}-{version}.dist-info/"
    metadata = f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n"
    files = {}
    if name == "mssql-python":
        metadata += "Requires-Dist: mssql-python-odbc==18.6.2.1\n"
        files["mssql_python/__init__.py"] = b""
        if rs:
            metadata += f"Requires-Dist: mssql-python-rs=={rs}\n"
    if name == "mssql-python-rs" or (name == "mssql-python" and not rs):
        files.update(
            {
                "mssql_py_core/__init__.py": b"",
                "mssql_py_core/mssql_py_core.cp313-win_amd64.pyd": b"native fixture",
            }
        )
    if name == "mssql-python-rs":
        files.update(
            {path: b"private native fixture" for path in contracts.rs_private_libraries("win-64")}
        )
    tag = "py3-none-win_amd64" if name == "mssql-python-odbc" else "cp313-cp313-win_amd64"
    files[prefix + "METADATA"] = metadata.encode()
    files[prefix + "WHEEL"] = f"Wheel-Version: 1.0\nTag: {tag}\n".encode()
    files[prefix + "RECORD"] = "".join(
        f"{path},,\n" for path in [*files, prefix + "RECORD"]
    ).encode()
    return files


def _source_dependencies(sources, dependencies):
    sources["setup.py"] = sources["setup.py"].replace(
        "install_requires=[]", f"install_requires={dependencies}"
    )


@pytest.mark.parametrize(
    "problem",
    [
        "success",
        "valid-parentheses",
        "unpublished",
        "wrong-public-version",
        "bad-digest",
        "yanked-only",
        "range",
        "conditional",
        "wrong-pin",
        "missing-pin",
        "duplicate-pin",
        "wrong-metadata-version",
        "wrong-metadata-name",
        "duplicate-name",
        "nested-metadata",
        "duplicate-metadata",
        "extra-nested-metadata",
        "extra-dot-metadata",
        "extra-case-metadata",
        "extra-backslash-metadata",
        "extra-wheel",
        "valid-rs",
        "new-source-legacy",
        "rs-unpublished",
        "rs-source-mismatch",
        "rs-conditional",
        "rs-binding-owns-core",
        "rs-provider-unowned",
        "rs-target-mismatch",
        "rs-binding-target-mismatch",
        "rs-binding-python-mismatch",
        "rs-binding-abi-mismatch",
        "rs-odbc-target-mismatch",
        "rs-provider-extra-unowned",
        "legacy-extra-unowned",
        "odbc-owns-core",
        "odbc-core-data",
        "odbc-core-case",
        "changed-wheel-hash",
        "wheel-tag-mismatch",
        *(
            f"{profile}{member}-alias-{spelling}"
            for profile in ("", "rs-")
            for member in ("record", "wheel")
            for spelling in ("dist-info-case", "name-case", "backslash", "nested")
        ),
    ],
)
def test_public_wheel_controls_use_actual_metadata_ownership_and_hashes(
    tmp_path, monkeypatch, problem
):
    versions = {"mssql-python": "1.15.0", "mssql-python-odbc": "18.6.2.1"}
    with_rs = problem == "valid-rs" or problem.startswith("rs-")
    source = {**versions, "mssql-python-rs": "0.2.0"}
    if with_rs:
        versions["mssql-python-rs"] = "0.2.0"
    if problem == "rs-source-mismatch":
        source["mssql-python-rs"] = "0.3.0"
    wheels = {}
    for name, version in versions.items():
        files = _component_files(name, version, "0.2.0" if with_rs else None)
        key = next(path for path in files if path.endswith("/METADATA"))
        metadata = files[key].decode()
        if name == "mssql-python":
            pin = "Requires-Dist: mssql-python-odbc==18.6.2.1\n"
            replacement = {
                "valid-parentheses": "Requires-Dist: MSSQL_python_ODBC (==18.6.2.1)\n",
                "range": pin.replace("==", ">="),
                "conditional": pin.rstrip() + '; python_version >= "3.10"\n',
                "wrong-pin": pin.replace("18.6.2.1", "0.0.0"),
                "missing-pin": "",
                "duplicate-pin": pin * 2,
            }.get(problem, pin)
            metadata = metadata.replace(pin, replacement)
            if problem == "rs-conditional":
                metadata = metadata.replace("rs==0.2.0", 'rs==0.2.0; python_version >= "3.10"')
            if problem == "rs-binding-owns-core":
                files["mssql_py_core/__init__.py"] = b""
        if problem == "wrong-metadata-version":
            metadata = metadata.replace(f"Version: {version}", "Version: 0.0.0")
        elif problem == "wrong-metadata-name":
            metadata = metadata.replace(f"Name: {name}", "Name: unexpected-package")
        elif problem == "duplicate-name":
            metadata += f"Name: {name}\n"
        files[key] = metadata.encode()
        if problem == "nested-metadata":
            files["nested/" + key] = files.pop(key)
        elif problem == "duplicate-metadata":
            files["other-0.dist-info/METADATA"] = metadata.encode()
        elif problem in {
            "extra-nested-metadata",
            "extra-dot-metadata",
            "extra-case-metadata",
            "extra-backslash-metadata",
        }:
            extra = {
                "extra-nested-metadata": "nested/" + key,
                "extra-dot-metadata": "./" + key,
                "extra-case-metadata": key.lower(),
                "extra-backslash-metadata": "nested\\" + key.replace("/", "\\"),
            }[problem]
            files[extra] = metadata.encode()
        if name == "mssql-python-odbc" and problem == "odbc-owns-core":
            files["mssql_py_core/unrecorded.py"] = b""
        elif name == "mssql-python-odbc" and problem == "odbc-core-data":
            files[f"mssql_python_odbc-{version}.data/platlib/mssql_py_core/__init__.py"] = b""
        elif name == "mssql-python-odbc" and problem == "odbc-core-case":
            files["MSSQL_PY_CORE/__init__.py"] = b""
        if (name == "mssql-python" and problem == "legacy-extra-unowned") or (
            name == "mssql-python-rs" and problem == "rs-provider-extra-unowned"
        ):
            files["mssql_py_core/unrecorded.py"] = b""
        if name == "mssql-python-rs" and problem == "rs-provider-unowned":
            record = next(path for path in files if path.endswith("/RECORD"))
            files[record] = b""
        tag = "py3-none-win_amd64" if name == "mssql-python-odbc" else "cp313-cp313-win_amd64"
        if name == "mssql-python-rs" and problem == "rs-target-mismatch":
            tag = tag.replace("313", "312")
            files[next(path for path in files if path.endswith("/WHEEL"))] = (
                f"Tag: {tag}\n".encode()
            )
        if name == "mssql-python" and problem in {
            "rs-binding-target-mismatch",
            "rs-binding-python-mismatch",
            "rs-binding-abi-mismatch",
        }:
            tag = {
                "rs-binding-target-mismatch": "cp313-cp313-win_arm64",
                "rs-binding-python-mismatch": "cp312-cp312-win_amd64",
                "rs-binding-abi-mismatch": "cp313-cp313t-win_amd64",
            }[problem]
            files[next(path for path in files if path.endswith("/WHEEL"))] = (
                f"Tag: {tag}\n".encode()
            )
        if name == "mssql-python-odbc" and problem == "rs-odbc-target-mismatch":
            tag = "py3-none-win_arm64"
            files[next(path for path in files if path.endswith("/WHEEL"))] = (
                f"Tag: {tag}\n".encode()
            )
        if problem == "wheel-tag-mismatch":
            files[next(path for path in files if path.endswith("/WHEEL"))] = b"Tag: wrong\n"
        if "-alias-" in problem:
            field, _, spelling = problem.removeprefix("rs-").split("-", 2)
            original = next(path for path in files if path.endswith(".dist-info/" + field.upper()))
            alias = {
                "dist-info-case": original.replace(".dist-info", ".DIST-INFO"),
                "name-case": original.rsplit("/", 1)[0] + "/" + field,
                "backslash": original.replace("/", "\\"),
                "nested": "extra/" + original,
            }[spelling]
            files[alias] = files[original]
        filename = f"{name.replace('-', '_')}-{version}-{tag}.whl"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as wheel:
            for path, data in files.items():
                entry = zipfile.ZipInfo()
                entry.filename = path
                wheel.writestr(entry, data)
        wheels[name] = filename, buffer.getvalue()
    calls = []

    def fetch(url, *, timeout):
        name, version = url.split("/")[-3:-1]
        assert url == f"https://pypi.org/pypi/{name}/{versions[name]}/json"
        assert timeout == 60
        calls.append(("GET", name))
        if problem == "unpublished" or (problem == "rs-unpublished" and name == "mssql-python-rs"):
            raise HTTPError(url, 404, "not published", {}, None)
        filename, data = wheels[name]
        digest = hashlib.sha256(data).hexdigest()
        response = {
            "info": {"name": name, "version": version},
            "urls": [
                {
                    "filename": filename,
                    "packagetype": "bdist_wheel",
                    "yanked": problem == "yanked-only",
                    "digests": {"sha256": "invalid" if problem == "bad-digest" else digest},
                }
            ],
        }
        if problem == "wrong-public-version":
            response["info"]["version"] = "0.0.0"
        return io.BytesIO(json.dumps(response).encode())

    def download(argv, *, check):
        assert check and argv[1:11] == [
            "-m",
            "pip",
            "--isolated",
            "download",
            "--index-url",
            "https://pypi.org/simple",
            "--no-deps",
            "--only-binary=:all:",
            "--require-hashes",
            "-r",
        ]
        lock = Path(argv[11]).read_text()
        directory = Path(argv[13])
        calls.append(("pip", lock))
        for requirement in lock.splitlines():
            name = requirement.split("==")[0]
            filename, data = wheels[name]
            assert f"--hash=sha256:{hashlib.sha256(data).hexdigest()}" in requirement
            (directory / filename).write_bytes(
                data + (b"changed" if problem == "changed-wheel-hash" else b"")
            )
        if problem == "extra-wheel":
            (directory / "extra.whl").touch()

    monkeypatch.setattr(inputs, "urlopen", fetch)
    monkeypatch.setattr(inputs.subprocess, "run", download)
    arguments = (
        source,
        tmp_path / "wheels & spaces",
        tmp_path / "wheel inputs.txt",
        "cp313",
        "win-64",
    )
    if problem in {"success", "valid-parentheses", "valid-rs", "new-source-legacy"}:
        assert inputs.fetch_wheels(*arguments) == versions
        assert sum(call[0] == "pip" for call in calls) == (2 if with_rs else 1)
    else:
        with pytest.raises(ValueError):
            inputs.fetch_wheels(*arguments)
    if not with_rs:
        assert ("GET", "mssql-python-rs") not in calls


@pytest.mark.parametrize(
    "problem",
    [
        "legacy",
        "rs",
        "new-source-legacy",
        "missing-provider",
        "wrong-odbc-pin",
        "wrong-rs-pin",
        "wrong-binding-version",
        "missing-record",
        "missing-wheel",
        "wrong-root",
        "duplicate-metadata",
        "unowned-core",
        "unrecorded-core",
        "legacy-unrecorded-core",
        "odbc-recorded-core",
        "binding-recorded-core",
        "core-path-alias",
        "core-root-alias",
        "rs-extra-owned",
        "rs-pyc-owned",
        "missing-core",
        "missing-private-driver",
        "missing-receipt",
        "wrong-transport",
        "missing-selected-wheel",
        "wrong-selected-wheel",
        "wrong-wheel-tags",
        "wrong-wheel-target",
        "duplicate-payload",
        "missing-version-env",
        "mismatched-binding-override",
        *(
            f"{profile}-{member}-alias-{spelling}"
            for profile in ("legacy", "rs")
            for member in ("metadata", "record", "wheel")
            for spelling in (
                "dist-info-case",
                "name-case",
                "prefix-case",
                "backslash",
                "dot",
                "parent",
                "nested",
                "dist-info-case-first",
                "backslash-first",
            )
        ),
    ],
)
def test_source_bound_release_components(tmp_path, monkeypatch, capsys, problem):
    with_rs = problem not in {"legacy", "new-source-legacy"} and not problem.startswith("legacy-")
    versions = {"mssql-python": "1.15.0", "mssql-python-odbc": "18.6.2.1"}
    if with_rs:
        versions["mssql-python-rs"] = "0.2.0"
    files = {}
    for name, version in versions.items():
        files.update(_component_files(name, version, "0.2.0" if with_rs else None))
    if problem == "new-source-legacy":
        versions["mssql-python-rs"] = "0.2.0"
    rs_prefix = "mssql_python_rs-0.2.0.dist-info/"
    binding_key = "mssql_python-1.15.0.dist-info/METADATA"
    selected = "mssql_python_rs-0.2.0-cp313-cp313-win_amd64.whl"
    receipt = {
        "distribution_version": "0.2.0",
        "transport_version": "0.2.0-dev.transport",
        "feed_url": inputs.DEFAULT_FEED,
        "package_id": inputs.PACKAGE_ID,
        "package_sha256": "a" * 64,
        "wheel_sha256": {selected: "b" * 64},
    }
    if with_rs:
        files[rs_prefix + "conda-wheel-source.txt"] = (selected + "\n").encode()
    if "-alias-" in problem:
        _, member, _, spelling = problem.split("-", 3)
        first = spelling.endswith("-first")
        spelling = spelling.removesuffix("-first")
        original = "mssql_python-1.15.0.dist-info/" + member.upper()
        alias = {
            "dist-info-case": original.replace(".dist-info", ".DIST-INFO"),
            "name-case": original.rsplit("/", 1)[0] + "/" + member,
            "prefix-case": original.replace("mssql_python-", "MSSQL_PYTHON-"),
            "backslash": original.replace("/", "\\"),
            "dot": "./" + original,
            "parent": "extra/../" + original,
            "nested": "extra/" + original,
        }[spelling]
        data = files[original].replace(b"1.15.0", b"9.9.9").replace(b"cp313", b"cp310")
        files = {alias: data, **files} if first else {**files, alias: data}
    elif problem == "missing-provider":
        files = {key: value for key, value in files.items() if not key.startswith(rs_prefix)}
    elif problem in {"wrong-odbc-pin", "wrong-rs-pin", "wrong-binding-version"}:
        old = {
            "wrong-odbc-pin": "18.6.2.1",
            "wrong-rs-pin": "rs==0.2.0",
            "wrong-binding-version": "Version: 1.15.0",
        }[problem]
        files[binding_key] = files[binding_key].replace(
            old.encode(), b"Version: 0.0.0" if problem == "wrong-binding-version" else b"0.0.0"
        )
    elif problem in {"missing-record", "missing-wheel"}:
        files.pop(rs_prefix + ("RECORD" if problem == "missing-record" else "WHEEL"))
    elif problem == "duplicate-metadata":
        files["other-0.dist-info/METADATA"] = files[binding_key]
    elif problem == "unowned-core":
        files[rs_prefix + "RECORD"] = b""
    elif problem in {"unrecorded-core", "legacy-unrecorded-core"}:
        files["mssql_py_core/unrecorded.py"] = b""
    elif problem in {"odbc-recorded-core", "binding-recorded-core"}:
        owner = (
            "mssql_python_odbc-18.6.2.1"
            if problem == "odbc-recorded-core"
            else "mssql_python-1.15.0"
        )
        files[f"{owner}.dist-info/RECORD"] += b"mssql_py_core/__init__.py,,\n"
    elif problem == "core-path-alias":
        files["MSSQL_PY_CORE/__init__.py"] = b""
    elif problem in {"rs-extra-owned", "rs-pyc-owned"}:
        member = (
            "mssql_py_core/extra.py"
            if problem == "rs-extra-owned"
            else "mssql_py_core/__pycache__/__init__.cpython-313.pyc"
        )
        files[member] = b""
        files[rs_prefix + "RECORD"] += f"{member},,\n".encode()
    elif problem == "missing-core":
        files.pop("mssql_py_core/__init__.py")
    elif problem == "missing-private-driver":
        files.pop(contracts.rs_private_libraries("win-64")[0])
    elif problem == "missing-selected-wheel":
        files.pop(rs_prefix + "conda-wheel-source.txt")
    elif problem == "wrong-selected-wheel":
        files[rs_prefix + "conda-wheel-source.txt"] = b"unexpected.whl\n"
    elif problem == "wrong-wheel-tags":
        files[rs_prefix + "WHEEL"] = b"Tag: cp310-cp310-win_amd64\n"
    elif problem == "wrong-wheel-target":
        wrong = selected.replace("313", "312")
        receipt["wheel_sha256"] = {wrong: "b" * 64}
        files[rs_prefix + "conda-wheel-source.txt"] = (wrong + "\n").encode()
        files[rs_prefix + "WHEEL"] = b"Tag: cp312-cp312-win_amd64\n"
    root = tmp_path / "conda"
    package = root / "win-64" / "mssql-python-1.15.0-py313_0.tar.bz2"
    package.parent.mkdir(parents=True)
    index = {
        "name": "mssql-python",
        "version": "1.15.0",
        "subdir": "win-64",
        "build": "py313_0",
        "depends": ["python >=3.13,<3.14.0a0", "python_abi 3.13.* *_cp313"],
    }
    prefix = "wrong/site-packages/" if problem == "wrong-root" else "Lib/site-packages/"
    members = [
        ("info/index.json", json.dumps(index).encode()),
        *((prefix + key, value) for key, value in files.items()),
    ]
    if problem == "duplicate-payload":
        members.append(members[-1])
    elif problem == "core-root-alias":
        members.append(("LIB/SITE-PACKAGES/mssql_py_core/unrecorded.py", b""))
    with tarfile.open(package, "w:bz2") as contents:
        for name, data in members:
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            contents.addfile(entry, io.BytesIO(data))
    if "mssql-python-rs" in versions and problem != "missing-receipt":
        (root / "rs-transport.json").write_text(json.dumps(receipt))
    monkeypatch.setenv(
        "RELEASE_VERSIONS", json.dumps(versions) if problem != "missing-version-env" else ""
    )
    monkeypatch.setenv(
        "RS_TRANSPORT_VERSION",
        (
            "wrong"
            if problem == "wrong-transport"
            else "0.2.0-dev.transport" if "mssql-python-rs" in versions else ""
        ),
    )
    result = cli.main(
        [
            "validate",
            "--root",
            str(root),
            "--required-subdirs",
            "win-64",
            "--allowed-subdirs",
            "win-64",
            "--pythons",
            "3.13",
            "--mssql-python-version",
            "0.0.0" if problem == "mismatched-binding-override" else "1.15.0",
            "--release-versions",
        ]
    )
    assert result == (
        0 if problem in {"legacy", "rs", "rs-extra-owned", "rs-pyc-owned"} else 1
    ), capsys.readouterr()


@pytest.mark.parametrize("with_rs", [False, True])
def test_public_cli_reports_qualified_profile_without_importing_runtime(
    tmp_path, monkeypatch, capsys, with_rs
):
    versions = {"mssql-python": "1.15.0", "mssql-python-odbc": "18.6.2.1"}
    if with_rs:
        versions["mssql-python-rs"] = "0.2.0"
    monkeypatch.setattr(inputs, "read_release_versions", lambda read: versions)
    monkeypatch.setattr(inputs, "fetch_wheels", lambda *args: versions)
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert (
        cli.main(
            [
                "fetch-wheels",
                "--wheel-dir",
                str(tmp_path),
                "--requirements-file",
                str(tmp_path / "pins"),
                "--python-tag",
                "cp313",
                "--conda-subdir",
                "win-64",
            ]
        )
        == 0
    )
    assert ("NOT current-source RS qualification" in capsys.readouterr().out) == (not with_rs)
    assert output.read_text() == f"rsRequired={str(with_rs).lower()}\n"


@pytest.mark.parametrize("problem", ["input-error", "bug", "output-error"])
def test_public_cli_expected_failures_do_not_hide_programming_errors(
    tmp_path, monkeypatch, capsys, problem
):
    monkeypatch.setattr(inputs, "read_release_versions", lambda read: {})

    def fetch(*args):
        if problem != "output-error":
            raise (
                ValueError("expected input failure")
                if problem == "input-error"
                else TypeError("programming bug")
            )
        return {}

    monkeypatch.setattr(inputs, "fetch_wheels", fetch)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path))
    arguments = cli.parser().parse_args(
        [
            "fetch-wheels",
            "--wheel-dir",
            str(tmp_path),
            "--requirements-file",
            str(tmp_path / "pins"),
            "--python-tag",
            "cp313",
            "--conda-subdir",
            "win-64",
        ]
    )
    if problem == "bug":
        with pytest.raises(TypeError, match="programming bug"):
            inputs.fetch_cli(arguments)
    else:
        assert inputs.fetch_cli(arguments) == 1
        captured = capsys.readouterr()
        assert "ERROR:" in captured.err
        assert "PUBLIC_WHEEL_INPUT_OK" not in captured.out


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
    records.update(_source_record(path, content) for path, content in release_sources.items())
    return records


def _verify(chain, **kwargs):
    return provenance.verify_provenance(
        kwargs.pop("get_json", chain.__getitem__),
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
        inputs.read_release_versions(release_sources.__getitem__)


@pytest.mark.parametrize("problem", ["absent", "ambiguous"])
def test_absent_or_ambiguous_source_dependencies_cannot_be_classified_as_legacy(
    release_sources, problem
):
    if problem == "absent":
        release_sources["setup.py"] = release_sources["setup.py"].replace(
            "    install_requires=[],\n", ""
        )
    else:
        release_sources["setup.py"] += "setup(install_requires=[])\n"
    with pytest.raises(ValueError, match="exactly one explicit"):
        inputs.read_release_versions(release_sources.__getitem__)


def test_release_source_is_read_not_executed(release_sources):
    release_sources["setup.py"] += "raise AssertionError('must not execute setup.py')\n"
    assert inputs.read_release_versions(release_sources.__getitem__)["mssql-python"] == "1.15.0"


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
    provenance.execute()
    output = capsys.readouterr().out
    result = json.loads(output.splitlines()[0].split(": ", 1)[1])
    assert result["wheel"]["run"] == 173176
    assert ("Validate-only:" in output) == (publish == "false")
    directive = "##vso[task.setvariable variable=releaseVersions;isOutput=true]"
    assert (
        json.loads(
            next(
                line[len(directive) :] for line in output.splitlines() if line.startswith(directive)
            )
        )
        == result["versions"]
    )


def test_invalid_publish_flag_is_not_treated_as_dry_run(monkeypatch):
    monkeypatch.setenv("PUBLISH_TO_CONDA", "perhaps")
    with pytest.raises(ValueError, match="true or false"):
        provenance.execute()


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
        provenance.execute()
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
            provenance.execute()
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


def _source_record(path, content):
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
    return f"git/repositories/{provenance._REPOSITORY_ID}/items?{query}", {
        "path": "/" + path,
        "commitId": "b" * 40,
        "gitObjectType": "blob",
        "content": content,
    }


def test_rs_source_contract_reads_distribution_pin_not_transport(release_sources):
    _source_dependencies(release_sources, '[f"mssql-python-rs=={_read_mssql_python_rs_version()}"]')
    release_sources["eng/versions/mssql-python-rs.version"] = "0.2.0\n"
    release_sources["eng/versions/mssql-python-rs-nuget.version"] = "9.9.9-dev.transport"
    reads = []

    def read_source(path):
        reads.append(path)
        return release_sources[path]

    assert provenance.read_release_versions(read_source) == {
        "mssql-python": "1.15.0",
        "mssql-python-odbc": "18.6.2.1",
        "mssql-python-rs": "0.2.0",
    }
    assert reads == [
        "setup.py",
        "mssql_python/__init__.py",
        "mssql_python_odbc/__init__.py",
        "eng/versions/mssql-python-rs.version",
    ]


@pytest.mark.parametrize("version", ["", "0.2.0\n0.3.0", "$(version)", "../0.2.0", "0.2.0;bad"])
def test_rs_distribution_pin_must_be_present_and_unambiguous(release_sources, version):
    _source_dependencies(release_sources, '[f"mssql-python-rs=={_read_mssql_python_rs_version()}"]')
    release_sources["eng/versions/mssql-python-rs.version"] = version
    with pytest.raises(ValueError, match="RS distribution release version"):
        provenance.read_release_versions(release_sources.__getitem__)


@pytest.mark.parametrize(
    "dependencies",
    [
        '["mssql-python-rs==0.2.0"]',
        '[f"mssql-python-rs>={_read_mssql_python_rs_version()}"]',
        '[f"mssql-python-rs=={other_version()}"]',
        "[f\"mssql-python-rs=={_read_mssql_python_rs_version()}; python_version >= '3.10'\"]",
        '[f"mssql-python-rs=={_read_mssql_python_rs_version()}"] * 2',
        '["mssql-python-" + component]',
        '[f"mssql-python-{component}=={version}"]',
        '[f"{name}=={version}"]',
        '[f" mssql-python-rs=={_read_mssql_python_rs_version()}"]',
        '[f""]',
        "[None]",
        "get_dependencies()",
    ],
)
def test_rs_source_contract_cannot_use_unverified_or_conditional_pins(
    release_sources, dependencies
):
    _source_dependencies(release_sources, dependencies)
    with pytest.raises(ValueError, match="maintained RS distribution pin|explicit"):
        provenance.read_release_versions(release_sources.__getitem__)


def test_unreadable_source_dependency_contract_never_becomes_legacy(release_sources):
    release_sources["setup.py"] += "setup(install_requires=[\n"
    with pytest.raises(ValueError, match="dependency contract cannot be read"):
        provenance.read_release_versions(release_sources.__getitem__)


@pytest.mark.parametrize(
    "problem",
    ["valid", "missing", "wrong-commit", "transport-path", "missing-content", "forbidden"],
)
def test_recorded_rs_source_is_read_only_at_verified_wheel_commit(release_sources, chain, problem):
    _source_dependencies(release_sources, '[f"mssql-python-rs=={_read_mssql_python_rs_version()}"]')
    chain.update([_source_record("setup.py", release_sources["setup.py"])])
    key, record = _source_record("eng/versions/mssql-python-rs.version", "0.2.0\n")
    if problem == "wrong-commit":
        record["commitId"] = "c" * 40
    elif problem == "transport-path":
        record["path"] = "/eng/versions/mssql-python-rs-nuget.version"
    elif problem == "missing-content":
        record.pop("content")
    if problem != "missing":
        chain[key] = record
    transport_key, transport = _source_record(
        "eng/versions/mssql-python-rs-nuget.version", "0.2.0-dev.transport"
    )
    chain[transport_key] = transport
    reads = []

    def get_json(path):
        reads.append(path)
        if path == key and problem == "forbidden":
            raise HTTPError("https://dev.azure.com", 403, "Forbidden", {}, None)
        return chain[path]

    if problem == "valid":
        result = _verify(chain, get_json=get_json)
        assert result["versions"]["mssql-python-rs"] == "0.2.0"
        assert result["rsTransportVersion"] == "0.2.0-dev.transport"
        assert reads[-2:] == [key, transport_key]
    else:
        with pytest.raises((ValueError, KeyError, HTTPError)):
            _verify(chain, get_json=get_json)
        assert reads[-1] == key
    assert len(reads) == (9 if problem == "valid" else 8)


@pytest.mark.parametrize("problem", ["missing", "wrong-commit", "invalid", "forbidden"])
def test_rs_transport_source_cannot_fall_back_to_distribution_version(
    release_sources, chain, problem
):
    _source_dependencies(release_sources, '[f"mssql-python-rs=={_read_mssql_python_rs_version()}"]')
    chain.update([_source_record("setup.py", release_sources["setup.py"])])
    chain.update([_source_record("eng/versions/mssql-python-rs.version", "0.2.0")])
    key, record = _source_record(
        "eng/versions/mssql-python-rs-nuget.version", "0.2.0-dev.transport"
    )
    if problem == "wrong-commit":
        record["commitId"] = "c" * 40
    elif problem == "invalid":
        record["content"] = "0.2.0\n0.3.0"
    if problem != "missing":
        chain[key] = record

    def get_json(path):
        if path == key and problem == "forbidden":
            raise HTTPError("https://dev.azure.com", 403, "Forbidden", {}, None)
        return chain[path]

    with pytest.raises((ValueError, KeyError, HTTPError)):
        _verify(chain, get_json=get_json)


@pytest.mark.parametrize(
    "problem",
    [
        "valid",
        "absent",
        "distribution",
        "transport",
        "feed",
        "package",
        "package-hash",
        "empty-wheels",
        "wheel-hash",
        "wheel-version",
        "wheel-path",
    ],
)
def test_rs_transport_receipt_is_source_bound_without_inferred_producer_id(problem):
    receipt = {
        "distribution_version": "0.2.0",
        "transport_version": "0.2.0-dev.transport",
        "feed_url": (
            "https://pkgs.dev.azure.com/sqlclientdrivers/public/"
            "_packaging/mssql-rs_Public/nuget/v3/index.json"
        ),
        "package_id": "mssql-python-rs-wheels",
        "package_sha256": "a" * 64,
        "wheel_sha256": {"mssql_python_rs-0.2.0-cp313-cp313-win_amd64.whl": "b" * 64},
        "nuspec_description": "InternalBuild; no independently verified numeric producer ID",
    }
    fields = {
        "distribution": "distribution_version",
        "transport": "transport_version",
        "feed": "feed_url",
        "package": "package_id",
        "package-hash": "package_sha256",
    }
    if problem in fields:
        receipt[fields[problem]] = "wrong"
    elif problem == "absent":
        receipt = None
    elif problem == "empty-wheels":
        receipt["wheel_sha256"] = {}
    elif problem.startswith("wheel-"):
        name, digest = next(iter(receipt["wheel_sha256"].items()))
        if problem == "wheel-hash":
            digest = "wrong"
        elif problem == "wheel-version":
            name = name.replace("0.2.0", "0.3.0")
        else:
            name = "../" + name
        receipt["wheel_sha256"] = {name: digest}
    arguments = (receipt, {"mssql-python-rs": "0.2.0"}, "0.2.0-dev.transport")
    if problem == "valid":
        inputs.validate_rs_transport(*arguments)
    else:
        with pytest.raises(ValueError, match="RS"):
            inputs.validate_rs_transport(*arguments)


def test_verified_legacy_transport_absence_is_not_a_new_producer_fallback():
    inputs.validate_rs_transport(None, {"mssql-python": "1.15.0"}, "")
    with pytest.raises(ValueError, match="embedded-core"):
        inputs.validate_rs_transport({}, {"mssql-python": "1.15.0"}, "")
    with pytest.raises(ValueError, match="recorded"):
        inputs.validate_rs_transport(None, {"mssql-python-rs": "0.2.0"}, "")


@pytest.mark.parametrize(
    "raw",
    [
        "null",
        "[]",
        "{}",
        '{"mssql-python":"1.15.0"}',
        '{"mssql-python":"1.15.0","mssql-python-odbc":null}',
        '{"mssql-python":"1.15.0","mssql-python-odbc":" 18.6.2.1"}',
        '{"mssql-python":"1.15.0","mssql-python-odbc":"18.6.2.1","transport":"0.2.0-dev.x"}',
        '{"mssql-python":"$(version)","mssql-python-odbc":"18.6.2.1"}',
    ],
)
def test_release_output_versions_cannot_lose_or_substitute_source_assertions(raw):
    with pytest.raises(ValueError, match="distribution versions"):
        inputs.parse_release_versions(raw)


@pytest.mark.parametrize("with_rs", [False, True])
def test_release_output_versions_preserve_explicit_old_or_new_source_contract(with_rs):
    versions = {"mssql-python": "1.15.0", "mssql-python-odbc": "18.6.2.1"}
    if with_rs:
        versions["mssql-python-rs"] = "0.2.0"
    assert inputs.parse_release_versions(json.dumps(versions)) == versions

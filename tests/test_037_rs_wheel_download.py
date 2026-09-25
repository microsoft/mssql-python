import hashlib
import io
import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "eng" / "scripts"
MODULE_PATH = SCRIPTS_DIR / "download_mssql_python_rs_wheels.py"

if not MODULE_PATH.is_file() or not (Path(__file__).parents[1] / "OneBranchPipelines").is_dir():
    pytest.skip(
        "rs wheel download contracts require a complete source checkout",
        allow_module_level=True,
    )


from eng.scripts import download_mssql_python_rs_wheels as downloader


def _package(entries, transport_version="0.1.0-dev.1"):
    package = io.BytesIO()
    with zipfile.ZipFile(package, "w") as archive:
        archive.writestr(
            "mssql-python-rs-wheels.nuspec",
            "<package><metadata><id>mssql-python-rs-wheels</id>"
            f"<version>{transport_version}</version>"
            "<description>RS transport fixture</description></metadata></package>",
        )
        for name in entries:
            archive.writestr(name, name.encode("ascii"))
    package.seek(0)
    return package.getvalue()


def _mock_download(monkeypatch, module, content, package_base="https://example.test/flat/"):
    requested = []
    monkeypatch.setattr(module, "resolve", lambda _url: package_base)

    def urlopen(url, timeout):
        requested.append((url, timeout))
        return io.BytesIO(content)

    monkeypatch.setattr(module.urllib.request, "urlopen", urlopen)
    return requested


def test_downloads_only_matching_wheels_and_clears_stale_output(tmp_path, monkeypatch):
    module = downloader
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0\n", encoding="ascii")
    transport_file = tmp_path / "transport-version"
    transport_file.write_text("0.1.0-dev.20260914.174990\n", encoding="ascii")
    output = tmp_path / "wheels"
    output.mkdir()
    (output / "stale.whl").write_bytes(b"stale")
    content = _package(
        [
            "wheels/mssql_python_rs-0.1.0-cp310-abi3-win_amd64.whl",
            "symbols/debug.pdb",
        ],
        "0.1.0-dev.20260914.174990",
    )
    requested = _mock_download(monkeypatch, module, content, "https://example.test/flat")

    wheels = module.download_wheels(
        "https://example.test/index.json", version_file, transport_file, output
    )

    assert [wheel.name for wheel in wheels] == ["mssql_python_rs-0.1.0-cp310-abi3-win_amd64.whl"]
    assert sorted(path.name for path in output.iterdir()) == [wheels[0].name, "transport.json"]
    receipt = json.loads((output / "transport.json").read_text())
    assert receipt == {
        "distribution_version": "0.1.0",
        "transport_version": "0.1.0-dev.20260914.174990",
        "feed_url": "https://example.test/index.json",
        "package_id": "mssql-python-rs-wheels",
        "package_sha256": hashlib.sha256(content).hexdigest(),
        "nuspec_description": "RS transport fixture",
        "wheel_sha256": {wheels[0].name: hashlib.sha256(wheels[0].read_bytes()).hexdigest()},
    }
    assert requested == [
        (
            "https://example.test/flat/mssql-python-rs-wheels/0.1.0-dev.20260914.174990/"
            "mssql-python-rs-wheels.0.1.0-dev.20260914.174990.nupkg",
            120,
        )
    ]


def test_rejects_wheel_with_unexpected_version(tmp_path, monkeypatch):
    module = downloader
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    transport_file = tmp_path / "transport-version"
    transport_file.write_text("0.1.0-dev.1", encoding="ascii")
    content = _package(["wheels/mssql_python_rs-0.1.1-cp310-abi3-win_amd64.whl"])
    _mock_download(monkeypatch, module, content)

    with pytest.raises(ValueError, match="Unexpected wheel name"):
        module.download_wheels(
            "https://example.test/index.json",
            version_file,
            transport_file,
            tmp_path / "out",
        )


def test_rejects_duplicate_wheel_filename(tmp_path, monkeypatch):
    module = downloader
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    transport_file = tmp_path / "transport-version"
    transport_file.write_text("0.1.0-dev.1", encoding="ascii")
    wheel = "wheels/mssql_python_rs-0.1.0-cp310-abi3-win_amd64.whl"
    with pytest.warns(UserWarning, match="Duplicate name"):
        content = _package([wheel, wheel])
    _mock_download(monkeypatch, module, content)

    with pytest.raises(ValueError, match="Duplicate wheel filename"):
        module.download_wheels(
            "https://example.test/index.json",
            version_file,
            transport_file,
            tmp_path / "out",
        )


def test_rejects_filesystem_root_as_output(tmp_path):
    module = downloader
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    transport_file = tmp_path / "transport-version"
    transport_file.write_text("0.1.0-dev.1", encoding="ascii")

    with pytest.raises(ValueError, match="filesystem root"):
        module.download_wheels(
            "https://example.test/index.json",
            version_file,
            transport_file,
            Path(tmp_path.anchor),
        )


def test_rejects_working_directory_and_ancestor_as_output(tmp_path, monkeypatch):
    module = downloader
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    transport_file = tmp_path / "transport-version"
    transport_file.write_text("0.1.0-dev.1", encoding="ascii")
    working_directory = tmp_path / "checkout" / "subdirectory"
    working_directory.mkdir(parents=True)
    monkeypatch.chdir(working_directory)

    for unsafe in (working_directory, working_directory.parent):
        with pytest.raises(ValueError, match="current working directory|ancestors"):
            module.download_wheels(
                "https://example.test/index.json",
                version_file,
                transport_file,
                unsafe,
            )


def test_rejects_symlink_output_without_deleting_target(tmp_path, monkeypatch):
    module = downloader
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    transport_file = tmp_path / "transport-version"
    transport_file.write_text("0.1.0-dev.1", encoding="ascii")
    target = tmp_path / "unrelated"
    target.mkdir()
    marker = target / "keep.txt"
    marker.write_text("keep", encoding="ascii")
    output = tmp_path / "output-link"
    if os.name == "nt":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(output), str(target)],
            check=True,
            capture_output=True,
        )
    else:
        output.symlink_to(target, target_is_directory=True)

    content = _package(["wheels/mssql_python_rs-0.1.0-cp310-abi3-win_amd64.whl"])
    _mock_download(monkeypatch, module, content)

    with pytest.raises(ValueError, match="symbolic link|junction"):
        module.download_wheels(
            "https://example.test/index.json", version_file, transport_file, output
        )

    assert marker.read_text(encoding="ascii") == "keep"


def test_stress_jobs_resolve_dependency_from_pinned_transport():
    pipeline = (
        Path(__file__).parents[1] / "OneBranchPipelines" / "stress-test-pipeline.yml"
    ).read_text(encoding="utf-8")

    assert pipeline.count("download_mssql_python_rs_wheels.py --output-dir") == 2
    assert pipeline.count("--find-links=$(Pipeline.Workspace)\\mssql-python-rs-wheels") == 1
    assert pipeline.count("--find-links=$(Pipeline.Workspace)/mssql-python-rs-wheels") == 1


@pytest.mark.parametrize(
    "script_name",
    ["install-mssql-py-core.ps1", "install-mssql-py-core.sh"],
)
def test_local_installers_select_stable_abi_wheel(script_name):
    script = (SCRIPTS_DIR / script_name).read_text(encoding="utf-8")

    assert "cp310-abi3" in script
    assert "PyVersion-$script:PyVersion" not in script
    assert "PY_VERSION}-${PY_VERSION" not in script

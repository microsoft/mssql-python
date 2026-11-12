import hashlib
import io
import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if (
    not (ROOT / "eng/scripts/download_mssql_python_rs_wheels.py").is_file()
    or not (ROOT / "OneBranchPipelines").is_dir()
):
    pytest.skip(
        "Runtime staging contracts require a complete source checkout", allow_module_level=True
    )

from eng.scripts import download_mssql_python_rs_wheels as downloader
from eng.scripts.mssql_python_rs import DEFAULT_INDEX, DEV_INDEX, VERSION_ENV


@pytest.fixture(autouse=True)
def clear_override(monkeypatch):
    monkeypatch.delenv(VERSION_ENV, raising=False)


def _wheel(version="0.3.0", metadata_version=None):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            f"mssql_python_rs-{version}.dist-info/METADATA",
            f"Name: mssql-python-rs\nVersion: {metadata_version or version}\n",
        )
    return stream.getvalue()


def _index(monkeypatch, *, version="0.3.0", content=None, duplicate=False, hash_value=None):
    content = _wheel(version) if content is None else content
    name = f"mssql_python_rs-{version}-cp310-abi3-win_amd64.whl"
    digest = hash_value or hashlib.sha256(content).hexdigest()
    anchor = f'<a href="https://files.example.test/{name}#sha256={digest}">{name}</a>'
    page = (anchor * (2 if duplicate else 1)).encode()
    requested = []

    def urlopen(url, timeout):
        requested.append(url)
        return io.BytesIO(page if url.endswith("/mssql-python-rs/") else content)

    monkeypatch.setattr(downloader.urllib.request, "urlopen", urlopen)
    return name, content, requested


def _pin(tmp_path):
    path = tmp_path / "version"
    path.write_text("0.3.0\n", encoding="ascii")
    return path


@pytest.mark.parametrize("override", [None, "0.4.0.dev20261028187000"])
def test_stages_selected_wheels_and_receipt(tmp_path, monkeypatch, override):
    pin = _pin(tmp_path)
    if override:
        monkeypatch.setenv(VERSION_ENV, override)
    version = override or "0.3.0"
    name, content, requested = _index(monkeypatch, version=version)
    output = tmp_path / "out"
    output.mkdir()
    (output / "stale.whl").touch()

    wheels = downloader.download_wheels(None, pin, output)

    index = DEV_INDEX if override else DEFAULT_INDEX
    assert wheels == [output / name]
    assert sorted(path.name for path in output.iterdir()) == [name, "transport.json"]
    assert json.loads((output / "transport.json").read_text()) == {
        "distribution_version": version,
        "index_url": index,
        "version_override": bool(override),
        "wheel_sha256": {name: hashlib.sha256(content).hexdigest()},
    }
    assert requested == [
        index + "mssql-python-rs/",
        f"https://files.example.test/{name}",
    ]


def test_explicit_version_selects_dev_index(tmp_path, monkeypatch):
    _index(monkeypatch, version="0.4.0.dev1")
    wheels = downloader.download_wheels(
        None, _pin(tmp_path), tmp_path / "out", version="0.4.0.dev1"
    )
    assert wheels[0].name.startswith("mssql_python_rs-0.4.0.dev1-")


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"duplicate": True}, "Duplicate wheel filename"),
        ({"hash_value": "0" * 64}, "SHA-256 mismatch"),
        ({"hash_value": "bad"}, "Missing or invalid SHA-256"),
        ({"content": _wheel(metadata_version="0.4.0")}, "metadata does not match"),
        ({"version": "0.4.0"}, "No mssql-python-rs==0.3.0 wheels"),
    ],
)
def test_rejects_bad_or_missing_wheels(tmp_path, monkeypatch, options, error):
    _index(monkeypatch, **options)
    with pytest.raises(ValueError, match=error):
        downloader.download_wheels(None, _pin(tmp_path), tmp_path / "out")
    assert not (tmp_path / "out" / "transport.json").exists()


def test_missing_requested_dev_build_does_not_fall_back(tmp_path, monkeypatch):
    _index(monkeypatch)
    monkeypatch.setenv(VERSION_ENV, "0.4.0.dev1")
    with pytest.raises(ValueError, match="No mssql-python-rs==0.4.0.dev1"):
        downloader.download_wheels(None, _pin(tmp_path), tmp_path / "out")


def test_rejects_filesystem_root_as_output(tmp_path):
    with pytest.raises(ValueError, match="filesystem root"):
        downloader.download_wheels(None, _pin(tmp_path), Path(tmp_path.anchor))


def test_rejects_working_directory_and_ancestor_as_output(tmp_path, monkeypatch):
    pin = _pin(tmp_path)
    cwd = tmp_path / "checkout" / "subdirectory"
    cwd.mkdir(parents=True)
    monkeypatch.chdir(cwd)
    for unsafe in (cwd, cwd.parent):
        with pytest.raises(ValueError, match="current working directory|ancestors"):
            downloader.download_wheels(None, pin, unsafe)


def test_rejects_symlink_output_without_deleting_target(tmp_path):
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
    with pytest.raises(ValueError, match="symbolic link|junction"):
        downloader.download_wheels(None, _pin(tmp_path), output)
    assert marker.read_text(encoding="ascii") == "keep"


def test_rejects_insecure_index(tmp_path):
    with pytest.raises(ValueError, match="HTTPS"):
        downloader.download_wheels(
            "http://example.test/simple/",  # DevSkim: ignore DS137138 - HTTPS rejection fixture
            _pin(tmp_path),
            tmp_path / "out",
        )


def test_ignores_yanked_wheels(tmp_path, monkeypatch):
    name = "mssql_python_rs-0.3.0-cp310-abi3-win_amd64.whl"
    page = f'<a href="https://example.test/{name}" data-yanked="broken">wheel</a>'
    monkeypatch.setattr(
        downloader.urllib.request, "urlopen", lambda *args, **kwargs: io.BytesIO(page.encode())
    )
    with pytest.raises(ValueError, match="No mssql-python-rs"):
        downloader.download_wheels(None, _pin(tmp_path), tmp_path / "out")

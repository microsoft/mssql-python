import hashlib
import http.client
import io
import json
import os
import subprocess
import sys
import urllib.error
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


@pytest.mark.parametrize("target", ["index", "wheel"])
@pytest.mark.parametrize("failure", ["timeout", "503", "incomplete"])
def test_transient_download_retries_cleanly(tmp_path, monkeypatch, target, failure):
    name, content, requested = _index(monkeypatch)
    original = downloader.urllib.request.urlopen
    calls = []
    sleeps = []

    class Interrupted(io.BytesIO):
        def read(self, *args):
            super().read(10)
            raise downloader.http.client.IncompleteRead(b"partial")

    def flaky(url, timeout):
        selected = url.endswith("/mssql-python-rs/") == (target == "index")
        if selected:
            calls.append(url)
            if len(calls) == 1:
                if failure == "timeout":
                    raise urllib.error.URLError(TimeoutError("fixture timeout"))
                if failure == "503":
                    raise urllib.error.HTTPError(url, 503, "fixture unavailable", {}, None)
                return Interrupted(content)
        return original(url, timeout)

    monkeypatch.setattr(downloader.urllib.request, "urlopen", flaky)
    monkeypatch.setattr(downloader.time, "sleep", sleeps.append)
    output = tmp_path / "out"
    downloader.download_wheels(None, _pin(tmp_path), output)
    assert len(calls) == 2
    assert sleeps == [2]
    assert (output / name).read_bytes() == content
    assert not list(output.glob("*.part"))


@pytest.mark.parametrize(
    "failure", ["truncated-once", "truncated-always", "complete-hash", "complete-metadata"]
)
def test_http_response_length_controls_transport_retries(tmp_path, monkeypatch, failure):
    options = (
        {"hash_value": "0" * 64}
        if failure == "complete-hash"
        else {"content": _wheel(metadata_version="0.4.0")} if failure == "complete-metadata" else {}
    )
    name, content, _ = _index(monkeypatch, **options)
    original = downloader.urllib.request.urlopen
    calls, sleeps = [], []

    def urlopen(url, timeout):
        if url.endswith("/mssql-python-rs/"):
            return original(url, timeout)
        calls.append(url)
        truncated = failure == "truncated-always" or (
            failure == "truncated-once" and len(calls) == 1
        )
        payload = content[: len(content) // 2] if truncated else content

        class Socket:
            def makefile(self, *args, **kwargs):
                return io.BytesIO(
                    f"HTTP/1.1 200 OK\r\nContent-Length: {len(content)}\r\n\r\n".encode() + payload
                )

        response = http.client.HTTPResponse(Socket())
        response.begin()
        return response

    monkeypatch.setattr(downloader.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(downloader.time, "sleep", sleeps.append)
    output = tmp_path / "out"
    pin = _pin(tmp_path)
    if failure == "truncated-once":
        downloader.download_wheels(None, pin, output)
        assert (output / name).read_bytes() == content
        assert len(calls) == 2
        assert sleeps == [2]
        assert not list(output.glob("*.part"))
    else:
        error = http.client.IncompleteRead if failure == "truncated-always" else ValueError
        with pytest.raises(error):
            downloader.download_wheels(None, pin, output)
        assert len(calls) == (3 if failure == "truncated-always" else 1)
        assert sleeps == ([2, 4] if failure == "truncated-always" else [])
        assert list(output.iterdir()) == []


@pytest.mark.parametrize("code", [400, 401, 403, 404])
def test_permanent_http_error_is_not_retried(tmp_path, monkeypatch, code):
    calls = []

    def unavailable(url, timeout):
        calls.append(url)
        raise urllib.error.HTTPError(url, code, "fixture rejection", {}, None)

    monkeypatch.setattr(downloader.urllib.request, "urlopen", unavailable)
    with pytest.raises(urllib.error.HTTPError):
        downloader.download_wheels(None, _pin(tmp_path), tmp_path / "out")
    assert len(calls) == 1


def test_retry_exhaustion_removes_partial_files_and_receipt(tmp_path, monkeypatch):
    _index(monkeypatch)
    original = downloader.urllib.request.urlopen
    calls, sleeps = [], []

    class Interrupted(io.BytesIO):
        def read(self, *args):
            raise TimeoutError("fixture interrupted stream")

    def unavailable(url, timeout):
        if url.endswith("/mssql-python-rs/"):
            return original(url, timeout)
        calls.append(url)
        return Interrupted(b"partial")

    monkeypatch.setattr(downloader.urllib.request, "urlopen", unavailable)
    monkeypatch.setattr(downloader.time, "sleep", sleeps.append)
    output = tmp_path / "out"
    with pytest.raises(TimeoutError):
        downloader.download_wheels(None, _pin(tmp_path), output)
    assert len(calls) == 3
    assert sleeps == [2, 4]
    assert list(output.iterdir()) == []


@pytest.mark.parametrize("failure", ["hash", "metadata"])
def test_invalid_payload_is_not_retried_or_left_staged(tmp_path, monkeypatch, failure):
    options = (
        {"hash_value": "0" * 64}
        if failure == "hash"
        else {"content": _wheel(metadata_version="0.4.0")}
    )
    _, _, requested = _index(monkeypatch, **options)
    output = tmp_path / "out"
    with pytest.raises(ValueError):
        downloader.download_wheels(None, _pin(tmp_path), output)
    assert len(requested) == 2
    assert list(output.iterdir()) == []


@pytest.mark.parametrize("tamper", [None, "hash", "version", "extra", "override"])
def test_rechecks_early_transport_offline(tmp_path, monkeypatch, tamper):
    name, _, _ = _index(monkeypatch)
    pin, output = _pin(tmp_path), tmp_path / "out"
    downloader.download_wheels(None, pin, output)

    def forbidden(*args, **kwargs):
        pytest.fail("Offline verification must not access the network")

    monkeypatch.setattr(downloader.urllib.request, "urlopen", forbidden)
    if tamper == "hash":
        (output / name).write_bytes(b"corrupt")
    elif tamper == "version":
        pin.write_text("0.4.0\n", encoding="ascii")
    elif tamper == "extra":
        (output / "extra.whl").touch()
    elif tamper == "override":
        monkeypatch.setenv(VERSION_ENV, "0.3.0")
    if tamper:
        with pytest.raises(ValueError):
            downloader.verify_staged_wheels(output, pin)
    else:
        assert downloader.verify_staged_wheels(output, pin) == [output / name]


@pytest.mark.parametrize("download_slash", ["", "/"])
@pytest.mark.parametrize("verify_slash", ["", "/"])
def test_explicit_index_round_trip_is_verified_offline(
    tmp_path, monkeypatch, download_slash, verify_slash
):
    index = "https://mirror.example.test/simple"
    name, _, _ = _index(monkeypatch)
    pin, output = _pin(tmp_path), tmp_path / "out"
    downloader.download_wheels(index + download_slash, pin, output)

    def forbidden(*args, **kwargs):
        pytest.fail("Offline verification must not access the network")

    monkeypatch.setattr(downloader.urllib.request, "urlopen", forbidden)
    assert downloader.verify_staged_wheels(output, pin, index_url=index + verify_slash) == [
        output / name
    ]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_mssql_python_rs_wheels.py",
            "--verify-only",
            "--index-url",
            index + verify_slash,
            "--output-dir",
            str(output),
            "--distribution-version-file",
            str(pin),
        ],
    )
    downloader.main()
    for wrong_index in (None, "https://different.example.test/simple"):
        with pytest.raises(ValueError, match="effective runtime"):
            downloader.verify_staged_wheels(output, pin, index_url=wrong_index)

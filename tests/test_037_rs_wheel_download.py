import importlib.util
import io
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "eng" / "scripts"
MODULE_PATH = SCRIPTS_DIR / "download_mssql_python_rs_wheels.py"


def _load_downloader():
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        spec = importlib.util.spec_from_file_location("rs_wheel_downloader_under_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(SCRIPTS_DIR))


def _package(entries):
    package = io.BytesIO()
    with zipfile.ZipFile(package, "w") as archive:
        for name in entries:
            archive.writestr(name, name.encode("ascii"))
    package.seek(0)
    return package.getvalue()


def _mock_download(monkeypatch, module, content):
    requested = []
    monkeypatch.setattr(module, "resolve", lambda _url: "https://example.test/flat/")

    def urlopen(url, timeout):
        requested.append((url, timeout))
        return io.BytesIO(content)

    monkeypatch.setattr(module.urllib.request, "urlopen", urlopen)
    return requested


def test_downloads_only_matching_wheels_and_clears_stale_output(tmp_path, monkeypatch):
    module = _load_downloader()
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0\n", encoding="ascii")
    output = tmp_path / "wheels"
    output.mkdir()
    (output / "stale.whl").write_bytes(b"stale")
    content = _package(
        [
            "wheels/mssql_python_rs-0.1.0-cp313-cp313-win_amd64.whl",
            "symbols/debug.pdb",
        ]
    )
    requested = _mock_download(monkeypatch, module, content)

    wheels = module.download_wheels("https://example.test/index.json", version_file, output)

    assert [wheel.name for wheel in wheels] == ["mssql_python_rs-0.1.0-cp313-cp313-win_amd64.whl"]
    assert sorted(path.name for path in output.iterdir()) == [wheels[0].name]
    assert requested == [
        (
            "https://example.test/flat/mssql-python-rs-wheels/0.1.0/"
            "mssql-python-rs-wheels.0.1.0.nupkg",
            120,
        )
    ]


def test_rejects_wheel_with_unexpected_version(tmp_path, monkeypatch):
    module = _load_downloader()
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    content = _package(["wheels/mssql_python_rs-0.1.1-cp313-cp313-win_amd64.whl"])
    _mock_download(monkeypatch, module, content)

    with pytest.raises(ValueError, match="Unexpected wheel name"):
        module.download_wheels("https://example.test/index.json", version_file, tmp_path / "out")


def test_rejects_duplicate_wheel_filename(tmp_path, monkeypatch):
    module = _load_downloader()
    version_file = tmp_path / "version"
    version_file.write_text("0.1.0", encoding="ascii")
    wheel = "wheels/mssql_python_rs-0.1.0-cp313-cp313-win_amd64.whl"
    with pytest.warns(UserWarning, match="Duplicate name"):
        content = _package([wheel, wheel])
    _mock_download(monkeypatch, module, content)

    with pytest.raises(ValueError, match="Duplicate wheel filename"):
        module.download_wheels("https://example.test/index.json", version_file, tmp_path / "out")

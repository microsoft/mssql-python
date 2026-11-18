import io
import json
import subprocess
import sys
import urllib.error
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if not all(
    (ROOT / "eng" / "scripts" / name).is_file()
    for name in (
        "resolve_pr_rs_runtime.py",
        "download_mssql_python_rs_wheels.py",
        "mssql_python_rs.py",
        "mssql_python_build_safety.py",
    )
):
    pytest.skip("Nightly selection requires a complete source checkout", allow_module_level=True)

from eng.scripts import resolve_pr_rs_runtime as resolver
from eng.scripts.mssql_python_rs import DEFAULT_INDEX, DEV_INDEX, VERSION_ENV

VERSION = "0.4.0.dev20261026"
DIGEST = "abcdef01" * 8
PLATFORMS = (
    "win_amd64",
    "win_arm64",
    "manylinux_2_28_x86_64",
    "manylinux_2_28_aarch64",
    "manylinux_2_34_x86_64",
    "manylinux_2_34_aarch64",
    "musllinux_1_2_x86_64",
    "musllinux_1_2_aarch64",
    "macosx_15_0_universal2",
)


@pytest.fixture(autouse=True)
def clear_override(monkeypatch):
    monkeypatch.delenv(VERSION_ENV, raising=False)


def _anchor(version=VERSION, platform="win_amd64", *, hash_value=DIGEST, yanked=False):
    name = f"mssql_python_rs-{version}-cp310-abi3-{platform}.whl"
    attribute = ' data-yanked="broken"' if yanked else ""
    fragment = f"#sha256={hash_value}" if hash_value is not None else ""
    return f'<a href="https://files.example.test/{name}{fragment}"{attribute}>{name}</a>'


def _page(version=VERSION, platforms=PLATFORMS, **kwargs):
    return (
        "<html><body>"
        + "".join(_anchor(version, p, **kwargs) for p in platforms)
        + "</body></html>"
    )


def _serve(monkeypatch, page):
    requests = []

    def urlopen(url, timeout):
        requests.append((url, timeout))
        return io.BytesIO(page.encode("utf-8"))

    monkeypatch.setattr(resolver.urllib.request, "urlopen", urlopen)
    return requests


def test_selects_all_nine_platforms_with_hash_and_url_evidence(monkeypatch):
    requests = _serve(monkeypatch, _page())
    selection = resolver.resolve_pr_runtime()
    assert selection.version == VERSION
    assert selection.index_url == DEV_INDEX
    assert selection.channel == "nightly"
    assert selection.version_override is False
    assert resolver.EXPECTED_PLATFORMS == set(PLATFORMS)
    assert len(selection.wheel_sha256) == len(selection.wheel_urls) == 9
    for name, digest in selection.wheel_sha256.items():
        assert digest == DIGEST
        assert selection.wheel_urls[name] == f"https://files.example.test/{name}"
    assert requests == [(DEV_INDEX + "mssql-python-rs/", 120)]


@pytest.mark.parametrize(
    ("versions", "expected"),
    [
        (["99.99.99.dev20261025", VERSION], VERSION),
        (["0.9.9.dev20261026", "0.10.0.dev20261026"], "0.10.0.dev20261026"),
        (["9.0.0.dev20261026", "10.0.0.dev20261026"], "10.0.0.dev20261026"),
        (["0.0.9.dev20261026", "0.0.10.dev20261026"], "0.0.10.dev20261026"),
        ([VERSION, "99.0.0.dev20261026186010", "999.0.0"], VERSION),
        (["0.1.0.dev20240229", "0.1.0.dev20240301"], "0.1.0.dev20240301"),
    ],
)
def test_date_first_then_numeric_base_ordering(versions, expected):
    assert (
        resolver.select_nightly("".join(_page(version) for version in versions)).version == expected
    )


@pytest.mark.parametrize("version", ["1.0.0.dev20260229", "1.0.0.dev20261301", "1.0.0.dev20261000"])
def test_rejects_invalid_calendar_dates(version):
    with pytest.raises(ValueError, match="Invalid nightly calendar date"):
        resolver.select_nightly(_page(version))


@pytest.mark.parametrize(
    "page",
    [
        "",
        "<html><body>Please log in</body></html>",
        _page("0.3.0"),
        _page("0.4.0.dev20261026186010"),
        _page("0.4.0.dev1"),
        _page("0.4.0.dev2026102"),
    ],
)
def test_no_nightly_fails_with_exact_override_remedy(page):
    with pytest.raises(ValueError, match="No scheduled nightly.*MSSQL_PYTHON_RS_VERSION"):
        resolver.select_nightly(page)


@pytest.mark.parametrize(
    "page",
    ['<a href="unfinished', "<a>wheel</a>", _anchor()[:-4], "<a href='x'><a href='y'></a></a>"],
)
def test_rejects_malformed_html(page):
    with pytest.raises(ValueError, match="Malformed wheel index"):
        resolver.select_nightly(page)


def test_rejects_duplicate_href_attributes():
    page = _page().replace('href="', 'href="ignored" href="', 1)
    with pytest.raises(ValueError, match="Malformed wheel index anchor"):
        resolver.select_nightly(page)


def test_http_error_is_visible_and_not_retried(monkeypatch):
    requests = []

    def urlopen(url, timeout):
        requests.append(url)
        raise urllib.error.HTTPError(url, 503, "Unavailable", {}, None)

    monkeypatch.setattr(resolver.urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError, match="503"):
        resolver.resolve_pr_runtime()
    assert requests == [DEV_INDEX + "mssql-python-rs/"]


@pytest.mark.parametrize("yanked", [False, True])
def test_newest_partial_or_yanked_never_falls_back_to_complete_older(yanked):
    page = _page("99.0.0.dev20261025") + _page(platforms=PLATFORMS[:1], yanked=yanked)
    with pytest.raises(ValueError, match="Newest nightly.*(incomplete|yanked)"):
        resolver.select_nightly(page)


def test_fully_yanked_newest_never_falls_back():
    with pytest.raises(ValueError, match="Newest nightly.*yanked"):
        resolver.select_nightly(_page("0.3.0.dev20261025") + _page(yanked=True))


def test_partial_yank_is_not_treated_as_complete():
    page = _page(platforms=PLATFORMS[1:]) + _anchor(yanked=True)
    with pytest.raises(ValueError, match="yanked"):
        resolver.select_nightly(page)


@pytest.mark.parametrize(
    "hash_value", [None, "", "bad", "g" * 64, DIGEST + "&sha256=" + DIGEST, DIGEST + "&sha256="]
)
def test_rejects_missing_invalid_or_duplicate_hashes(hash_value):
    page = _page(platforms=PLATFORMS[1:]) + _anchor(hash_value=hash_value)
    with pytest.raises(ValueError, match="Missing or invalid SHA-256"):
        resolver.select_nightly(page)


def test_uppercase_hash_is_normalized():
    assert set(resolver.select_nightly(_page(hash_value=DIGEST.upper())).wheel_sha256.values()) == {
        DIGEST
    }


def test_duplicate_wheel_links_fail():
    with pytest.raises(ValueError, match="Duplicate wheel filename"):
        resolver.select_nightly(_page() + _anchor())


@pytest.mark.parametrize(
    "replacement",
    [
        "http://files.example.test/",
        "//",
        "https://:443/",
        "https://user:password@files.example.test/",
    ],
)
def test_rejects_invalid_wheel_urls(replacement):
    page = _page().replace("https://files.example.test/", replacement)
    with pytest.raises(ValueError, match="HTTPS"):
        resolver.select_nightly(page)


@pytest.mark.parametrize("index", ["http://example.test/simple/", "https:///simple/"])
def test_rejects_insecure_or_missing_index_host(index):
    with pytest.raises(ValueError, match="HTTPS"):
        resolver.select_nightly(_page(), index)


def test_relative_wheel_urls_resolve_against_package_index():
    selection = resolver.select_nightly(_page().replace("https://files.example.test/", ""))
    assert all(
        url.startswith(DEV_INDEX + "mssql-python-rs/") for url in selection.wheel_urls.values()
    )


@pytest.mark.parametrize(
    "tags", ["cp311-abi3-win_amd64", "cp310-abi3-unexpected", "cp310-cp310-win_amd64"]
)
def test_rejects_unexpected_newest_wheel_tags(tags):
    page = _page().replace("cp310-abi3-win_amd64", tags)
    with pytest.raises(ValueError, match="Unexpected newest nightly wheel tags"):
        resolver.select_nightly(page)


def test_malformed_newest_wheel_name_does_not_fall_back():
    page = _page("0.3.0.dev20261025") + _anchor().replace("-cp310-abi3-win_amd64", "")
    with pytest.raises(ValueError, match="Unexpected newest nightly wheel tags"):
        resolver.select_nightly(page)


def test_rejects_ambiguous_numeric_version_spellings():
    with pytest.raises(ValueError, match="Ambiguous nightly"):
        resolver.select_nightly(_page() + _page("00.4.0.dev20261026"))


@pytest.mark.parametrize("source", ["environment", "argument"])
@pytest.mark.parametrize("version", ["0.3.0", "0.4.0.dev20261026186010", " 0.4.0.dev1 "])
def test_exact_overrides_do_not_access_network(monkeypatch, source, version):
    def unexpected_network(*args, **kwargs):
        pytest.fail("Exact override must not access the network")

    monkeypatch.setattr(resolver.urllib.request, "urlopen", unexpected_network)
    if source == "environment":
        monkeypatch.setenv(VERSION_ENV, version)
    selection = resolver.resolve_pr_runtime(
        **({"version": version} if source == "argument" else {})
    )
    assert selection.version == version.strip()
    assert selection.index_url == (DEV_INDEX if ".dev" in version else DEFAULT_INDEX)
    assert selection.channel == "override"
    assert selection.version_override
    assert selection.wheel_sha256 == selection.wheel_urls == {}


def test_explicit_version_takes_precedence_over_environment(monkeypatch):
    monkeypatch.setenv(VERSION_ENV, "invalid")
    assert resolver.resolve_pr_runtime(version="0.3.0").version == "0.3.0"


@pytest.mark.parametrize(
    "version", ["latest", ">=0.3.0", "0.3.0\n##vso[bad]", "0.3.0;echo bad", "0.3.0rc1"]
)
def test_invalid_override_fails_before_network(monkeypatch, version):
    def unexpected_network(*args, **kwargs):
        pytest.fail("Invalid override must not access the network")

    monkeypatch.setattr(resolver.urllib.request, "urlopen", unexpected_network)
    monkeypatch.setenv(VERSION_ENV, version)
    with pytest.raises(ValueError, match="Invalid MSSQL_PYTHON_RS_VERSION"):
        resolver.resolve_pr_runtime()


@pytest.mark.parametrize("value", ["", "  "])
def test_empty_environment_selects_feed_not_committed_release(monkeypatch, value):
    monkeypatch.setenv(VERSION_ENV, value)
    _serve(monkeypatch, _page())
    assert resolver.resolve_pr_runtime().version == VERSION


def test_explicit_empty_version_selects_nightly_despite_environment(monkeypatch):
    monkeypatch.setenv(VERSION_ENV, "0.3.0")
    _serve(monkeypatch, _page())
    assert resolver.resolve_pr_runtime(version="").version == VERSION


def test_writes_reproducible_selection_files(tmp_path):
    selection = resolver.select_nightly(_page())
    output = resolver.write_selection(selection, tmp_path / "selection")
    assert (output / "mssql-python-rs.version").read_bytes() == (VERSION + "\n").encode("ascii")
    receipt = json.loads((output / "runtime-selection.json").read_text(encoding="utf-8"))
    assert receipt == {
        "version": VERSION,
        "index_url": DEV_INDEX,
        "channel": "nightly",
        "version_override": False,
        "wheel_sha256": selection.wheel_sha256,
        "wheel_urls": selection.wheel_urls,
    }
    assert sorted(path.name for path in output.iterdir()) == [
        "mssql-python-rs.version",
        "runtime-selection.json",
    ]


def test_output_directory_uses_shared_safety_helper(monkeypatch):
    monkeypatch.chdir(ROOT)
    with pytest.raises(ValueError, match="current working directory"):
        resolver.write_selection(resolver.select_nightly(_page()), ROOT)


def test_output_artifact_directory_is_rejected_without_partial_write(tmp_path):
    output = tmp_path / "selection"
    (output / "mssql-python-rs.version").mkdir(parents=True)
    with pytest.raises(ValueError, match="Unsafe selection artifact"):
        resolver.write_selection(resolver.select_nightly(_page()), output)
    assert not (output / "runtime-selection.json").exists()


@pytest.mark.parametrize("version", ["bad", "0.3.0\n##vso[bad]", "0.3.0\n"])
def test_artifact_writer_rejects_unvalidated_version(tmp_path, version):
    selection = replace(resolver.select_nightly(_page()), version=version)
    with pytest.raises(ValueError):
        resolver.write_selection(selection, tmp_path / "selection")
    assert not (tmp_path / "selection").exists()


def test_nightly_cli_logs_feed_validation_and_output_variable(monkeypatch, tmp_path, capsys):
    _serve(monkeypatch, _page())
    monkeypatch.setattr(
        sys, "argv", ["resolve_pr_rs_runtime.py", "--output-dir", str(tmp_path / "out")]
    )
    resolver.main()
    stdout = capsys.readouterr().out
    assert "Selected feed-validated nightly:" in stdout
    assert "successful build" not in stdout
    assert f"##vso[task.setvariable variable=version;isOutput=true]{VERSION}\n" in stdout
    assert (tmp_path / "out" / "mssql-python-rs.version").read_text() == VERSION + "\n"


def test_failed_selection_emits_no_output_variable_or_files(monkeypatch, tmp_path, capsys):
    _serve(monkeypatch, _page("0.4.0.dev20261026186010"))
    output = tmp_path / "out"
    monkeypatch.setattr(sys, "argv", ["resolve_pr_rs_runtime.py", "--output-dir", str(output)])
    with pytest.raises(ValueError, match="No scheduled nightly"):
        resolver.main()
    assert "##vso[" not in capsys.readouterr().out
    assert not output.exists()


def test_standalone_cli_supports_override_and_writes_files(tmp_path, monkeypatch):
    monkeypatch.setenv(VERSION_ENV, "0.4.0.dev20261026186010")
    output = tmp_path / "out"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "eng" / "scripts" / "resolve_pr_rs_runtime.py"),
            "--output-dir",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Selected exact override:" in result.stdout
    assert (
        "##vso[task.setvariable variable=version;isOutput=true]0.4.0.dev20261026186010"
        in result.stdout
    )
    assert json.loads((output / "runtime-selection.json").read_text())["channel"] == "override"

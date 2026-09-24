"""Source-only tests of macOS dylib configuration using stubbed native tools."""

from pathlib import Path
import shutil
import subprocess

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "mssql_python" / "pybind" / "configure_dylibs.sh"
if not _SCRIPT.is_file():
    pytest.skip("The dylib configuration script is not shipped in wheels.", allow_module_level=True)


@pytest.fixture
def bash():
    executable = shutil.which("bash")
    if not executable:
        pytest.skip("Dylib configuration tests require Bash.")
    result = subprocess.run(
        [executable, "-c", "command -v sed"], capture_output=True, text=True, timeout=10
    )
    if result.returncode:
        pytest.skip("Dylib configuration tests require Bash with sed.")
    return executable


def _configure(tmp_path, bash, declaration, major="18"):
    root = tmp_path / "project with spaces"
    script = root / "mssql_python" / "pybind" / "configure_dylibs.sh"
    script.parent.mkdir(parents=True)
    shutil.copyfile(_SCRIPT, script)
    provider = root / "mssql_python_odbc"
    provider.mkdir()
    (provider / "__init__.py").write_text(declaration, encoding="utf-8")
    for arch in ("arm64", "x86_64"):
        libraries = provider / "libs" / "macos" / arch / "lib"
        libraries.mkdir(parents=True)
        for name in (f"libmsodbcsql.{major}.dylib", "libodbcinst.2.dylib", "libltdl.7.dylib"):
            (libraries / name).touch()

    wrapper = r"""
record() {
  printf '%s' "$1" >> calls.log
  shift
  printf '\t%s' "$@" >> calls.log
  printf '\n' >> calls.log
}
otool() {
  record otool "$@"
  printf '%s:\n' "$2"
  case "${2##*/}" in
    libmsodbcsql.*.dylib)
      printf '\t/old/libodbcinst.2.dylib (compatibility version 1.0.0)\n' ;;
    libodbcinst.2.dylib)
      printf '\t/old/libltdl.7.dylib (compatibility version 1.0.0)\n' ;;
  esac
}
install_name_tool() { record install_name_tool "$@"; }
codesign() { record codesign "$@"; }
source mssql_python/pybind/configure_dylibs.sh
"""
    result = subprocess.run(
        [bash, "--noprofile", "--norc", "-c", wrapper],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
    )
    log = root / "calls.log"
    calls = [line.split("\t") for line in log.read_text().splitlines()] if log.exists() else []
    return result, calls


@pytest.mark.parametrize(
    "version, major",
    [("18.6.2", "18"), ("18.6.2.1", "18"), ("18.6.2.2", "18"), ("19.1.3.42", "19")],
)
@pytest.mark.parametrize("quote", ['"', "'"])
def test_configure_dylibs_accepts_driver_and_packaging_versions(
    tmp_path, bash, version, major, quote
):
    result, calls = _configure(tmp_path, bash, f"__version__ = {quote}{version}{quote}\n", major)
    assert result.returncode == 0, result.stdout + result.stderr
    for arch in ("arm64", "x86_64"):
        assert f"Library configuration complete for {arch}!" in result.stdout
        for library in (f"libmsodbcsql.{major}.dylib", "libodbcinst.2.dylib", "libltdl.7.dylib"):
            target = f"/libs/macos/{arch}/lib/{library}"
            operations = [call[:-1] for call in calls if call[-1].endswith(target)]
            assert ["install_name_tool", "-id", f"@loader_path/{library}"] in operations
            assert ["codesign", "-s", "-", "-f"] in operations
            dependency = {
                f"libmsodbcsql.{major}.dylib": "libodbcinst.2.dylib",
                "libodbcinst.2.dylib": "libltdl.7.dylib",
            }.get(library)
            if dependency:
                assert [
                    "install_name_tool",
                    "-change",
                    f"/old/{dependency}",
                    f"@loader_path/{dependency}",
                ] in operations
    assert sum(call[0] == "codesign" for call in calls) == 6


@pytest.mark.parametrize(
    "declaration",
    [
        "",
        '__version__ = ""\n',
        '__version__ = "18.6"\n',
        '__version__ = "18.6.2."\n',
        '__version__ = "18.6.2.invalid"\n',
        '__version__ = "18.6.2.2.1"\n',
        "__version__ = 18.6.2.2\n",
    ],
)
def test_configure_dylibs_rejects_invalid_versions_before_native_tools(tmp_path, bash, declaration):
    result, calls = _configure(tmp_path, bash, declaration)
    assert result.returncode == 1
    assert "failed to parse __version__" in result.stdout
    assert calls == []

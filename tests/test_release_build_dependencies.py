import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "refresh-build-dependencies.yml"
RELEASE_PIPELINE = ROOT / "OneBranchPipelines" / "stages" / "build-linux-single-stage.yml"


def test_lock_generation_preserves_python_markers():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for output in (
        "eng/requirements-build-linux.txt",
        "eng/requirements-test-linux.txt",
    ):
        command = next(
            line for line in workflow.splitlines() if "uv pip compile" in line and output in line
        )
        assert "--universal" in command

    assert re.search(r"name: macos\b.*?universal: --universal", workflow, re.DOTALL)
    assert re.search(r"name: windows\b.*?universal: --universal", workflow, re.DOTALL)
    assert re.search(r"name: odbc\b.*?universal: \"\"", workflow, re.DOTALL)
    platform_command = next(
        line
        for line in workflow.splitlines()
        if "uv pip compile" in line and "matrix.output" in line
    )
    assert "${{ matrix.universal }}" in platform_command


def test_asyncio_backport_is_only_installed_below_python_311():
    affected_locks = (
        "requirements-build-windows.txt",
        "requirements-build-macos.txt",
        "requirements-test-linux.txt",
    )

    for lock_name in affected_locks:
        lock = (ROOT / "eng" / lock_name).read_text(encoding="utf-8")
        requirement = next(
            line for line in lock.splitlines() if line.startswith("backports-asyncio-runner==")
        )
        assert re.fullmatch(
            r"backports-asyncio-runner==1\.2\.0 ; "
            r"python_(?:full_)?version < ['\"]3\.11['\"] \\",
            requirement,
        )


def test_isolated_wheel_suite_excludes_release_repository_contracts():
    pipeline = RELEASE_PIPELINE.read_text(encoding="utf-8")

    assert pipeline.count("--ignore=tests/test_release_build_dependencies.py") == 2

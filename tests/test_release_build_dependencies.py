import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "refresh-build-dependencies.yml"
RELEASE_PIPELINE = ROOT / "OneBranchPipelines" / "stages" / "build-linux-single-stage.yml"


def _compile_command(workflow, output):
    commands = [
        line.strip()
        for line in workflow.splitlines()
        if "uv pip compile" in line and output in line
    ]
    assert (
        len(commands) == 1
    ), f"Expected exactly one compile command for {output}, found {len(commands)}: {commands}"
    return commands[0]


def _platform_matrix_item(workflow, name):
    compile_platform = workflow.split("  compile-platform:", maxsplit=1)[1]
    matrix = compile_platform.split("\n    steps:", maxsplit=1)[0]
    marker = f"          - name: {name}\n"
    items = matrix.split(marker)
    assert len(items) == 2, f"Expected exactly one {name} matrix item"
    return items[1].split("\n          - name:", maxsplit=1)[0]


def test_lock_generation_preserves_python_markers():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for output in (
        "eng/requirements-build-linux.txt",
        "eng/requirements-test-linux.txt",
    ):
        assert "--universal" in _compile_command(workflow, output)

    for name, universal in (
        ("macos", "--universal"),
        ("windows", "--universal"),
        ("odbc", '""'),
    ):
        item = _platform_matrix_item(workflow, name)
        values = [line.strip() for line in item.splitlines() if "universal:" in line]
        assert values == [f"universal: {universal}"]

    assert "${{ matrix.universal }}" in _compile_command(workflow, "matrix.output")


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
            r"backports-asyncio-runner==[^ ;]+ ; python_(?:full_)?version < ['\"]3\.11['\"] \\",
            requirement,
        )


def test_isolated_wheel_suite_excludes_release_repository_contracts():
    pipeline = RELEASE_PIPELINE.read_text(encoding="utf-8")

    assert pipeline.count("--ignore=tests/test_release_build_dependencies.py") == 2

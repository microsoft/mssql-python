"""Contracts for the qualified ARM64 emulator and restored shutdown coverage."""

import ast
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).parents[1]
PIPELINE = ROOT / "eng" / "pipelines" / "pr-validation-pipeline.yml"
ARM64_JOBS = ("PytestOnLinux_ARM64", "PytestOnLinux_Alpine_ARM64", "PytestOnLinux_RHEL9_ARM64")
QUALIFIED_IMAGE = (
    "tonistiigi/binfmt@sha256:" "400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0"
)

if not PIPELINE.is_file():
    pytest.skip("ARM64 CI contracts require a complete source checkout", allow_module_level=True)


@pytest.fixture(scope="module")
def pipeline():
    return PIPELINE.read_text(encoding="utf-8")


def test_arm64_emulator_is_pinned_to_the_qualified_image(pipeline):
    values = re.findall(r"^\s+arm64QemuImage:\s*'([^']+)'$", pipeline, re.MULTILINE)
    assert values == [QUALIFIED_IMAGE]


@pytest.mark.parametrize("job", ARM64_JOBS)
def test_each_arm64_setup_replaces_the_handler_and_fails_on_errors(pipeline, job):
    jobs = re.split(r"^- job:\s*", pipeline, flags=re.MULTILINE)[1:]
    block = next(block for block in jobs if block.splitlines()[0] == job)
    setup = block.split("  - script: |", 2)[1]
    assert "retryCountOnTaskFailure:" not in block
    assert "set -euo pipefail" in setup
    assert "multiarch/qemu-user-static" not in setup
    assert "if [ -e /proc/sys/fs/binfmt_misc/qemu-aarch64 ]; then" in setup
    command = 'docker run --rm --privileged --platform linux/amd64 "$(arm64QemuImage)"'
    assert setup.count(f"{command} --uninstall qemu-aarch64") == 1
    assert setup.count(f"{command} --install arm64") == 1
    assert setup.index("--uninstall qemu-aarch64") < setup.index("--install arm64")


@pytest.mark.parametrize(
    "filename, class_name, expected_condition",
    (
        ("test_013_SqlHandle_free_shutdown.py", "TestHandleFreeShutdown", None),
        ("test_024_context_manager_transaction.py", "TestContextManagerCommit", "not CONN_STR"),
    ),
)
def test_shutdown_classes_are_not_skipped_for_emulation(filename, class_name, expected_condition):
    tree = ast.parse((ROOT / "tests" / filename).read_text(encoding="utf-8"))
    node = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    skips = [
        decorator
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr in {"skip", "skipif"}
    ]
    if expected_condition is None:
        assert not skips
    else:
        assert len(skips) == 1 and skips[0].func.attr == "skipif"
        assert ast.dump(skips[0].args[0]) == ast.dump(
            ast.parse(expected_condition, mode="eval").body
        )

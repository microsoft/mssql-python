"""Keep bounded compiler concurrency scoped to PR validation native builds."""

from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "eng/pipelines/pr-validation-pipeline.yml"
if not PIPELINE.is_file():
    pytest.skip("Pipeline sources are not installed in driver wheels", allow_module_level=True)


@pytest.fixture
def jobs():
    pipeline = "\n".join(
        line
        for line in PIPELINE.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    sections = re.split(r"^- job: (\w+)\n", pipeline, flags=re.MULTILINE)
    return dict(zip(sections[1::2], sections[2::2]))


@pytest.mark.parametrize(
    "job,container,expected_build_steps",
    [
        ("CodeQLAnalysis", False, 1),
        ("PytestOnMacOS", False, 1),
        ("PytestOnLinux", True, 2),
        ("PytestOnLinux_ARM64", True, 1),
        ("PytestOnLinux_RHEL9", True, 1),
        ("PytestOnLinux_RHEL9_ARM64", True, 1),
        ("PytestOnLinux_Alpine", True, 1),
        ("PytestOnLinux_Alpine_ARM64", True, 1),
        ("CodeCoverageReport", False, 1),
    ],
)
def test_native_build_steps_receive_bounded_concurrency(jobs, job, container, expected_build_steps):
    steps = re.split(r"^  - ", jobs[job], flags=re.MULTILINE)
    builds = [
        step for step in steps if "./build.sh" in step or "controller --reuse-candidate" in step
    ]
    assert len(builds) == expected_build_steps
    for step in builds:
        assert "\n    env:\n" in step
        assert "\n      CMAKE_BUILD_PARALLEL_LEVEL: $(unixBuildParallelism)" in step
        if container:
            command = step.replace("\\\n", " ")
            assert re.search(r"docker exec\s+-e CMAKE_BUILD_PARALLEL_LEVEL\b", command)


def test_concurrency_is_bounded_and_does_not_change_windows_or_test_execution(jobs):
    pipeline = PIPELINE.read_text(encoding="utf-8")
    assert re.search(r"^  unixBuildParallelism: '2'$", pipeline, re.MULTILINE)
    assert "CMAKE_BUILD_PARALLEL_LEVEL" not in jobs["pytestonwindows"]
    for job in jobs.values():
        for step in re.split(r"^  - ", job, flags=re.MULTILINE):
            if "python -m pytest" in step:
                assert "CMAKE_BUILD_PARALLEL_LEVEL" not in step

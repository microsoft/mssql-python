"""Contracts for Python 3.15 release-wheel production."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_release_pipeline_builds_python_315_wheels():
    pipeline = (ROOT / "OneBranchPipelines" / "build-release-package-pipeline.yml").read_text(
        encoding="utf-8"
    )
    linux = (ROOT / "OneBranchPipelines" / "stages" / "build-linux-single-stage.yml").read_text(
        encoding="utf-8"
    )

    assert pipeline.count("pyVer: '315'") == 3
    assert "Win_py315_x64" in pipeline
    assert "Win_py315_arm64" in pipeline
    assert "MacOS_py315" in pipeline
    assert "cp310 cp311 cp312 cp313 cp314 cp315" in linux


def test_release_pipeline_requires_all_41_wheels():
    consolidate = (
        ROOT / "OneBranchPipelines" / "jobs" / "consolidate-artifacts-job.yml"
    ).read_text(encoding="utf-8")

    assert 'if [ "$WHEEL_COUNT" -ne 41 ]' in consolidate


def test_python_315_uses_dedicated_hashed_locks():
    macos = (ROOT / "OneBranchPipelines" / "stages" / "build-macos-single-stage.yml").read_text(
        encoding="utf-8"
    )
    linux = (ROOT / "OneBranchPipelines" / "stages" / "build-linux-single-stage.yml").read_text(
        encoding="utf-8"
    )

    assert "requirements-build-macos-py315.txt" in macos
    windows = (ROOT / "OneBranchPipelines" / "stages" / "build-windows-single-stage.yml").read_text(
        encoding="utf-8"
    )
    assert "requirements-build-windows-py315.txt" in windows
    assert "requirements-test-linux-py315.txt" in linux

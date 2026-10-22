"""Type-check only mssql_python source and stubs without database operations."""

from pathlib import Path
import subprocess
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = "mssql_python"
pytestmark = pytest.mark.typing


def test_typing(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--config-file=",
            "--strict",
            "--explicit-package-bases",
            "--exclude",
            r"(^|/)build/",
            "--no-incremental",
            "--cache-dir",
            str(tmp_path / "mypy"),
            SOURCE_DIR,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        pytest.fail(
            f"mypy failed for {SOURCE_DIR} (exit {result.returncode}):\n"
            f"{result.stdout}\n{result.stderr}",
            pytrace=False,
        )

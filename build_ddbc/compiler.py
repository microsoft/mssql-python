"""
Core compiler logic for ddbc_bindings.

This module contains the build script execution logic.
Platform detection is provided by mssql_python.platform_utils.
"""

import sys
import subprocess
from pathlib import Path
from typing import Optional

from mssql_python.platform_utils import get_platform_info


def find_pybind_dir() -> Path:
    """Find the pybind directory containing build scripts."""
    # Try relative to this file first (for installed package)
    possible_paths = [
        Path(__file__).parent.parent / "mssql_python" / "pybind",
        Path.cwd() / "mssql_python" / "pybind",
    ]

    # Check for platform-appropriate build script
    build_script = "build.bat" if sys.platform.startswith("win") else "build.sh"

    for path in possible_paths:
        if path.exists() and (path / build_script).exists():
            return path

    raise FileNotFoundError(
        f"Could not find mssql_python/pybind directory with {build_script}. "
        "Make sure you're running from the project root."
    )


def compile_ddbc(
    arch: Optional[str] = None,
    coverage: bool = False,
    verbose: bool = True,
) -> bool:
    """
    Compile ddbc_bindings using the platform-specific build script.

    Args:
        arch: Target architecture (Windows only: x64, x86, arm64)
        coverage: Enable coverage instrumentation (Linux/macOS only)
        verbose: Print build output

    Returns:
        True if build succeeded, False otherwise

    Raises:
        FileNotFoundError: If build script is not found
        RuntimeError: If build fails
    """
    pybind_dir = find_pybind_dir()

    if arch is None:
        arch, _ = get_platform_info()

    if sys.platform.startswith("win"):
        return _run_windows_build(pybind_dir, arch, verbose)
    else:
        return _run_unix_build(pybind_dir, coverage, verbose)


def _run_windows_build(pybind_dir: Path, arch: str, verbose: bool) -> bool:
    """Run build.bat on Windows."""
    build_script = pybind_dir / "build.bat"
    if not build_script.exists():
        raise FileNotFoundError(f"Build script not found: {build_script}")

    cmd = [str(build_script), arch]

    if verbose:
        print(f"[build_ddbc] Running: {' '.join(cmd)}")
        print(f"[build_ddbc] Working directory: {pybind_dir}")

    result = subprocess.run(
        cmd,
        cwd=pybind_dir,
        check=False,
        capture_output=not verbose,
    )

    if result.returncode != 0:
        if not verbose:
            if result.stdout:
                print(result.stdout.decode(), file=sys.stderr)
            if result.stderr:
                print(result.stderr.decode(), file=sys.stderr)
        raise RuntimeError(f"build.bat failed with exit code {result.returncode}")

    if verbose:
        print("[build_ddbc] Windows build completed successfully!")

    return True


def _run_unix_build(pybind_dir: Path, coverage: bool, verbose: bool) -> bool:
    """Run build.sh on macOS/Linux."""
    build_script = pybind_dir / "build.sh"
    if not build_script.exists():
        raise FileNotFoundError(f"Build script not found: {build_script}")

    # Make sure the script is executable
    build_script.chmod(0o755)

    cmd = ["bash", str(build_script)]
    if coverage:
        cmd.append("--coverage")

    if verbose:
        print(f"[build_ddbc] Running: {' '.join(cmd)}")
        print(f"[build_ddbc] Working directory: {pybind_dir}")

    result = subprocess.run(
        cmd,
        cwd=pybind_dir,
        check=False,
        capture_output=not verbose,
    )

    if result.returncode != 0:
        if not verbose:
            if result.stdout:
                print(result.stdout.decode(), file=sys.stderr)
            if result.stderr:
                print(result.stderr.decode(), file=sys.stderr)
        raise RuntimeError(f"build.sh failed with exit code {result.returncode}")

    if verbose:
        print("[build_ddbc] Unix build completed successfully!")

    return True

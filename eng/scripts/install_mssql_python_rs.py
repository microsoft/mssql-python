"""Install the selected runtime, or download a Windows cross-architecture wheel."""

from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
import tempfile
from pathlib import Path

if __package__:
    from .download_mssql_python_rs_wheels import validate_wheel
    from .mssql_python_build_safety import resolve_safe_output_directory
    from .mssql_python_rs import resolve_runtime, verify_installed
else:
    from download_mssql_python_rs_wheels import validate_wheel
    from mssql_python_build_safety import resolve_safe_output_directory
    from mssql_python_rs import resolve_runtime, verify_installed


def install_runtime(target_arch: str = "", output_dir: Path | None = None) -> None:
    runtime = resolve_runtime()
    host = platform.machine().lower()
    aliases = {"amd64": "x64", "x86_64": "x64", "arm64": "arm64", "aarch64": "arm64"}
    host_arch = aliases.get(host, host)
    cross_arch = bool(target_arch and target_arch != host_arch)
    if target_arch and target_arch not in ("x64", "arm64"):
        raise ValueError(f"Unsupported target architecture: {target_arch}")
    if cross_arch and sys.platform != "win32":
        raise ValueError("--target-arch cross-compilation is supported only on Windows")
    parent = None
    if output_dir is not None:
        parent = resolve_safe_output_directory(output_dir)
        parent.mkdir(parents=True, exist_ok=True)
    transport_settings = {"PIP_CERT", "PIP_CLIENT_CERT", "PIP_PROXY", "PIP_TIMEOUT", "PIP_RETRIES"}
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith("PIP_") or key.upper() in transport_settings
    }
    env["PIP_CONFIG_FILE"] = os.devnull
    print(
        f"Selected mssql-python-rs=={runtime.version} from {runtime.index_url} "
        f"(override={runtime.version_override})",
        flush=True,
    )
    with tempfile.TemporaryDirectory(prefix="mssql-python-rs-", dir=parent) as directory:
        command = [
            sys.executable,
            "-m",
            "pip",
            "--disable-pip-version-check",
            "--no-input",
            "download",
            "--only-binary=:all:",
            "--no-deps",
            "--index-url",
            runtime.index_url,
            "--dest",
            directory,
        ]
        if cross_arch:
            command.extend(
                [
                    "--platform",
                    "win_arm64" if target_arch == "arm64" else "win_amd64",
                    "--implementation",
                    "cp",
                    "--python-version",
                    "311",
                    "--abi",
                    "abi3",
                ]
            )
        command.append(f"mssql-python-rs=={runtime.version}")
        subprocess.run(command, env=env, check=True)
        wheels = list(Path(directory).glob("*.whl"))
        if len(wheels) != 1:
            raise ValueError(f"Expected exactly one runtime wheel, found {len(wheels)}")
        validate_wheel(wheels[0], runtime.version)
        if cross_arch:
            print(f"Validated {wheels[0].name}; not installed into the {host_arch} host")
            return
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "--disable-pip-version-check",
                "--no-input",
                "install",
                "--force-reinstall",
                "--no-deps",
                str(wheels[0]),
            ],
            env=env,
            check=True,
        )
    verify_installed(runtime)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-arch", choices=("x64", "arm64"), default="")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    install_runtime(args.target_arch, args.output_dir)


if __name__ == "__main__":
    main()

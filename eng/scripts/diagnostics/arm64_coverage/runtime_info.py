"""Read-only dependency, architecture, and exact native-build identity checks."""

import hashlib
import importlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys

from packaging.requirements import Requirement

ROOT = Path.cwd()
sys.path.insert(0, str(ROOT))


def main():
    if platform.system() != "Linux" or platform.machine().lower() not in ("arm64", "aarch64"):
        raise RuntimeError("This diagnostic requires a Linux ARM64 Python process")
    required = {}
    for line in (ROOT / "requirements.txt").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        requirement = Requirement(line)
        if requirement.marker and not requirement.marker.evaluate():
            continue
        version = importlib.metadata.version(requirement.name)
        if requirement.specifier and version not in requirement.specifier:
            raise RuntimeError(f"Unsatisfied requirement: {requirement}")
        required[requirement.name] = version
    for name in ("pyarrow", "polars", "zstandard", "psutil", "azure.identity", "mssql_py_core"):
        importlib.import_module(name)
    import mssql_python

    expected = json.loads(Path(__file__).with_name("expected.json").read_text())
    for name, digest in expected["protected_sources"].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"Protected source changed during setup: {name}")
    native = sorted((ROOT / "mssql_python").glob("ddbc_bindings*.so"))
    core = sorted((ROOT / "mssql_py_core").glob("*.so"))
    if len(native) != 1 or not core:
        raise RuntimeError("Expected the built ARM64 binding and the normal CI Rust core")
    binaries = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in native + core
    }
    result = {
        "machine": platform.machine(),
        "python": platform.python_version(),
        "executable": sys.executable,
        "libc": platform.libc_ver(),
        "package_origin": mssql_python.__file__,
        "required_distributions": required,
        "installed_distributions": sorted(
            (dist.metadata["Name"], dist.version) for dist in importlib.metadata.distributions()
        ),
        "native_binaries": binaries,
        "protected_sources": expected["protected_sources"],
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

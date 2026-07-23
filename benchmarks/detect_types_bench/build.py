"""Build the three benchmark extensions: raw CPython, pybind11, nanobind.

Run with:  /usr/bin/python build.py
Produces detect_cpython*.so, detect_pybind11*.so, detect_nanobind*.so in the
current directory.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD_DIR = HERE / "build_tmp"

# ---------------------------------------------------------------------------
# Environment discovery
# ---------------------------------------------------------------------------

def python_include() -> str:
    return sysconfig.get_paths()["include"]


def python_ext_suffix() -> str:
    return sysconfig.get_config_var("EXT_SUFFIX") or ".so"


def pybind11_include() -> str:
    import pybind11
    return pybind11.get_include()


def nanobind_paths() -> tuple[str, str]:
    """Returns (include_dir, src_dir with nb_combined.cpp)."""
    import nanobind
    inc = nanobind.include_dir()
    # Nanobind ships nb_combined.cpp under <install>/src/
    src_dir = Path(inc).parent / "src"
    return inc, str(src_dir)


# ---------------------------------------------------------------------------
# Compiler invocation
# ---------------------------------------------------------------------------

BASE_CFLAGS = [
    "-O3",
    "-fPIC",
    "-fvisibility=hidden",
    "-Wall",
    "-fno-plt",
    "-DNDEBUG",
]


def run(cmd: list[str]) -> None:
    print(">>", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)


def compile_object(src: Path, cxx_std: str, extra_includes: list[str], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "g++", "-c", str(src), "-o", str(out),
        f"-std={cxx_std}",
        *BASE_CFLAGS,
        f"-I{python_include()}",
    ]
    for inc in extra_includes:
        cmd.append(f"-I{inc}")
    run(cmd)


def link_shared(objs: list[Path], out: Path) -> None:
    cmd = [
        "g++", "-shared", "-o", str(out),
        *[str(o) for o in objs],
        "-lpthread", "-ldl",
    ]
    run(cmd)


# ---------------------------------------------------------------------------
# Per-target builds
# ---------------------------------------------------------------------------

def build_cpython() -> Path:
    obj = BUILD_DIR / "detect_cpython.o"
    compile_object(HERE / "detect_cpython.cpp", "c++17", [], obj)
    so = HERE / f"detect_cpython{python_ext_suffix()}"
    link_shared([obj], so)
    return so


def build_pybind11() -> Path:
    obj = BUILD_DIR / "detect_pybind11.o"
    compile_object(
        HERE / "detect_pybind11.cpp", "c++17", [pybind11_include()], obj
    )
    so = HERE / f"detect_pybind11{python_ext_suffix()}"
    link_shared([obj], so)
    return so


def build_nanobind() -> Path:
    """Nanobind ships its runtime as nb_combined.cpp which must be compiled
    into every extension. We compile it once and link both objects together.
    """
    nb_inc, nb_src = nanobind_paths()
    nb_combined = Path(nb_src) / "nb_combined.cpp"
    if not nb_combined.exists():
        raise SystemExit(f"nb_combined.cpp not found at {nb_combined}")

    obj_ext = BUILD_DIR / "detect_nanobind_ext.o"
    obj_nb = BUILD_DIR / "detect_nanobind_runtime.o"

    # Include robin_map header vendored by nanobind
    robin_inc = str(Path(nb_inc).parent / "ext" / "robin_map" / "include")

    compile_object(
        HERE / "detect_nanobind.cpp", "c++17",
        [nb_inc, robin_inc], obj_ext,
    )
    compile_object(nb_combined, "c++17", [nb_inc, robin_inc], obj_nb)

    so = HERE / f"detect_nanobind{python_ext_suffix()}"
    link_shared([obj_ext, obj_nb], so)
    return so


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> None:
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True)

    print("=" * 70)
    print("Environment:")
    print(f"  python:        {sys.executable}")
    print(f"  python inc:    {python_include()}")
    print(f"  ext suffix:    {python_ext_suffix()}")
    print(f"  pybind11 inc:  {pybind11_include()}")
    nb_inc, nb_src = nanobind_paths()
    print(f"  nanobind inc:  {nb_inc}")
    print(f"  nanobind src:  {nb_src}")
    print("=" * 70)

    outputs: list[Path] = []
    outputs.append(build_cpython())
    outputs.append(build_pybind11())
    outputs.append(build_nanobind())

    print()
    print("Built:")
    for o in outputs:
        print(f"  {o.name}  ({o.stat().st_size} bytes)")


if __name__ == "__main__":
    main()

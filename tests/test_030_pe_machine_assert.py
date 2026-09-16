"""Unit tests for the Windows PE machine-type assert (eng.conda_tools pe).

The win-arm64 conda package is cross-built on x64 where the arm64 Python can't run, so
the arch is otherwise trusted from the wheel filename. ``assert_pe_machine`` reads the
PE COFF Machine field of every vendored ``.pyd``/``.dll`` and fails if it does not match
the package subdir. These tests exercise the pure PE parser plus a ``.conda`` round-trip.
"""

import io
import json
import os
import shutil
import struct
import subprocess
import sys
import sysconfig
import tarfile
import zipfile
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_TOOLS_DIR = _ROOT / "eng" / "conda_tools"

if not _TOOLS_DIR.is_dir():
    pytest.skip(
        f"Conda tooling source not present ({_TOOLS_DIR}); skipping source-only audit tests",
        allow_module_level=True,
    )

from eng.conda_tools import archive, audit, contracts
from eng.conda_tools.formats import pe

_ARM64 = 0xAA64
_AMD64 = 0x8664
_CORE_INIT = "Lib/site-packages/mssql_py_core/__init__.py"


def _fake_pe(machine: int) -> bytes:
    """A minimal PE32+ with one complete section table entry and in-range raw byte."""
    buf = bytearray(b"\x00" * 0x200)
    buf[0:2] = b"MZ"
    e_lfanew = 0x80
    struct.pack_into("<I", buf, 0x3C, e_lfanew)
    buf[e_lfanew : e_lfanew + 4] = b"PE\x00\x00"
    struct.pack_into("<H", buf, e_lfanew + 4, machine)
    struct.pack_into("<H", buf, e_lfanew + 6, 1)  # NumberOfSections
    optional_size = 0xF0
    struct.pack_into("<H", buf, e_lfanew + 20, optional_size)
    struct.pack_into("<H", buf, e_lfanew + 24, 0x20B)  # PE32+
    section_offset = e_lfanew + 24 + optional_size
    struct.pack_into("<II", buf, section_offset + 16, 1, 0x1F0)
    buf[0x1F0] = 1
    return bytes(buf)


def test_pe_machine_parses_arch():
    assert pe.pe_machine(_fake_pe(_ARM64)) == _ARM64
    assert pe.pe_machine(_fake_pe(_AMD64)) == _AMD64


def test_pe_machine_rejects_non_pe():
    assert pe.pe_machine(b"not a pe file at all, no MZ header") is None
    assert pe.pe_machine(b"MZ") is None  # too short
    # MZ present but the PE signature does not resolve.
    bad = bytearray(b"\x00" * 0x100)
    bad[0:2] = b"MZ"
    struct.pack_into("<I", bad, 0x3C, 0x80)  # e_lfanew points at zeros (no 'PE\0\0')
    assert pe.pe_machine(bytes(bad)) is None


def test_pe_machine_rejects_header_only_pe():
    header_only = bytearray(b"\x00" * 0x100)
    header_only[0:2] = b"MZ"
    struct.pack_into("<I", header_only, 0x3C, 0x80)
    header_only[0x80:0x84] = b"PE\x00\x00"
    struct.pack_into("<H", header_only, 0x84, _ARM64)
    assert pe.pe_machine(bytes(header_only)) is None


def test_pe_machine_rejects_out_of_range_section_data():
    invalid = bytearray(_fake_pe(_ARM64))
    section_offset = 0x80 + 24 + 0xF0
    struct.pack_into("<II", invalid, section_offset + 16, 32, len(invalid) - 1)
    assert pe.pe_machine(bytes(invalid)) is None


def _zstd_available():
    try:
        from compression import zstd  # noqa: F401  # py3.14+

        return True
    except Exception:
        try:
            import zstandard  # noqa: F401

            return True
        except Exception:
            return False


def _zstd_compress(raw: bytes) -> bytes:
    try:
        from compression import zstd  # py3.14+

        return zstd.compress(raw)
    except Exception:
        import zstandard

        return zstandard.ZstdCompressor().compress(raw)


def test_zstd_backend_is_available_for_conda_audit_tests():
    assert _zstd_available(), (
        "reading synthetic .conda packages requires Python 3.14+ compression.zstd or the "
        "declared zstandard test dependency"
    )


def test_binding_wheel_excludes_core_and_requires_source_rs_version(tmp_path):
    pytest.importorskip("setuptools", reason="Wheel archive regression requires setuptools")
    pytest.importorskip("wheel", reason="Wheel archive regression requires the wheel build backend")
    shutil.copy2(_ROOT / "setup.py", tmp_path / "setup.py")
    sources = {
        "PyPI_Description.md": "Packaging fixture",
        "mssql_python/__init__.py": "",
        "mssql_python_odbc/__init__.py": '__version__ = "18.6.2.1"\n',
        "mssql_py_core/__init__.py": "from .mssql_py_core import *\n",
        "eng/versions/mssql-python-rs.version": (
            _ROOT / "eng/versions/mssql-python-rs.version"
        ).read_text(encoding="ascii"),
    }
    for relative, content in sources.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    shutil.copytree(
        _TOOLS_DIR, tmp_path / "eng" / "conda_tools", ignore=shutil.ignore_patterns("__pycache__")
    )
    extensions = (
        "mssql_py_core.cp312-win_arm64.pyd",
        "mssql_py_core.cpython-312-x86_64-linux-gnu.so",
        "mssql_py_core.pyd",
        "mssql_py_core.abi3.so",
    )
    for name in extensions:
        (tmp_path / "mssql_py_core" / name).write_bytes(b"native payload fixture")

    result = subprocess.run(
        [sys.executable, "setup.py", "--quiet", "bdist_wheel"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wheels = list((tmp_path / "dist").glob("*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as wheel:
        assert not any(name.startswith("eng/") for name in wheel.namelist())
        assert not any(name.startswith("mssql_py_core/") for name in wheel.namelist())
        metadata = next(name for name in wheel.namelist() if name.endswith(".dist-info/METADATA"))
        rs_version = sources["eng/versions/mssql-python-rs.version"].strip()
        distribution = archive.parse_distribution_metadata(wheel.read(metadata))
        assert (
            contracts.exact_dependency_pin(distribution["requires_dist"], "mssql-python-rs")
            == rs_version
        )


@pytest.mark.parametrize("spacing", ["", " "], ids=["compact", "setuptools-spaced"])
def test_binding_rs_requirement_accepts_metadata_spacing(spacing):
    rs_version = (
        (_ROOT / "eng/versions/mssql-python-rs.version").read_text(encoding="ascii").strip()
    )
    metadata = archive.parse_distribution_metadata(
        (
            "Metadata-Version: 2.1\nName: mssql-python\nVersion: 0\n"
            f"Requires-Dist: mssql-python-rs{spacing}=={rs_version}\n"
        ).encode()
    )
    assert (
        contracts.exact_dependency_pin(metadata["requires_dist"], "mssql-python-rs") == rs_version
    )


@pytest.mark.skipif(sys.platform != "win32", reason="Windows recipe requires cmd.exe")
@pytest.mark.parametrize("cross_build", [False, True])
@pytest.mark.parametrize(
    ("rs_owned", "state"),
    [
        (rs_owned, state)
        for rs_owned in (False, True)
        for state in ("valid", "missing", "missing-init", "wrong-tag", "abi3")
    ]
    + [(True, "missing-private-library")],
)
def test_windows_recipe_requires_core_on_both_install_paths(tmp_path, cross_build, rs_owned, state):
    wheels = tmp_path / "wheels"
    wheels.mkdir()
    prefix = tmp_path / "prefix"
    site_packages = prefix / "Lib" / "site-packages"
    tag = f"{sys.version_info.major}{sys.version_info.minor}"
    arch = sysconfig.get_platform().replace("-", "_")
    machine = _ARM64 if arch == "win_arm64" else _AMD64
    core = f"mssql_py_core/mssql_py_core.cp{tag}-{arch}.pyd"
    payload = {
        "mssql_python/__init__.py": b"",
        f"mssql_python/ddbc_bindings.cp{tag}-{arch}.pyd": _fake_pe(machine),
        "mssql_py_core/__init__.py": b"from .mssql_py_core import *\n",
        core: _fake_pe(machine),
    }
    if state == "missing":
        del payload[core]
    elif state == "missing-init":
        del payload["mssql_py_core/__init__.py"]
    elif state == "wrong-tag":
        payload[core.replace(f".cp{tag}-", ".cp999-")] = payload.pop(core)
    elif state == "abi3":
        payload["mssql_py_core/mssql_py_core.pyd"] = payload.pop(core)
    if rs_owned:
        rs_payload = {
            name: payload.pop(name) for name in list(payload) if name.startswith("mssql_py_core/")
        }
        if state != "missing-private-library":
            private_arch = "arm64" if arch == "win_arm64" else "x64"
            rs_payload[f"mssql_py_core/libs/windows/{private_arch}/mssqlodbc.dll"] = _fake_pe(
                machine
            )
        rs_info = "mssql_python_rs-0.1.0.dist-info"
        rs_payload[f"{rs_info}/METADATA"] = b"Name: mssql-python-rs\nVersion: 0.1.0\n"
        rs_payload[f"{rs_info}/WHEEL"] = (
            f"Wheel-Version: 1.0\nTag: cp{tag}-cp{tag}-{arch}\n".encode()
        )
        rs_payload[f"{rs_info}/RECORD"] = "\n".join(f"{name},," for name in rs_payload).encode()
        rs_name = f"mssql_python_rs-0.1.0-cp{tag}-cp{tag}-{arch}.whl"
        with zipfile.ZipFile(wheels / rs_name, "w") as wheel:
            for name, data in rs_payload.items():
                wheel.writestr(name, data)
        (wheels / f"rs-wheel-cp{tag}.txt").write_text(rs_name + "\n", newline="\n")
    dist_info = "mssql_python-1.13.0.dist-info"
    payload[f"{dist_info}/METADATA"] = (
        b"Metadata-Version: 2.1\nName: mssql-python\nVersion: 1.13.0\n"
        + (b"Requires-Dist: mssql-python-rs==0.1.0\n" if rs_owned else b"")
    )
    payload[f"{dist_info}/WHEEL"] = (
        f"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: cp{tag}-cp{tag}-{arch}\n".encode()
    )
    payload[f"{dist_info}/RECORD"] = "\n".join(f"{name},," for name in payload).encode()
    with zipfile.ZipFile(wheels / f"mssql_python-1.13.0-cp{tag}-cp{tag}-{arch}.whl", "w") as wheel:
        for name, data in payload.items():
            wheel.writestr(name, data)
    with zipfile.ZipFile(wheels / f"mssql_python_odbc-18.6.2.1-py3-none-{arch}.whl", "w") as wheel:
        wheel.writestr("mssql_python_odbc/__init__.py", "")
    env = dict(
        os.environ,
        PREFIX=str(prefix),
        PYTHON=str(tmp_path / "nonexecutable-python") if cross_build else sys.executable,
        PKG_NAME="mssql-python",
        PKG_VERSION="1.13.0",
        CONDA_PY=tag,
        target_platform="win-arm64" if arch == "win_arm64" else "win-64",
        WHEELS_DIR=str(wheels),
        MSSQL_ODBC_VERSION="18.6.2.1",
        MSSQL_RS_VERSION="0.1.0" if rs_owned else "",
        PIP_TARGET=str(site_packages),
        PIP_CONFIG_FILE=os.devnull,
        PIP_USER="0",
    )
    result = subprocess.run(
        [
            os.environ["COMSPEC"],
            "/d",
            "/c",
            str(_ROOT / "conda/mssql-python/bld.bat"),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    output = result.stdout + result.stderr
    if state in ("valid", "abi3"):
        assert result.returncode == 0, output
        assert (site_packages / "mssql_python_odbc/__init__.py").is_file()
        if rs_owned:
            assert (site_packages / "mssql_python_rs-0.1.0.dist-info/RECORD").is_file()
            assert (
                site_packages / "mssql_python_rs-0.1.0.dist-info/conda-wheel-source.txt"
            ).read_text().strip() == rs_name
    else:
        assert result.returncode != 0, output
        assert (
            "ERROR: required RS private"
            if state == "missing-private-library"
            else "ERROR: required mssql_py_core"
        ) in output


def _make_conda(tmp_path, subdir, payload, depends=("python_abi 3.12.* *_cp312",)):
    """Build a minimal .conda (info-*.tar.zst + pkg-*.tar.zst) with the given payload files."""
    name = "mssql-python-1.13.0-py312_0"

    pkg_buf = io.BytesIO()
    with tarfile.open(fileobj=pkg_buf, mode="w") as tf:
        for arc, data in payload.items():
            ti = tarfile.TarInfo(arc)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))

    index = {
        "name": "mssql-python",
        "version": "1.13.0",
        "build": "py312_0",
        "subdir": subdir,
        "depends": depends,
    }
    idx = json.dumps(index).encode()
    info_buf = io.BytesIO()
    with tarfile.open(fileobj=info_buf, mode="w") as tf:
        ti = tarfile.TarInfo("info/index.json")
        ti.size = len(idx)
        tf.addfile(ti, io.BytesIO(idx))

    conda_path = tmp_path / f"{name}.conda"
    with zipfile.ZipFile(conda_path, "w") as zf:
        zf.writestr(f"pkg-{name}.tar.zst", _zstd_compress(pkg_buf.getvalue()))
        zf.writestr(f"info-{name}.tar.zst", _zstd_compress(info_buf.getvalue()))
    return str(conda_path)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_malformed_dependencies_are_reported(tmp_path):
    errors = audit.audit_package(
        _make_conda(tmp_path, "win-arm64", {}, depends=None), "pe"
    ).violations
    assert any("malformed" in error and "depends" in error for error in errors)


@pytest.mark.parametrize("member_prefix", ["info-", "pkg-"])
def test_corrupt_zstd_member_is_an_explicit_audit_violation(tmp_path, member_prefix):
    path = _make_conda(tmp_path, "win-arm64", {})
    with zipfile.ZipFile(path) as package:
        contents = {name: package.read(name) for name in package.namelist()}
    with zipfile.ZipFile(path, "w") as package:
        for name, data in contents.items():
            package.writestr(name, b"not a zstd frame" if name.startswith(member_prefix) else data)
    result = audit.audit_package(path, "pe")
    assert any("unreadable/malformed package" in error for error in result.violations)


@pytest.mark.parametrize("component", ["pkg", "info"])
@pytest.mark.parametrize("layout", ["single", "missing", "multiple", "duplicate-name"])
def test_conda_component_cardinality(tmp_path, component, layout):
    path = _make_conda(tmp_path, "win-arm64", {"sentinel.txt": b"payload"})
    with zipfile.ZipFile(path) as package:
        contents = [(member.filename, package.read(member)) for member in package.infolist()]
    name, data = next(item for item in contents if item[0].startswith(f"{component}-"))
    with zipfile.ZipFile(path, "w") as package:
        for member_name, member_data in contents:
            if layout != "missing" or member_name != name:
                package.writestr(member_name, member_data)
        if layout == "multiple":
            package.writestr(f"{component}-extra.tar.zst", data)
        elif layout == "duplicate-name":
            with pytest.warns(UserWarning, match="Duplicate name"):
                package.writestr(name, data)

    def read_component():
        if component == "pkg":
            return list(archive.iter_payload_members(path))
        return archive.read_index(path)

    if layout == "single":
        result = read_component()
        if component == "pkg":
            assert result == [("sentinel.txt", b"payload")]
        else:
            assert result["subdir"] == "win-arm64"
        return
    with pytest.raises(ValueError, match="expected exactly one") as error:
        read_component()
    assert f"{component}-*.tar.zst" in str(error.value)
    assert f"found {0 if layout == 'missing' else 2}" in str(error.value)
    errors = audit.audit_package(path, "pe").violations
    assert any(f"expected exactly one {component}-*.tar.zst" in message for message in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
@pytest.mark.parametrize("metadata_state", ["valid", "missing", "case", "backslash"])
def test_win_arm64_binaries_require_binding_metadata(tmp_path, metadata_state):
    payload = {
        _CORE_INIT: b"from .mssql_py_core import *\n",
        "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
        "Lib/site-packages/mssql_py_core/mssql_py_core.cp312-win_arm64.pyd": _fake_pe(_ARM64),
        "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(_ARM64),
        "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/mssql-auth.dll": _fake_pe(_ARM64),
    }
    root = "Lib/site-packages/"
    prefix = root + "mssql_python-1.13.0.dist-info/"
    if metadata_state != "missing":
        payload[prefix + "METADATA"] = b"Name: mssql-python\nVersion: 1.13.0\n"
        payload[prefix + "RECORD"] = "".join(
            f"{name.removeprefix(root)},,\n" for name in [*payload, prefix + "RECORD"]
        ).encode()
        if metadata_state != "valid":
            payload = {
                (
                    (name.lower() if metadata_state == "case" else name.replace("/", "\\"))
                    if ".dist-info/" in name
                    else name
                ): data
                for name, data in payload.items()
            }
    errors = audit.audit_package(_make_conda(tmp_path, "win-arm64", payload), "pe").violations
    if metadata_state == "valid":
        assert errors == []
    else:
        assert any("binding" in error and "metadata" in error.lower() for error in errors)


@pytest.mark.parametrize(
    "state",
    [
        "valid",
        "missing",
        "missing-init",
        "wrong-arch",
        "wrong-tag",
        "abi3",
        "missing-abi",
        "wrong-abi",
    ],
)
def test_required_core_contract(tmp_path, state):
    pin = "python_abi 3.12.* *_cp312"
    # The observed defaults CP312 host supplied only the Python range, not an ABI export.
    depends = ["vc14_runtime", "python >=3.12,<3.13.0a0", "azure-identity >=1.12.0"]
    if state != "missing-abi":
        depends.append(pin if state != "wrong-abi" else "python_abi 3.12.* *_cp313")
    core = "Lib/site-packages/mssql_py_core/mssql_py_core.cp312-win_arm64.pyd"
    payload = {
        _CORE_INIT: b"from .mssql_py_core import *\n",
        "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
        core: _fake_pe(_ARM64),
        "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(_ARM64),
        "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/mssql-auth.dll": _fake_pe(_ARM64),
    }
    if state == "missing":
        del payload[core]
    elif state == "missing-init":
        del payload[_CORE_INIT]
    elif state == "wrong-arch":
        payload[core] = _fake_pe(_AMD64)
    elif state == "wrong-tag":
        payload[core.replace("312", "311")] = payload.pop(core)
    elif state == "abi3":
        payload[core.replace(".cp312-win_arm64", "")] = payload.pop(core)
    prefix = "Lib/site-packages/mssql_python-1.13.0.dist-info/"
    payload[prefix + "METADATA"] = b"Name: mssql-python\nVersion: 1.13.0\n"
    payload[prefix + "RECORD"] = "".join(
        f"{name.removeprefix('Lib/site-packages/')},,\n" for name in [*payload, prefix + "RECORD"]
    ).encode()
    errors = audit.audit_package(
        _make_conda(tmp_path, "win-arm64", payload, depends=depends), "pe"
    ).violations
    if state in ("valid", "abi3"):
        assert errors == []
    elif state in ("missing-abi", "wrong-abi"):
        assert any("matching normal CPython python_abi pin" in error for error in errors)
    else:
        assert any("mssql_py_core" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_arm64_driver_dlls_in_x64_directory_fail_presence_gate(tmp_path):
    # Correct ARM64 machine fields do not help when the loader searches windows/arm64,
    # while both required DLLs were misplaced under windows/x64.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/x64/msodbcsql18.dll": _fake_pe(
                _ARM64
            ),
            "Lib/site-packages/mssql_python_odbc/libs/windows/x64/mssql-auth.dll": _fake_pe(_ARM64),
        },
    )

    errors = audit.audit_package(p, "pe").violations

    assert any("windows/arm64/msodbcsql18" in error for error in errors)
    assert any("windows/arm64/mssql-auth" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_missing_auth_dll_fails(tmp_path):
    # Binding + core driver present, but mssql-auth.dll missing -> the loader THROWS at connect,
    # so the presence gate (win-arm64's only check) must fail.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(
                _ARM64
            ),
        },
    )
    errors = audit.audit_package(p, "pe").violations
    assert any("mssql-auth" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_arm64_x64_binary_fails(tmp_path):
    # The exact bug this guard exists for: an x64 .pyd inside a win-arm64 package.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_AMD64),
        },
    )
    errors = audit.audit_package(p, "pe").violations
    assert any("amd64" in e and "arm64" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_package_without_native_fails(tmp_path):
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {"Lib/site-packages/mssql_python/__init__.py": b"# pure python, no native binary\n"},
    )
    errors = audit.audit_package(p, "pe").violations
    assert any("no .pyd/.dll" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_missing_driver_dll_fails(tmp_path):
    # Correct-arch binding present, but the vendored ODBC driver DLLs are missing.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {"Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64)},
    )
    errors = audit.audit_package(p, "pe").violations
    assert any("driver DLL" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_missing_binding_fails(tmp_path):
    # Correct-arch driver DLL present, but the native binding .pyd is missing.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(
                _ARM64
            )
        },
    )
    errors = audit.audit_package(p, "pe").violations
    assert any("native binding" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_support_dll_only_fails(tmp_path):
    # Binding + mssql-auth present, but the CORE driver msodbcsql18*.dll is missing -> the
    # presence gate must still fail on the core driver (win-arm64's sole check).
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/mssql-auth.dll": _fake_pe(
                _ARM64
            ),
        },
    )
    errors = audit.audit_package(p, "pe").violations
    assert any("driver DLL" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_non_windows_package_skipped(tmp_path):
    # A linux-64 package has no PE payload -> skipped clean (not failed).
    p = _make_conda(
        tmp_path,
        "linux-64",
        {"lib/python3.12/site-packages/mssql_python/_core.so": b"\x7fELF fake"},
    )
    assert audit.audit_package(p, "pe").violations == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_conda_missing_pkg_payload_fails(tmp_path):
    # A .conda with info/index.json but NO pkg-*.tar.zst payload must FAIL (not silently pass
    # with zero members): iter_payload_members raises, and audit_package -> a violation.
    name = "mssql-python-1.13.0-py312_0"
    index = {
        "name": "mssql-python",
        "version": "1.13.0",
        "build": "py312_0",
        "subdir": "win-arm64",
    }
    info_buf = io.BytesIO()
    with tarfile.open(fileobj=info_buf, mode="w") as tf:
        idx = json.dumps(index).encode()
        ti = tarfile.TarInfo("info/index.json")
        ti.size = len(idx)
        tf.addfile(ti, io.BytesIO(idx))
    conda_path = tmp_path / f"{name}.conda"
    with zipfile.ZipFile(conda_path, "w") as zf:
        zf.writestr(f"info-{name}.tar.zst", _zstd_compress(info_buf.getvalue()))
    errors = audit.audit_package(str(conda_path), "pe").violations
    assert any("pkg-" in e for e in errors)

"""Regression guards for POSIX native-extension hardening."""

import struct
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_CMAKE = _ROOT / "mssql_python" / "pybind" / "CMakeLists.txt"
_PT_DYNAMIC = 2
_PT_GNU_STACK = 0x6474E551
_PT_GNU_RELRO = 0x6474E552
_PF_X = 0x1
_DT_NULL = 0
_DT_BIND_NOW = 24
_DT_FLAGS = 30
_DF_BIND_NOW = 0x8
_DT_FLAGS_1 = 0x6FFFFFFB
_DF_1_NOW = 0x1


def _make_elf64(*, relro=True, bind_now=True, executable_stack=False):
    header_size = 64
    program_header_size = 56
    program_count = 3
    dynamic_offset = header_size + program_header_size * program_count
    dynamic = struct.pack("<qQ", _DT_FLAGS_1, _DF_1_NOW if bind_now else 0)
    dynamic += struct.pack("<qQ", _DT_NULL, 0)
    total_size = dynamic_offset + len(dynamic)

    ident = b"\x7fELF" + bytes([2, 1, 1, 0]) + b"\x00" * 8
    header = ident + struct.pack(
        "<HHIQQQIHHHHHH",
        3,
        62,
        1,
        0,
        header_size,
        0,
        0,
        header_size,
        program_header_size,
        program_count,
        0,
        0,
        0,
    )
    relro_header = struct.pack(
        "<IIQQQQQQ",
        _PT_GNU_RELRO if relro else 1,
        4,
        0,
        0,
        0,
        0,
        0,
        1,
    )
    stack_header = struct.pack(
        "<IIQQQQQQ",
        _PT_GNU_STACK,
        7 if executable_stack else 6,
        0,
        0,
        0,
        0,
        0,
        16,
    )
    dynamic_header = struct.pack(
        "<IIQQQQQQ",
        _PT_DYNAMIC,
        6,
        dynamic_offset,
        dynamic_offset,
        dynamic_offset,
        len(dynamic),
        len(dynamic),
        8,
    )
    result = header + relro_header + stack_header + dynamic_header + dynamic
    assert len(result) == total_size
    return result


def _elf_hardening(data):
    if len(data) < 64 or data[:4] != b"\x7fELF" or data[4] != 2:
        raise ValueError("not a complete ELF file")
    if data[5] not in (1, 2):
        raise ValueError("invalid ELF endianness")

    endian = "<" if data[5] == 1 else ">"
    program_offset = struct.unpack_from(endian + "Q", data, 0x20)[0]
    entry_size = struct.unpack_from(endian + "H", data, 0x36)[0]
    entry_count = struct.unpack_from(endian + "H", data, 0x38)[0]

    if (
        not program_offset
        or not entry_count
        or entry_size < 56
        or program_offset + entry_count * entry_size > len(data)
    ):
        raise ValueError("invalid ELF program headers")

    has_relro = False
    stack_executable = None
    dynamic_segment = None
    for index in range(entry_count):
        offset = program_offset + index * entry_size
        program_type = struct.unpack_from(endian + "I", data, offset)[0]
        flags = struct.unpack_from(endian + "I", data, offset + 4)[0]
        file_offset = struct.unpack_from(endian + "Q", data, offset + 8)[0]
        file_size = struct.unpack_from(endian + "Q", data, offset + 32)[0]
        if file_offset + file_size > len(data):
            raise ValueError("ELF segment extends beyond the file")
        if program_type == _PT_GNU_RELRO:
            has_relro = True
        elif program_type == _PT_GNU_STACK:
            stack_executable = bool(flags & _PF_X)
        elif program_type == _PT_DYNAMIC:
            dynamic_segment = file_offset, file_size

    if dynamic_segment is None:
        raise ValueError("ELF has no PT_DYNAMIC segment")

    dynamic_offset, dynamic_size = dynamic_segment
    dynamic_entry_size = 16
    if dynamic_size % dynamic_entry_size:
        raise ValueError("ELF dynamic segment has a partial entry")

    bind_now = False
    terminated = False
    for offset in range(dynamic_offset, dynamic_offset + dynamic_size, dynamic_entry_size):
        tag = struct.unpack_from(endian + "q", data, offset)[0]
        value = struct.unpack_from(endian + "Q", data, offset + 8)[0]
        if tag == _DT_NULL:
            terminated = True
            break
        bind_now = bind_now or tag == _DT_BIND_NOW
        bind_now = bind_now or tag == _DT_FLAGS and bool(value & _DF_BIND_NOW)
        bind_now = bind_now or tag == _DT_FLAGS_1 and bool(value & _DF_1_NOW)

    if not terminated:
        raise ValueError("ELF dynamic segment lacks DT_NULL")
    return has_relro, bind_now, stack_executable


def test_elf_hardening_parser_reads_program_and_dynamic_flags():
    assert _elf_hardening(_make_elf64()) == (True, True, False)
    assert _elf_hardening(_make_elf64(relro=False)) == (False, True, False)
    assert _elf_hardening(_make_elf64(bind_now=False)) == (True, False, False)
    assert _elf_hardening(_make_elf64(executable_stack=True)) == (True, True, True)


@pytest.mark.skipif(
    not _CMAKE.is_file(),
    reason="requires a source checkout; isolated wheel tests omit the source tree",
)
def test_posix_hardening_flags_are_explicit():
    cmake = _CMAKE.read_text(encoding="utf-8")

    assert "set(CMAKE_BUILD_TYPE Release" in cmake
    assert "-fstack-protector-strong" in cmake
    assert "-U_FORTIFY_SOURCE" in cmake
    assert "DDBC_SUPPORTS_FORTIFY_SOURCE_3" in cmake
    assert "-D_FORTIFY_SOURCE=${DDBC_FORTIFY_LEVEL}" in cmake
    assert "$<NOT:$<CONFIG:Debug>>" in cmake
    assert "if(UNIX AND NOT APPLE)" in cmake
    for flag in ("-Wl,-z,relro", "-Wl,-z,now", "-Wl,-z,noexecstack"):
        assert flag in cmake


@pytest.mark.skipif(sys.platform != "linux", reason="ELF hardening applies to Linux")
def test_linux_extension_has_linker_hardening():
    from mssql_python import ddbc_bindings

    extension = Path(ddbc_bindings.module_path)
    has_relro, bind_now, stack_executable = _elf_hardening(extension.read_bytes())
    assert has_relro, "native extension is missing PT_GNU_RELRO"
    assert bind_now, "native extension is missing immediate binding (BIND_NOW)"
    assert stack_executable is not None, "native extension has no PT_GNU_STACK declaration"
    assert stack_executable is False, "native extension requests an executable stack"

"""Regression guards for native printf-style logging."""

import re
from pathlib import Path

import pytest

_PYBIND_DIR = Path(__file__).resolve().parents[1] / "mssql_python" / "pybind"
_LOGGER_HEADER = _PYBIND_DIR / "logger_bridge.hpp"
_CMAKE = _PYBIND_DIR / "CMakeLists.txt"
pytestmark = pytest.mark.skipif(
    not _PYBIND_DIR.is_dir(),
    reason="requires a source checkout; isolated wheel tests omit the source tree",
)


def _code_without_comments(text):
    text = re.sub(
        r"/\*.*?\*/",
        lambda match: "\n" * match.group().count("\n"),
        text,
        flags=re.DOTALL,
    )
    return "\n".join(
        "" if line.lstrip().startswith("#define LOG") else re.sub(r"//.*", "", line)
        for line in text.splitlines()
    )


def test_native_log_calls_use_literal_format_strings():
    dynamic_calls = []
    pattern = re.compile(r"\bLOG(?:_INFO|_WARNING|_ERROR)?\s*\(\s*(.)")
    paths = (
        path
        for suffix in ("*.cpp", "*.hpp", "*.h")
        for path in _PYBIND_DIR.rglob(suffix)
        if "build" not in path.relative_to(_PYBIND_DIR).parts
    )
    for path in paths:
        code = _code_without_comments(path.read_text(encoding="utf-8"))
        for match in pattern.finditer(code):
            if match.group(1) != '"':
                line_number = code.count("\n", 0, match.start()) + 1
                source_line = code.splitlines()[line_number - 1].strip()
                dynamic_calls.append(
                    f"{path.relative_to(_PYBIND_DIR)}:{line_number}: {source_line}"
                )

    assert not dynamic_calls, f"native LOG calls must use literal format strings: {dynamic_calls}"


def test_logger_bridge_enables_compile_time_format_checks():
    header = _LOGGER_HEADER.read_text(encoding="utf-8")
    cmake = _CMAKE.read_text(encoding="utf-8")

    assert "__attribute__((format(printf, format_index, first_argument)))" in header
    assert "DevSkim: ignore DS154189" in header
    assert "MSSQL_PRINTF_FORMAT(4, 5)" in header
    assert "MSSQL_PRINTF_FORMAT(1, 0)" in header
    assert 'CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"' in cmake
    assert "-Wformat=2" in cmake
    assert "-Werror=format-security" in cmake

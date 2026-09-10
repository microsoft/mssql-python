"""Behavior checks for native build options and built-wheel scan coverage."""

import ctypes
import importlib.util
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "eng/scripts/scan_wheel_binaries.py"
sys.path.insert(0, str(SCRIPT.parent))
try:
    spec = importlib.util.spec_from_file_location("wheel_scan", SCRIPT)
    scan = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scan)
finally:
    sys.path.pop(0)


def wheel_at(path, members):
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return path


def test_inventory_includes_versioned_libraries_resources_and_renamed_binaries(tmp_path):
    members = {
        "package/nested/lib.so.2.1": b"\x7fELF",
        "package/locale/messages.rll": b"MZxx",
        "package/core.pyd": b"MZxx",
        "package/unusual.payload": b"\x7fELF",
        "package/not_native.txt": b"text",
    }
    wheel = wheel_at(tmp_path / "data.whl", members)
    binaries = scan.extract_binaries(wheel, tmp_path / "payload")
    assert sorted(binaries.values()) == ["ELF", "ELF", "PE", "PE"]
    assert len(binaries) == 4


@pytest.mark.parametrize("name", ["../escape.so", "/escape.so", "pkg\\escape.so", "C:escape.so"])
def test_invalid_wheel_paths_fail(tmp_path, name):
    wheel = wheel_at(tmp_path / "data.whl", {name: b"\x7fELF"})
    with pytest.raises(ValueError, match="Invalid wheel member"):
        scan.extract_binaries(wheel, tmp_path / "payload")


@pytest.mark.parametrize("members", [{"empty.py": b""}, {"bad.so.2": b"invalid"}])
def test_missing_or_unrecognized_binaries_fail(tmp_path, members):
    wheel = wheel_at(tmp_path / "data.whl", members)
    with pytest.raises(ValueError, match="No native binaries|Unrecognized native binary"):
        scan.extract_binaries(wheel, tmp_path / "payload")


def report_for(uri):
    return {
        "runs": [
            {
                "invocations": [{"executionSuccessful": True}],
                "results": [
                    {
                        "ruleId": rule,
                        "kind": "pass",
                        "level": "none",
                        "locations": [{"physicalLocation": {"artifactLocation": {"uri": uri}}}],
                    }
                    for rule in ("BA3006", "BA3010", "BA3011")
                ],
            }
        ]
    }


def test_complete_report_passes(tmp_path):
    uri = (tmp_path / "lib.so.2").as_uri()
    report = tmp_path / "scan.sarif"
    report.write_text(json.dumps(report_for(uri)))
    scan.verify_report(report, {uri: "ELF"})


def test_resource_pe_can_pass_without_executable_checks(tmp_path):
    uri = (tmp_path / "messages.rll").as_uri()
    document = report_for(uri)
    document["runs"][0]["results"] = [document["runs"][0]["results"][0]]
    document["runs"][0]["results"][0]["ruleId"] = "BA2009"
    report = tmp_path / "scan.sarif"
    report.write_text(json.dumps(document))
    scan.verify_report(report, {uri: "PE"})


def test_pe_with_only_not_applicable_results_fails(tmp_path):
    uri = (tmp_path / "bad.pyd").as_uri()
    document = report_for(uri)
    for result in document["runs"][0]["results"]:
        result["kind"] = "notApplicable"
    report = tmp_path / "scan.sarif"
    report.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="did not evaluate any PE"):
        scan.verify_report(report, {uri: "PE"})


@pytest.mark.parametrize(
    "change",
    [
        "no_runs",
        "no_invocations",
        "failed_invocation",
        "notification",
        "missing_file",
        "no_results",
        "missing_relro",
        "missing_now",
        "missing_stack",
        "not_applicable",
        "failed_rule",
    ],
)
def test_incomplete_or_failed_reports_fail(tmp_path, change):
    uri = (tmp_path / "lib.so.2").as_uri()
    document = report_for(uri)
    run = document["runs"][0]
    if change == "no_runs":
        document["runs"] = []
    elif change == "no_invocations":
        run["invocations"] = []
    elif change == "failed_invocation":
        run["invocations"][0]["executionSuccessful"] = False
    elif change == "notification":
        run["invocations"][0]["toolExecutionNotifications"] = [{"level": "error"}]
    elif change == "missing_file":
        uri = (tmp_path / "other.so.2").as_uri()
    elif change == "no_results":
        run["results"] = []
    elif change.startswith("missing_"):
        rule = {"missing_relro": "BA3010", "missing_now": "BA3011", "missing_stack": "BA3006"}[
            change
        ]
        run["results"] = [r for r in run["results"] if r["ruleId"] != rule]
    elif change == "not_applicable":
        for result in run["results"]:
            result["kind"] = "notApplicable"
    elif change == "failed_rule":
        run["results"][0].update(kind="fail", level="error")
    report = tmp_path / "scan.sarif"
    report.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        scan.verify_report(report, {uri: "ELF"})


def thin_macho(cpu, flags=0):
    # MH_BUNDLE with a valid LC_UUID command.
    return (
        struct.pack("<IIIIIIII", 0xFEEDFACF, cpu, 0, 8, 1, 24, flags, 0)
        + struct.pack("<II", 0x1B, 24)
        + bytes(16)
    )


def fat_macho(slices):
    offset = 8 + 20 * len(slices)
    table = b""
    body = b""
    for cpu, data in slices:
        table += struct.pack(">IIIII", cpu, 0, offset, len(data), 0)
        body += data
        offset += len(data)
    return struct.pack(">II", 0xCAFEBABE, len(slices)) + table + body


def test_universal_bundle_without_pie_passes_stack_check(tmp_path):
    path = tmp_path / "bundle.so"
    path.write_bytes(fat_macho([(cpu, thin_macho(cpu)) for cpu in (0x01000007, 0x0100000C)]))
    scan.check_macho_stack(path)


@pytest.mark.parametrize("bad_slice", [0, 1])
def test_executable_stack_in_either_macho_slice_fails(tmp_path, bad_slice):
    path = tmp_path / "bundle.so"
    path.write_bytes(
        fat_macho(
            [
                (cpu, thin_macho(cpu, 0x20000 if index == bad_slice else 0))
                for index, cpu in enumerate((0x01000007, 0x0100000C))
            ]
        )
    )
    with pytest.raises(ValueError, match="Executable Mach-O stack"):
        scan.check_macho_stack(path)


def test_truncated_macho_fails(tmp_path):
    path = tmp_path / "bundle.so"
    path.write_bytes(thin_macho(0x01000007)[:32])
    with pytest.raises(ValueError, match="Malformed Mach-O"):
        scan.check_macho_stack(path)


@pytest.mark.parametrize("count", [0, 1])
def test_missing_wheels_fail_before_scanner_runs(tmp_path, count):
    if count:
        wheel_at(tmp_path / "data.whl", {"lib.so.2": b"\x7fELF"})
    with pytest.raises(ValueError, match="No wheels|Expected 2 wheels"):
        scan.scan_wheels(tmp_path, tmp_path / "work", Path("not-a-scanner"), 2)


def test_scanner_failure_is_not_hidden_by_valid_report(tmp_path, monkeypatch):
    wheel_at(tmp_path / "data.whl", {"lib.so.2": b"\x7fELF"})

    def failing_scanner(command, **kwargs):
        payload = tmp_path / "work/0/lib.so.2"
        report = Path(command[command.index("--output") + 1])
        report.write_text(json.dumps(report_for(payload.as_uri())))
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(scan.subprocess, "run", failing_scanner)
    with pytest.raises(ValueError, match="exit code 1"):
        scan.scan_wheels(tmp_path, tmp_path / "work", Path("scanner"), 1)


def test_successful_scan_records_inventory_and_supplies_symbols(tmp_path, monkeypatch):
    wheel_at(tmp_path / "data.whl", {"nested/lib.so.2": b"\x7fELF"})
    symbols = tmp_path / "symbols"
    symbols.mkdir()
    (symbols / "binding.pdb").touch()

    def scanner(command, **kwargs):
        assert command[2] == str(tmp_path / "work/0/*")
        assert command[command.index("--recurse") + 1] == "true"
        assert command[command.index("--local-symbol-directories") + 1] == str(symbols)
        payload = tmp_path / "work/0/nested/lib.so.2"
        report = Path(command[command.index("--output") + 1])
        report.write_text(json.dumps(report_for(payload.as_uri())))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(scan.subprocess, "run", scanner)
    scan.scan_wheels(tmp_path, tmp_path / "work", Path("scanner"), 1, symbols)
    inventory = json.loads((tmp_path / "work/reports/inventory.json").read_text())
    assert len(inventory) == 1
    assert list(inventory[0]["binaries"].values()) == ["ELF"]


def test_zero_exit_without_report_fails(tmp_path, monkeypatch):
    wheel_at(tmp_path / "data.whl", {"lib.so.2": b"\x7fELF"})
    monkeypatch.setattr(
        scan.subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(command, 0)
    )
    with pytest.raises(ValueError):
        scan.scan_wheels(tmp_path, tmp_path / "work", Path("scanner"), 1)


@pytest.mark.parametrize(
    "configuration,coverage", [(None, False), ("Debug", False), ("Debug", True)]
)
def test_build_entry_point_preserves_requested_configuration(tmp_path, configuration, coverage):
    if sys.platform == "win32":
        pytest.skip("Unix build entry point")
    source = tmp_path / "mssql_python/pybind"
    source.mkdir(parents=True)
    (source / "probe.cpp").write_text("int probe() { return 0; }\n")
    shutil.copy(ROOT / "mssql_python/pybind/build.sh", source / "build.sh")
    tools = tmp_path / "tools"
    tools.mkdir()
    cmake = tools / "cmake"
    cmake.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "$@" >> "$BUILD_CALLS"\n'
        'if [ "$1" = "--build" ]; then touch probe.so; fi\n'
    )
    uname = tools / "uname"
    uname.write_text('#!/bin/sh\nif [ "$1" = "-s" ]; then echo Linux; else echo x86_64; fi\n')
    cmake.chmod(0o755)
    uname.chmod(0o755)
    env = dict(os.environ, PATH=f"{tools}{os.pathsep}{os.environ['PATH']}")
    env.pop("CMAKE_BUILD_TYPE", None)
    if configuration:
        env["CMAKE_BUILD_TYPE"] = configuration
    env["CXXFLAGS"] = "-D_FORTIFY_SOURCE=3 -fstack-protector-all"
    env["BUILD_CALLS"] = str(tmp_path / "calls")
    subprocess.run(
        ["bash", "build.sh"] + (["codecov"] if coverage else []),
        cwd=source,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    calls = (tmp_path / "calls").read_text().splitlines()
    expected = configuration or "Release"
    assert f"-DCMAKE_BUILD_TYPE={expected}" in calls
    assert calls[calls.index("--config") + 1] == expected
    if coverage:
        flags = next(arg for arg in calls if arg.startswith("-DCMAKE_CXX_FLAGS="))
        assert "-D_FORTIFY_SOURCE=3 -fstack-protector-all" in flags
        assert "-fprofile-instr-generate -fcoverage-mapping" in flags


@pytest.mark.parametrize(
    "configuration,flags,fortify,optimized",
    [
        ("Release", "", 2, 1),
        ("Debug", "", 0, 0),
        ("Release", "-D_FORTIFY_SOURCE=3 -fstack-protector-all", 3, 1),
    ],
)
def test_native_options_build_real_shared_module(
    tmp_path, configuration, flags, fortify, optimized
):
    if sys.platform not in ("linux", "darwin") or not shutil.which("cmake"):
        pytest.skip("Requires the native GCC/Clang CMake toolchain")
    source = tmp_path / "source"
    source.mkdir()
    module = ROOT / "mssql_python/pybind/cmake/NativeBuildOptions.cmake"
    (source / "CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.15)\nproject(probe CXX)\n"
        "add_library(probe MODULE probe.cpp)\n"
        f'include("{module.as_posix()}")\nddbc_native_build_options(probe)\n'
    )
    (source / "probe.cpp").write_text(
        "#include <cstring>\n"
        'extern "C" int fortify_level() {\n'
        "#ifdef _FORTIFY_SOURCE\nreturn _FORTIFY_SOURCE;\n#else\nreturn 0;\n#endif\n}\n"
        'extern "C" int optimized() {\n'
        "#ifdef __OPTIMIZE__\nreturn 1;\n#else\nreturn 0;\n#endif\n}\n"
        'extern "C" int buffer(const char *s, unsigned n) {\n'
        "char b[64]; memcpy(b, s, n); return b[n - 1]; }\n"
    )
    build = tmp_path / "build"
    subprocess.run(
        [
            "cmake",
            "-S",
            str(source),
            "-B",
            str(build),
            f"-DCMAKE_BUILD_TYPE={configuration}",
            f"-DCMAKE_CXX_FLAGS={flags}",
            "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["cmake", "--build", str(build)], check=True, capture_output=True, text=True)
    binary = build / "libprobe.so"
    library = ctypes.CDLL(str(binary))
    assert library.optimized() == optimized
    if sys.platform == "linux":
        assert library.fortify_level() == fortify
        headers = subprocess.check_output(["readelf", "-W", "-l", str(binary)], text=True)
        stack = next(line for line in headers.splitlines() if "GNU_STACK" in line)
        assert "RWE" not in stack
        assert "GNU_RELRO" in headers
        dynamic = subprocess.check_output(["readelf", "-d", str(binary)], text=True)
        assert "BIND_NOW" in dynamic
        symbols = subprocess.check_output(["readelf", "-Ws", str(binary)], text=True)
        assert "__stack_chk_fail" in symbols
    else:
        scan.check_macho_stack(binary)
    commands = json.loads((build / "compile_commands.json").read_text())
    command = commands[0]["command"]
    if "-fstack-protector-all" in flags:
        assert "-fstack-protector-all" in command
        assert "-fstack-protector-strong" not in command
    else:
        assert "-fstack-protector-strong" in command

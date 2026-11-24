import importlib.util
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


class QualificationTests(unittest.TestCase):
    def test_qualification_plugin_includes_every_marker(self):
        runner = self.load_runner()
        config = SimpleNamespace(option=SimpleNamespace(markexpr="not stress and not typing"))
        runner.pytest_configure(config)
        self.assertEqual(config.option.markexpr, "")

    def load_runner(self, version="18.07.0001", provider="msodbcsql18"):
        expected_server = patch.dict("os.environ", {"MSSQL_PYTHON_EXPECT_SQL_MAJOR": ""})
        expected_server.start()
        self.addCleanup(expected_server.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        libs = Path(temporary.name) / "libs"
        target = libs / "windows" / "x64"
        target.mkdir(parents=True)
        binary = target / "msodbcsql18.dll"
        binary.write_bytes(b"candidate-driver")

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def getinfo(self, key):
                return version if key == 7 else "msodbcsql18.dll"

            def cursor(self):
                return self

            def execute(self, query):
                self.query = query

            def fetchone(self):
                return (17,)

        driver = SimpleNamespace(
            get_native_provider_info=lambda: {"id": provider, "driver_path": str(binary)},
            connect=lambda _: Connection(),
            SQL_DRIVER_VER=7,
            SQL_DRIVER_NAME=6,
        )
        path = Path(__file__).parents[1] / "eng/scripts/run_odbc_187_tests.py"
        spec = importlib.util.spec_from_file_location("candidate_runner", path)
        module = importlib.util.module_from_spec(spec)
        package = SimpleNamespace(__version__="18.7.1.1", get_libs_dir=lambda: str(libs))
        with (
            patch.dict(sys.modules, {"mssql_python": driver, "mssql_python_odbc": package}),
            patch.object(sys, "path", [str(path.parents[2]), *sys.path]),
        ):
            spec.loader.exec_module(module)
        _, url, digest, _ = module.PACKAGES[0]
        (target / "odbc-187-driver.json").write_text(
            json.dumps(
                {
                    "version": module.VERSION,
                    "target": "windows/x64",
                    "source_url": url,
                    "source_sha256": digest,
                    "driver": {
                        "path": binary.name,
                        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    },
                }
            ),
            encoding="utf-8",
        )
        metadata = patch.object(module.importlib.metadata, "version", return_value=module.VERSION)
        metadata.start()
        self.addCleanup(metadata.stop)
        mapped = patch.object(module, "loaded_driver_path", return_value=binary.resolve())
        mapped.start()
        self.addCleanup(mapped.stop)
        return module

    def test_failure_or_crash_does_not_prevent_remaining_files(self):
        runner = self.load_runner()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            tests = root / "tests"
            tests.mkdir()
            for name in ("test_a.py", "test_b.py", "test_c.py"):
                (tests / name).touch()
            results = root / "results"
            arguments = ["runner", "--tests", str(tests), "--results", str(results), "--coverage"]
            with (
                patch.object(sys, "argv", arguments),
                patch.dict("os.environ", {"DB_CONNECTION_STRING": "dummy"}),
                patch.object(runner.platform, "platform", return_value="Test OS"),
                patch.object(
                    runner.subprocess,
                    "run",
                    side_effect=[SimpleNamespace(returncode=code) for code in (-11, 1, 0)],
                ) as execute,
            ):
                self.assertEqual(runner.main(), 1)
            self.assertEqual(execute.call_count, 3)
            summary = json.loads((results / "summary.json").read_text())
            self.assertEqual(summary["files_attempted"], 3)
            self.assertEqual([item["exit_code"] for item in summary["failures"]], [-11, 1])
            for call in execute.call_args_list:
                command = call.args[0]
                self.assertEqual(command[command.index("-m") + 1], "pytest")
                self.assertEqual(command[command.index("-m", 3) + 1], "")
                self.assertNotIn("--maxfail", command)
                self.assertIn("eng.scripts.run_odbc_187_tests", command)
                self.assertIn("--cov-append", command)
                self.assertIn(f"--cov-report=xml:{(results / 'coverage.xml').resolve()}", command)
                self.assertEqual(call.kwargs["env"]["MSSQL_PYTHON_NATIVE_PROVIDER"], "msodbcsql18")
                self.assertTrue(
                    call.kwargs["env"]["MSSQL_PYTHON_ODBC_187_EVIDENCE"].endswith(".driver.json")
                )

    def test_wrong_driver_is_rejected_before_tests_run(self):
        for version, provider in (
            ("18.06.0002", "msodbcsql18"),
            ("18.07.0001", "mssql-odbc"),
            ("18.07.0001.2", "msodbcsql18"),
        ):
            with (
                self.subTest(version=version, provider=provider),
                tempfile.TemporaryDirectory() as temp,
            ):
                runner = self.load_runner(version, provider)
                arguments = ["runner", "--tests", temp, "--results", temp]
                with (
                    patch.object(sys, "argv", arguments),
                    patch.dict("os.environ", {"DB_CONNECTION_STRING": "dummy"}),
                    patch.object(runner.subprocess, "run") as execute,
                ):
                    with self.assertRaises(RuntimeError):
                        runner.main()
                    execute.assert_not_called()

    def test_wrong_package_version_is_rejected(self):
        runner = self.load_runner()
        with patch.object(runner.importlib.metadata, "version", return_value="18.6.2.2"):
            with self.assertRaisesRegex(RuntimeError, "Expected mssql-python-odbc"):
                runner.verify_candidate(Path("unused.json"))

    def test_each_pytest_process_rejects_a_changed_driver(self):
        runner = self.load_runner()
        binary = Path(runner.mssql_python.get_native_provider_info()["driver_path"])
        binary.write_bytes(b"old-driver")
        with (
            patch.dict("os.environ", {"MSSQL_PYTHON_ODBC_187_EVIDENCE": "unused.json"}),
            patch.object(runner.mssql_python, "connect") as connect,
        ):
            with self.assertRaisesRegex(RuntimeError, "does not match its staged binary"):
                runner.pytest_sessionstart(None)
            connect.assert_not_called()

    def test_driver_outside_candidate_package_is_rejected(self):
        runner = self.load_runner()
        with patch.object(
            runner.mssql_python,
            "get_native_provider_info",
            return_value={
                "id": "msodbcsql18",
                "driver_path": str(Path("system-driver.dll").resolve()),
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "outside the candidate package"):
                runner.verify_candidate(Path("unused.json"))

    def test_os_mapping_must_match_the_staged_driver(self):
        runner = self.load_runner()
        with (
            patch.dict("os.environ", {"DB_CONNECTION_STRING": "dummy"}),
            patch.object(
                runner, "loaded_driver_path", return_value=Path("other-driver.dll").resolve()
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "not the staged driver"):
                runner.verify_candidate(Path("unused.json"))

    def test_sql_server_matrix_identity_is_checked(self):
        runner = self.load_runner()
        with (
            patch.dict(
                "os.environ",
                {"DB_CONNECTION_STRING": "dummy", "MSSQL_PYTHON_EXPECT_SQL_MAJOR": "16"},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "Expected SQL Server major 16, found 17"):
                runner.verify_candidate(Path("unused.json"))

    def test_changed_resource_is_rejected_before_connecting(self):
        runner = self.load_runner()
        binary = Path(runner.mssql_python.get_native_provider_info()["driver_path"])
        receipt = binary.parent / "odbc-187-driver.json"
        manifest = json.loads(receipt.read_text())
        resource = binary.parent / "1033" / "msodbcsqlr18.rll"
        resource.parent.mkdir()
        resource.write_bytes(b"official-resource")
        manifest["payload_sha256"] = {
            "1033/msodbcsqlr18.rll": hashlib.sha256(resource.read_bytes()).hexdigest()
        }
        receipt.write_text(json.dumps(manifest))
        resource.write_bytes(b"stale-resource")
        with patch.object(runner.mssql_python, "connect") as connect:
            with self.assertRaisesRegex(RuntimeError, "resource or license"):
                runner.verify_candidate(Path("unused.json"))
            connect.assert_not_called()


class ArchiveExtractionTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("cmake"), "CMake is required for the filename contract")
    def test_linux_filename_follows_payload_and_rejects_mixed_sonames(self):
        source = (Path(__file__).parents[1] / "mssql_python/pybind/CMakeLists.txt").read_text(
            encoding="utf-8"
        )
        block = source.split('if(CMAKE_SYSTEM_NAME STREQUAL "Linux")', 1)[1].split(
            "# For macOS, always set universal build", 1
        )[0]
        block = 'if(CMAKE_SYSTEM_NAME STREQUAL "Linux")' + block
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "mssql_python" / "pybind"
            project.mkdir(parents=True)
            libs = root / "mssql_python_odbc" / "libs" / "linux"
            for version, suffix in (("18.6", "2.1"), ("18.7", "1.1")):
                for arch in ("x86_64", "arm64"):
                    directory = libs / "debian_ubuntu" / arch / "lib"
                    directory.mkdir(parents=True, exist_ok=True)
                    (directory / f"libmsodbcsql-{version}.so.{suffix}").touch()
                (project / "CMakeLists.txt").write_text(
                    "cmake_minimum_required(VERSION 3.20)\n"
                    "project(odbc_filename_contract NONE)\n"
                    'set(CMAKE_SYSTEM_NAME "Linux")\n'
                    f'set(MSODBCSQL_VERSION_MAJOR_MINOR "{version}")\n'
                    + block
                    + 'file(WRITE "${CMAKE_BINARY_DIR}/filename.txt" '
                    '"${MSODBCSQL_LINUX_DRIVER_FILENAME}")\n',
                    encoding="utf-8",
                )
                result = subprocess.run(
                    ["cmake", "-S", str(project), "-B", str(root / "build")],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(
                    (root / "build" / "filename.txt").read_text(),
                    f"libmsodbcsql-{version}.so.{suffix}",
                )
            (directory / "libmsodbcsql-18.7.so.2.1").touch()
            result = subprocess.run(
                ["cmake", "-S", str(project), "-B", str(root / "build")],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Expected one Linux ODBC driver filename", result.stderr)

    def test_runtime_registration_is_candidate_only(self):
        path = Path(__file__).parents[1] / "eng/scripts/stage_odbc_187.py"
        spec = importlib.util.spec_from_file_location("candidate_registration", path)
        stager = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stager)
        with (
            patch.dict("os.environ", {"MSSQL_PYTHON_QUALIFY_ODBC_187": "0"}),
            patch.object(stager.subprocess, "run") as execute,
        ):
            stager.register_runtime()
            execute.assert_not_called()
        with (
            patch.dict("os.environ", {"MSSQL_PYTHON_QUALIFY_ODBC_187": "1"}),
            patch.object(stager.subprocess, "run") as execute,
        ):
            stager.register_runtime()
            execute.assert_called_once_with(
                [sys.executable, "setup_odbc.py", "egg_info"], check=True
            )

    def test_license_audit_accepts_utf16_and_rejects_non_license_content(self):
        path = Path(__file__).parents[1] / "eng/scripts/stage_odbc_187.py"
        spec = importlib.util.spec_from_file_location("candidate_license_audit", path)
        stager = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stager)
        with tempfile.TemporaryDirectory() as temp:
            license_path = Path(temp) / "LICENSE.txt"
            license_path.write_text("MICROSOFT LICENSE", encoding="utf-16")
            stager.audit_license(license_path)
            license_path.write_bytes(b"MICROSOFT LICENSE \x92")
            stager.audit_license(license_path)
            license_path.write_text("ChangeLog", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "Missing or corrupted"):
                stager.audit_license(license_path)

    def test_deb_payload_ignores_unix_symlinks_without_invoking_tar(self):
        path = Path(__file__).parents[1] / "eng/scripts/stage_odbc_187.py"
        spec = importlib.util.spec_from_file_location("candidate_stager", path)
        stager = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stager)
        stream = io.BytesIO()
        relative = "opt/microsoft/msodbcsql18/lib64/driver.so"
        with tarfile.open(fileobj=stream, mode="w:xz") as archive:
            member = tarfile.TarInfo(relative)
            member.size = 6
            archive.addfile(member, io.BytesIO(b"driver"))
            link = tarfile.TarInfo("usr/lib/driver.so")
            link.type = tarfile.SYMTYPE
            link.linkname = "/" + relative
            archive.addfile(link)
        payload = stream.getvalue()
        header = (
            f"{'data.tar.xz/':<16}{0:<12}{0:<6}{0:<6}{'100644':<8}{len(payload):<10}`\n"
        ).encode("ascii")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            deb = root / "driver.deb"
            deb.write_bytes(b"!<arch>\n" + header + payload + b"\n" * (len(payload) % 2))
            extracted = root / "extracted"
            extracted.mkdir()
            with patch.object(stager.subprocess, "run") as execute:
                stager.unpack_deb_payload(deb, extracted)
                execute.assert_not_called()
            self.assertEqual((extracted / relative).read_bytes(), b"driver")
            self.assertFalse((extracted / "usr/lib/driver.so").exists())


if __name__ == "__main__":
    unittest.main()

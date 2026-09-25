# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).parents[1]
RUNNER = ROOT / "eng" / "scripts" / "run-mssql-odbc-tests.sh"
PIPELINE = ROOT / "eng" / "pipelines" / "mssql-odbc-daily-validation-pipeline.yml"
PR_PIPELINE = ROOT / "eng" / "pipelines" / "pr-validation-pipeline.yml"
PREFLIGHT = ROOT / "eng" / "scripts" / "verify_mssql_odbc_provider.py"
RS_WHEEL_SELECTOR = ROOT / "eng" / "scripts" / "select_mssql_python_rs_wheel.py"


@unittest.skipUnless(sys.platform.startswith("linux"), "runner requires Linux GNU timeout and bash")
class RunnerTests(unittest.TestCase):
    def run_runner(self, behavior: str, total_budget: str = "60s", provider: str = "mssql-odbc"):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "tests").mkdir()
            (work / "tests" / "test_sample.py").write_text("pass\n", encoding="utf-8")
            fake_bin = work / "bin"
            fake_bin.mkdir()
            fake_python = fake_bin / "python"
            fake_python.write_text(
                "#!/bin/sh\n"
                f"real_python={sys.executable!r}\n"
                'if [ "$1" = "-" ]; then exec "$real_python" "$@"; fi\n'
                "case \"$*\" in *'--version'*) exit 0 ;; esac\n"
                "report=\n"
                'for arg in "$@"; do\n'
                '  case "$arg" in --junitxml=*) report=${arg#--junitxml=} ;; esac\n'
                "done\n"
                f"case {behavior!r} in\n"
                '  success) printf \'%s\\n\' \'<testsuites><testsuite tests="1"><testcase name="ok"/></testsuite></testsuites>\' > "$report"; exit 0 ;;\n'
                "  crash) printf '%s\\n' '<testsuites>' > \"$report\"; exit 139 ;;\n"
                "  missing) exit 0 ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            results = work / "results"
            env = {
                **os.environ,
                "PATH": f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}",
                "TEST_RESULTS_DIR": str(results),
                "PYTEST_FILE_TIMEOUT": "30s",
                "PYTEST_TOTAL_BUDGET": total_budget,
                "MSSQL_PYTHON_NATIVE_PROVIDER": provider,
            }
            proc = subprocess.run(
                ["bash", str(RUNNER)],
                cwd=work,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            status = (results / "runner.status").read_text(encoding="utf-8").strip()
            reports = {
                path.name: ElementTree.parse(path).getroot() for path in results.glob("*.xml")
            }
            return proc, status, reports

    def test_success_writes_authenticated_status(self):
        proc, status, reports = self.run_runner("success")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(status, "success")
        self.assertEqual(set(reports), {"results-test_sample.xml"})

    def test_wrong_provider_is_blocking_harness_failure(self):
        proc, status, reports = self.run_runner("success", provider="msodbcsql18")

        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertEqual(status, "harness")
        self.assertEqual(reports, {})

    def test_budget_exhaustion_is_advisory_not_success(self):
        proc, status, reports = self.run_runner("success", total_budget="1s")

        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertEqual(status, "advisory")
        self.assertEqual(
            reports["results-test_sample.xml"].find(".//error").attrib["type"], "ProcessTerminated"
        )

    def test_crash_replaces_malformed_report_and_adds_process_result(self):
        proc, status, reports = self.run_runner("crash")

        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertEqual(status, "advisory")
        self.assertEqual(
            set(reports),
            {"results-test_sample.xml", "results-test_sample-process.xml"},
        )

    def test_success_without_junit_is_blocking_harness_failure(self):
        proc, status, reports = self.run_runner("missing")

        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertEqual(status, "harness")
        self.assertEqual(reports, {})


class PipelineContractTests(unittest.TestCase):
    def test_runner_uses_busybox_compatible_timeout_options(self):
        runner = RUNNER.read_text(encoding="utf-8")

        self.assertIn('timeout -k "${KILL_GRACE_SECONDS}s"', runner)
        self.assertNotIn("--kill-after", runner)

    def test_optimized_preflight_rejects_wrong_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            fake_module = Path(directory) / "mssql_python.py"
            fake_module.write_text(
                "def get_native_provider_info():\n"
                "    return {'id': 'msodbcsql18', 'package': 'wrong', 'source': 'default'}\n",
                encoding="utf-8",
            )
            env = {**os.environ, "PYTHONPATH": directory, "DB_CONNECTION_STRING": "unused"}

            proc = subprocess.run(
                [sys.executable, "-O", str(PREFLIGHT)],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn("MSSQL_ODBC_PREFLIGHT_OK", proc.stdout)

    def test_pipeline_authenticates_advisory_status_and_keeps_publish_strict(self):
        pipeline = PIPELINE.read_text(encoding="utf-8")

        self.assertIn('if [ "$status" != advisory ]', pipeline)
        self.assertIn('if [ "$status" != success ]', pipeline)
        self.assertNotIn("continueOnError: true", pipeline)
        self.assertIn('exit "$cleanup_rc"', pipeline)
        self.assertIn("grep -q 'MSSQL_ODBC_PREFLIGHT_OK'", pipeline)

    def test_rs_transport_is_pinned(self):
        version = (ROOT / "eng" / "versions" / "mssql-python-rs-nuget.version").read_text(
            encoding="ascii"
        )

        self.assertEqual(version.strip(), "0.2.0-nightly.20260924")

    def test_python_315_validation_adds_preview_matrix_legs(self):
        pipeline = PR_PIPELINE.read_text(encoding="utf-8")
        active_python_versions = {
            line.split(":", 1)[1].strip(" '\"")
            for line in pipeline.splitlines()
            if line.lstrip().startswith("pythonVersion:")
        }

        self.assertEqual(active_python_versions, {"3.13", "3.14", "3.15.0-rc.2"})
        self.assertIn("python:3.15.0rc2-bookworm", pipeline)

    def test_python_315_validation_installs_pyarrow_nightly(self):
        requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")

        self.assertIn("scientific-python-nightly-wheels", requirements)
        self.assertIn('pyarrow==26.0.0.dev296; python_version >= "3.15"', requirements)

    def test_rs_wheel_selection_accepts_abi3_on_newer_python(self):
        with tempfile.TemporaryDirectory() as directory:
            wheels = Path(directory)
            abi3 = wheels / "mssql_python_rs-0.2.0-cp310-abi3-win_amd64.whl"
            abi3.touch()

            selected = subprocess.run(
                [
                    sys.executable,
                    str(RS_WHEEL_SELECTOR),
                    str(wheels),
                    "0.2.0",
                    "cp315",
                    "win_amd64",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(selected.returncode, 0, selected.stderr)
        self.assertEqual(Path(selected.stdout.strip()), abi3)

    def test_rs_wheel_selection_prefers_exact_python_wheel(self):
        with tempfile.TemporaryDirectory() as directory:
            wheels = Path(directory)
            (wheels / "mssql_python_rs-0.2.0-cp310-abi3-win_amd64.whl").touch()
            exact = wheels / "mssql_python_rs-0.2.0-cp315-cp315-win_amd64.whl"
            exact.touch()

            selected = subprocess.run(
                [
                    sys.executable,
                    str(RS_WHEEL_SELECTOR),
                    str(wheels),
                    "0.2.0",
                    "cp315",
                    "win_amd64",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(selected.returncode, 0, selected.stderr)
        self.assertEqual(Path(selected.stdout.strip()), exact)


if __name__ == "__main__":
    unittest.main()

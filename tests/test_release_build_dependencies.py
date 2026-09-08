import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
ENG = ROOT / "eng"
WORKFLOW = ROOT / ".github" / "workflows" / "refresh-build-dependencies.yml"

if not ENG.is_dir() or not (ROOT / "OneBranchPipelines").is_dir():
    pytest.skip(
        "release dependency contracts require a complete source checkout",
        allow_module_level=True,
    )

DIRECT_REQUIREMENTS = {
    "requirements-build-linux": {"pip", "pybind11", "pytest", "setuptools", "wheel"},
    "requirements-build-macos": {"cmake", "cryptography", "pip", "wheel"},
    "requirements-build-odbc": {"build", "pip", "setuptools", "twine", "wheel"},
    "requirements-build-windows": {
        "pip",
        "psutil",
        "pybind11",
        "pyodbc",
        "pytest",
        "setuptools",
        "wheel",
    },
}

PIPELINE_LOCKS = {
    "OneBranchPipelines/stages/build-linux-single-stage.yml": {
        "/workspace/eng/requirements-build-linux.txt": 2,
        "/workspace/eng/requirements-test-linux.txt": 2,
    },
    "OneBranchPipelines/stages/build-macos-single-stage.yml": {
        "eng/requirements-build-macos.txt": 1,
    },
    "OneBranchPipelines/stages/build-odbc-all-stage.yml": {
        "eng/requirements-build-odbc.txt": 1,
    },
    "OneBranchPipelines/stages/build-windows-single-stage.yml": {
        "eng/requirements-build-windows.txt": 1,
    },
}

SUPPORTED_PYTHONS = "3.10 3.11 3.12 3.13 3.14"
LINUX_PLATFORMS = (
    "x86_64-manylinux_2_28 aarch64-manylinux_2_28 "
    "x86_64-unknown-linux-musl aarch64-unknown-linux-musl"
)

VALIDATION_MATRIX = {
    "Linux build": (
        "linux",
        "requirements-build-linux.txt",
        SUPPORTED_PYTHONS,
        LINUX_PLATFORMS,
    ),
    "Linux test": (
        "linux",
        "requirements-test-linux.txt",
        SUPPORTED_PYTHONS,
        LINUX_PLATFORMS,
    ),
    "macOS": (
        "macos",
        "requirements-build-macos.txt",
        SUPPORTED_PYTHONS,
        "x86_64-apple-darwin aarch64-apple-darwin",
    ),
    "Windows build host": (
        "windows",
        "requirements-build-windows.txt",
        SUPPORTED_PYTHONS,
        "x86_64-pc-windows-msvc",
    ),
    "ODBC": (
        "odbc",
        "requirements-build-odbc.txt",
        "3.12",
        "x86_64-pc-windows-msvc",
    ),
}

PIN = re.compile(
    r"(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[^;\s\\]+)" r"(?:\s*;\s*[^\\]+)?\s+\\"
)
HASH = re.compile(r"--hash=sha256:[0-9a-f]{64}(?:\s+\\)?")
ACTION = re.compile(r"^\s*-\s+uses:\s+([^@\s]+)@([^\s#]+)", re.MULTILINE)


def _canonicalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def _active_lines(path):
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _requirement_names(lines):
    return {
        _canonicalize(re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", line).group())
        for line in lines
        if not line.startswith(("-r ", "-c "))
    }


def _lock_versions(path):
    text = path.read_text(encoding="utf-8")
    assert "--index-url" not in text
    assert "--trusted-host" not in text

    versions = {}
    current_name = None
    hashes = []
    continued = False

    def finish_entry():
        if current_name is None:
            return
        assert hashes
        assert len(hashes) == len(set(hashes))
        assert not continued

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if line[0].isspace():
            assert current_name is not None and continued
            match = HASH.fullmatch(stripped)
            assert match
            hashes.append(stripped.removesuffix("\\").rstrip())
            continued = stripped.endswith("\\")
            continue

        finish_entry()
        match = PIN.fullmatch(line)
        assert match
        current_name = _canonicalize(match["name"])
        assert current_name not in versions
        versions[current_name] = match["version"]
        hashes = []
        continued = True

    finish_entry()
    assert versions
    return versions


def _section(text, start, end=None):
    start_index = text.index(start)
    end_index = text.index(end, start_index) if end else len(text)
    return text[start_index:end_index]


def _matrix_item(section, name):
    marker = f"          - name: {name}\n"
    return _section(section, marker).split("\n          - name:", maxsplit=1)[0]


@pytest.fixture(scope="module")
def workflow():
    return WORKFLOW.read_text(encoding="utf-8")


@pytest.mark.parametrize("stem, expected", DIRECT_REQUIREMENTS.items())
def test_platform_locks_cover_their_direct_requirements(stem, expected):
    assert _requirement_names(_active_lines(ENG / f"{stem}.in")) == expected
    assert expected <= _lock_versions(ENG / f"{stem}.txt").keys()


@pytest.mark.parametrize(
    "lock_name",
    [f"{stem}.txt" for stem in DIRECT_REQUIREMENTS] + ["requirements-test-linux.txt"],
)
def test_lockfiles_are_exactly_pinned_and_sha256_hashed(lock_name):
    _lock_versions(ENG / lock_name)


@pytest.mark.parametrize(
    "invalid_lock",
    (
        "demo>=1\n",
        "demo @ https://example.invalid/demo.whl\n",
        f"    --hash=sha256:{'a' * 64}\n",
        f"demo==1 \\\n    --hash=sha512:{'a' * 128}\n",
        "demo==1 \\\nother requirement\n",
        "demo==1\n",
        f"demo==1\n    --hash=sha256:{'a' * 64}\n",
        f"demo==1 \\\n    --hash=sha256:{'a' * 64} \\\n",
        f"demo==1 \\\n    --hash=sha256:{'a' * 64} junk\n",
        f"demo==1 \\\n    --hash=sha256:{'a' * 64} \\\n" f"    --hash=sha256:{'a' * 64}\n",
    ),
)
def test_lockfile_validation_rejects_unpinned_or_malformed_records(tmp_path, invalid_lock):
    lock = tmp_path / "invalid-lock.txt"
    lock.write_text(invalid_lock, encoding="utf-8")

    with pytest.raises(AssertionError):
        _lock_versions(lock)


def test_runtime_and_build_requirements_flow_into_combined_locks():
    runtime = _requirement_names(_active_lines(ROOT / "requirements.txt"))
    linux_build = _lock_versions(ENG / "requirements-build-linux.txt").keys()
    linux_test = _lock_versions(ENG / "requirements-test-linux.txt").keys()
    macos = _lock_versions(ENG / "requirements-build-macos.txt").keys()

    assert _active_lines(ENG / "requirements-test-linux.in") == [
        "-r requirements-build-linux.in",
        "-c requirements-build-linux.txt",
        "-r ../requirements.txt",
    ]
    assert linux_build <= linux_test
    assert runtime <= linux_test
    assert runtime <= macos


def test_macos_lock_preserves_python_310_cryptography_compatibility():
    assert "cryptography<49" in _active_lines(ENG / "requirements-build-macos.in")
    version = _lock_versions(ENG / "requirements-build-macos.txt")["cryptography"]
    assert int(version.split(".", maxsplit=1)[0]) < 49


@pytest.mark.parametrize("pipeline_path, locks", PIPELINE_LOCKS.items())
def test_release_pipelines_only_use_locked_requirements(pipeline_path, locks):
    pipeline = (ROOT / pipeline_path).read_text(encoding="utf-8")
    installs = [
        line.strip()
        for line in pipeline.splitlines()
        if "pip install" in line and not line.lstrip().startswith("#")
    ]

    for lock, expected_count in locks.items():
        matching = [line for line in installs if f"-r {lock}" in line]
        assert len(matching) == expected_count
        assert all("--require-hashes" in line for line in matching)

    for command in installs:
        if " -r " not in command:
            assert '"$WHEEL"' in command or "--no-index --find-links" in command


def test_linux_runtime_lock_is_installed_before_each_product_wheel():
    pipeline = (ROOT / "OneBranchPipelines/stages/build-linux-single-stage.yml").read_text(
        encoding="utf-8"
    )
    runtime_install = (
        "$PY -m pip install -q --require-hashes "
        "-r /workspace/eng/requirements-test-linux.txt;"
    )
    wheel_install = '$PY -m pip install -q "$WHEEL";'
    runtime_positions = [match.start() for match in re.finditer(re.escape(runtime_install), pipeline)]
    wheel_positions = [match.start() for match in re.finditer(re.escape(wheel_install), pipeline)]

    assert len(runtime_positions) == len(wheel_positions) == 2
    assert runtime_positions[0] < wheel_positions[0] < runtime_positions[1] < wheel_positions[1]


def test_refresh_workflow_is_pr_safe_and_immutable(workflow):
    before_pr_job, pr_job = workflow.split("  open-pull-request:", maxsplit=1)
    trigger_paths = (
        ".github/workflows/refresh-build-dependencies.yml",
        "eng/requirements-build-*.in",
        "eng/requirements-build-*.txt",
        "eng/requirements-test-linux.in",
        "eng/requirements-test-linux.txt",
        "requirements.txt",
        "OneBranchPipelines/stages/build-*-single-stage.yml",
        "OneBranchPipelines/stages/build-odbc-all-stage.yml",
    )

    assert "pull_request_target:" not in workflow
    assert all(f"      - {path}" in workflow for path in trigger_paths)
    assert 'cron: "0 8 * * 1"' in workflow
    assert "timezone: America/Los_Angeles" in workflow
    assert "group: refresh-release-build-dependencies" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "permissions:\n  contents: read" in workflow
    assert "contents: write" not in before_pr_job
    assert "if: github.event_name != 'pull_request'" in pr_job
    assert "contents: write" in pr_job
    assert "pull-requests: write" in pr_job
    assert workflow.count("persist-credentials: false") == 2
    assert workflow.count("github.event.pull_request.head.sha || 'main'") == 2

    actions = ACTION.findall(workflow)
    assert actions
    assert all(re.fullmatch(r"[0-9a-f]{40}", revision) for _, revision in actions)


def test_refresh_workflow_compiles_and_verifies_committed_locks(workflow):
    compile_jobs = _section(workflow, "  compile-linux:", "\n  validate-locks:")
    compile_commands = [
        line.strip() for line in compile_jobs.splitlines() if "uv pip compile" in line
    ]
    conditional_upgrade = "${{ github.event_name != 'pull_request' && '--upgrade' || '' }}"
    required_options = (
        conditional_upgrade,
        "--generate-hashes",
        "--no-emit-index-url",
        "--no-header",
        "--strip-extras",
        "--python-version 3.10",
        "--default-index https://pypi.org/simple",
    )

    assert len(compile_commands) == 3
    assert all(
        all(option in command for option in required_options) for command in compile_commands
    )
    assert "uv pip compile --upgrade" not in workflow
    assert "eng/requirements-build-linux.txt eng/requirements-build-linux.in" in compile_commands[0]
    assert "eng/requirements-test-linux.txt eng/requirements-test-linux.in" in compile_commands[1]
    assert '"${{ matrix.output }}" "${{ matrix.input }}"' in compile_commands[2]

    for name, runner, input_path, output_path in (
        (
            "macos",
            "macos-latest",
            "eng/requirements-build-macos.in",
            "eng/requirements-build-macos.txt",
        ),
        (
            "windows",
            "windows-latest",
            "eng/requirements-build-windows.in",
            "eng/requirements-build-windows.txt",
        ),
        (
            "odbc",
            "windows-latest",
            "eng/requirements-build-odbc.in",
            "eng/requirements-build-odbc.txt",
        ),
    ):
        item = _matrix_item(compile_jobs, name)
        assert f"os: {runner}" in item
        assert f"input: {input_path}" in item
        assert f"output: {output_path}" in item

    assert compile_jobs.count("if: github.event_name == 'pull_request'") == 2
    assert "git ls-files --error-unmatch -- eng/requirements-build-linux.txt" in compile_jobs
    assert 'git ls-files --error-unmatch -- "${{ matrix.output }}"' in compile_jobs
    assert "git diff --exit-code -- eng/requirements-build-linux.txt" in compile_jobs
    assert 'git diff --exit-code -- "${{ matrix.output }}"' in compile_jobs


@pytest.mark.parametrize("name, expected", VALIDATION_MATRIX.items())
def test_refresh_workflow_validates_each_release_target(workflow, name, expected):
    validation = _section(workflow, "  validate-locks:", "\n  open-pull-request:")
    artifact, lock, versions, platforms = expected
    item = _matrix_item(validation, name)

    assert f"artifact: {artifact}" in item
    assert f"lock: {lock}" in item
    assert f'versions: "{versions}"' in item
    assert f'platforms: "{platforms}"' in item

    for option in (
        "--dry-run",
        "--no-cache",
        "--only-binary :all:",
        "--require-hashes",
        '--python-version "$version"',
        '--python-platform "$platform"',
    ):
        assert option in validation


def test_refresh_workflow_opens_one_tracked_update_pr(workflow):
    pr_job = _section(workflow, "  open-pull-request:")

    assert "needs: validate-locks" in pr_job
    assert "ref: main" in pr_job
    assert "BUILD_DEPENDENCY_WORK_ITEM:" in pr_job
    assert "> AB#${BUILD_DEPENDENCY_WORK_ITEM}" in pr_job
    assert 'branch="automation/refresh-build-dependencies"' in pr_job
    assert "git diff --quiet -- eng/requirements-build-*.txt" in pr_job
    assert '--force-with-lease="refs/heads/$branch:$remote_sha"' in pr_job
    pr_lookup = (
        'pr_count="$(gh pr list --head "$branch" --base main '
        '--state open --json number --jq length)"'
    )
    assert pr_lookup in pr_job
    assert 'if [ "$pr_count" = "0" ]; then' in pr_job
    assert 'if [ "$(gh pr list' not in pr_job
    assert "gh pr create \\" in pr_job

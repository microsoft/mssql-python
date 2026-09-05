import re
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).parents[1]
ENG_ROOT = REPOSITORY_ROOT / "eng"
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "refresh-build-dependencies.yml"

if not ENG_ROOT.is_dir() or not (REPOSITORY_ROOT / "OneBranchPipelines").is_dir():
    pytest.skip(
        "release dependency contracts require a complete source checkout",
        allow_module_level=True,
    )

LOCK_CASES = {
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

PIPELINE_CASES = (
    (
        "OneBranchPipelines/stages/build-linux-single-stage.yml",
        "/workspace/eng/requirements-build-linux.txt",
        2,
    ),
    (
        "OneBranchPipelines/stages/build-linux-single-stage.yml",
        "/workspace/eng/requirements-test-linux.txt",
        2,
    ),
    (
        "OneBranchPipelines/stages/build-macos-single-stage.yml",
        "eng/requirements-build-macos.txt",
        1,
    ),
    (
        "OneBranchPipelines/stages/build-odbc-all-stage.yml",
        "eng/requirements-build-odbc.txt",
        1,
    ),
    (
        "OneBranchPipelines/stages/build-windows-single-stage.yml",
        "eng/requirements-build-windows.txt",
        1,
    ),
)

LOCK_LINE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[^;\s\\]+)" r"(?:\s*;\s*[^\\]+)?\s*\\?$"
)
HASH = re.compile(r"--hash=sha256:([0-9a-f]{64})(?:\s|\\|$)")
HASH_LINE = re.compile(r"--hash=sha256:[0-9a-f]{64}(?:\s+\\)?$")
ACTION = re.compile(r"^\s*-\s+uses:\s+([^@\s]+)@([^\s#]+)", re.MULTILINE)


def _canonicalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def _active_input_lines(path):
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _requirement_names(lines):
    names = set()
    for line in lines:
        if line.startswith(("-r ", "-c ")):
            continue
        match = re.match(r"([A-Za-z0-9][A-Za-z0-9._-]*)", line)
        assert match is not None, f"Unsupported requirement line: {line}"
        names.add(_canonicalize(match.group(1)))
    return names


def _lock_entries_from_text(text, source):
    entries = {}
    current = None
    block = []
    last_content_line = None

    def finish_entry():
        if current is None:
            return
        name, version = current
        assert not last_content_line.rstrip().endswith(
            "\\"
        ), f"{source}: {name} has an unterminated continuation"
        assert name not in entries, f"{source} contains duplicate entry {name}"
        entries[name] = {"version": version, "text": "\n".join(block)}

    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            if current is not None:
                block.append(line)
            continue

        if line[0].isspace():
            assert current is not None, f"{source}:{line_number} has an orphan continuation"
            assert last_content_line.rstrip().endswith(
                "\\"
            ), f"{source}:{line_number} follows a record without a continuation"
            assert HASH_LINE.fullmatch(
                stripped
            ), f"{source}:{line_number} has unsupported continuation {stripped!r}"
            block.append(line)
            last_content_line = line
            continue

        match = LOCK_LINE.fullmatch(line)
        assert match is not None, f"{source}:{line_number} is not an exact pin: {line!r}"
        finish_entry()
        current = (
            _canonicalize(match.group("name")),
            match.group("version"),
        )
        block = [line]
        last_content_line = line

    finish_entry()
    return entries


def _lock_entries(path):
    return _lock_entries_from_text(path.read_text(encoding="utf-8"), path.name)


def _validate_lock_text(text, source):
    entries = _lock_entries_from_text(text, source)

    assert entries, f"{source} must contain at least one locked dependency"
    assert "--index-url" not in text
    assert "--trusted-host" not in text
    assert "--hash=" not in re.sub(r"--hash=sha256:[0-9a-f]{64}", "", text)

    for name, entry in entries.items():
        hashes = HASH.findall(entry["text"])
        assert hashes, f"{source}: {name} has no SHA-256 hash"
        assert len(hashes) == len(set(hashes)), f"{source}: {name} repeats a hash"

    return entries


def _workflow_matrix_entry(workflow, name):
    marker = f"          - name: {name}\n"
    entry_start = workflow.index(marker)
    next_entry = workflow.find("\n          - name:", entry_start + len(marker))
    return workflow[entry_start : next_entry if next_entry != -1 else None]


@pytest.mark.parametrize("lock_stem, expected_direct", LOCK_CASES.items())
def test_platform_lock_contains_every_direct_requirement(lock_stem, expected_direct):
    input_path = ENG_ROOT / f"{lock_stem}.in"
    lock_path = ENG_ROOT / f"{lock_stem}.txt"

    assert _requirement_names(_active_input_lines(input_path)) == expected_direct
    assert expected_direct <= _lock_entries(lock_path).keys()


def test_linux_test_lock_contains_build_and_runtime_requirements():
    input_lines = _active_input_lines(ENG_ROOT / "requirements-test-linux.in")
    assert input_lines == [
        "-r requirements-build-linux.in",
        "-c requirements-build-linux.txt",
        "-r ../requirements.txt",
    ]

    test_entries = _lock_entries(ENG_ROOT / "requirements-test-linux.txt")
    build_entries = _lock_entries(ENG_ROOT / "requirements-build-linux.txt")
    runtime_requirements = _requirement_names(
        _active_input_lines(REPOSITORY_ROOT / "requirements.txt")
    )

    assert build_entries.keys() <= test_entries.keys()
    assert runtime_requirements <= test_entries.keys()


def test_macos_lock_contains_runtime_requirements():
    input_lines = _active_input_lines(ENG_ROOT / "requirements-build-macos.in")
    assert "-r ../requirements.txt" in input_lines

    runtime_requirements = _requirement_names(
        _active_input_lines(REPOSITORY_ROOT / "requirements.txt")
    )
    assert runtime_requirements <= _lock_entries(ENG_ROOT / "requirements-build-macos.txt").keys()


def test_macos_cryptography_cap_is_preserved_in_lock():
    input_lines = _active_input_lines(ENG_ROOT / "requirements-build-macos.in")
    assert "cryptography<49" in input_lines

    version = _lock_entries(ENG_ROOT / "requirements-build-macos.txt")["cryptography"]["version"]
    assert int(version.split(".", maxsplit=1)[0]) < 49


@pytest.mark.parametrize(
    "lock_name",
    [f"{stem}.txt" for stem in LOCK_CASES] + ["requirements-test-linux.txt"],
)
def test_lockfile_entries_are_exactly_pinned_with_sha256_hashes(lock_name):
    path = ENG_ROOT / lock_name
    _validate_lock_text(path.read_text(encoding="utf-8"), lock_name)


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
    ),
)
def test_lockfile_validation_rejects_unpinned_or_malformed_records(invalid_lock):
    with pytest.raises(AssertionError):
        _validate_lock_text(invalid_lock, "invalid-lock.txt")


@pytest.mark.parametrize("pipeline_path, lock_path, expected_count", PIPELINE_CASES)
def test_release_pipeline_installs_the_expected_hash_locked_file(
    pipeline_path, lock_path, expected_count
):
    pipeline = (REPOSITORY_ROOT / pipeline_path).read_text(encoding="utf-8")
    matching_installs = [
        line
        for line in pipeline.splitlines()
        if "pip install" in line and f"-r {lock_path}" in line
    ]

    assert len(matching_installs) == expected_count
    assert all("--require-hashes" in line for line in matching_installs)


@pytest.mark.parametrize(
    "pipeline_path",
    sorted({case[0] for case in PIPELINE_CASES}),
)
def test_release_pipeline_has_no_unhashed_requirements_install(pipeline_path):
    pipeline = (REPOSITORY_ROOT / pipeline_path).read_text(encoding="utf-8")

    install_commands = [
        line.strip()
        for line in pipeline.splitlines()
        if "pip install" in line and not line.lstrip().startswith("#")
    ]
    assert install_commands

    for command in install_commands:
        if " -r " in command:
            assert "--require-hashes" in command
        else:
            assert '"$WHEEL"' in command or "--no-index --find-links" in command


def test_refresh_workflow_uses_pr_safe_triggers_and_permissions():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    before_pr_job, pr_job = workflow.split("  open-pull-request:", maxsplit=1)

    assert "pull_request_target:" not in workflow
    assert "  pull_request:" in workflow
    for path_filter in (
        ".github/workflows/refresh-build-dependencies.yml",
        "eng/requirements-build-*.in",
        "eng/requirements-build-*.txt",
        "eng/requirements-test-linux.in",
        "eng/requirements-test-linux.txt",
        "requirements.txt",
        "OneBranchPipelines/stages/build-*-single-stage.yml",
        "OneBranchPipelines/stages/build-odbc-all-stage.yml",
    ):
        assert f"      - {path_filter}" in workflow
    assert 'cron: "0 8 * * 1"' in workflow
    assert "timezone: America/Los_Angeles" in workflow
    assert "group: refresh-release-build-dependencies" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "permissions:\n  contents: read" in workflow
    assert "contents: write" not in before_pr_job
    assert "if: github.event_name != 'pull_request'" in pr_job
    assert "contents: write" in pr_job
    assert "pull-requests: write" in pr_job


def test_refresh_workflow_checks_out_the_pr_revision_without_credentials():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    pr_head_ref = (
        "ref: ${{ github.event_name == 'pull_request' "
        "&& github.event.pull_request.head.sha || 'main' }}"
    )

    assert workflow.count(pr_head_ref) == 2
    assert workflow.count("persist-credentials: false") == 2


def test_refresh_workflow_pins_external_actions_to_commit_shas():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    actions = ACTION.findall(workflow)

    assert actions
    for action, revision in actions:
        assert re.fullmatch(
            r"[0-9a-f]{40}", revision
        ), f"{action} must use an immutable commit SHA, got {revision}"


def test_refresh_workflow_compiles_hash_locked_python_310_inputs():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    compile_commands = [line.strip() for line in workflow.splitlines() if "uv pip compile" in line]
    conditional_upgrade = "${{ github.event_name != 'pull_request' && '--upgrade' || '' }}"

    assert len(compile_commands) == 3
    for command in compile_commands:
        for option in (
            conditional_upgrade,
            "--generate-hashes",
            "--no-emit-index-url",
            "--no-header",
            "--strip-extras",
            "--python-version 3.10",
            "--default-index https://pypi.org/simple",
        ):
            assert option in command

    assert "uv pip compile --upgrade" not in workflow
    assert "eng/requirements-build-linux.txt eng/requirements-build-linux.in" in compile_commands[0]
    assert "eng/requirements-test-linux.txt eng/requirements-test-linux.in" in compile_commands[1]
    assert '"${{ matrix.output }}" "${{ matrix.input }}"' in compile_commands[2]


@pytest.mark.parametrize(
    "name, runner, input_path, output_path",
    (
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
    ),
)
def test_refresh_workflow_maps_platform_inputs_to_outputs(name, runner, input_path, output_path):
    entry = _workflow_matrix_entry(WORKFLOW_PATH.read_text(encoding="utf-8"), name)

    assert f"os: {runner}" in entry
    assert f"input: {input_path}" in entry
    assert f"output: {output_path}" in entry


@pytest.mark.parametrize(
    "name, versions, platforms",
    (
        (
            "Linux build",
            "3.10 3.11 3.12 3.13 3.14",
            "x86_64-manylinux_2_28 aarch64-manylinux_2_28 "
            "x86_64-unknown-linux-musl aarch64-unknown-linux-musl",
        ),
        (
            "Linux test",
            "3.10 3.11 3.12 3.13 3.14",
            "x86_64-manylinux_2_28 aarch64-manylinux_2_28 "
            "x86_64-unknown-linux-musl aarch64-unknown-linux-musl",
        ),
        (
            "macOS",
            "3.10 3.11 3.12 3.13 3.14",
            "x86_64-apple-darwin aarch64-apple-darwin",
        ),
        (
            "Windows build host",
            "3.10 3.11 3.12 3.13 3.14",
            "x86_64-pc-windows-msvc",
        ),
        ("ODBC", "3.12", "x86_64-pc-windows-msvc"),
    ),
)
def test_refresh_workflow_validates_the_release_matrix(name, versions, platforms):
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    entry = _workflow_matrix_entry(workflow, name)

    assert f'versions: "{versions}"' in entry
    assert f'platforms: "{platforms}"' in entry


def test_refresh_workflow_validates_binary_hash_compatibility():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    validation_start = workflow.index("      - name: Verify wheels for the release matrix")
    validation_end = workflow.index("\n  open-pull-request:", validation_start)
    validation_step = workflow[validation_start:validation_end]

    for option in (
        "--dry-run",
        "--no-cache",
        "--only-binary :all:",
        "--require-hashes",
        '--python-version "$version"',
        '--python-platform "$platform"',
    ):
        assert option in validation_step


def test_refresh_workflow_verifies_committed_locks_before_uploading():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    linux_tracked = (
        "git ls-files --error-unmatch -- eng/requirements-build-linux.txt "
        "eng/requirements-test-linux.txt"
    )
    linux_diff = (
        "git diff --exit-code -- eng/requirements-build-linux.txt "
        "eng/requirements-test-linux.txt"
    )
    platform_tracked = 'git ls-files --error-unmatch -- "${{ matrix.output }}"'
    platform_diff = 'git diff --exit-code -- "${{ matrix.output }}"'

    assert workflow.count("if: github.event_name == 'pull_request'") == 2
    assert linux_tracked in workflow
    assert linux_diff in workflow
    assert platform_tracked in workflow
    assert platform_diff in workflow
    assert workflow.index(linux_tracked) < workflow.index("name: build-lock-linux")
    assert workflow.index(platform_tracked) < workflow.index("name: build-lock-${{ matrix.name }}")


def test_refresh_workflow_wires_every_compiled_lock_to_validation():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    for name, artifact, lock in (
        ("Linux build", "linux", "requirements-build-linux.txt"),
        ("Linux test", "linux", "requirements-test-linux.txt"),
        ("macOS", "macos", "requirements-build-macos.txt"),
        ("Windows build host", "windows", "requirements-build-windows.txt"),
        ("ODBC", "odbc", "requirements-build-odbc.txt"),
    ):
        entry = _workflow_matrix_entry(workflow, name)
        assert f"artifact: {artifact}" in entry
        assert f"lock: {lock}" in entry

    assert "name: build-lock-linux" in workflow
    assert "name: build-lock-${{ matrix.name }}" in workflow
    assert "pattern: build-lock-*" in workflow


def test_refresh_workflow_validates_the_tracking_work_item():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert workflow.count("BUILD_DEPENDENCY_WORK_ITEM:") == 2
    assert "=~ ^[0-9]+$" in workflow
    assert "> AB#${BUILD_DEPENDENCY_WORK_ITEM}" in workflow


def test_refresh_workflow_updates_one_managed_branch_safely():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    _, pr_job = workflow.split("  open-pull-request:", maxsplit=1)

    assert 'branch="automation/refresh-build-dependencies"' in pr_job
    assert "git diff --quiet -- eng/requirements-build-*.txt" in pr_job
    assert '--force-with-lease="refs/heads/$branch:$remote_sha"' in pr_job
    assert 'gh pr list --head "$branch" --base main --state open' in pr_job
    assert "gh pr create \\" in pr_job

# Contributing to mssql-python

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Before Contributing

### Selecting the Rust Runtime

Normal builds and tests install the exact released `mssql-python-rs` version in
[`eng/versions/mssql-python-rs.version`](eng/versions/mssql-python-rs.version)
from public PyPI. The runtime is a separate dependency, not embedded in the
`mssql-python` wheel. No NuGet transport package is needed.

To test a Python feature against merged but unreleased Rust changes, select the
exact version from a successful `mssql-rs` dev or nightly publishing run:

```powershell
# Windows, with the development environment activated
$env:MSSQL_PYTHON_RS_VERSION = "0.4.0.dev20261028187000"  # Replace with a published build
.\eng\scripts\install-mssql-py-core.ps1
```

```bash
# Linux or macOS, with the development environment activated
export MSSQL_PYTHON_RS_VERSION=0.4.0.dev20261028187000  # Replace with a published build
bash eng/scripts/install-mssql-py-core.sh
```

Keep the environment variable set while building/installing `mssql-python` and
running tests. Its generated dependency metadata then uses the same selected
version. Development versions come from the anonymous Azure Artifacts PyPI
index; clean releases come from public PyPI. A missing requested wheel fails;
the installer never falls back to another version. Cross-architecture Windows
builds validate the target wheel without installing it into the host Python.

Azure build/test pipelines expose the same selection as `rsVersionOverride`.
Leave it empty for the committed release pin. Record the exact dev version in
the feature's PR so another developer can reproduce the tests. Before running
tests after other package installations, verify the runtime has not changed:

```console
python eng/scripts/mssql_python_rs.py --verify
```

Feature-test artifacts are not eligible for public release, even if an override
selects a stable version. Once the required Rust runtime is released on PyPI,
update the committed pin and validate with no override before releasing Python.
Unset `MSSQL_PYTHON_RS_VERSION` to return to the committed release pin and rerun
the installer.

### Install Local Formatting Hooks

From the repository root, with your Python 3.10+ development virtual environment
activated, run this once per clone (including existing clones):

```console
python -m pip install -r requirements-lint.txt
python -m pre_commit install --install-hooks
```

This installs both **pre-commit** and **pre-push** hooks on Windows, macOS, and
Linux. Devcontainers install them automatically. Git does not install hooks when
cloning, and installing Python dependencies alone does not activate them. Rerun
the commands if you recreate your virtual environment.

Copilot setup guidance lives in
[local-formatting.instructions.md](.github/instructions/local-formatting.instructions.md).
It directs Copilot to perform missing setup when starting a local editing task,
subject to tool permissions. Pulling `main` alone does not run setup.

- **On commit:** Black formats staged `.py` and `.pyi` files under `mssql_python`
  and `tests`. If it changes a file, the commit stops; review and stage the fixes,
  then commit again. Unstaged changes are temporarily saved and restored by
  pre-commit.
- **On push:** Black checks both directories in full, without modifying files,
  and blocks the push on formatting errors. This also catches formatting errors
  outside the files changed in your latest commit.
- **In CI:** the same full-directory hook, pinned Black version, and
  `pyproject.toml` settings are used. Flake8, Pylint, mypy, clang-format, and
  cpplint remain informational, matching the existing CI policy.

Run the CI formatting check before opening a PR:

```console
python -m pre_commit run black-check --all-files --hook-stage pre-push
```

To fix formatting, run the pinned formatter, review the changes, and stage the
affected files before retrying your commit or push:

```console
python -m pre_commit run black --all-files
```

The formatter returns a nonzero status when it changes files; rerun after
reviewing the fixes. Hook environments need network access on first installation
and when the pinned version changes. The checks do not need a native build or a
SQL Server.

**Repository enforcement:** local hooks can be bypassed and cannot prevent
someone from opening a GitHub PR. Maintainers must require the **Linting Summary**
status check in the `main` branch ruleset/branch protection, with bypasses
restricted, to prevent merging a PR with failing formatting. The lint workflow
runs for every PR, including documentation-only changes, so a required check is
not left pending by path filters. Hook installation is a contributor setup step,
not a repository-wide setting.

When upgrading Black, update its revision in `.pre-commit-config.yaml` and run
both commands above; local hooks and CI then use the new version together.

### For External Contributors

If you are an external contributor (not a Microsoft organization member), please follow these steps:

1. **Create a GitHub Issue First**: Before submitting a pull request, create a GitHub issue describing the bug, feature request, or improvement you want to contribute.
2. **Link the Issue in Your PR**: When you submit your pull request, please use the PR template and include a link to the GitHub issue in the PR description using the format: `https://github.com/microsoft/mssql-python/issues/XXX`
3. **Follow PR Guidelines**: Ensure your PR title follows the required prefix format (FEAT:, FIX:, DOC:, etc.) and includes a meaningful summary.

### For Microsoft Organization Members

If you are a Microsoft organization member (internal contributor):

1. **Create an ADO Work Item**: Follow your internal process to create an Azure DevOps (ADO) work item.
2. **Link the ADO Work Item**: Include the ADO work item link in your PR description using the format: `https://sqlclientdrivers.visualstudio.com/.../workitems/edit/ID`
3. **Follow PR Guidelines**: Ensure your PR title follows the required prefix format and includes a meaningful summary.

## Pull Request Requirements

All pull requests must include:

- **Valid Title Prefix**: Your PR title must start with one of: `FEAT:`, `CHORE:`, `FIX:`, `DOC:`, `STYLE:`, `REFACTOR:`, `PERF:`, `RELEASE:`, or `AI:`
- **Meaningful Summary**: Include a clear description of your changes under the "### Summary" section in the PR description (minimum 10 characters)
- **Issue/Work Item Link** (only one required): 
  - External contributors: Link to a GitHub issue
  - Microsoft org members: Link to an ADO work item

Use `AI:` for changes to AI tooling, agents, skills, prompts, or AI-assisted
development workflows. It describes the subject of the change, not whether
AI helped write it; ordinary driver fixes and features keep their usual prefixes.

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

## Type Checking

The typing harness checks Python source (`.py`) and stub (`.pyi`) files recursively
under `mssql_python` only, excluding generated `build` directories and their
downloaded third-party sources. `tests` and `mssql_python_odbc` are not targets of
this typing gate. It does not execute the checked code or require a live database;
the existing runtime test jobs are unchanged.

After installing `requirements.txt` and building the native extension using
`.github/prompts/build-ddbc.prompt.md`, run:

```console
python -m pytest tests/test_typing.py -m typing -v
```

The harness runs mypy in strict mode, including checks inside unannotated function
bodies. It reports source, stub, and import errors without suppressing errors in
the driver. Explicit package bases resolve module names from the repository root.
Keep the mypy version pinned in `requirements.txt` aligned
with the existing locked development dependencies.

To run the same package check directly:

```console
python -m mypy --config-file= --strict --explicit-package-bases --exclude "(^|/)build/" --no-incremental mssql_python
```

Private `_ddbc_types.pyi` and `_pycore_types.pyi` declarations describe the native
boundaries; they do not replace or hide the Python implementations from mypy.
Keep these declarations aligned with the C++/Rust APIs, and keep the static
constant declarations aligned with the dynamically exported integer aliases.
The dependency tests check the native export names and constant declaration parity.

The existing required `MSSQL-Python-PR-Validation` pipeline runs the harness once,
in the Ubuntu CodeQL job immediately after its native build. Typing failures fail
the pipeline and block PR merging; no separate GitHub workflow or required check
is needed. The `typing` marker is excluded from default pytest runs so the same
static checks are not repeated across the database/OS matrix.

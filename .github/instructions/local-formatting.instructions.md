---
applyTo: "**"
---

# Local formatting setup

When starting a local editing, commit, or push task, ensure this clone has the
repository's formatting hooks installed. Perform the setup when needed rather
than only suggesting commands. Do not install tools or change hooks for read-only
questions, code reviews, or CI jobs. Respect the user's tool permissions.

These instructions take effect when Copilot works on the checkout. Pulling
`main` does not execute them, and a developer without an active Copilot session
must follow the setup in `CONTRIBUTING.md`.

## One-time setup per clone

1. Use the project's Python 3.10+ virtual environment, or the devcontainer's
   configured Python. If no development environment exists, follow
   `.github/prompts/setup-dev-env.prompt.md` for virtual environment setup first.
   Do not install into an unrelated or global Python environment.
2. Check `python -m pre_commit --version` against `requirements-lint.txt`.
   If the module is missing or its version differs, install it:

   ```console
   python -m pip install -r requirements-lint.txt
   ```

3. Inspect `git config --get core.hooksPath` and resolve the default hook directory
   with `git rev-parse --git-path hooks`; do not assume `.git` is a directory.
   If a custom hooks path is configured, stop and ask how to integrate with it;
   do not unset it or change global Git settings. Check that both `pre-commit` and
   `pre-push` are pre-commit-managed hooks for `.pre-commit-config.yaml` and use a
   valid development interpreter. If missing or stale, run:

   ```console
   python -m pre_commit install --install-hooks
   ```

   Preserve existing hooks; do not use `--overwrite`. Verify both hooks were
   installed. Skip reinstallation when they are already correct, including in
   devcontainers. Reinstall if the development environment was recreated.
4. Verify the configuration with `python -m pre_commit validate-config`.
   Report installation, network, or environment errors explicitly. Do not claim
   setup succeeded or bypass a failure.

## Before commits, pushes, and PRs

- Let the commit hook format staged Python files. If it changes files, inspect
  the diff and stage only the intended fixes before retrying. Preserve unrelated
  and partially staged edits; never use `git add .` to accept formatter changes.
- Before pushing or opening a PR, run the same full-directory check as CI:

  ```console
  python -m pre_commit run black-check --all-files --hook-stage pre-push
  ```

- To fix reported formatting errors, use
  `python -m pre_commit run black --all-files`. It returns nonzero when it changes
  files. Review the diff, keep unrelated edits out of the commit, and rerun the
  check. Do not commit or push unless the user has authorized those actions.
- Use the pinned hook environment, not a separately installed Black version.
  These formatting checks need neither a native extension build nor SQL Server.
  Keep other linters informational, matching the existing CI policy.
- Never use `--no-verify`, `SKIP`, or configuration changes to bypass the checks.
  Local hooks cannot enforce PR creation or merging: requiring **Linting Summary**
  in branch protection is a separate administrator action, not part of setup.

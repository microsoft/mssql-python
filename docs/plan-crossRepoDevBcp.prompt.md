## Plan: Cross-Repo Development for mssql-python + mssql-tds BCP

Enable parallel development across `mssql-python` (GitHub, `public` ADO project) and `mssql-tds` (ADO repo, `mssql-rs` project) with linked PRs for BCP feature implementation using Rust bindings.

### Steps

1. **Add artifact publishing to `mssql-tds` PR pipeline**: Modify [validation-pipeline.yml](../../mssql-tds/.pipeline/validation-pipeline.yml) to publish `mssql_py_core` `.so` files as named pipeline artifacts with PR metadata, e.g., `mssql-py-core-$(Build.BuildId)`. Ensure artifacts are published for PR builds (currently may be filtered).

2. **Create cross-repo artifact download in `mssql-python` PR pipeline**: Add a conditional stage to [pr-validation-pipeline.yml](../eng/pipelines/pr-validation-pipeline.yml) that: parses PR description for `Depends-On: mssql-tds#<PR>`, uses `DownloadPipelineArtifact@2` with `project: mssql-rs` to fetch `.so` artifacts for all platforms (Linux/macOS/Alpine), places them in `mssql_python/` folder. Skip BCP tests if no `Depends-On` found.

3. **Verify cross-project service connection**: Confirm `Public Artifact Access` connection in `public` project can access `mssql-rs` pipelines. If not, create via Project Settings → Service Connections → Azure DevOps pointing to `mssql-rs`.

4. **Update PR template with Depends-On section**: Add commented section to [PULL_REQUEST_TEMPLATE.MD](../.github/PULL_REQUEST_TEMPLATE.MD) for BCP developers to specify cross-repo dependency. Add after the GitHub Issue section:
   ```markdown
   <!-- 
   ### Cross-Repo Dependency (BCP Development Only)
   If this PR depends on changes in mssql-tds repo, uncomment and specify the PR number:
   Depends-On: mssql-tds#<PR_NUMBER>
   -->
   ```

5. **Create local dev script `scripts/build-py-core.sh`**: Add to `mssql-python` repo—expects `mssql-tds` as sibling folder, runs `maturin develop` or `maturin build` in `../mssql-tds/mssql-py-core/`, copies `.so` to `mssql_python/` with platform-appropriate naming.

6. **Create devcontainer for dual-repo development**: Add `.devcontainer/` to `mssql-python` with Rust + Python + maturin, expects both repos mounted as siblings, auto-runs `build-py-core.sh` on start.

7. **Document the workflow**: Add section to `CONTRIBUTING.md` or `docs/cross-repo-dev.md` explaining: sibling folder structure, PR template usage, local dev script.

### Folder Structure (Local Development)

```
parent-folder/
├── mssql-python/     # GitHub repo
└── mssql-tds/        # ADO repo (cloned alongside)
```

### CI Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│  mssql-tds repo (mssql-rs project)                              │
│                                                                 │
│  PR #123: "Add BCP bindings"                                    │
│     └──► Pipeline builds mssql_py_core .so (all platforms)      │
│              └──► Publishes artifact: mssql-py-core-{BuildId}   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  mssql-python repo (public project)                             │
│                                                                 │
│  PR #456: "Add Cursor.bulkcopy() API"                           │
│     │  PR Description: "Depends-On: mssql-tds#123"              │
│     │                                                           │
│     └──► Pipeline parses PR desc → extracts mssql-tds#123       │
│              └──► Downloads all platform .so artifacts          │
│              └──► Runs tests on each platform with .so          │
└─────────────────────────────────────────────────────────────────┘
```

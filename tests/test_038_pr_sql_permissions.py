"""Source-only checks for the disposable PR SQL Server test-login prerequisites."""

from pathlib import Path

import pytest

_PIPELINE = (
    Path(__file__).resolve().parent.parent / "eng" / "pipelines" / "pr-validation-pipeline.yml"
)
if not _PIPELINE.is_file():
    pytest.skip("PR pipeline sources are not shipped in wheels.", allow_module_level=True)

import yaml


@pytest.mark.parametrize("server", ["2022", "2025"])
def test_ci_test_login_has_verified_read_only_pool_observation_permission(server):
    document = yaml.safe_load(_PIPELINE.read_text(encoding="utf-8"))
    job = next(job for job in document["jobs"] if job["job"] == "pytestonwindows")
    step = next(
        step
        for step in job["steps"]
        if step.get("displayName") == f"Setup database and user for SQL Server {server}"
    )
    body = step["powershell"]
    grant = next(
        line for line in body.splitlines() if "GRANT VIEW SERVER PERFORMANCE STATE" in line
    )
    assert 'sqlcmd -S "localhost" -U "sa"' in grant
    assert '-P "$env:DB_PASSWORD" -b ' in grant
    assert "GRANT VIEW SERVER PERFORMANCE STATE TO [testuser]" in grant
    assert "EXECUTE AS LOGIN = 'testuser'" in grant
    assert "SELECT connection_id FROM sys.dm_exec_connections WHERE session_id = @@SPID" in grant
    assert "REVERT;" in grant
    assert body.index("CREATE LOGIN testuser") < body.index(grant.strip())
    assert "if ($LASTEXITCODE -ne 0) { throw " in body
    assert "sysadmin" not in body.lower()
    assert "continueOnError" not in step

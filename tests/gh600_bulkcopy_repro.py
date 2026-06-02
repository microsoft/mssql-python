"""
Repro script for GitHub issue #600:
  cursor.bulkcopy() fails with "Protocol Error: Failed to receive token
  during login response parsing" on SQL Server 2016.

This script is designed to run in CI on a Windows agent with SQL Server 2016
installed. It exercises bulkcopy through all connection-string variants the
reporter tested and captures detailed output for triage.

Expects:
  - DB_CONNECTION_STRING env var (Server=localhost;Database=...;Uid=...;Pwd=...)
  - MSSQL_TDS_TRACE=true / MSSQL_TDS_TRACE_LEVEL=debug for Rust TDS logs
  - mssql_python and mssql_py_core installed
"""
import os
import sys
import traceback

import mssql_python

try:
    import mssql_py_core
    PYCORE_AVAILABLE = True
except ImportError:
    PYCORE_AVAILABLE = False

CONN_STR = os.environ.get("DB_CONNECTION_STRING", "")
if not CONN_STR:
    print("ERROR: DB_CONNECTION_STRING not set")
    sys.exit(1)


def banner(msg):
    print(f"\n{'=' * 60}")
    print(f"  {msg}")
    print(f"{'=' * 60}")


def parse_connstr(cs):
    """Parse semicolon-delimited connection string into dict."""
    parts = {}
    for kv in cs.split(";"):
        kv = kv.strip()
        if "=" in kv:
            k, v = kv.split("=", 1)
            parts[k.strip()] = v.strip()
    return parts


# ── 0. Environment ──────────────────────────────────────────
banner("Environment")
print(f"Python:        {sys.version}")
print(f"mssql_python:  {mssql_python.__version__}")
print(f"mssql_py_core: {'available' if PYCORE_AVAILABLE else 'NOT AVAILABLE'}")
print(f"Connection:    {CONN_STR}")

# ── 1. Verify normal ODBC connection ────────────────────────
banner("1. Normal ODBC connection")
try:
    conn = mssql_python.connect(CONN_STR)
    c = conn.cursor()
    c.execute("SELECT @@VERSION")
    ver = c.fetchone()[0]
    print(f"OK: {ver.split(chr(10))[0].strip()}")
    conn.close()
except Exception as e:
    print(f"FAIL: {e}")
    traceback.print_exc()
    sys.exit(1)

# ── 2. Create test table ───────────────────────────────────
banner("2. Create test table")
conn = mssql_python.connect(CONN_STR)
c = conn.cursor()
c.execute("IF OBJECT_ID(N'gh600_bulkcopy_test', N'U') IS NOT NULL "
          "DROP TABLE gh600_bulkcopy_test")
conn.commit()
c.execute("CREATE TABLE gh600_bulkcopy_test (id INT, name NVARCHAR(50), val NVARCHAR(100))")
conn.commit()
print("OK: gh600_bulkcopy_test created")
conn.close()

# ── 3. Test bulkcopy (baseline connection string) ──────────
banner("3. Bulkcopy — baseline connection string")
conn = mssql_python.connect(CONN_STR)
c = conn.cursor()
data = [(1, "test1", "val1"), (2, "test2", "val2"), (3, "test3", "val3")]
try:
    result = c.bulkcopy(
        "gh600_bulkcopy_test", data,
        batch_size=0, timeout=30,
        column_mappings=["id", "name", "val"],
    )
    print(f"OK: {result}")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
conn.close()

# ── 4. Verify row count ────────────────────────────────────
banner("4. Verify row count")
conn = mssql_python.connect(CONN_STR)
c = conn.cursor()
c.execute("SELECT COUNT(*) FROM gh600_bulkcopy_test")
print(f"Row count: {c.fetchone()[0]}")
conn.close()

# ── 5. Connection string variant matrix ────────────────────
banner("5. Connection string variant matrix")
params = parse_connstr(CONN_STR)
server = params.get("Server", params.get("server", "localhost"))
database = params.get("Database", params.get("database", "TestDB"))
uid = params.get("Uid", params.get("uid", "testuser"))
pwd = params.get("Pwd", params.get("pwd", ""))

BASE = f"SERVER={server};DATABASE={database};UID={uid};PWD={pwd}"
data2 = [(10, "v1", "c1"), (20, "v2", "c2")]

variants = [
    (f"{BASE};TrustServerCertificate=yes;",             "TrustSC=yes (no Encrypt)"),
    (f"{BASE};Encrypt=yes;TrustServerCertificate=yes;",  "Encrypt=yes;TrustSC=yes"),
    (f"{BASE};Encrypt=no;TrustServerCertificate=yes;",   "Encrypt=no;TrustSC=yes"),
    (f"{BASE};Encrypt=no;",                              "Encrypt=no (no TrustSC)"),
]

for cs, label in variants:
    # First check ODBC connect
    odbc_ok = False
    try:
        t = mssql_python.connect(cs)
        t.close()
        odbc_ok = True
    except Exception:
        pass

    # Then check bulkcopy
    try:
        # Truncate table first
        conn = mssql_python.connect(CONN_STR)
        c = conn.cursor()
        c.execute("TRUNCATE TABLE gh600_bulkcopy_test")
        conn.commit()
        conn.close()

        b = mssql_python.connect(cs)
        bc = b.cursor()
        bc.bulkcopy(
            "gh600_bulkcopy_test", data2,
            batch_size=0, timeout=30,
            column_mappings=["id", "name", "val"],
        )
        b.close()
        status = "OK"
    except Exception as e:
        err = str(e).replace("\n", " ")[:120]
        status = f"FAIL: {err}"

    odbc_str = "OK " if odbc_ok else "ERR"
    print(f"[{label:35s}] ODBC={odbc_str} | BULKCOPY={status}")

# ── 6. Wide table test (56 columns like reporter) ─────────
banner("6. Wide table (56 NVARCHAR columns)")
conn = mssql_python.connect(CONN_STR)
c = conn.cursor()
c.execute("IF OBJECT_ID(N'gh600_wide_test', N'U') IS NOT NULL DROP TABLE gh600_wide_test")
conn.commit()
cols_ddl = ", ".join([f"col{i} NVARCHAR(100)" for i in range(56)])
c.execute(f"CREATE TABLE gh600_wide_test ({cols_ddl})")
conn.commit()
col_names = [f"col{i}" for i in range(56)]
rows = [tuple(f"r{r}c{j}" for j in range(56)) for r in range(5)]
try:
    result = c.bulkcopy(
        "gh600_wide_test", rows,
        batch_size=0, timeout=30,
        column_mappings=col_names,
    )
    print(f"OK: {result}")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
conn.close()

# ── 7. Cleanup ─────────────────────────────────────────────
banner("7. Cleanup")
conn = mssql_python.connect(CONN_STR)
c = conn.cursor()
c.execute("DROP TABLE IF EXISTS gh600_bulkcopy_test")
c.execute("DROP TABLE IF EXISTS gh600_wide_test")
conn.commit()
conn.close()
print("OK: tables dropped")

banner("DONE")

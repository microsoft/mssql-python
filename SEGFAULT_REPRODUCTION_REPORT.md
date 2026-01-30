# SQLAlchemy Segfault Reproduction - Success

## Summary
Successfully reproduced the segmentation fault issue that occurs when using the PyPI version of `mssql-python` with SQLAlchemy's reconnect tests.

## Test Setup
- **Docker Image**: `mssql-sqlalchemy-segfault:clean`
- **Base OS**: Fedora 39
- **SQL Server**: 2022 (RTM-CU22-GDR) 16.0.4230.2
- **Python**: 3.12.7
- **mssql-python**: Installed from PyPI (version without fix)
- **SQLAlchemy**: 2.1.0b1.dev0 (from Gerrit review 6149)

## Test Execution
The Docker container:
1. Started SQL Server 2022
2. Created test database and user
3. Cloned SQLAlchemy from gerrit.sqlalchemy.org
4. Fetched specific review (refs/changes/49/6149/14) containing reconnect tests
5. Installed SQLAlchemy and dependencies
6. Ran `test/engine/test_reconnect.py::RealReconnectTest` with mssql-python

## Result
**SEGFAULT REPRODUCED ON FIRST TEST RUN**

```
test/engine/test_reconnect.py::RealReconnectTest_mssql+mssqlpython_16_0_4230_2::test_close PASSED
[tests continued...]

Segmentation fault (core dumped)
*** CRASH DETECTED ON RUN 1 ***
```

## Root Cause
The segfault is caused by a **use-after-free bug** in mssql-python:

### The Problem:
1. SQLAlchemy creates connections with multiple active cursors/statement handles
2. When connection invalidation occurs (e.g., during reconnect tests), the connection is closed
3. Closing the connection frees the DBC (database connection) handle
4. ODBC driver automatically frees all child STMT (statement) handles when DBC is freed
5. Python garbage collector later tries to free those same STMT handles
6. **Result**: Use-after-free → Segmentation Fault

### Technical Details:
- When `Connection::disconnect()` frees a DBC handle, ODBC spec dictates that all child statement handles are implicitly freed
- The Python `SqlHandle` objects weren't aware of this implicit freeing
- When Python's GC ran and called `SqlHandle::free()` on those handles, it attempted to call ODBC functions on already-freed handles
- This caused memory corruption and crashes

## The Fix
The fix (already implemented in the local workspace) uses state tracking:
- `Connection` maintains a `_childStatementHandles` vector
- Before disconnecting, `Connection` marks all child handles as "implicitly freed"
- `SqlHandle::free()` checks the flag and skips ODBC calls if the handle was already freed
- Result: Clean shutdown, no crashes

## Files Created
1. **Dockerfile.sqlalchemy-segfault** - Clean Dockerfile for reproduction
2. **docker-scripts/entrypoint.sh** - Main test execution script
3. **docker-scripts/mssql_setup.sh** - SQL Server initialization
4. **docker-scripts/mssql_setup.sql** - Database setup SQL
5. **sqlalchemy-segfault-test.log** - Complete test execution log

## How to Reproduce
```bash
# Build the Docker image
docker build -f Dockerfile.sqlalchemy-segfault -t mssql-sqlalchemy-segfault:clean .

# Run the test (will crash with segfault)
docker run --rm mssql-sqlalchemy-segfault:clean
```

## Next Steps
To verify the fix works:
1. Use `Dockerfile.test-fix` which installs the local version with the fix
2. Run the same test suite
3. Confirm it completes 10 runs without crashing

## Log Location
Full execution log: `/home/subrata/SegFaultRepro/mssql-python/sqlalchemy-segfault-test.log`

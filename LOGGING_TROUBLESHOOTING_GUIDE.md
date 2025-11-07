# mssql-python Logging Troubleshooting Guide

**Version:** 1.0  
**Last Updated:** November 4, 2025  

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Enable Debug Logging](#enable-debug-logging)
3. [Common Customer Issues](#common-customer-issues)
4. [Step-by-Step Troubleshooting Workflows](#step-by-step-troubleshooting-workflows)
5. [Permission Issues](#permission-issues)
6. [Log Collection Guide](#log-collection-guide)
7. [Log Analysis](#log-analysis)
8. [Escalation Criteria](#escalation-criteria)
9. [FAQ](#faq)
10. [Scripts & Commands](#scripts--commands)

---

## Quick Reference

### Fastest Way to Enable Logging

```python
import mssql_python

# Enable logging - shows everything
mssql_python.setup_logging(output='both')
```

This enables logging with:
- ✅ File output (in `./mssql_python_logs/` folder)
- ✅ Console output (immediate visibility)
- ✅ Debug level (everything)

### Logging Philosophy

mssql-python uses an **all-or-nothing** approach:
- **One Level**: DEBUG level only - no level categorization
- **All or Nothing**: When enabled, you see EVERYTHING
- **Troubleshooting Focus**: Turn on when something breaks, off otherwise

### Output Modes

| Mode | Value | Behavior | Use Case |
|------|-------|----------|----------|
| **File** | `'file'` | Logs to file only | Default, production |
| **Stdout** | `'stdout'` | Logs to console only | No file access |
| **Both** | `'both'` | Logs to file + console | Active troubleshooting |

---

## Enable Debug Logging

The mssql-python driver includes a comprehensive logging system that captures detailed information about driver operations, SQL queries, parameters, and internal state.

### Quick Start

Enable logging with one line before creating connections:

```python
import mssql_python

# Enable logging - shows EVERYTHING
mssql_python.setup_logging()

# Use the driver - all operations are now logged
conn = mssql_python.connect("Server=localhost;Database=test")
# Log file: ./mssql_python_logs/mssql_python_trace_*.log
```

### Output Options

Control where logs are written:

```python
# File only (default) - logs saved to file
mssql_python.setup_logging()

# Console only - logs printed to stdout
mssql_python.setup_logging(output='stdout')

# Both file and console
mssql_python.setup_logging(output='both')

# Custom file path (must use .txt, .log, or .csv extension)
mssql_python.setup_logging(log_file_path="/var/log/myapp/debug.log")
```

### What Gets Logged

When enabled, logging shows **everything** at DEBUG level:

- ✅ **Connection operations**: Opening, closing, configuration
- ✅ **SQL queries**: Full query text and parameters
- ✅ **Internal operations**: ODBC calls, handle management, memory allocations
- ✅ **Error details**: Exceptions with stack traces and error codes
- ✅ **Thread tracking**: OS native thread IDs for multi-threaded debugging

### Log Format

Logs use comma-separated format with structured fields:

```
# MSSQL-Python Driver Log | Script: main.py | PID: 12345 | Log Level: DEBUG | Python: 3.13.7 | Start: 2025-11-06 10:30:15
Timestamp, ThreadID, Level, Location, Source, Message
2025-11-06 10:30:15.100, 8581947520, DEBUG, connection.py:156, Python, Connection opened
2025-11-06 10:30:15.101, 8581947520, DEBUG, connection.cpp:22, DDBC, Allocating ODBC environment handle
2025-11-06 10:30:15.102, 8581947520, DEBUG, cursor.py:89, Python, Executing query: SELECT * FROM users WHERE id = ?
2025-11-06 10:30:15.103, 8581947520, DEBUG, cursor.py:90, Python, Query parameters: [42]
```

**Field Descriptions:**
- **Timestamp**: Precise time with milliseconds
- **ThreadID**: OS native thread ID (matches debugger thread IDs)
- **Level**: Always DEBUG when logging enabled
- **Location**: Source file and line number
- **Source**: Python (Python layer) or DDBC (C++ layer)
- **Message**: Operation details, queries, parameters, etc.

**Why Thread IDs?**
- Track operations in multi-threaded applications
- Distinguish concurrent connections/queries
- Correlate with debugger thread views
- Filter logs by specific thread

### Performance Notes

⚠️ **Important**: Logging adds ~2-5% overhead. Enable only when troubleshooting.

```python
# ❌ DON'T enable by default in production
# ✅ DO enable only when diagnosing issues
```

### Using Driver Logger in Your Application

Integrate the driver's logger into your own code:

```python
import mssql_python
from mssql_python.logging import driver_logger

# Enable logging
mssql_python.setup_logging()

# Use driver_logger in your application
driver_logger.debug("[App] Starting data processing")
driver_logger.info("[App] Processing complete")
driver_logger.warning("[App] Resource usage high")
driver_logger.error("[App] Failed to process record")

# Your logs appear in the same file as driver logs
```

### Common Troubleshooting

**No log output?**
```python
# Force stdout to verify logging works
mssql_python.setup_logging(output='stdout')
```

**Where is the log file?**
```python
from mssql_python import driver_logger
mssql_python.setup_logging()
# Access log file path from driver_logger handlers if needed
```

**Logs not showing in CI/CD?**
```python
# Use stdout for CI/CD pipelines
mssql_python.setup_logging(output='stdout')
```

**Invalid file extension error?**
```python
# Only .txt, .log, or .csv extensions allowed
mssql_python.setup_logging(log_file_path="/tmp/debug.log")  # ✓
mssql_python.setup_logging(log_file_path="/tmp/debug.json") # ✗ ValueError
```

---

## Common Customer Issues

### Issue 1: "I can't connect to the database"

**Symptoms:**
- Connection timeout
- Authentication failures
- Network errors

**Solution Steps:**

1. **Enable logging to see connection attempts:**

```python
import mssql_python

# Enable logging
mssql_python.setup_logging(output='both')

# Then run customer's connection code
conn = mssql_python.connect(connection_string)
```

2. **What to look for in logs:**
- `[Python] Connecting to server: <servername>` - Connection initiated
- `[Python] Connection established` - Success
- Error messages with connection details

3. **Common log patterns:**

**Success:**
```
2025-11-04 10:30:15 [CONN-12345-67890-1] - DEBUG - connection.py:42 - [Python] Connecting to server: localhost
2025-11-04 10:30:15 [CONN-12345-67890-1] - DEBUG - connection.py:89 - [Python] Connection established
```

**Failure (wrong server):**
```
2025-11-04 10:30:15 [CONN-12345-67890-1] - DEBUG - connection.py:42 - [Python] Connecting to server: wrongserver
2025-11-04 10:30:20 [CONN-12345-67890-1] - ERROR - connection.py:156 - [Python] Connection failed: timeout
```

**Action:** Check server name, network connectivity, firewall rules

---

### Issue 2: "Query returns wrong results"

**Symptoms:**
- Incorrect data returned
- Missing rows
- Wrong column values

**Solution Steps:**

1. **Enable logging to see SQL + parameters:**

```python
import mssql_python

# Enable logging
mssql_python.setup_logging(output='both')

# Run customer's query
cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
```

2. **What to look for:**
- Actual SQL being executed
- Parameter values being passed
- Parameter types

3. **Common issues:**
- Wrong parameter value: `Parameter 1: value=999` (expected 123)
- Wrong parameter order: `Parameter 1: value='John', Parameter 2: value=123` (swapped)
- Type mismatch: `Parameter 1: type=str, value='123'` (should be int)

**Action:** Verify SQL statement and parameter values match customer expectations

---

### Issue 3: "Query is very slow"

**Symptoms:**
- Long execution time
- Timeouts
- Performance degradation

**Solution Steps:**

1. **Enable logging with timing:**

```python
import mssql_python
import time

mssql_python.setup_logging(output='both')

start = time.time()
cursor.execute("SELECT * FROM large_table WHERE ...")
rows = cursor.fetchall()
end = time.time()

print(f"Query took {end - start:.2f} seconds")
```

2. **What to look for in logs:**
- Query execution timestamp
- Large result sets: `Fetched 1000000 rows`
- Multiple round trips to database

3. **Common patterns:**

**Inefficient query:**
```
2025-11-04 10:30:15 - DEBUG - cursor.py:28 - [Python] Executing query: SELECT * FROM huge_table
2025-11-04 10:35:20 - DEBUG - cursor.py:89 - [Python] Query completed, 5000000 rows fetched
```

**Action:** Check if query can be optimized, add WHERE clause, use pagination

---

### Issue 4: "I get a parameter binding error"

**Symptoms:**
- `Invalid parameter type`
- `Cannot convert parameter`
- Data truncation errors

**Solution Steps:**

1. **Enable logging to see parameter binding:**

```python
import mssql_python

mssql_python.setup_logging(output='both')

cursor.execute("SELECT * FROM table WHERE col = ?", (param,))
```

2. **What to look for:**
- `_map_sql_type: Mapping param index=0, type=<typename>`
- `_map_sql_type: INT detected` (or other type)
- `_map_sql_type: INT -> BIGINT` (type conversion)

3. **Example log output:**

```
2025-11-04 10:30:15 - DEBUG - cursor.py:310 - _map_sql_type: Mapping param index=0, type=Decimal
2025-11-04 10:30:15 - DEBUG - cursor.py:385 - _map_sql_type: DECIMAL detected - index=0
2025-11-04 10:30:15 - DEBUG - cursor.py:406 - _map_sql_type: DECIMAL precision calculated - index=0, precision=18
```

**Action:** Verify parameter type matches database column type, convert if needed

---

### Issue 5: "executemany fails with batch data"

**Symptoms:**
- Batch insert/update fails
- Some rows succeed, others fail
- Transaction rollback

**Solution Steps:**

1. **Enable logging to see batch operations:**

```python
import mssql_python
mssql_python.setup_logging(output='both')

data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
cursor.executemany("INSERT INTO users (id, name) VALUES (?, ?)", data)
```

2. **What to look for:**
- `executemany: Starting - batch_count=<number>`
- Individual parameter sets being processed
- Errors on specific batch items

**Action:** Check if all rows in batch have consistent types and valid data

---

## Step-by-Step Troubleshooting Workflows

### Workflow 1: Connection Issues

**Customer says:** "I can't connect to my database"

**Step 1: Enable logging**
```python
import mssql_python
mssql_python.setup_logging(output='both')
```

**Step 2: Attempt connection**
```python
import mssql_python
try:
    conn = mssql_python.connect(
        server='servername',
        database='dbname',
        username='user',
        password='pass'
    )
    print("✅ Connected successfully!")
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

**Step 3: Check console output**
Look for:
- Server name in logs matches expected server
- No "connection timeout" errors
- No "login failed" errors

**Step 4: Check log file**
```python
print(f"Log file: {logging.logger.log_file}")
```
Open the file and search for "ERROR" or "Connection"

**Step 5: Collect information**
- Server name (sanitized)
- Database name
- Authentication method (Windows/SQL)
- Error message
- Log file

**Escalate if:**
- Logs show "connection established" but customer says it fails
- Unusual error messages
- Consistent timeout at specific interval

---

### Workflow 2: Query Problems

**Customer says:** "My query doesn't work"

**Step 1: Enable logging**
```python
import mssql_python
mssql_python.setup_logging(output='both')
```

**Step 2: Run the query**
```python
cursor = conn.cursor()
try:
    cursor.execute("SELECT * FROM table WHERE id = ?", (123,))
    rows = cursor.fetchall()
    print(f"✅ Fetched {len(rows)} rows")
except Exception as e:
    print(f"❌ Query failed: {e}")
```

**Step 3: Check logs for:**
- Exact SQL statement executed
- Parameter values (are they what customer expects?)
- Row count returned

**Step 4: Verify customer expectations**
Ask:
- "Is the SQL statement correct?"
- "Are the parameter values correct?"
- "How many rows should be returned?"

**Step 5: Collect information**
- SQL statement (sanitized)
- Parameter values (sanitized)
- Expected vs actual results
- Error message (if any)
- Log file

**Escalate if:**
- SQL and parameters look correct but results are wrong
- Driver returns different results than SSMS
- Reproducible data corruption

---

### Workflow 3: Performance Issues

**Customer says:** "Queries are too slow"

**Step 1: Enable timing measurements**
```python
import mssql_python
import time

mssql_python.setup_logging(output='both')

start = time.time()
cursor.execute("SELECT * FROM large_table")
rows = cursor.fetchall()
elapsed = time.time() - start

print(f"Query took {elapsed:.2f} seconds, fetched {len(rows)} rows")
```

**Step 2: Check log file for patterns**
```python
print(f"Log file: {logging.logger.log_file}")
```

Look for:
- Very large row counts: `Fetched 1000000 rows`
- Multiple queries: Customer might be in a loop
- Long timestamps between execute and fetch

**Step 3: Compare logging overhead**

Run with logging disabled:
```python
# Don't call setup_logging() - logging disabled by default
start = time.time()
cursor.execute("SELECT * FROM large_table")
rows = cursor.fetchall()
elapsed = time.time() - start
print(f"Without logging: {elapsed:.2f} seconds")
```

If significantly faster, logging overhead is the issue.

**Step 4: Profile the query**
Ask customer to run same query in SSMS or Azure Data Studio:
- If fast there: Driver issue (escalate)
- If slow there: Query optimization needed (not driver issue)

**Step 5: Collect information**
- Query execution time
- Row count
- Query complexity
- Database server specs
- Network latency
- Logging level used

**Escalate if:**
- Query is fast in SSMS but slow with driver
- Same query was fast before, slow now
- Logging overhead exceeds 10% with logging enabled

---

## Permission Issues

### Issue: Customer Can't Create Log Files

**Symptom:** Error when enabling logging
```
PermissionError: [Errno 13] Permission denied: './mssql_python_logs/mssql_python_trace_...'
```

**Root Cause:** No write permission in current directory or specified path

**Solutions:**

#### Solution 1: Use STDOUT Only (No File Access Needed)

```python
import mssql_python

# Console output only - no file created
mssql_python.setup_logging(output='stdout')

# Customer can copy console output to share with you
```

**Advantages:**
- ✅ No file permissions required
- ✅ Immediate visibility
- ✅ Works in restricted environments (Docker, CI/CD)

**Disadvantages:**
- ❌ Output lost when console closed
- ❌ Large logs hard to manage in console

---

#### Solution 2: Use Temp Directory

```python
import tempfile
import os
import mssql_python

# Get temp directory (usually writable by all users)
temp_dir = tempfile.gettempdir()
log_file = os.path.join(temp_dir, "mssql_python_debug.log")

mssql_python.setup_logging(log_file_path=log_file)
print(f"Logging to: {log_file}")

# On Windows: Usually C:\Users\<username>\AppData\Local\Temp\mssql_python_debug.log
# On Linux/Mac: Usually /tmp/mssql_python_debug.log
```

**Advantages:**
- ✅ Temp directories are usually writable
- ✅ Log file persists during session
- ✅ Easy to locate and share

---

#### Solution 3: Use User Home Directory

```python
import os
from pathlib import Path
import mssql_python

# User home directory - always writable by user
home_dir = Path.home()
log_dir = home_dir / "mssql_python_logs"
log_dir.mkdir(exist_ok=True)

log_file = log_dir / "debug.log"
mssql_python.setup_logging(log_file_path=str(log_file))
print(f"Logging to: {log_file}")

# On Windows: C:\Users\<username>\mssql_python_logs\debug.log
# On Linux/Mac: /home/<username>/mssql_python_logs/debug.log
```

**Advantages:**
- ✅ Always writable (it's user's home)
- ✅ Logs persist across sessions
- ✅ Easy for user to find

---

#### Solution 4: Custom Writable Path

Ask customer where they have write access:

```python
import mssql_python

# Ask customer: "Where can you create files?"
# Example paths:
# - Desktop: "C:/Users/username/Desktop/mssql_logs"
# - Documents: "C:/Users/username/Documents/mssql_logs"
# - Network share: "//server/share/logs"

custom_path = "C:/Users/john/Desktop/mssql_debug.log"
mssql_python.setup_logging(log_file_path=custom_path)
print(f"Logging to: {custom_path}")
```

---

#### Solution 5: Use BOTH Mode with Temp File

Best of both worlds:

```python
import tempfile
import os
import mssql_python

temp_dir = tempfile.gettempdir()
log_file = os.path.join(temp_dir, "mssql_python_debug.log")

# Both console (immediate) and file (persistent)
mssql_python.setup_logging(output='both', log_file_path=log_file)

print(f"✅ Logging to console AND file: {log_file}")
print("You can see logs immediately, and share the file later!")
```

---

### Testing Write Permissions

Help customer test if they can write to a location:

```python
import os
from pathlib import Path

def test_write_permission(path):
    """Test if customer can write to a directory."""
    try:
        test_file = Path(path) / "test_write.txt"
        test_file.write_text("test")
        test_file.unlink()  # Delete test file
        return True, "✅ Write permission OK"
    except Exception as e:
        return False, f"❌ Cannot write: {e}"

# Test current directory
can_write, msg = test_write_permission(".")
print(f"Current directory: {msg}")

# Test temp directory
import tempfile
temp_dir = tempfile.gettempdir()
can_write, msg = test_write_permission(temp_dir)
print(f"Temp directory ({temp_dir}): {msg}")

# Test home directory
home_dir = Path.home()
can_write, msg = test_write_permission(home_dir)
print(f"Home directory ({home_dir}): {msg}")
```

---

### Issue: Log Files Too Large

**Symptom:** Log files consuming too much disk space

**Solution 1: Logging is All-or-Nothing**

```python
# Logging shows everything when enabled
mssql_python.setup_logging()  # All operations logged at DEBUG level
```

Logging in mssql-python uses a simple DEBUG level - no granular levels to choose from.

**Solution 2: Check Rotation Settings**

Log files automatically rotate at 512MB with 5 backups. This means max ~2.5GB total.

If customer needs smaller files:
```python
import logging as py_logging
from mssql_python import driver_logger

# After enabling logging, modify the handler
mssql_python.setup_logging()

for handler in driver_logger.handlers:
    if isinstance(handler, py_logging.handlers.RotatingFileHandler):
        handler.maxBytes = 50 * 1024 * 1024  # 50MB instead of 512MB
        handler.backupCount = 2  # 2 backups instead of 5
```

**Solution 3: Don't Enable Logging Unless Troubleshooting**

```python
# ❌ DON'T enable by default
# mssql_python.setup_logging()  # Comment out when not needed

# ✅ DO enable only when troubleshooting
if debugging:
    mssql_python.setup_logging()
```

---

## Log Collection Guide

### How to Collect Logs from Customer

**Step 1: Ask customer to enable logging**

Send them this code:
```python
import mssql_python
import tempfile
import os

# Use temp directory (always writable)
log_file = os.path.join(tempfile.gettempdir(), "mssql_python_debug.log")
mssql_python.setup_logging(output='both', log_file_path=log_file)

print(f"✅ Logging enabled")
print(f"📂 Log file: {log_file}")
print("Please run your code that reproduces the issue, then send me the log file.")
```

**Step 2: Customer reproduces issue**

Customer runs their code that has the problem.

**Step 3: Customer finds log file**

The code above prints the log file path. Customer can:
- Copy the path
- Open in Notepad/TextEdit
- Attach to support ticket

**Step 4: Customer sends log file**

Options:
- Email attachment
- Support portal upload
- Paste in ticket (if small)

---

### What to Ask For

**Minimum information:**
1. ✅ Log file (with logging enabled)
2. ✅ Code snippet that reproduces issue (sanitized)
3. ✅ Error message (if any)
4. ✅ Expected vs actual behavior

**Nice to have:**
5. Python version: `python --version`
6. Driver version: `pip show mssql-python`
7. Operating system: Windows/Linux/Mac
8. Database server version: SQL Server 2019/2022, Azure SQL, etc.

---

### Sample Email Template for Customer

```
Subject: mssql-python Logging Instructions

Hi [Customer],

To help troubleshoot your issue, please enable logging and send us the log file.

1. Add these lines at the start of your code:

import mssql_python
import tempfile
import os

log_file = os.path.join(tempfile.gettempdir(), "mssql_python_debug.log")
mssql_python.setup_logging(output='both', log_file_path=log_file)
print(f"Log file: {log_file}")

2. Run your code that reproduces the issue

3. Find the log file (path printed in step 1)

4. Send us:
   - The log file
   - Your code (remove any passwords!)
   - The error message you see

This will help us diagnose the problem quickly.

Thanks!
```

---

## Log Analysis

### Reading Log Files

**Log Format:**
```
2025-11-04 10:30:15,123 [CONN-12345-67890-1] - DEBUG - connection.py:42 - [Python] Message
│                        │                      │       │                   │
│                        │                      │       │                   └─ Log message
│                        │                      │       └─ Source file:line
│                        │                      └─ Log level (always DEBUG)
│                        └─ Trace ID (PREFIX-PID-ThreadID-Counter)
└─ Timestamp (YYYY-MM-DD HH:MM:SS,milliseconds)
```

**Trace ID Components:**
- `CONN-12345-67890-1` = Connection, Process 12345, Thread 67890, Sequence 1
- `CURS-12345-67890-2` = Cursor, Process 12345, Thread 67890, Sequence 2

**Why Trace IDs matter:**
- Multi-threaded apps: Distinguish logs from different threads
- Multiple connections: Track which connection did what
- Debugging: Filter logs with `grep "CONN-12345-67890-1" logfile.log`

---

### Common Log Patterns

#### Pattern 1: Successful Connection

```
2025-11-04 10:30:15,100 [CONN-12345-67890-1] - DEBUG - connection.py:42 - [Python] Connecting to server: localhost
2025-11-04 10:30:15,250 [CONN-12345-67890-1] - DEBUG - connection.py:89 - [Python] Connection established
```

**Interpretation:** Connection succeeded in ~150ms

---

#### Pattern 2: Query Execution

```
2025-11-04 10:30:16,100 [CURS-12345-67890-2] - DEBUG - cursor.py:1040 - execute: Starting - operation_length=45, param_count=2, use_prepare=False
2025-11-04 10:30:16,350 [CURS-12345-67890-2] - DEBUG - cursor.py:1200 - [Python] Query completed, 42 rows fetched
```

**Interpretation:** 
- Query took ~250ms
- Had 2 parameters
- Returned 42 rows

---

#### Pattern 3: Parameter Binding

```
2025-11-04 10:30:16,100 [CURS-12345-67890-2] - DEBUG - cursor.py:1063 - execute: Setting query timeout=30 seconds
2025-11-04 10:30:16,105 [CURS-12345-67890-2] - DEBUG - cursor.py:310 - _map_sql_type: Mapping param index=0, type=int
2025-11-04 10:30:16,106 [CURS-12345-67890-2] - DEBUG - cursor.py:335 - _map_sql_type: INT detected - index=0, min=100, max=100
2025-11-04 10:30:16,107 [CURS-12345-67890-2] - DEBUG - cursor.py:339 - _map_sql_type: INT -> TINYINT - index=0
```

**Interpretation:**
- Parameter 0 is an integer with value 100
- Driver chose TINYINT (smallest int type that fits)

---

#### Pattern 4: Error

```
2025-11-04 10:30:16,100 [CURS-12345-67890-2] - DEBUG - cursor.py:1040 - execute: Starting - operation_length=45, param_count=2, use_prepare=False
2025-11-04 10:30:16,200 [CURS-12345-67890-2] - ERROR - cursor.py:1500 - [Python] Query failed: Invalid object name 'users'
```

**Interpretation:**
- Query tried to access table 'users' that doesn't exist
- Failed after 100ms

---

### Searching Logs Effectively

**Find all errors:**
```bash
grep "ERROR" mssql_python_trace_*.log
```

**Find specific connection:**
```bash
grep "CONN-12345-67890-1" mssql_python_trace_*.log
```

**Find slow queries (multi-second timestamps):**
```bash
grep "Query completed" mssql_python_trace_*.log
```

**Find parameter issues:**
```bash
grep "_map_sql_type" mssql_python_trace_*.log | grep "DEBUG\|ERROR"
```

**On Windows PowerShell:**
```powershell
Select-String -Path "mssql_python_trace_*.log" -Pattern "ERROR"
```

---

### Red Flags in Logs

🚩 **Multiple connection attempts:**
```
10:30:15 - Connecting to server: localhost
10:30:20 - Connection failed: timeout
10:30:21 - Connecting to server: localhost
10:30:26 - Connection failed: timeout
```
→ Network or firewall issue

🚩 **Massive row counts:**
```
10:30:15 - Query completed, 5000000 rows fetched
```
→ Query needs pagination or WHERE clause

🚩 **Repeated failed queries:**
```
10:30:15 - ERROR - Query failed: Invalid column name 'xyz'
10:30:16 - ERROR - Query failed: Invalid column name 'xyz'
10:30:17 - ERROR - Query failed: Invalid column name 'xyz'
```
→ Customer code in a retry loop with broken query

🚩 **Type conversion warnings:**
```
10:30:15 - DEBUG - _map_sql_type: DECIMAL precision too high - index=0, precision=50
```
→ Customer passing Decimal with precision exceeding SQL Server limits (38)

🚩 **Password in logs (should never happen):**
```
10:30:15 - Connection string: Server=...;PWD=***REDACTED***
```
✅ Good - password sanitized

```
10:30:15 - Connection string: Server=...;PWD=MyPassword123
```
❌ BAD - sanitization failed, escalate immediately

---

## Escalation Criteria

### Escalate to Engineering If:

1. **Data Corruption**
   - Logs show correct data, customer sees wrong data
   - Reproducible with minimal code
   - Not an application logic issue

2. **Driver Crashes**
   - Python crashes/segfaults
   - C++ exceptions in logs
   - Memory access violations

3. **Performance Regression**
   - Query is fast in SSMS, slow in driver
   - Same query was fast before, slow now
   - Logging overhead exceeds 10% with logging enabled

4. **Security Issues**
   - Passwords not sanitized in logs
   - SQL injection vulnerability
   - Authentication bypass

5. **Inconsistent Behavior**
   - Works on one machine, fails on another (same environment)
   - Intermittent failures with no pattern
   - Different results between driver and SSMS

6. **Cannot Reproduce**
   - Customer provides logs showing issue
   - You cannot reproduce with same code
   - Issue appears to be environment-specific but customer insists environment is standard

### Escalation Package

When escalating, include:

1. ✅ **Log files** (logging enabled)
2. ✅ **Minimal reproduction code** (sanitized)
3. ✅ **Customer environment:**
   - Python version
   - Driver version (`pip show mssql-python`)
   - OS (Windows/Linux/Mac) + version
   - Database server (SQL Server version, Azure SQL, etc.)
4. ✅ **Steps to reproduce**
5. ✅ **Expected vs actual behavior**
6. ✅ **Your analysis** (what you've tried, why you're escalating)
7. ✅ **Customer impact** (severity, business impact)

### Do NOT Escalate If:

1. ❌ Customer's SQL query is incorrect (not a driver issue)
2. ❌ Database permissions issue (customer can't access table)
3. ❌ Network connectivity issue (firewall, DNS, etc.)
4. ❌ Application logic bug (customer's code issue)
5. ❌ Customer hasn't provided logs yet
6. ❌ You haven't tried basic troubleshooting steps

---

## FAQ

### Q1: Why do I see `[Python]` in log messages?

**A:** This prefix distinguishes Python-side operations from C++ internal operations. You may also see `[DDBC]` for C++ driver operations.

```
[Python] Connecting to server - Python layer
[DDBC] Allocating connection handle - C++ layer
```

---

### Q2: Customer says logging "doesn't work"

**Checklist:**

1. Did they call `setup_logging()`?
   ```python
   # ❌ Won't work - logging not enabled
   import mssql_python
   conn = mssql_python.connect(...)
   
   # ✅ Will work - logging enabled
   import mssql_python
   mssql_python.setup_logging()
   conn = mssql_python.connect(...)
   ```

2. Are they looking in the right place?
   - Default: `./mssql_python_logs/` directory
   - Custom path if specified with `log_file_path`

3. Do they have write permissions?
3. Do they have write permissions?
   ```python
   # Try STDOUT instead
   mssql_python.setup_logging(output='stdout')
   ```

---

### Q3: Log file is empty

**Possible causes:**

1. **Logging enabled after operations:** Must enable BEFORE operations
   ```python
   # ❌ Wrong order
   conn = mssql_python.connect(...)     # Not logged
   mssql_python.setup_logging()         # Too late!
   
   # ✅ Correct order
   mssql_python.setup_logging()         # Enable first
   conn = mssql_python.connect(...)     # Now logged
   ```

2. **Python buffering:** Logs may not flush until script ends
   ```python
   # Force flush after operations
   from mssql_python import driver_logger
   for handler in driver_logger.handlers:
       handler.flush()
   ```

3. **Wrong log file:** Customer looking at old file

---

### Q4: How much overhead does logging add?

**Performance impact:**

| Level | Overhead | File Size (1000 queries) |
|-------|----------|-------------------------|
| DISABLED | 0% | 0 KB |
| DEBUG (enabled) | 2-10% | ~100-500 KB |

**Note:** Logging is all-or-nothing in mssql-python - when enabled, all operations are logged at DEBUG level.

---

### Q5: Can customer use their own log file name?

**A:** Yes! They can specify any path:

```python
# Custom name in default folder
mssql_python.setup_logging(log_file_path="./mssql_python_logs/my_app.log")

# Completely custom path
mssql_python.setup_logging(log_file_path="C:/Logs/database_debug.log")

# Only .txt, .log, .csv extensions allowed
mssql_python.setup_logging(log_file_path="./mssql_python_logs/debug.csv")
```

---

### Q6: Are passwords visible in logs?

**A:** No! Passwords are automatically sanitized:

```
# In logs you'll see:
Connection string: Server=localhost;Database=test;UID=admin;PWD=***REDACTED***
```

**If you see actual passwords in logs, ESCALATE IMMEDIATELY** - this is a security bug.

---

### Q7: Can we send logs to our logging system?

**A:** Yes! The driver uses standard Python logging, so you can add custom handlers:

```python
import mssql_python
from mssql_python import driver_logger

# Add Splunk/DataDog/CloudWatch handler
custom_handler = MySplunkHandler(...)
driver_logger.addHandler(custom_handler)

# Now logs go to both file and your system
mssql_python.setup_logging()
```

---

### Q8: How long are logs kept?

**A:** 
- Files rotate at 512MB
- Keeps 5 backup files
- Total max: ~2.5GB
- No automatic deletion - customer must clean up old files

---

### Q9: Customer has multiple Python scripts - which one generates which logs?

**A:** Each script creates its own log file with timestamp + PID:

```
mssql_python_logs/
├── mssql_python_trace_20251104_100000_12345.log  ← Script 1 (PID 12345)
├── mssql_python_trace_20251104_100100_12346.log  ← Script 2 (PID 12346)
└── mssql_python_trace_20251104_100200_12347.log  ← Script 3 (PID 12347)
```

Trace IDs also include PID for correlation.

---

### Q10: What if customer is using Docker/Kubernetes?

**Solution:** Use STDOUT mode so logs go to container logs:

```python
import mssql_python
mssql_python.setup_logging(output='stdout')

# Logs appear in: docker logs <container_id>
# or: kubectl logs <pod_name>
```

---

## Scripts & Commands

### Script 1: Quick Diagnostic

Send this to customer for quick info collection:

```python
"""
Quick Diagnostic Script for mssql-python
Collects environment info and tests logging
"""

import sys
import platform
import tempfile
import os

print("=" * 70)
print("mssql-python Diagnostic Script")
print("=" * 70)
print()

# Environment info
print("📋 Environment Information:")
print(f"  Python version: {sys.version}")
print(f"  Platform: {platform.system()} {platform.release()}")
print(f"  Architecture: {platform.machine()}")
print()

# Driver version
try:
    import mssql_python
    print(f"  mssql-python version: {mssql_python.__version__}")
except Exception as e:
    print(f"  ❌ Cannot import mssql-python: {e}")
    sys.exit(1)
print()

# Test logging
print("🔧 Testing Logging:")

temp_dir = tempfile.gettempdir()
log_file = os.path.join(temp_dir, "mssql_python_diagnostic.log")

try:
    mssql_python.setup_logging(output='both', log_file_path=log_file)
    print(f"  ✅ Logging enabled successfully")
    print(f"  📂 Log file: {log_file}")
except Exception as e:
    print(f"  ❌ Logging failed: {e}")
    print(f"  Try STDOUT mode instead:")
    print(f"     mssql_python.setup_logging(output='stdout')")
print()

# Test connection (if connection string provided)
conn_str = os.getenv("DB_CONNECTION_STRING")
if conn_str:
    print("🔌 Testing Connection:")
    try:
        conn = mssql_python.connect(conn_str)
        print("  ✅ Connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"  Database: {version[:80]}...")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
    print()
else:
    print("ℹ️  Set DB_CONNECTION_STRING env var to test connection")
    print()

print("=" * 70)
print("✅ Diagnostic complete!")
print(f"📂 Log file: {log_file}")
print("Please send this output and the log file to support.")
print("=" * 70)
```

---

### Script 2: Permission Tester

Test where customer can write log files:

```python
"""
Test write permissions in various directories
"""

import os
import tempfile
from pathlib import Path

def test_write(path, name):
    """Test if we can write to a path."""
    try:
        test_file = Path(path) / "test_write.txt"
        test_file.write_text("test")
        test_file.unlink()
        print(f"  ✅ {name}: {path}")
        return True
    except Exception as e:
        print(f"  ❌ {name}: {path}")
        print(f"     Error: {e}")
        return False

print("Testing write permissions...")
print()

# Current directory
test_write(".", "Current directory")

# Temp directory
test_write(tempfile.gettempdir(), "Temp directory")

# Home directory
test_write(Path.home(), "Home directory")

# Desktop (if exists)
desktop = Path.home() / "Desktop"
if desktop.exists():
    test_write(desktop, "Desktop")

# Documents (if exists)
documents = Path.home() / "Documents"
if documents.exists():
    test_write(documents, "Documents")

print()
print("Use one of the ✅ paths for log files!")
```

---

### Script 3: Log Analyzer

Help analyze log files:

```python
"""
Simple log analyzer for mssql-python logs
"""

import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: python analyze_log.py <log_file>")
    sys.exit(1)

log_file = Path(sys.argv[1])
if not log_file.exists():
    print(f"❌ File not found: {log_file}")
    sys.exit(1)

print(f"📊 Analyzing: {log_file}")
print("=" * 70)
print()

with open(log_file) as f:
    lines = f.readlines()

# Counts
total_lines = len(lines)
error_count = sum(1 for line in lines if '- ERROR -' in line)
warning_count = sum(1 for line in lines if '- WARNING -' in line)
debug_count = sum(1 for line in lines if '- DEBUG -' in line)

# Connection count
conn_count = sum(1 for line in lines if 'Connecting to server' in line)
query_count = sum(1 for line in lines if 'execute: Starting' in line)

print(f"📈 Statistics:")
print(f"  Total log lines: {total_lines:,}")
print(f"  Errors: {error_count}")
print(f"  Warnings: {warning_count}")
print(f"  Debug messages: {debug_count:,}")
print(f"  Connections: {conn_count}")
print(f"  Queries: {query_count}")
print()

# Show errors
if error_count > 0:
    print(f"🚨 Errors Found ({error_count}):")
    for line in lines:
        if '- ERROR -' in line:
            print(f"  {line.strip()}")
    print()

# Show warnings
if warning_count > 0:
    print(f"⚠️  Warnings Found ({warning_count}):")
    for line in lines:
        if '- WARNING -' in line:
            print(f"  {line.strip()}")
    print()

# Show first and last timestamps
if total_lines > 0:
    first_line = lines[0]
    last_line = lines[-1]
    print(f"⏱️  Time Range:")
    print(f"  First: {first_line[:23]}")
    print(f"  Last:  {last_line[:23]}")
    print()

print("=" * 70)
```

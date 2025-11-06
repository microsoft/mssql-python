# Logging Guide for mssql-python

This guide explains how to use the logging system in mssql-python for comprehensive diagnostics and troubleshooting.

## Table of Contents

- [Quick Start](#quick-start)
- [Philosophy](#philosophy)
- [Basic Usage](#basic-usage)
- [Log Output Examples](#log-output-examples)
- [Advanced Features](#advanced-features)
- [API Reference](#api-reference)
- [Extensibility](#extensibility)

## Quick Start

### Minimal Usage (Recommended)

```python
import mssql_python

# Enable logging - shows EVERYTHING (one line)
mssql_python.setup_logging()

# Use the driver - all operations are now logged
conn = mssql_python.connect("Server=localhost;Database=test")
# Check the log file: ./mssql_python_logs/mssql_python_trace_*.log
```

### With Output Control

```python
import mssql_python

# Enable logging (default: file only)
mssql_python.setup_logging()

# Output to stdout instead of file
mssql_python.setup_logging(output='stdout')

# Output to both file and stdout
mssql_python.setup_logging(output='both')

# Custom log file path
mssql_python.setup_logging(log_file_path="/var/log/myapp.log")
```

## Philosophy

**Simple and Purposeful:**
- **One Level**: All logs are DEBUG level - no categorization needed
- **All or Nothing**: When you enable logging, you see EVERYTHING (SQL, parameters, internal operations)
- **Troubleshooting Focus**: Turn on logging when something is broken, turn it off otherwise
- **⚠️ Performance Warning**: Logging has overhead - DO NOT enable in production without reason

**Why No Multiple Levels?**
- If you need logging, you need to see what's broken - partial information doesn't help
- Simplifies the API and mental model
- Future enhancement: Universal profiler for performance analysis (separate from logging)

**When to Enable Logging:**
- ✅ Debugging connection issues
- ✅ Troubleshooting query execution problems
- ✅ Investigating unexpected behavior
- ✅ Reproducing customer issues
- ❌ Evaluating query performance (use profiler instead - coming soon)
- ❌ Production monitoring (use proper monitoring tools)
- ❌ "Just in case" logging (adds unnecessary overhead)

## Basic Usage

### Default - File Logging

```python
import mssql_python

# Enable logging (logs to file by default)
mssql_python.setup_logging()

# Use the library - logs will appear in file
conn = mssql_python.connect(server='localhost', database='testdb')
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")

# Access logger for file path (advanced)
from mssql_python.logging import logger
print(f"Logs written to: {logger.log_file}")
```

### Console Logging

```python
import mssql_python

# Enable logging to stdout
mssql_python.setup_logging(output='stdout')

# Now use the library - logs will appear in console
conn = mssql_python.connect(server='localhost', database='testdb')
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
```

### Both File and Console

```python
import mssql_python

# Enable logging to both file and stdout
mssql_python.setup_logging(output='both')

# Logs appear in both console and file
conn = mssql_python.connect(server='localhost', database='testdb')
```

### Custom Log File Path

```python
import mssql_python

# Specify custom log file path
mssql_python.setup_logging(log_file_path="/var/log/myapp/mssql.log")

# Or with both file and stdout
mssql_python.setup_logging(output='both', log_file_path="/tmp/debug.log")

conn = mssql_python.connect(server='localhost', database='testdb')

# Check log file location
from mssql_python.logging import logger
print(f"Logging to: {logger.log_file}")
# Output: Logging to: /var/log/myapp/mssql.log
```

## Output Destinations

### File Only (Default)

```python
import mssql_python

# File logging is enabled by default
mssql_python.setup_logging()

# Files are automatically rotated at 512MB, keeps 5 backups
# File location: ./mssql_python_logs/mssql_python_trace_YYYYMMDDHHMMSS_PID.log
# (mssql_python_logs folder is created automatically if it doesn't exist)

conn = mssql_python.connect(server='localhost', database='testdb')

from mssql_python.logging import logger
print(f"Logging to: {logger.log_file}")
```

### Stdout Only

```python
import mssql_python

# Log to stdout only (useful for CI/CD, Docker containers)
mssql_python.setup_logging(output='stdout')

conn = mssql_python.connect(server='localhost', database='testdb')
# Logs appear in console, no file created
```

### Both File and Stdout

```python
import mssql_python

# Log to both destinations (useful for development)
mssql_python.setup_logging(output='both')

conn = mssql_python.connect(server='localhost', database='testdb')
# Logs appear in both console and file
```

## Log Output Examples

### Standard Output

When logging is enabled, you see EVERYTHING - SQL statements, parameters, internal operations.

**File Header:**
```
# MSSQL-Python Driver Log | Script: main.py | PID: 12345 | Log Level: DEBUG | Python: 3.13.7 | Start: 2025-11-06 10:30:15
Timestamp, ThreadID, Level, Location, Source, Message
```

**Sample Entries:**
```
2025-11-06 10:30:15.100, 8581947520, DEBUG, connection.py:156, Python, Allocating environment handle
2025-11-06 10:30:15.101, 8581947520, DEBUG, connection.cpp:22, DDBC, Allocating ODBC environment handle
2025-11-06 10:30:15.123, 8581947520, DEBUG, connection.py:42, Python, Connecting to server: localhost
2025-11-06 10:30:15.456, 8581947520, DEBUG, cursor.py:28, Python, Executing query: SELECT * FROM users WHERE id = ?
2025-11-06 10:30:15.457, 8581947520, DEBUG, cursor.py:89, Python, Query parameters: [42]
2025-11-06 10:30:15.789, 8581947520, DEBUG, cursor.py:145, Python, Fetched 1 row
2025-11-06 10:30:15.790, 8581947520, DEBUG, cursor.py:201, Python, Row buffer allocated
```

**Log Format:**
- **Timestamp**: Date and time with milliseconds
- **ThreadID**: OS native thread ID (matches debugger thread IDs)
- **Level**: DEBUG, INFO, WARNING, ERROR
- **Location**: filename:line_number
- **Source**: Python or DDBC (C++ layer)
- **Message**: The actual log message

**What You'll See:**
- ✅ Connection establishment and configuration
- ✅ SQL query text
- ✅ Query parameters (with PII sanitization)
- ✅ Result set information
- ✅ Internal ODBC operations
- ✅ Memory allocations and handle management
- ✅ Transaction state changes
- ✅ Everything the driver does

## Advanced Features

### Password Sanitization

Sensitive data like passwords and access tokens are automatically sanitized in logs:

```python
conn = mssql_python.connect(
    server='localhost',
    database='testdb',
    username='admin',
    password='MySecretPass123!'
)

# Log output shows:
# Connection string: Server=localhost;Database=testdb;UID=admin;PWD=***REDACTED***
```

Keywords automatically sanitized:
- `password`, `pwd`, `passwd`
- `access_token`, `accesstoken`
- `secret`, `api_key`, `apikey`
- `token`, `auth`, `authentication`

### Thread Tracking

Each log entry includes the **OS native thread ID** for tracking operations in multi-threaded applications:

**Thread ID Benefits:**
- **Debugger Compatible**: Thread IDs match those shown in debuggers (Visual Studio, gdb, lldb)
- **OS Native**: Same thread ID visible in system monitoring tools
- **Multi-threaded Tracking**: Easily identify which thread performed which operations
- **Performance Analysis**: Correlate logs with profiler/debugger thread views

**Example:**
```python
import mssql_python
import threading

# Enable logging
mssql_python.setup_logging()

conn = mssql_python.connect("Server=localhost;Database=test")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")

# Log output shows (CSV format):
# 2025-11-06 10:30:15.100, 8581947520, DEBUG, connection.py:42, Python, Connection established
# 2025-11-06 10:30:15.102, 8581947520, DEBUG, cursor.py:15, Python, Cursor created
# 2025-11-06 10:30:15.103, 8581947520, DEBUG, cursor.py:28, Python, Executing query: SELECT * FROM users

# Different thread/connection (note different ThreadID):
# 2025-11-06 10:30:15.200, 8582001664, DEBUG, connection.py:42, Python, Connection established
```

**Why Thread IDs Matter:**
- **Multi-threading**: Distinguish logs from different threads writing to the same file
- **Connection pools**: Track which thread is handling which connection
- **Debugging**: Filter logs by thread ID using text tools (grep, awk, etc.)
- **Performance analysis**: Measure duration of specific operations per thread
- **Debugger Correlation**: Thread ID matches debugger views for easy debugging

### Using mssql-python's Logger in Your Application

You can access the same logger used by mssql-python in your application code:

```python
import mssql_python
from mssql_python.logging import driver_logger

# Enable logging first
mssql_python.setup_logging()

# Now use driver_logger in your application
driver_logger.debug("[App] Starting data processing")
driver_logger.info("[App] Processing complete")
driver_logger.warning("[App] Resource usage high")
driver_logger.error("[App] Failed to process record")

# Your logs will appear in the same file as driver logs,
# with the same format and thread tracking
```

**Benefits:**
- Unified logging - all logs in one place
- Same format and structure as driver logs
- Automatic thread ID tracking
- No need to configure separate loggers

### Importing Logs as CSV (Optional)

Log files use comma-separated format and can be imported into spreadsheet tools:

```python
import pandas as pd

# Import log file (skip header lines starting with #)
df = pd.read_csv('mssql_python_logs/mssql_python_trace_20251106103015_12345.log', 
                 comment='#')

# Filter by thread, analyze queries, etc.
thread_logs = df[df['ThreadID'] == 8581947520]
```

### Programmatic Log Access (Advanced)

```python
import mssql_python
from mssql_python.logging import logger
import logging as py_logging

# Add custom handler to process logs programmatically
class MyLogHandler(py_logging.Handler):
    def emit(self, record):
        # Process log record
        print(f"Custom handler: {record.getMessage()}")
        
        # Access trace ID
        trace_id = getattr(record, 'trace_id', None)
        if trace_id:
            print(f"  Trace ID: {trace_id}")

handler = MyLogHandler()
logger.addHandler(handler)

# Now enable logging
mssql_python.setup_logging()
```

## API Reference

### Primary Function

**`mssql_python.setup_logging(output: str = 'file', log_file_path: str = None) -> None`**

Enable comprehensive DEBUG logging for troubleshooting.

**Parameters:**
- `output` (str, optional): Where to send logs. Options: `'file'` (default), `'stdout'`, `'both'`
- `log_file_path` (str, optional): Custom log file path. Must have extension: `.txt`, `.log`, or `.csv`. If not specified, auto-generates path in `./mssql_python_logs/`

**Raises:**
- `ValueError`: If `log_file_path` has an invalid extension (only `.txt`, `.log`, `.csv` are allowed)

**Examples:**

```python
import mssql_python

# Basic usage - file logging (default, auto-generated path)
mssql_python.setup_logging()

# Output to stdout only
mssql_python.setup_logging(output='stdout')

# Output to both file and stdout
mssql_python.setup_logging(output='both')

# Custom log file path (must use .txt, .log, or .csv extension)
mssql_python.setup_logging(log_file_path="/var/log/myapp.log")
mssql_python.setup_logging(log_file_path="/tmp/debug.txt")
mssql_python.setup_logging(log_file_path="/tmp/data.csv")

# Custom path with both outputs
mssql_python.setup_logging(output='both', log_file_path="/tmp/debug.log")

# Invalid extensions will raise ValueError
try:
    mssql_python.setup_logging(log_file_path="/tmp/debug.json")  # ✗ Error
except ValueError as e:
    print(e)  # "Invalid log file extension '.json'. Allowed extensions: .csv, .log, .txt"
```

### Advanced - Using driver_logger in Your Code

Access the same logger used by mssql-python in your application:

```python
from mssql_python.logging import driver_logger
import mssql_python

# Enable logging
mssql_python.setup_logging()

# Use driver_logger in your application
driver_logger.debug("[App] Starting data processing")
driver_logger.info("[App] Processing complete")
driver_logger.warning("[App] Resource usage high")
driver_logger.error("[App] Failed to process record")

# Your logs appear in the same file with same format
```

### Advanced - Logger Instance

For advanced use cases, you can access the logger instance directly:

```python
from mssql_python.logging import logger

# Get log file path
print(f"Logging to: {logger.log_file}")

# Add custom handlers (for integration)
import logging as py_logging
custom_handler = py_logging.StreamHandler()
logger.addHandler(custom_handler)

# Direct logging calls (if needed)
logger.debug("Custom debug message")
```

## Extensibility

### Pattern 1: Use Driver Logger Across Your Application

If you want to use the driver's logger for your own application logging:

```python
import mssql_python
from mssql_python.logging import logger

# Enable driver logging
mssql_python.setup_logging(output='stdout')

# Use the logger in your application
class MyApp:
    def __init__(self):
        logger.debug("Application starting")
        self.db = self._connect_db()
        logger.debug("Application ready")
    
    def _connect_db(self):
        logger.debug("Connecting to database")
        conn = mssql_python.connect("Server=localhost;Database=test")
        logger.debug("Database connected successfully")
        return conn
    
    def process_data(self):
        logger.debug("Processing data")
        cursor = self.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        logger.debug(f"Processed {count} users")
        return count

if __name__ == '__main__':
    app = MyApp()
    result = app.process_data()
```

**Output shows unified logging:**
```
2025-11-03 10:15:22 - mssql_python - DEBUG - Application starting
2025-11-03 10:15:22 - mssql_python - DEBUG - Connecting to database
2025-11-03 10:15:22 - mssql_python - DEBUG - [Python] Initializing connection
2025-11-03 10:15:22 - mssql_python - DEBUG - Database connected successfully
2025-11-03 10:15:22 - mssql_python - DEBUG - Application ready
2025-11-03 10:15:22 - mssql_python - DEBUG - Processing data
2025-11-03 10:15:22 - mssql_python - DEBUG - [Python] Executing query
2025-11-03 10:15:22 - mssql_python - DEBUG - Processed 1000 users
```

### Pattern 2: Plug Driver Logger Into Your Existing Logger

If you already have application logging configured and want to integrate driver logs:

```python
import logging
import mssql_python
from mssql_python.logging import logger as mssql_logger

# Your existing application logger setup
app_logger = logging.getLogger('myapp')
app_logger.setLevel(logging.INFO)

# Your existing handler and formatter
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
app_logger.addHandler(handler)

# Now plug the driver logger into your handler
mssql_logger.addHandler(handler)  # Use your handler
mssql_python.setup_logging()  # Enable driver diagnostics

# Use your app logger as normal
app_logger.info("Application starting")

# Driver logs go to the same destination
conn = mssql_python.connect("Server=localhost;Database=test")

app_logger.info("Querying database")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")

app_logger.info("Application complete")
```

**Output shows both app and driver logs in your format:**
```
2025-11-03 10:15:22 - myapp - INFO - Application starting
2025-11-03 10:15:22 - mssql_python - DEBUG - [Python] Initializing connection
2025-11-03 10:15:22 - mssql_python - DEBUG - [Python] Connection established
2025-11-03 10:15:22 - myapp - INFO - Querying database
2025-11-03 10:15:22 - mssql_python - DEBUG - [Python] Executing query
2025-11-03 10:15:22 - myapp - INFO - Application complete
```

**Key Benefits:**
- All logs go to your existing handlers (file, console, cloud, etc.)
- Use your existing formatters and filters
- Centralized log management
- No separate log files to manage

### Pattern 3: Advanced - Custom Log Processing

For advanced scenarios where you want to process driver logs programmatically:

```python
import logging
import mssql_python
from mssql_python.logging import logger as mssql_logger

class DatabaseAuditHandler(logging.Handler):
    """Custom handler that audits database operations."""
    
    def __init__(self):
        super().__init__()
        self.queries = []
        self.connections = []
    
    def emit(self, record):
        msg = record.getMessage()
        
        # Track queries
        if 'Executing query' in msg:
            self.queries.append({
                'time': record.created,
                'query': msg
            })
        
        # Track connections
        if 'Connection established' in msg:
            self.connections.append({
                'time': record.created,
                'level': record.levelname
            })

# Setup audit handler
audit_handler = DatabaseAuditHandler()
mssql_logger.addHandler(audit_handler)
mssql_python.setup_logging()

# Use the driver
conn = mssql_python.connect("Server=localhost;Database=test")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
cursor.execute("SELECT * FROM orders")
conn.close()

# Access audit data
print(f"Total queries executed: {len(audit_handler.queries)}")
print(f"Total connections: {len(audit_handler.connections)}")
for query in audit_handler.queries:
    print(f"  - {query['query']}")
```

## Common Patterns

### Development Setup

```python
import mssql_python

# Both console and file - see everything
mssql_python.setup_logging(output='both')

# Use the driver - see everything in console and file
conn = mssql_python.connect("Server=localhost;Database=test")
```

### Production Setup

```python
import mssql_python

# ⚠️ DO NOT enable logging in production without reason
# Logging adds overhead and should only be used for troubleshooting

# If needed for specific troubleshooting:
# mssql_python.setup_logging()  # Temporary only!
```

### CI/CD Pipeline Setup

```python
import mssql_python

# Stdout only (captured by CI system, no files)
mssql_python.setup_logging(output='stdout')

# CI will capture all driver logs
conn = mssql_python.connect(connection_string)
```

### Debugging Specific Issues

```python
import mssql_python

# For ANY debugging - just enable logging (shows everything)
mssql_python.setup_logging(output='both')  # See in console + save to file

# Save debug logs to specific location for analysis
mssql_python.setup_logging(log_file_path="/tmp/mssql_debug.log")

# For CI/CD troubleshooting
mssql_python.setup_logging(output='stdout')
```

### Integrate with Application Logging

```python
import logging as py_logging
import mssql_python
from mssql_python.logging import logger as mssql_logger

# Setup your application logger
app_logger = py_logging.getLogger('myapp')
app_logger.setLevel(py_logging.INFO)

# Setup handler
handler = py_logging.StreamHandler()
handler.setFormatter(py_logging.Formatter('%(name)s - %(message)s'))
app_logger.addHandler(handler)

# Plug driver logger into your handler
mssql_logger.addHandler(handler)
mssql_python.setup_logging()

# Both logs go to same destination
app_logger.info("App started")
conn = mssql_python.connect("Server=localhost;Database=test")
app_logger.info("Database connected")
```

## Troubleshooting

### No Log Output

```python
import mssql_python
from mssql_python.logging import logger

# Make sure you called setup_logging
mssql_python.setup_logging(output='stdout')  # Force stdout output

# Check logger level
print(f"Logger level: {logger.level}")
```

### Where is the Log File?

```python
import mssql_python
from mssql_python.logging import logger

# Enable logging first
mssql_python.setup_logging()

# Then check location
print(f"Log file: {logger.log_file}")
# Output: ./mssql_python_logs/mssql_python_trace_20251103_101522_12345.log
```

### Logs Not Showing in CI/CD

```python
# Use stdout for CI/CD systems
import mssql_python

mssql_python.setup_logging(output='stdout')
# Now logs go to stdout and CI can capture them
```

## Best Practices

1. **⚠️ Performance Warning**: Logging has overhead - only enable when troubleshooting
   ```python
   # ❌ DON'T enable logging by default
   # ✅ DO enable only when investigating issues
   ```

2. **Enable Early**: Configure logging before creating connections
   ```python
   mssql_python.setup_logging()  # Do this first
   conn = mssql_python.connect(...)  # Then connect
   ```

3. **Choose Right Output Destination**:
   - **Development/Troubleshooting**: `output='both'` (see logs immediately + keep file)
   - **CI/CD**: `output='stdout'` (no file clutter, captured by CI)
   - **Customer debugging**: `output='file'` with custom path (default)

4. **Log Files Auto-Rotate**: Files automatically rotate at 512MB, keeps 5 backups

5. **Sanitization is Automatic**: Passwords are automatically redacted in logs

6. **One-Line Setup**: Simple API:
   ```python
   mssql_python.setup_logging()  # That's it!
   ```

7. **Not for Performance Analysis**: Use profiler (future enhancement) for query performance, not logging

## Examples

### Complete Application Example

```python
#!/usr/bin/env python3
"""Example application with optional logging."""

import sys
import mssql_python
from mssql_python.logging import logger

def main(debug: bool = False):
    """Run the application with optional debug logging."""
    
    # Setup logging only if debugging
    if debug:
        # Development: both file and console
        mssql_python.setup_logging(output='both')
        print(f"Logging to: {logger.log_file}")
    
    # Connect to database
    conn = mssql_python.connect(
        server='localhost',
        database='testdb',
        trusted_connection='yes'
    )
    
    # Execute query
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 10 * FROM users WHERE active = ?", (1,))
    
    # Process results
    for row in cursor:
        print(f"User: {row.username}")
    
    # Cleanup
    cursor.close()
    conn.close()

if __name__ == '__main__':
    import sys
    debug = '--debug' in sys.argv
    main(debug=debug)
```

## Performance Considerations

- **⚠️ Logging Has Overhead**: When enabled, logging adds ~2-5% performance overhead
  ```python
  # Logging disabled by default - no overhead
  conn = mssql_python.connect(...)  # Full performance
  
  # Enable only when troubleshooting
  mssql_python.setup_logging()  # Now has ~2-5% overhead
  ```

- **Not for Performance Analysis**: Do NOT use logging to measure query performance
  - Logging itself adds latency
  - Use profiler (future enhancement) for accurate performance metrics

- **Lazy Initialization**: Handlers are only created when `setup_logging()` is called

- **File I/O**: File logging has minimal overhead with buffering

- **Automatic Rotation**: Files rotate at 512MB to prevent disk space issues

## Design Philosophy

**Simple and Purposeful**

1. **All or Nothing**: No levels to choose from - either debug everything or don't log
2. **Troubleshooting Tool**: Logging is for diagnosing problems, not production monitoring
3. **Performance Conscious**: Clear warning that logging has overhead
4. **Future-Proof**: Profiler (future) will handle performance analysis properly

### Minimal API Surface

Most users only need one line:

```python
mssql_python.setup_logging()  # That's it!
```

This follows the [Zen of Python](https://www.python.org/dev/peps/pep-0020/): "Simple is better than complex."

## Support

For issues or questions:
- GitHub Issues: [microsoft/mssql-python](https://github.com/microsoft/mssql-python)
- Documentation: See `MSSQL-Python-Logging-Design.md` for technical details

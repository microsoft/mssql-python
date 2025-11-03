# Logging Guide for mssql-python

This guide explains how to use the enhanced logging system in mssql-python, which follows JDBC-style logging patterns with custom log levels and comprehensive diagnostic capabilities.

## Table of Contents

- [Quick Start](#quick-start)
- [Log Levels](#log-levels)
- [Basic Usage](#basic-usage)
- [File Logging](#file-logging)
- [Log Output Examples](#log-output-examples)
- [Advanced Features](#advanced-features)
- [API Reference](#api-reference)
- [Extensibility](#extensibility)

## Quick Start

### Minimal Usage (Recommended)

```python
import mssql_python
from mssql_python import logging

# Enable driver diagnostics (one line)
logging.setLevel(logging.FINE)

# Use the driver - all operations are now logged
conn = mssql_python.connect("Server=localhost;Database=test")
# Check the log file: mssql_python_trace_*.log
```

### With More Control

```python
import mssql_python
from mssql_python import logging

# Enable detailed SQL logging
logging.setLevel(logging.FINE)  # Logs SQL statements

# Enable very detailed logging
logging.setLevel(logging.FINER)  # Logs SQL + parameters

# Enable maximum detail logging
logging.setLevel(logging.FINEST)  # Logs everything including internal operations

# Output to stdout instead of file
logging.setLevel(logging.FINE, logging.STDOUT)

# Output to both file and stdout
logging.setLevel(logging.FINE, logging.BOTH)
```

## Log Levels

The logging system uses both standard Python levels and custom JDBC-style levels:

| Level | Value | Description | Use Case |
|-------|-------|-------------|----------|
| **FINEST** | 5 | Most detailed logging | Deep debugging, tracing all operations |
| **DEBUG** | 10 | Standard debug | General debugging (Python standard) |
| **FINER** | 15 | Very detailed logging | SQL with parameters, connection details |
| **FINE** | 18 | Detailed logging | SQL statements, major operations |
| **INFO** | 20 | Informational | Connection status, important events |
| **WARNING** | 30 | Warnings | Recoverable errors, deprecations |
| **ERROR** | 40 | Errors | Operation failures |
| **CRITICAL** | 50 | Critical errors | System failures |

**Important**: In Python logging, **LOWER numbers = MORE detailed** output. When you set `logger.setLevel(FINEST)`, you'll see all log levels including FINEST, FINER, FINE, DEBUG, INFO, WARNING, ERROR, and CRITICAL.

### Level Hierarchy

```
FINEST (5) ← Most detailed
    ↓
DEBUG (10)
    ↓
FINER (15)
    ↓
INFO (20)
    ↓
FINE (25)
    ↓
WARNING (30)
    ↓
ERROR (40)
    ↓
CRITICAL (50) ← Least detailed
```

## Basic Usage

### Default - File Logging

```python
import mssql_python
from mssql_python import logging

# Enable logging (logs to file by default)
logging.setLevel(logging.FINE)

# Use the library - logs will appear in file
conn = mssql_python.connect(server='localhost', database='testdb')
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")

print(f"Logs written to: {logging.logger.log_file}")
```

### Console Logging

```python
import mssql_python
from mssql_python import logging

# Enable logging to stdout
logging.setLevel(logging.FINE, logging.STDOUT)

# Now use the library - logs will appear in console
conn = mssql_python.connect(server='localhost', database='testdb')
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
```

### Both File and Console

```python
import mssql_python
from mssql_python import logging

# Enable logging to both file and stdout
logging.setLevel(logging.FINE, logging.BOTH)

# Logs appear in both console and file
conn = mssql_python.connect(server='localhost', database='testdb')
```

## Output Destinations

### File Only (Default)

```python
from mssql_python import logging

# File logging is enabled by default
logging.setLevel(logging.FINE)

# Files are automatically rotated at 512MB, keeps 5 backups
# File location: ./mssql_python_trace_YYYYMMDD_HHMMSS_PID.log

conn = mssql_python.connect(server='localhost', database='testdb')
print(f"Logging to: {logging.logger.log_file}")
```

### Stdout Only

```python
from mssql_python import logging

# Log to stdout only (useful for CI/CD, Docker containers)
logging.setLevel(logging.FINE, logging.STDOUT)

conn = mssql_python.connect(server='localhost', database='testdb')
# Logs appear in console, no file created
```

### Both File and Stdout

```python
from mssql_python import logging

# Log to both destinations (useful for development)
logging.setLevel(logging.FINE, logging.BOTH)

conn = mssql_python.connect(server='localhost', database='testdb')
# Logs appear in both console and file
```

## Log Output Examples

### FINE Level Output

Shows SQL statements and major operations:

```
2024-10-31 10:30:15,123 [CONN-12345-67890-1] - FINE - connection.py:42 - [Python] Connecting to server: localhost
2024-10-31 10:30:15,456 [CURS-12345-67890-2] - FINE - cursor.py:28 - [Python] Executing query: SELECT * FROM users WHERE id = ?
2024-10-31 10:30:15,789 [CURS-12345-67890-2] - FINE - cursor.py:89 - [Python] Query completed, 42 rows fetched
```

### FINER Level Output

Shows SQL statements with parameters:

```
2024-10-31 10:30:15,123 [CONN-12345-67890-1] - FINER - connection.py:42 - [Python] Connection parameters: {'server': 'localhost', 'database': 'testdb', 'trusted_connection': 'yes'}
2024-10-31 10:30:15,456 [CURS-12345-67890-2] - FINER - cursor.py:28 - [Python] Executing query: SELECT * FROM users WHERE id = ?
2024-10-31 10:30:15,457 [CURS-12345-67890-2] - FINER - cursor.py:89 - [Python] Query parameters: [42]
2024-10-31 10:30:15,789 [CURS-12345-67890-2] - FINER - cursor.py:145 - [Python] Fetched 1 row
```

### FINEST Level Output

Shows all internal operations:

```
2024-10-31 10:30:15,100 [CONN-12345-67890-1] - FINEST - connection.py:156 - [Python] Allocating environment handle
2024-10-31 10:30:15,101 [CONN-12345-67890-1] - FINEST - connection.py:178 - [Python] Setting ODBC version to 3.8
2024-10-31 10:30:15,123 [CONN-12345-67890-1] - FINEST - connection.py:201 - [Python] Building connection string
2024-10-31 10:30:15,456 [CURS-12345-67890-2] - FINEST - cursor.py:89 - [Python] Preparing statement handle
2024-10-31 10:30:15,457 [CURS-12345-67890-2] - FINEST - cursor.py:134 - [Python] Binding parameter 1: type=int, value=42
2024-10-31 10:30:15,789 [CURS-12345-67890-2] - FINEST - cursor.py:201 - [Python] Row buffer allocated
```

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

### Trace IDs

Each connection and cursor gets a unique trace ID for tracking in multi-threaded applications:

**Trace ID Format:**
- Connection: `CONN-<PID>-<ThreadID>-<Counter>`
- Cursor: `CURS-<PID>-<ThreadID>-<Counter>`

**Example:**
```python
from mssql_python import logging

# Enable logging
logging.setLevel(logging.FINE, logging.STDOUT)

# Trace IDs are automatically included in all log records
conn = mssql_python.connect("Server=localhost;Database=test")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")

# Log output shows:
# [CONN-12345-67890-1] - Connection established
# [CURS-12345-67890-2] - Cursor created
# [CURS-12345-67890-2] - Executing query: SELECT * FROM users

# Different thread/connection:
# [CONN-12345-98765-3] - Connection established  (different ThreadID)
```

**Why Trace IDs Matter:**
- **Multi-threading**: Distinguish logs from different threads writing to the same file
- **Connection pools**: Track which connection performed which operation
- **Debugging**: Filter logs with `grep "CONN-12345-67890-1" logfile.log`
- **Performance analysis**: Measure duration of specific operations

**Custom Trace IDs** (Advanced):
```python
from mssql_python import logging

# Generate custom trace ID (e.g., for background tasks)
trace_id = logging.logger.generate_trace_id("TASK")
logging.logger.set_trace_id(trace_id)

logging.logger.info("Task started")
# Output: [TASK-12345-67890-1] - Task started

# Clear when done
logging.logger.clear_trace_id()
```

### Programmatic Log Access

```python
from mssql_python import logger
import logging

# Add custom handler to process logs programmatically
class MyLogHandler(logging.Handler):
    def emit(self, record):
        # Process log record
        print(f"Custom handler: {record.getMessage()}")
        
        # Access trace ID
        trace_id = getattr(record, 'trace_id', None)
        if trace_id:
            print(f"  Trace ID: {trace_id}")

handler = MyLogHandler()
logger.addHandler(handler)
```

### Reset Handlers

Remove all configured handlers:

```python
from mssql_python import logger

# Remove all handlers (useful for reconfiguration)
logger.reset_handlers()

# Reconfigure from scratch
logger.setLevel('INFO')
# Add new handlers...
```

## API Reference

### Module-Level Functions (Recommended)

```python
from mssql_python import logging
```

**`logging.setLevel(level: int, output: str = None) -> None`**

Set the logging threshold level and optionally configure output destination.

```python
# Basic usage - file logging (default)
logging.setLevel(logging.FINEST)
logging.setLevel(logging.FINER)
logging.setLevel(logging.FINE)

# With output control
logging.setLevel(logging.FINE, logging.STDOUT)  # Stdout only
logging.setLevel(logging.FINE, logging.BOTH)    # Both file and stdout
```

**`logging.getLevel() -> int`**

Get the current logging level.

```python
current_level = logging.getLevel()
print(f"Current level: {current_level}")
```

**`logging.isEnabledFor(level: int) -> bool`**

Check if a specific log level is enabled.

```python
if logging.isEnabledFor(logging.FINEST):
    expensive_data = compute_diagnostics()
    logging.logger.finest(f"Diagnostics: {expensive_data}")
```

### Log Level Constants

```python
from mssql_python import logging

# Driver Levels (use these for driver diagnostics)
logging.FINEST  # Value: 5  - Ultra-detailed
logging.FINER   # Value: 15 - Detailed
logging.FINE    # Value: 18 - Standard (recommended default)

# Python standard levels (also available)
logging.INFO    # Value: 20
logging.WARNING # Value: 30
logging.ERROR   # Value: 40
```

### Output Destination Constants

```python
from mssql_python import logging

logging.FILE    # 'file'   - Log to file only (default)
logging.STDOUT  # 'stdout' - Log to stdout only
logging.BOTH    # 'both'   - Log to both destinations
```

### Logger Instance (Advanced)

For advanced use cases, you can access the logger instance directly:

```python
from mssql_python import logging

# Access the logger instance
logger = logging.logger

# Direct method calls
logger.fine("Standard diagnostic message")
logger.finer("Detailed diagnostic message")
logger.finest("Ultra-detailed trace message")

# Get log file path
print(f"Logging to: {logger.log_file}")

# Add custom handlers (for integration)
import logging as py_logging
custom_handler = py_logging.StreamHandler()
logger.addHandler(custom_handler)
```

## Extensibility

### Pattern 1: Use Driver Logger Across Your Application

If you want to use the driver's logger for your own application logging:

```python
import mssql_python
from mssql_python import logging

# Enable driver logging
logging.setLevel(logging.FINE, logging.STDOUT)

# Get the logger instance for your app code
logger = logging.logger

# Use it in your application
class MyApp:
    def __init__(self):
        logger.info("Application starting")
        self.db = self._connect_db()
        logger.info("Application ready")
    
    def _connect_db(self):
        logger.fine("Connecting to database")
        conn = mssql_python.connect("Server=localhost;Database=test")
        logger.info("Database connected successfully")
        return conn
    
    def process_data(self):
        logger.info("Processing data")
        cursor = self.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        logger.info(f"Processed {count} users")
        return count

if __name__ == '__main__':
    app = MyApp()
    result = app.process_data()
```

**Output shows unified logging:**
```
2025-11-03 10:15:22 - mssql_python - INFO - Application starting
2025-11-03 10:15:22 - mssql_python - FINE - Connecting to database
2025-11-03 10:15:22 - mssql_python - FINE - [Python] Initializing connection
2025-11-03 10:15:22 - mssql_python - INFO - Database connected successfully
2025-11-03 10:15:22 - mssql_python - INFO - Application ready
2025-11-03 10:15:22 - mssql_python - INFO - Processing data
2025-11-03 10:15:22 - mssql_python - FINE - [Python] Executing query
2025-11-03 10:15:22 - mssql_python - INFO - Processed 1000 users
```

### Pattern 2: Plug Driver Logger Into Your Existing Logger

If you already have application logging configured and want to integrate driver logs:

```python
import logging
import mssql_python
from mssql_python import logging as mssql_logging

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
mssql_driver_logger = mssql_logging.logger
mssql_driver_logger.addHandler(handler)  # Use your handler
mssql_driver_logger.setLevel(mssql_logging.FINE)  # Enable driver diagnostics

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
2025-11-03 10:15:22 - mssql_python - FINE - [Python] Initializing connection
2025-11-03 10:15:22 - mssql_python - FINE - [Python] Connection established
2025-11-03 10:15:22 - myapp - INFO - Querying database
2025-11-03 10:15:22 - mssql_python - FINE - [Python] Executing query
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
from mssql_python import logging as mssql_logging

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
mssql_logging.logger.addHandler(audit_handler)
mssql_logging.setLevel(mssql_logging.FINE)

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
from mssql_python import logging

# Both console and file with full details
logging.setLevel(logging.FINEST, logging.BOTH)

# Use the driver - see everything in console and file
conn = mssql_python.connect("Server=localhost;Database=test")
```

### Production Setup

```python
from mssql_python import logging

# File logging only (default), standard detail level
logging.setLevel(logging.FINE)

# Or disable logging entirely for production
logging.setLevel(logging.CRITICAL)  # Effectively OFF
```

### CI/CD Pipeline Setup

```python
from mssql_python import logging

# Stdout only (captured by CI system, no files)
logging.setLevel(logging.FINE, logging.STDOUT)

# CI will capture all driver logs
conn = mssql_python.connect(connection_string)
```

### Debugging Specific Issues

```python
from mssql_python import logging

# Debug connection issues: use FINER to see connection parameters
logging.setLevel(logging.FINER)

# Debug SQL execution: use FINE to see SQL statements
logging.setLevel(logging.FINE)

# Debug parameter binding: use FINER to see parameters
logging.setLevel(logging.FINER)

# Debug internal operations: use FINEST to see everything
logging.setLevel(logging.FINEST)
```

### Integrate with Application Logging

```python
import logging as py_logging
from mssql_python import logging as mssql_logging

# Setup your application logger
app_logger = py_logging.getLogger('myapp')
app_logger.setLevel(py_logging.INFO)

# Setup handler
handler = py_logging.StreamHandler()
handler.setFormatter(py_logging.Formatter('%(name)s - %(message)s'))
app_logger.addHandler(handler)

# Plug driver logger into your handler
mssql_logging.logger.addHandler(handler)
mssql_logging.setLevel(mssql_logging.FINE)

# Both logs go to same destination
app_logger.info("App started")
conn = mssql_python.connect("Server=localhost;Database=test")
app_logger.info("Database connected")
```

## Troubleshooting

### No Log Output

```python
from mssql_python import logging

# Check if logging is enabled
print(f"Current level: {logging.getLevel()}")
print(f"Is FINE enabled? {logging.isEnabledFor(logging.FINE)}")

# Make sure you called setLevel
logging.setLevel(logging.FINE, logging.STDOUT)  # Force stdout output
```

### Too Much Output

```python
from mssql_python import logging

# Reduce logging level
logging.setLevel(logging.ERROR)  # Only errors
logging.setLevel(logging.CRITICAL)  # Effectively OFF
```

### Where is the Log File?

```python
from mssql_python import logging

# Enable logging first
logging.setLevel(logging.FINE)

# Then check location
print(f"Log file: {logging.logger.log_file}")
# Output: ./mssql_python_trace_20251103_101522_12345.log
```

### Logs Not Showing in CI/CD

```python
# Use STDOUT for CI/CD systems
from mssql_python import logging

logging.setLevel(logging.FINE, logging.STDOUT)
# Now logs go to stdout and CI can capture them
```

## Best Practices

1. **Set Level Early**: Configure logging before creating connections
   ```python
   logging.setLevel(logging.FINE)  # Do this first
   conn = mssql_python.connect(...)  # Then connect
   ```

2. **Use Appropriate Levels**: 
   - **Production**: `logging.CRITICAL` (effectively OFF) or `logging.ERROR`
   - **Troubleshooting**: `logging.FINE` (standard diagnostics)
   - **Deep debugging**: `logging.FINER` or `logging.FINEST`

3. **Choose Right Output Destination**:
   - **Development**: `logging.BOTH` (see logs immediately + keep file)
   - **Production**: Default file logging
   - **CI/CD**: `logging.STDOUT` (no file clutter)

4. **Log Files Auto-Rotate**: Files automatically rotate at 512MB, keeps 5 backups

5. **Sanitization is Automatic**: Passwords are automatically redacted in logs

6. **One-Line Setup**: The new API is designed for simplicity:
   ```python
   logging.setLevel(logging.FINE, logging.STDOUT)  # That's it!
   ```

## Examples

### Complete Application Example

```python
#!/usr/bin/env python3
"""Example application with comprehensive logging."""

import sys
import mssql_python
from mssql_python import logging

def main(verbose: bool = False):
    """Run the application with optional verbose logging."""
    
    # Setup logging based on verbosity
    if verbose:
        # Development: both file and console, detailed
        logging.setLevel(logging.FINEST, logging.BOTH)
    else:
        # Production: file only, standard detail
        logging.setLevel(logging.FINE)
    
    print(f"Logging to: {logging.logger.log_file}")
    
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
    verbose = '--verbose' in sys.argv
    main(verbose=verbose)
```

## Performance Considerations

- **Zero Overhead When Disabled**: When logging is not enabled, there is virtually no performance impact
  ```python
  # Logging disabled by default - no overhead
  conn = mssql_python.connect(...)  # No logging cost
  
  # Enable only when needed
  logging.setLevel(logging.FINE)  # Now logging has ~2-5% overhead
  ```

- **Lazy Initialization**: Handlers are only created when `setLevel()` is called

- **File I/O**: File logging has minimal overhead with buffering

- **Automatic Rotation**: Files rotate at 512MB to prevent disk space issues and maintain performance

## Design Philosophy

The logging API is designed to match Python's standard library patterns:

### Pythonic Module Pattern

```python
# Just like Python's logging module
import logging
logging.info("message")
logging.DEBUG

# mssql-python follows the same pattern
from mssql_python import logging
logging.setLevel(logging.FINE)
logging.FINE
```

### Flat Namespace

Constants are at the module level, not nested in classes:

```python
# ✅ Good (flat, Pythonic)
logging.FINE
logging.STDOUT
logging.BOTH

# ❌ Not used (nested, verbose)
logging.OutputMode.STDOUT  # We don't do this
logging.LogLevel.FINE      # We don't do this
```

This follows the [Zen of Python](https://www.python.org/dev/peps/pep-0020/): "Flat is better than nested."

### Minimal API Surface

Most users only need one line:

```python
logging.setLevel(logging.FINE)  # That's it!
```

## Support

For issues or questions:
- GitHub Issues: [microsoft/mssql-python](https://github.com/microsoft/mssql-python)
- Documentation: See `MSSQL-Python-Logging-Design.md` for technical details

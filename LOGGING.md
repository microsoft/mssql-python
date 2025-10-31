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
- [Migration from Old Logging](#migration-from-old-logging)

## Quick Start

```python
import mssql_python
from mssql_python import logger, FINE, FINER, FINEST

# Enable logging at INFO level (default Python level)
logger.setLevel('INFO')

# Enable detailed SQL logging
logger.setLevel(FINE)  # Logs SQL statements

# Enable very detailed logging
logger.setLevel(FINER)  # Logs SQL + parameters

# Enable maximum detail logging
logger.setLevel(FINEST)  # Logs everything including internal operations
```

## Log Levels

The logging system uses both standard Python levels and custom JDBC-style levels:

| Level | Value | Description | Use Case |
|-------|-------|-------------|----------|
| **FINEST** | 5 | Most detailed logging | Deep debugging, tracing all operations |
| **FINER** | 15 | Very detailed logging | SQL with parameters, connection details |
| **FINE** | 25 | Detailed logging | SQL statements, major operations |
| **DEBUG** | 10 | Standard debug | General debugging (between FINEST and FINER) |
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

### Enable Console Logging

```python
import mssql_python
from mssql_python import logger, FINE, FINER, FINEST

# Set logging level
logger.setLevel(FINE)

# Add console handler (logs to stdout)
import logging
console_handler = logging.StreamHandler()
console_handler.setLevel(FINE)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Now use the library - logs will appear in console
conn = mssql_python.connect(server='localhost', database='testdb')
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
```

### Using Standard Level Names

```python
# You can use string names for standard levels
logger.setLevel('DEBUG')   # Sets to DEBUG (10)
logger.setLevel('INFO')    # Sets to INFO (20)
logger.setLevel('WARNING') # Sets to WARNING (30)

# Or use numeric values directly
logger.setLevel(5)   # FINEST
logger.setLevel(15)  # FINER
logger.setLevel(25)  # FINE
```

## File Logging

### Enable File Logging with Rotation

```python
from mssql_python import logger, FINEST

# Enable file logging (automatically rotates at 10MB, keeps 5 backups)
log_file = logger.enable_file_logging(
    log_dir='./logs',           # Directory for log files
    log_level=FINEST,           # Log level for file
    max_bytes=10*1024*1024,     # 10MB per file
    backup_count=5              # Keep 5 backup files
)

print(f"Logging to: {log_file}")

# Use the library - all operations logged to file
conn = mssql_python.connect(server='localhost', database='testdb')
```

### Custom File Handler

```python
import logging
from logging.handlers import RotatingFileHandler
from mssql_python import logger, FINER

# Create custom rotating file handler
file_handler = RotatingFileHandler(
    'my_app.log',
    maxBytes=50*1024*1024,  # 50MB
    backupCount=10           # Keep 10 backups
)
file_handler.setLevel(FINER)

# Add custom formatter with trace IDs
formatter = logging.Formatter(
    '%(asctime)s [%(trace_id)s] - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.setLevel(FINER)
```

## Log Output Examples

### FINE Level Output

Shows SQL statements and major operations:

```
2024-10-31 10:30:15,123 [TR-abc123] - mssql_python.connection - FINE - Connecting to server: localhost
2024-10-31 10:30:15,456 [TR-abc123] - mssql_python.cursor - FINE - Executing query: SELECT * FROM users WHERE id = ?
2024-10-31 10:30:15,789 [TR-abc123] - mssql_python.cursor - FINE - Query completed, 42 rows fetched
```

### FINER Level Output

Shows SQL statements with parameters:

```
2024-10-31 10:30:15,123 [TR-abc123] - mssql_python.connection - FINER - Connection parameters: {'server': 'localhost', 'database': 'testdb', 'trusted_connection': 'yes'}
2024-10-31 10:30:15,456 [TR-abc123] - mssql_python.cursor - FINER - Executing query: SELECT * FROM users WHERE id = ?
2024-10-31 10:30:15,457 [TR-abc123] - mssql_python.cursor - FINER - Query parameters: [42]
2024-10-31 10:30:15,789 [TR-abc123] - mssql_python.cursor - FINER - Fetched 1 row
```

### FINEST Level Output

Shows all internal operations:

```
2024-10-31 10:30:15,100 [TR-abc123] - mssql_python.connection - FINEST - Allocating environment handle
2024-10-31 10:30:15,101 [TR-abc123] - mssql_python.connection - FINEST - Setting ODBC version to 3.8
2024-10-31 10:30:15,123 [TR-abc123] - mssql_python.connection - FINEST - Building connection string
2024-10-31 10:30:15,456 [TR-abc123] - mssql_python.cursor - FINEST - Preparing statement handle
2024-10-31 10:30:15,457 [TR-abc123] - mssql_python.cursor - FINEST - Binding parameter 1: type=int, value=42
2024-10-31 10:30:15,789 [TR-abc123] - mssql_python.cursor - FINEST - Row buffer allocated
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

Each connection/operation gets a unique trace ID for tracking:

```python
from mssql_python import logger

# Trace IDs are automatically included in log records
# Access via: log_record.trace_id

# Example output:
# [TR-a1b2c3d4] - Connection established
# [TR-a1b2c3d4] - Query executed
# [TR-e5f6g7h8] - New connection from different context
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

### Logger Object

```python
from mssql_python import logger
```

#### Methods

**`setLevel(level: Union[int, str]) -> None`**

Set the logging threshold level.

```python
logger.setLevel(FINEST)        # Most detailed
logger.setLevel('DEBUG')       # Standard debug
logger.setLevel(20)            # INFO level
```

**`enable_file_logging(log_dir: str = './logs', log_level: int = FINE, max_bytes: int = 10485760, backup_count: int = 5) -> str`**

Enable file logging with automatic rotation.

- **log_dir**: Directory for log files (created if doesn't exist)
- **log_level**: Minimum level to log to file
- **max_bytes**: Maximum size per log file (default 10MB)
- **backup_count**: Number of backup files to keep (default 5)
- **Returns**: Path to the log file

```python
log_file = logger.enable_file_logging(
    log_dir='./my_logs',
    log_level=FINER,
    max_bytes=50*1024*1024,  # 50MB
    backup_count=10
)
```

**`addHandler(handler: logging.Handler) -> None`**

Add a custom log handler.

```python
import logging

handler = logging.StreamHandler()
handler.setLevel(FINE)
logger.addHandler(handler)
```

**`removeHandler(handler: logging.Handler) -> None`**

Remove a specific handler.

```python
logger.removeHandler(handler)
```

**`reset_handlers() -> None`**

Remove all configured handlers.

```python
logger.reset_handlers()
```

**`log(level: int, message: str, *args, **kwargs) -> None`**

Log a message at specified level.

```python
logger.log(FINE, "Processing %d records", record_count)
```

**`debug(message: str, *args, **kwargs) -> None`**

Log a debug message.

```python
logger.debug("Debug information: %s", debug_data)
```

### Log Level Constants

```python
from mssql_python import FINEST, FINER, FINE

# Use in your code
logger.setLevel(FINEST)  # Value: 5
logger.setLevel(FINER)   # Value: 15
logger.setLevel(FINE)    # Value: 25
```

### Log Levels Property

Access the level values:

```python
from mssql_python.logging import LOG_LEVELS

print(LOG_LEVELS)
# Output: {'FINEST': 5, 'FINER': 15, 'FINE': 25}
```

## Migration from Old Logging

### Old System (Deprecated)

```python
# Old way - DO NOT USE
from mssql_python.logging_config import setup_logging

setup_logging(level='DEBUG', log_file='app.log')
```

### New System

```python
# New way - RECOMMENDED
from mssql_python import logger, FINE

# Console logging
logger.setLevel(FINE)
import logging
console = logging.StreamHandler()
console.setLevel(FINE)
logger.addHandler(console)

# File logging
logger.enable_file_logging(log_dir='./logs', log_level=FINE)
```

### Key Differences

1. **Import**: Use `from mssql_python import logger` instead of `logging_config`
2. **Custom Levels**: Use `FINEST`, `FINER`, `FINE` for detailed SQL logging
3. **Handlers**: Directly add handlers via `logger.addHandler()`
4. **File Logging**: Use `enable_file_logging()` method
5. **Singleton**: Logger is a singleton, configure once and use throughout

## Common Patterns

### Development Setup

```python
from mssql_python import logger, FINEST
import logging

# Console logging with full details
logger.setLevel(FINEST)
console = logging.StreamHandler()
console.setLevel(FINEST)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)
```

### Production Setup

```python
from mssql_python import logger, FINE
import logging

# File logging with rotation, only warnings/errors to console
logger.setLevel(FINE)

# File: detailed logs
logger.enable_file_logging(
    log_dir='/var/log/myapp',
    log_level=FINE,
    max_bytes=100*1024*1024,  # 100MB
    backup_count=10
)

# Console: only warnings and above
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
formatter = logging.Formatter('%(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)
```

### Testing Setup

```python
from mssql_python import logger, FINEST
import logging

# Capture all logs for test assertions
logger.setLevel(FINEST)

# Memory handler for test assertions
class TestLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.logs = []
    
    def emit(self, record):
        self.logs.append(self.format(record))
    
    def reset(self):
        self.logs = []

test_handler = TestLogHandler()
logger.addHandler(test_handler)

# Run tests, then assert on test_handler.logs
```

### Debugging Specific Issues

```python
from mssql_python import logger, FINEST, FINER, FINE

# Debug connection issues: use FINER to see connection parameters
logger.setLevel(FINER)

# Debug SQL execution: use FINE to see SQL statements
logger.setLevel(FINE)

# Debug parameter binding: use FINER to see parameters
logger.setLevel(FINER)

# Debug internal operations: use FINEST to see everything
logger.setLevel(FINEST)
```

## Troubleshooting

### No Log Output

```python
from mssql_python import logger
import logging

# Check if logger has handlers
print(f"Handlers: {logger.handlers}")

# Check current level
print(f"Level: {logger.level}")

# Add a handler if none exist
if not logger.handlers:
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)
    logger.setLevel(logging.DEBUG)
```

### Too Much Output

```python
# Reduce logging level
logger.setLevel('WARNING')  # Only warnings and above

# Or use INFO for important events only
logger.setLevel('INFO')
```

### Check Handler Configuration

```python
from mssql_python import logger

for handler in logger.handlers:
    print(f"Handler: {handler.__class__.__name__}")
    print(f"  Level: {handler.level}")
    print(f"  Formatter: {handler.formatter}")
```

## Best Practices

1. **Set Level Early**: Configure logging before creating connections
2. **Use Appropriate Levels**: 
   - Production: `WARNING` or `INFO`
   - Development: `FINE` or `FINER`
   - Deep debugging: `FINEST`
3. **Rotate Log Files**: Always use rotation in production to prevent disk space issues
4. **Sanitization is Automatic**: Passwords are automatically redacted, but review logs before sharing
5. **Trace IDs**: Use trace IDs to correlate related log entries
6. **One Logger**: The logger is a singleton; configure once at application startup

## Examples

### Complete Application Example

```python
#!/usr/bin/env python3
"""Example application with comprehensive logging."""

import sys
import logging
from mssql_python import logger, FINE, connect

def setup_logging(verbose: bool = False):
    """Configure logging for the application."""
    level = FINE if verbose else logging.INFO
    logger.setLevel(level)
    
    # Console output
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s [%(trace_id)s] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    # File output with rotation
    log_file = logger.enable_file_logging(
        log_dir='./logs',
        log_level=FINE,  # Always detailed in files
        max_bytes=50*1024*1024,
        backup_count=10
    )
    print(f"Logging to: {log_file}")

def main():
    # Setup logging (verbose mode)
    setup_logging(verbose=True)
    
    # Connect to database
    conn = connect(
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
    main()
```

## Performance Considerations

- **Level Checking**: Logging checks are very fast when level is disabled
- **String Formatting**: Use `%` formatting in log calls for lazy evaluation:
  ```python
  # Good: String only formatted if level is enabled
  logger.debug("Processing %d items", count)
  
  # Bad: String formatted even if level is disabled
  logger.debug(f"Processing {count} items")
  ```
- **File I/O**: File logging has minimal overhead with buffering
- **Rotation**: Automatic rotation prevents performance degradation from large files

## Support

For issues or questions:
- GitHub Issues: [microsoft/mssql-python](https://github.com/microsoft/mssql-python)
- Documentation: See `Enhanced_Logging_Design.md` for technical details

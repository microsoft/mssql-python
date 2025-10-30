# Connection String Parser Changes Summary

## Date
October 27, 2025

## Overview
Updated the connection string parser to enforce strict validation instead of lenient parsing. The parser now raises errors for invalid input instead of silently ignoring malformed entries.

## Requirements Implemented

### 1. Duplicate Keywords → Errors
**Before**: Duplicates were silently handled with "first wins" behavior  
**After**: All duplicate keywords are flagged as errors

```python
# Before (lenient):
parser._parse("Server=first;Server=second")  # Returns {'server': 'first'}

# After (strict):
parser._parse("Server=first;Server=second")  # Raises ConnectionStringParseError
# Error: "Duplicate keyword 'server' found"
```

### 2. Unknown Keywords → Errors (when allowlist provided)
**Before**: Unknown keywords were filtered out with warnings  
**After**: All unknown keywords are flagged as errors in one batch

```python
# Before (lenient):
parser._parse("Server=localhost;BadParam=value")  # Returns {'server': 'localhost'}

# After (strict with allowlist):
allowlist = _ConnectionStringAllowList()
parser = ConnectionStringParser(allowlist=allowlist)
parser._parse("Server=localhost;BadParam=value")  # Raises ConnectionStringParseError
# Error: "Unknown keyword 'badparam' is not recognized"
```

### 3. Incomplete Specifications → Errors
**Before**: Incomplete specs (keyword without value) were silently ignored  
**After**: All incomplete specifications are flagged as errors

```python
# Before (lenient):
parser._parse("Server;Database=mydb")  # Returns {'database': 'mydb'}

# After (strict):
parser._parse("Server;Database=mydb")  # Raises ConnectionStringParseError
# Error: "Incomplete specification: keyword 'server' has no value (missing '=')"
```

### 4. Normalization Preserved
**Unchanged**: Key normalization to lowercase still works

```python
# Both before and after:
parser._parse("SERVER=localhost;DaTaBaSe=mydb")
# Returns {'server': 'localhost', 'database': 'mydb'}
```

## Error Batching

The parser collects **all errors** before raising the exception, so users can fix multiple issues at once:

```python
parser._parse("Server=first;BadEntry;Server=second;Database")
# Raises ConnectionStringParseError with multiple errors:
# - "Incomplete specification: keyword 'badentry' has no value"
# - "Duplicate keyword 'server' found"
# - "Incomplete specification: keyword 'database' has no value"
```

## Code Changes

### Files Modified

1. **mssql_python/connection_string_parser.py**
   - Added `ConnectionStringParseError` exception class
   - Modified `_parse()` to collect and raise errors instead of logging warnings
   - Added `allowlist` parameter to `__init__()` for keyword validation
   - Removed lenient error handling (no more silent ignoring)
   - Simplified code by removing logging infrastructure

2. **mssql_python/connection.py**
   - Updated `_construct_connection_string()` to pass allowlist to parser
   - Parser now validates keywords during parsing instead of after

3. **tests/test_010_connection_string_parser.py**
   - Added `TestConnectionStringParserErrors` class with 12 new error tests
   - Added `TestConnectionStringParserEdgeCases` class with 4 edge case tests
   - Updated existing tests to use error-based behavior
   - Total: 36 tests (21 basic + 12 error + 4 edge cases)

4. **tests/test_012_connection_string_integration.py**
   - Updated tests to expect errors instead of lenient parsing
   - Added tests for error batching behavior
   - Added tests for parser with/without allowlist
   - Total: 15 integration tests

## Test Coverage

All tests pass (74 total):
- **test_010_connection_string_parser.py**: 36 tests 
- **test_011_connection_string_allowlist.py**: 23 tests 
- **test_012_connection_string_integration.py**: 15 tests 

## API Changes

### New Exception

```python
class ConnectionStringParseError(Exception):
    """Exception raised when connection string parsing fails."""
    
    def __init__(self, errors: List[str]):
        self.errors = errors  # List of all validation errors
        # Creates message with all errors listed
```

### Updated Constructor

```python
# Before:
parser = ConnectionStringParser()

# After (optional allowlist):
allowlist = _ConnectionStringAllowList()
parser = ConnectionStringParser(allowlist=allowlist)  # Validates keywords
# OR
parser = ConnectionStringParser()  # No keyword validation
```

## Backward Compatibility

**Breaking Change**: Code that relies on lenient parsing will now raise exceptions.

### Migration Guide

```python
# Old code (lenient):
try:
    parser = ConnectionStringParser()
    params = parser._parse(connection_string)
    # params might have ignored malformed entries
except:
    # Never raised exceptions for malformed input
    pass

# New code (strict):
try:
    allowlist = _ConnectionStringAllowList()
    parser = ConnectionStringParser(allowlist=allowlist)
    params = parser._parse(connection_string)
    # params contains only valid, non-duplicate keywords
except ConnectionStringParseError as e:
    # Handle validation errors
    print("Connection string errors:")
    for error in e.errors:
        print(f"  - {error}")
```

## Benefits

1. **Better User Experience**: Users see all errors at once, can fix them together
2. **Fail Fast**: Invalid configurations caught immediately instead of runtime failures
3. **Clearer Errors**: Specific error messages for each type of problem
4. **Type Safety**: Parser raises typed exceptions instead of returning partial data

## Examples

### Valid Connection String
```python
parser = ConnectionStringParser()
result = parser._parse("Server=localhost;Database=mydb;UID=user;PWD=pass")
#  Returns {'server': 'localhost', 'database': 'mydb', 'uid': 'user', 'pwd': 'pass'}
```

### Duplicate Keywords
```python
parser = ConnectionStringParser()
parser._parse("Server=first;Server=second")
# ✗ ConnectionStringParseError: Duplicate keyword 'server' found
```

### Unknown Keywords (with allowlist)
```python
allowlist = _ConnectionStringAllowList()
parser = ConnectionStringParser(allowlist=allowlist)
parser._parse("Server=localhost;InvalidParam=value")
# ✗ ConnectionStringParseError: Unknown keyword 'invalidparam' is not recognized
```

### Incomplete Specification
```python
parser = ConnectionStringParser()
parser._parse("Server;Database=mydb")
# ✗ ConnectionStringParseError: Incomplete specification: keyword 'server' has no value
```

### Multiple Errors
```python
parser = ConnectionStringParser()
parser._parse("Server=a;NoValue;Server=b")
# ✗ ConnectionStringParseError with 2 errors:
#   - "Incomplete specification: keyword 'novalue' has no value"
#   - "Duplicate keyword 'server' found"
```

# Connection String Allow-List Design for mssql-python

**Date:** October 23, 2025  
**Author:** Engineering Team  
**Status:** Design Proposal  

---

## Executive Summary

This document outlines the design for implementing a **connection string parameter allow-list** in the mssql-python driver. Currently, the driver has a limited allow-list in `_construct_connection_string()` for **kwargs only**, but passes the base connection string **as-is** to the ODBC driver. The new design implements a comprehensive parser that validates **all** connection string parameters against an allow-list before passing them to ODBC Driver 18 for SQL Server.

This allow-list approach is necessary for three key reasons:

1. **ODBC Feature Compatibility**: Some ODBC connection string parameters require additional configurations (e.g., Always Encrypted extensibility modules) for which Python doesn't have a first-class experience yet. Allowing these parameters without proper support would create confusion and support burden.

2. **Future TDS Runtime Migration**: The driver is planned to migrate from ODBC to a Rust-based TDS runtime. While ODBC parity is a goal, not all ODBC features will be available during or after the migration. By deliberately allow-listing parameters now, we can ensure a smoother migration path and avoid the breaking change of removing previously exposed parameters later. It's easier to add parameters over time than to remove them once users depend on them.

3. **Simplified Connection Experience**: ODBC connection strings have accumulated many parameter synonyms over decades of backward compatibility (e.g., "server", "address", "addr", "network address" all mean the same thing). A modern Python driver should provide a cleaner, simplified API by exposing only a curated set of parameters with clear, consistent naming.

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Current Implementation Analysis](#current-implementation-analysis)
3. [Design Goals](#design-goals)
4. [Architecture Overview](#architecture-overview)
5. [Connection String Parser Design](#connection-string-parser-design)
6. [Allow-List Strategy](#allow-list-strategy)
7. [Performance Considerations](#performance-considerations)
8. [Data Flow Diagrams](#data-flow-diagrams)
9. [Implementation Details](#implementation-details)
10. [Testing Strategy](#testing-strategy)
11. [Design Considerations](#design-considerations)
12. [Future Enhancements](#future-enhancements)

---

## Problem Statement

### Current Issues

1. **Inconsistent Filtering**: The driver currently:
   - Filters **kwargs** through an allow-list (only 6 parameters: Server, Uid, Pwd, Database, Encrypt, TrustServerCertificate)
   - Passes the base `connection_str` parameter **directly** to ODBC without validation
   
2. **ODBC Feature Compatibility**: Some ODBC connection string parameters require additional infrastructure:
   - Always Encrypted with extensibility modules requires custom key store providers
   - Column Encryption Key caching requires additional Python bindings
   - These features don't have first-class Python API support yet
   - Allowing these parameters creates user confusion and support issues

3. **Future Migration Concerns**: The driver is planned to migrate to a Rust-based TDS runtime:
   - While ODBC parity is a goal, not all ODBC features will be available during or after migration
   - Some ODBC-specific parameters won't translate to the new runtime
   - Being deliberate about which parameters to expose avoids future breaking changes
   - It's easier to add parameters over time than to remove them once users depend on them
   - Gating parameters now prevents users from building dependencies on features that won't be available

4. **Parameter Synonym Bloat**: ODBC connection strings have accumulated many synonyms:
   - "Server", "Address", "Addr", "Network Address" all mean the same thing
   - "Uid", "User", "User ID" all mean the same thing
   - This creates confusion and inconsistent usage patterns
   - A modern Python driver should have a clean, minimal API surface

5. **No Parsing Logic**: The current implementation uses simple string splitting on `;` which doesn't handle:
   - Escaped characters (e.g., `{}` in values)
   - Quoted values
   - Empty values
   - Malformed connection strings
   
   **Citations**:
   - `mssql_python/helpers.py`, lines 28-30: `connection_attributes = connection_str.split(";")` - splits on semicolon without handling braced values. Passwords could have the special characters which are considered delimiters in connection strings.
   - `mssql_python/helpers.py`, line 33: `if attribute.lower().split("=")[0] == "driver":` - splits on `=` without handling escaped or braced values
   - `mssql_python/helpers.py`, lines 66-67: `for param in parameters:` / `if param.lower().startswith("app="):` - simple string operations, no ODBC-compliant parsing
   - `mssql_python/helpers.py`, line 69: `key, _ = param.split("=", 1)` - splits on first `=` only, doesn't handle braces or escaping

### Design Motivations

1. **Controlled Feature Set**: By implementing an allow-list, we can:
   - Only expose ODBC features that have proper Python API support
   - Prevent users from attempting to use unsupported features
   - Reduce the support burden by rejecting parameters we can't properly handle

2. **Migration Path**: The allow-list provides:
   - A stable API surface that will work across both ODBC and future Rust-based implementations
   - Clear documentation of what parameters are supported
   - A deliberate, controlled approach to exposing parameters (easier to add than remove)
   - Protection against breaking changes when migrating runtimes
   - Ability to achieve ODBC parity incrementally while maintaining backward compatibility

3. **Simplified API**: By normalizing synonyms and exposing only canonical parameter names:
   - Users have a consistent, predictable API
   - Documentation is clearer
   - Code examples are more uniform
   - New Python developers aren't confused by legacy ODBC conventions

### Requirements

1. Parse the complete connection string (base + kwargs)
2. Validate all parameters against an allow-list
3. Reconstruct a clean connection string with only allowed parameters
4. Maintain backward compatibility with existing code
5. Ensure high performance (sub-millisecond overhead)
6. Handle ODBC connection string syntax correctly
7. Normalize parameter synonyms to canonical names
8. Prepare for future Rust-based TDS runtime migration

---

## Current Implementation Analysis

### Code Flow (Before This Design)

```
User Input:
  ├─ connection_str: "Server=localhost;Database=mydb;SomeParam=value"
  └─ kwargs: {encrypt: "yes", server: "override"}

Current Flow:
  1. add_driver_name_to_app_parameter(connection_str)
     ├─ Finds any "APP=" parameter (case-insensitive)
     ├─ Overwrites value to "MSSQL-Python" (preserves key casing)
     └─ Adds "APP=MSSQL-Python" if not present
     
  2. add_driver_to_connection_str(connection_str)
     ├─ Strips any existing "Driver=" params (always removed)
     ├─ Adds "Driver={ODBC Driver 18 for SQL Server}" at position 0
     └─ Returns: "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=mydb;SomeParam=value;APP=MSSQL-Python"
  
  3. _construct_connection_string(connection_str, **kwargs)
     ├─ Takes output from step 2
     ├─ Appends only ALLOW-LISTED kwargs:
     │   ├─ server → "Server"
     │   ├─ user/uid → "Uid"
     │   ├─ password/pwd → "Pwd"
     │   ├─ database → "Database"
     │   ├─ encrypt → "Encrypt"
     │   └─ trust_server_certificate → "TrustServerCertificate"
     ├─ **else: continue** (filters out other kwargs)
     └─ Returns: "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=mydb;SomeParam=value;APP=MSSQL-Python;Encrypt=yes"
  
  4. ddbc_bindings.Connection(connection_str, ...)
     └─ Passes final string to ODBC (including "SomeParam=value" - UNFILTERED!)
```

### Key Observations

**1. Deliberate Driver and APP Control** (by design):

The driver **intentionally** controls these two parameters to ensure consistent behavior:

- **Driver Parameter** (`helpers.py:38-49`): Any user-provided `Driver=` value is **stripped and replaced** with `{ODBC Driver 18 for SQL Server}`. This ensures the Python driver always uses the correct ODBC driver version.
  
- **APP Parameter** (`helpers.py:99-109`): Any user-provided `APP=` value is **overwritten** to `MSSQL-Python`. This ensures proper application identification in SQL Server logs and monitoring tools, making it easy to identify connections from this Python driver.

These are **intentional design choices** that will be preserved in the new allow-list implementation.

**2. The base `connection_str` parameter bypasses all other filtering** (the problem):

Only kwargs go through the allow-list check for non-Driver/APP parameters. This means:

1. Users can pass unsupported ODBC parameters that the Python driver can't properly handle
2. Parameters that require additional infrastructure (like Always Encrypted extensibility) get passed to ODBC without validation
3. ODBC-specific parameters that won't work in the future Rust-based runtime can create forward compatibility issues
4. Multiple synonyms for the same parameter create API inconsistency

**3. The parsing is inadequate for ODBC connection strings**:

The current parsing in `add_driver_to_connection_str()` (helpers.py:28-30) uses simple `split(";")`:

```python
# Current implementation (helpers.py)
connection_attributes = connection_str.split(";")
for attribute in connection_attributes:
    if attribute.lower().split("=")[0] == "driver":
        continue
```

**This breaks with valid ODBC connection strings like**:
- `Server={local;host};Database=mydb` → incorrectly splits into 3 parts instead of 2
- `PWD={p}}w{{d}` → doesn't unescape `}}` to `}` and `{{` to `{`
- `Server=localhost;` → creates empty string element
- `Server=localhost` (no semicolon) → handled, but inconsistent with trailing semicolon case

---

## Design Goals

### Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1 | Parse complete ODBC connection strings correctly | **P0** |
| FR-2 | Filter all parameters against an allow-list | **P0** |
| FR-3 | Support ODBC connection string syntax (`;`, `{}`, `=`) | **P0** |
| FR-4 | Merge kwargs with connection string parameters | **P0** |
| FR-5 | Preserve parameter values exactly (including special chars) | **P0** |
| FR-6 | Maintain backward compatibility | **P1** |
| FR-7 | Provide clear error messages for invalid params | **P1** |

### Non-Functional Requirements

| ID | Requirement | Target | Priority |
|----|-------------|--------|----------|
| NFR-1 | Parsing overhead | < 1 millisecond | **P0** |
| NFR-2 | Memory efficiency | < 5KB per connection | **P1** |
| NFR-3 | Code maintainability | Clear, documented, testable | **P1** |
| NFR-4 | Thread safety | Thread-safe parsing | **P1** |

---

## Architecture Overview

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────┐
│                     Connection.__init__()                       │
│                                                                 │
│  Input: connection_str (str), **kwargs (dict)                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              ConnectionStringParser                             │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  1. parse(connection_str) → Dict[str, str]                │  │
│  │     - Tokenize connection string                          │  │
│  │     - Handle escaping/quoting                             │  │
│  │     - Return key-value pairs                              │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────────────┘
                     │ Parsed params dict
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              ParameterAllowList                                 │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  2. filter(params_dict) → Dict[str, str]                  │  │
│  │     - Check each param against allow-list                 │  │
│  │     - Normalize parameter names                           │  │
│  │     - Log warnings for rejected params                    │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────────────┘
                     │ Filtered params dict
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│           ConnectionStringBuilder                               │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  3. merge_kwargs(filtered_params, kwargs)                 │  │
│  │     - Merge kwargs into filtered params                   │  │
│  │     - kwargs override connection_str values               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  4. add_driver(merged_params)                             │  │
│  │     - Add Driver={ODBC Driver 18 for SQL Server}          │  │
│  │     - Add APP=MSSQL-Python                                │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  5. build() → str                                         │  │
│  │     - Reconstruct connection string                       │  │
│  │     - Proper escaping for values with special chars       │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────────────┘
                     │ Final connection string
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              ddbc_bindings.Connection()                         │
│                                                                 │
│              Passes to ODBC Driver                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Connection String Parser Design

### ODBC Connection String Syntax

Based on the official [ODBC Connection String specification (MS-ODBCSTR)](https://learn.microsoft.com/en-us/openspecs/sql_server_protocols/ms-odbcstr/55953f0e-2d30-4ad4-8e56-b4207e491409), specifically section 2.1.2 "ODBC Connection String Format":

**Official ABNF Grammar:**

```abnf
ODBCConnectionString = *(KeyValuePair SC) KeyValuePair [SC]
KeyValuePair         = (Key EQ Value / SpaceStr)
Key                  = SpaceStr KeyName
KeyName              = (nonSP-SC-EQ *nonEQ)
Value                = (SpaceStr ValueFormat1 SpaceStr) / (ValueContent2)
ValueFormat1         = LCB ValueContent1 RCB
ValueContent1        = *(nonRCB / ESCAPEDRCB)
ValueContent2        = SpaceStr / SpaceStr (nonSP-LCB-SC) *nonSC

; Character definitions
nonRCB        = %x01-7C / %x7E-FFFF       ; not "}"
nonSP-LCB-SC  = %x01-1F / %x21-3A / %x3C-7A / %x7C-FFFF  ; not space, "{" or ";"
nonSP-SC-EQ   = %x01-1F / %x21-3A / %x3C / %x3E-FFFF     ; not space, ";" or "="
nonEQ         = %x01-3C / %x3E-FFFF       ; not "="
nonSC         = %x01-3A / %x3C-FFFF       ; not ";"

; Where:
SC            = ";"    ; semicolon
EQ            = "="    ; equals
LCB           = "{"    ; left curly brace
RCB           = "}"    ; right curly brace
ESCAPEDRCB    = "}}"   ; escaped right curly brace
SpaceStr      = *SP    ; zero or more spaces
```

**Simplified Explanation:**

- **Connection string** = Zero or more key-value pairs separated by semicolons, with optional trailing semicolon
- **Key-value pair** = `Key=Value` or just whitespace
- **Key** = Optional leading spaces + key name (non-space, non-semicolon, non-equals characters)
- **Value** can be in two formats:
  - **Format 1 (Braced)**: `{content}` where `}` inside is escaped as `}}`
  - **Format 2 (Unbraced)**: Characters that don't contain unescaped semicolons or braces
- **Escaping**: Only `}` needs escaping inside braced values, done by doubling: `}}` → `}`

**Key Points:**
- Parameters separated by `;`
- Format: `KEY=VALUE`
- Values containing `;` or `{` must be wrapped in braces: `{value}`
- Right braces `}` in braced values are escaped by doubling: `}}` → `}`
- Left braces `{` in braced values are escaped by doubling: `{{` → `{`
- Trailing semicolons are allowed
- Whitespace around keys and values follows specific rules (see SpaceStr in grammar)

### Parser Implementation

```python
class ConnectionStringParser:
    """
    Parses ODBC connection strings into key-value dictionaries.
    Handles ODBC-specific syntax including braces, escaping, and semicolons.
    """
    
    def __init__(self):
        """Initialize the parser."""
        pass
    
    def parse(self, connection_str: str) -> Dict[str, str]:
        """
        Parse a connection string into a dictionary of parameters.
        
        Args:
            connection_str: ODBC-format connection string
            
        Returns:
            Dictionary mapping parameter names (lowercase) to values
            
        Raises:
            ValueError: If connection string is malformed
            
        Examples:
            >>> parser.parse("Server=localhost;Database=mydb")
            {'server': 'localhost', 'database': 'mydb'}
            
            >>> parser.parse("Server={;local;};PWD={p}}w{{d}")
            {'server': ';local;', 'pwd': 'p}w{d'}
        """
        if not connection_str:
            return {}
            
        connection_str = connection_str.strip()
        if not connection_str:
            return {}
        
        params = {}
        current_pos = 0
        str_len = len(connection_str)
        
        while current_pos < str_len:
            # Skip whitespace and semicolons
            while current_pos < str_len and connection_str[current_pos] in ' \t;':
                current_pos += 1
            
            if current_pos >= str_len:
                break
            
            # Parse key
            key_start = current_pos
            while current_pos < str_len and connection_str[current_pos] not in '=;':
                current_pos += 1
            
            if current_pos >= str_len or connection_str[current_pos] != '=':
                # No '=' found - malformed attribute
                raise ValueError(
                    f"Malformed connection string: expected '=' after key at position {current_pos}"
                )
            
            key = connection_str[key_start:current_pos].strip().lower()
            current_pos += 1  # Skip '='
            
            # Parse value
            value, current_pos = self._parse_value(connection_str, current_pos)
            
            # Store parameter (later ones override earlier ones)
            params[key] = value
        
        return params
    
    def _parse_value(self, connection_str: str, start_pos: int) -> Tuple[str, int]:
        """
        Parse a parameter value from the connection string.
        
        Handles both simple values and braced values with escaping.
        
        Args:
            connection_str: The connection string
            start_pos: Starting position of the value
            
        Returns:
            Tuple of (parsed_value, new_position)
        """
        str_len = len(connection_str)
        
        # Skip leading whitespace
        while start_pos < str_len and connection_str[start_pos] in ' \t':
            start_pos += 1
        
        if start_pos >= str_len:
            return '', start_pos
        
        # Check if value is braced
        if connection_str[start_pos] == '{':
            return self._parse_braced_value(connection_str, start_pos)
        else:
            return self._parse_simple_value(connection_str, start_pos)
    
    def _parse_simple_value(self, connection_str: str, start_pos: int) -> Tuple[str, int]:
        """Parse a simple (non-braced) value up to the next semicolon."""
        str_len = len(connection_str)
        value_start = start_pos
        
        # Read until semicolon or end of string
        while start_pos < str_len and connection_str[start_pos] != ';':
            start_pos += 1
        
        value = connection_str[value_start:start_pos].rstrip()
        return value, start_pos
    
    def _parse_braced_value(self, connection_str: str, start_pos: int) -> Tuple[str, int]:
        """
        Parse a braced value with proper handling of escaped braces.
        
        Braced values:
        - Start with '{' and end with '}'
        - '}' inside the value is escaped as '}}'
        - Example: {p}}w{{d} → p}w{d
        """
        str_len = len(connection_str)
        start_pos += 1  # Skip opening '{'
        value = []
        
        while start_pos < str_len:
            ch = connection_str[start_pos]
            
            if ch == '}':
                # Check if it's an escaped brace
                if start_pos + 1 < str_len and connection_str[start_pos + 1] == '}':
                    # Escaped brace: '}}' → '}'
                    value.append('}')
                    start_pos += 2
                else:
                    # End of braced value
                    start_pos += 1  # Skip closing '}'
                    return ''.join(value), start_pos
            else:
                value.append(ch)
                start_pos += 1
        
        # Unclosed braced value
        raise ValueError("Unclosed braced value in connection string")
```

---

## Allow-List Strategy

### Allowed Parameters

The allow-list is designed to include only parameters that:

1. **Have Python API Support**: Parameters that the driver can properly handle and configure
2. **Are Runtime-Agnostic**: Parameters that will work in both ODBC and future Rust-based TDS runtime (or can be mapped appropriately)
3. **Are Essential for Connectivity**: Core parameters needed for database connections
4. **Have Clear Semantics**: Parameters with well-defined behavior and no ambiguity

**Philosophy**: We take a deliberate, allow-list approach to exposing parameters because:
- It's easier to add parameters over time than to remove them once users depend on them
- This enables us to achieve ODBC parity incrementally while maintaining backward compatibility
- We can carefully evaluate each parameter's necessity and ensure proper Python API support before exposing it
- Users won't build dependencies on features that may not be available after runtime migration

**Special Parameters** (handled outside the allow-list):
- **Driver**: Always hardcoded to `{ODBC Driver 18 for SQL Server}`. User-provided values are stripped and replaced to ensure driver consistency.
- **APP**: Always set to `MSSQL-Python`. User-provided values are overwritten to ensure proper application identification in SQL Server logs and monitoring.

These special parameters maintain the existing behavior and ensure consistent driver operation.

**Excluded Parameters** include:
- Always Encrypted extensibility parameters (no Python key store provider API yet)
- Advanced ODBC-specific features without Python bindings or TDS runtime equivalents
- Deprecated or legacy parameters
- Parameters with unclear behavior or side effects
- ODBC driver configuration parameters that don't translate to TDS runtime

```python
# File: mssql_python/connection_string_allowlist.py

class ConnectionStringAllowList:
    """
    Manages the allow-list of permitted connection string parameters.
    """
    
    # Core connection parameters
    ALLOWED_PARAMS = {
        # Server identification
        'server': 'Server',
        'address': 'Server',
        'addr': 'Server',
        'network address': 'Server',
        
        # Authentication
        'uid': 'Uid',
        'user id': 'Uid',
        'user': 'Uid',
        'pwd': 'Pwd',
        'password': 'Pwd',
        'authentication': 'Authentication',
        'trusted_connection': 'Trusted_Connection',
        
        # Database
        'database': 'Database',
        'initial catalog': 'Database',
        
        # Driver (read-only - always set by mssql-python)
        'driver': 'Driver',
        
        # Encryption
        'encrypt': 'Encrypt',
        'trustservercertificate': 'TrustServerCertificate',
        'hostnameincertificate': 'HostNameInCertificate',
        
        # Connection behavior
        'connection timeout': 'Connection Timeout',
        'connect timeout': 'Connection Timeout',
        'timeout': 'Connection Timeout',
        'login timeout': 'Login Timeout',
        'multisubnetfailover': 'MultiSubnetFailover',
        'applicationintent': 'ApplicationIntent',
        'application intent': 'ApplicationIntent',
        'transparentnetworkipresolution': 'TransparentNetworkIPResolution',
        
        # Application identification
        'app': 'APP',
        'application name': 'APP',
        'workstation id': 'Workstation ID',
        'wsid': 'Workstation ID',
        
        # MARS (Multiple Active Result Sets)
        'mars_connection': 'MARS_Connection',
        'multipleactiveresultsets': 'MARS_Connection',
        
        # Language/Regional
        'language': 'Language',
        
        # Connection Pooling (driver level)
        'pooling': 'Pooling',
        
        # Column Encryption
        'columnencryption': 'ColumnEncryption',
        
        # Attach database file
        'attachdbfilename': 'AttachDbFilename',
        
        # Failover
        'failover partner': 'Failover_Partner',
        
        # Application name / intent
        'application role': 'ApplicationRole',
        
        # Packet size
        'packet size': 'Packet Size',
    }
    
    # Parameters that should be handled separately (not in allow-list)
    BLOCKED_PARAMS = {
        'pwd',  # Captured separately for logging sanitization
        'password',
    }
    
    @classmethod
    def normalize_key(cls, key: str) -> Optional[str]:
        """
        Normalize a parameter key to its canonical form.
        
        Args:
            key: Parameter key from connection string (case-insensitive)
            
        Returns:
            Canonical parameter name if allowed, None otherwise
            
        Examples:
            >>> ConnectionStringAllowList.normalize_key('SERVER')
            'Server'
            >>> ConnectionStringAllowList.normalize_key('UnsupportedParam')
            None
        """
        key_lower = key.lower().strip()
        return cls.ALLOWED_PARAMS.get(key_lower)
    
    @classmethod
    def filter_params(cls, params: Dict[str, str], warn_rejected: bool = True) -> Dict[str, str]:
        """
        Filter parameters against the allow-list.
        
        Args:
            params: Dictionary of connection string parameters
            warn_rejected: Whether to log warnings for rejected parameters
            
        Returns:
            Dictionary containing only allowed parameters with normalized keys
        """
        from mssql_python.logging_config import get_logger
        from mssql_python.helpers import sanitize_user_input
        
        logger = get_logger()
        filtered = {}
        rejected = []
        
        for key, value in params.items():
            normalized_key = cls.normalize_key(key)
            
            if normalized_key:
                # Parameter is allowed
                filtered[normalized_key] = value
            else:
                # Parameter is not in allow-list
                rejected.append(key)
                if warn_rejected and logger:
                    safe_key = sanitize_user_input(key)
                    logger.warning(
                        f"Connection string parameter '{safe_key}' is not in the allow-list and will be ignored"
                    )
        
        return filtered
```

### Allow-List Rationale

| Parameter Category | Purpose | Include in Allow-List? | Rationale |
|--------------------|---------|------------------------|-----------|
| Server/Authentication | Core connection functionality | **Yes** | Essential, runtime-agnostic |
| Encryption (TLS/SSL) | TLS/SSL configuration | **Yes** | Essential for security, supported in all runtimes |
| Connection Behavior | Timeouts, failover, MARS | **Yes** | Core functionality, can be mapped to TDS runtime |
| Application Identification | Logging, monitoring | **Yes** | Informational, no side effects |
| Always Encrypted Extensions | Custom key store providers | **No** | Requires Python key store provider API (not yet available) |
| ODBC Driver Internals | Driver-specific configuration | **No** | ODBC-specific, won't work in Rust-based runtime |
| Deprecated Parameters | Legacy ODBC parameters | **No** | Should not expose in modern Python API |
| Synonym Parameters | Alternative names for same parameter | **Normalize** | Accept but normalize to canonical name |

**Normalization Strategy**:
- Accept common synonyms (e.g., "user", "uid", "user id") for ease of use
- Always normalize to a single canonical name (e.g., "Uid")
- This provides flexibility while maintaining consistency
- Prepares for potential Python-style naming in future (e.g., "user_id")

---

## Performance Considerations

### Optimization Strategies

1. **Lazy Initialization**
   - Parse connection string only once during connection initialization
   - Cache the parsed dictionary

2. **Early Termination**
   - Simple parameter counting before full parse
   - Skip parsing if connection string is empty

3. **Minimal Allocations**
   - Reuse string builders
   - Single-pass parsing

4. **Compiled Regex (if needed)**
   - Pre-compile any regex patterns
   - Use simple string operations where possible

### Performance Targets

```python
# Benchmark targets (on modern hardware)
Performance Metric                    Target        Worst Case
────────────────────────────────────────────────────────────────
Parse simple connection string        < 0.1ms       < 0.5ms
Parse complex connection string       < 0.5ms       < 2ms
Filter against allow-list             < 0.1ms       < 0.5ms
Rebuild connection string             < 0.1ms       < 0.5ms
────────────────────────────────────────────────────────────────
Total overhead per connection         < 1ms         < 5ms
```

### Memory Usage

```python
# Estimated memory per connection
Component                            Size
──────────────────────────────────────────────
Parsed params dict                   ~1-2 KB
Filtered params dict                 ~1-2 KB
Rebuilt connection string            ~0.5-1 KB
──────────────────────────────────────────────
Total                                ~3-5 KB
```

---

## Data Flow Diagrams

### Diagram 1: Current Flow (Before This Design)

```
┌─────────────────────────────────────────────────────┐
│ User provides:                                      │
│   connection_str = "Server=myserver;Secret=value"   │
│   kwargs = {"encrypt": "yes"}                       │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│ add_driver_to_connection_str()                      │
│ ├─ Adds Driver={ODBC Driver 18 for SQL Server}      │
│ └─ NO PARAMETER FILTERING                           │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ Output: "Driver={ODBC Driver 18 for SQL Server};
                   │          Server=myserver;Secret=value"
                   ▼
┌─────────────────────────────────────────────────────┐
│ _construct_connection_string()                      │
│ ├─ Takes connection_str from above (UNFILTERED!)    │
│ └─ Appends FILTERED kwargs:                         │
│    ├─ "encrypt" → "Encrypt=yes"  ✓                  │
│    └─ Other kwargs rejected       ✗                 │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ Final: "Driver={ODBC Driver 18...};
                   │         Server=myserver;Secret=value;
                   │         Encrypt=yes"
                   │
                   │ ⚠️  "Secret=value" was NEVER FILTERED! ⚠️
                   │ ⚠️  Could be unsupported ODBC parameter! ⚠️
                   ▼
┌─────────────────────────────────────────────────────┐
│ ddbc_bindings.Connection(final_connection_str)      │
│ └─ Passes to ODBC driver (including "Secret")       │
└─────────────────────────────────────────────────────┘
```

### Diagram 2: Proposed Flow (With Allow-List)

```
┌──────────────────────────────────────────────────────────────┐
│ User provides:                                               │
│   connection_str = "Server=myserver;Secret=value;Encrypt=no" │
│   kwargs = {"encrypt": "yes", "database": "mydb"}            │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│ ConnectionStringParser.parse(connection_str)                 │
│                                                              │
│ Parse result:                                                │
│   {                                                          │
│     'server': 'myserver',      ← Allowed                     │
│     'secret': 'value',          ← NOT in allow-list ⚠️       |
│     'encrypt': 'no'             ← Allowed (will be           │
│   }                                overridden by kwargs)     │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│ ConnectionStringAllowList.filter_params(parsed_params)       │
│                                                              │
│ ├─ Check 'server' → ✓ Allowed  → Normalize to 'Server'     │
│ ├─ Check 'secret' → ✗ REJECTED → Log warning, drop param   │
│ │   (Not in allow-list - may be unsupported ODBC parameter) │
│ └─ Check 'encrypt' → ✓ Allowed  → Normalize to 'Encrypt'   │
│                                                              │
│ Filtered result:                                             │
│   {                                                          │
│     'Server': 'myserver',                                   │
│     'Encrypt': 'no'                                         │
│   }                                                          │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│ ConnectionStringBuilder.merge_kwargs(filtered, kwargs)       │
│                                                              │
│ ├─ Process kwargs through allow-list:                       │
│ │   ├─ 'encrypt' → ✓ Normalize to 'Encrypt'                │
│ │   └─ 'database' → ✓ Normalize to 'Database'              │
│ │                                                            │
│ ├─ Merge (kwargs override connection_str):                  │
│ │   ├─ 'Server': 'myserver'        (from connection_str)   │
│ │   ├─ 'Encrypt': 'yes'   ← OVERRIDES 'no' from conn_str  │
│ │   └─ 'Database': 'mydb'           (from kwargs)          │
│ │                                                            │
│ └─ Add driver-specific params:                              │
│     ├─ 'Driver': '{ODBC Driver 18 for SQL Server}'         │
│     └─ 'APP': 'MSSQL-Python'                               │
│                                                              │
│ Merged result:                                               │
│   {                                                          │
│     'Driver': '{ODBC Driver 18 for SQL Server}',           │
│     'Server': 'myserver',                                   │
│     'Database': 'mydb',                                     │
│     'Encrypt': 'yes',                                       │
│     'APP': 'MSSQL-Python'                                   │
│   }                                                          │
│                                                              │
│ Note: All parameters are validated and normalized           │
│       Only supported features are passed to ODBC             │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│ ConnectionStringBuilder.build()                              │
│                                                              │
│ ├─ Reconstruct connection string:                           │
│ │   ├─ Check each value for special chars (;, {, })        │
│ │   ├─ Add braces if needed: {value}                        │
│ │   ├─ Escape braces in value: } → }}                       │
│ │   └─ Join with semicolons                                 │
│ │                                                            │
│ └─ Final string:                                             │
│     "Driver={ODBC Driver 18 for SQL Server};"               │
│     "Server=myserver;"                                       │
│     "Database=mydb;"                                         │
│     "Encrypt=yes;"                                           │
│     "APP=MSSQL-Python"                                       │
│                                                              │
│ Benefits of this approach:                                   │
│   - "Secret=value" was filtered out (unsupported param)     │
│   - Only parameters with Python API support are passed      │
│   - Forward compatible with future Rust-based runtime       │
│   - Synonyms normalized to canonical names                  │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│ ddbc_bindings.Connection(final_connection_str)               │
│                                                              │
│ - Passes ONLY ALLOWED parameters to ODBC driver             │
│ - All parameters have proper Python API support             │
│ - Forward compatible with future Rust-based TDS runtime     │
└──────────────────────────────────────────────────────────────┘
```

### Diagram 3: Parser State Machine

```
                    ┌─────────┐
                    │ START   │
                    └────┬────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Skip whitespace/';' │◄─────────────┐
              └──────────┬───────────┘              │
                         │                          │
                         ▼                          │
                   ┌──────────┐                     │
                   │ Parse    │                     │
                   │ KEY      │                     │
                   └────┬─────┘                     │
                        │                           │
                        │ Found '='                 │
                        ▼                           │
                   ┌──────────┐                     │
          ┌────────┤ Check    │                     │
          │        │ next     │                     │
          │        │ char     │                     │
          │        └────┬─────┘                     │
          │             │                           │
    '{' ? │             │ Other                     │
          │             ▼                           │
          │     ┌──────────────┐                    │
          │     │ Parse SIMPLE │                    │
          │     │ VALUE        │                    │
          │     │ (until ';')  │                    │
          │     └──────┬───────┘                    │
          │            │                            │
          │            └─────────┬──────────────────┘
          │                      │
          │                      │ Store key=value
          │                      │ More params?
          │                      │
          ▼                      ▼
    ┌──────────────┐        ┌────────┐
    │ Parse BRACED │        │  END   │
    │ VALUE        │        └────────┘
    │ (handle '}}')|
    └──────┬───────┘
           │
           └────────────────────┘
```

---

## Implementation Details

### File Structure

```
mssql_python/
├── connection.py                      # Modified
├── helpers.py                          # Modified
├── connection_string_parser.py         # NEW
├── connection_string_allowlist.py      # NEW
└── connection_string_builder.py        # NEW
```

### Modified: connection.py

```python
def _construct_connection_string(self, connection_str: str = "", **kwargs) -> str:
    """
    Construct the connection string by parsing, filtering, and merging parameters.
    
    This method:
    1. Parses the base connection_str into parameters
    2. Filters parameters against an allow-list
    3. Merges kwargs (which also go through allow-list)
    4. Adds driver and APP parameters
    5. Rebuilds the connection string
    
    Args:
        connection_str: Base connection string from user
        **kwargs: Additional key/value pairs for the connection string
        
    Returns:
        str: The constructed and filtered connection string
    """
    from mssql_python.connection_string_parser import ConnectionStringParser
    from mssql_python.connection_string_allowlist import ConnectionStringAllowList
    from mssql_python.connection_string_builder import ConnectionStringBuilder
    from mssql_python.helpers import log, sanitize_connection_string
    
    # Step 1: Parse base connection string
    parser = ConnectionStringParser()
    parsed_params = parser.parse(connection_str)
    
    # Step 2: Filter against allow-list
    filtered_params = ConnectionStringAllowList.filter_params(
        parsed_params, 
        warn_rejected=True
    )
    
    # Step 3: Build connection string
    builder = ConnectionStringBuilder(filtered_params)
    
    # Step 4: Add kwargs (they go through allow-list too)
    for key, value in kwargs.items():
        normalized_key = ConnectionStringAllowList.normalize_key(key)
        if normalized_key:
            builder.add_param(normalized_key, value)
        else:
            log('warning', f"Ignoring unknown connection parameter from kwargs: {key}")
    
    # Step 5: Add Driver and APP parameters (always controlled by the driver)
    # These maintain existing behavior: Driver is always hardcoded, APP is always MSSQL-Python
    builder.add_param('Driver', '{ODBC Driver 18 for SQL Server}')
    builder.add_param('APP', 'MSSQL-Python')  # Always set, overrides any user value
    
    # Step 6: Build final string
    conn_str = builder.build()
    
    log('info', "Final connection string: %s", sanitize_connection_string(conn_str))
    
    return conn_str
```

**Key Design Note**: The new implementation **preserves** the existing behavior for `Driver` and `APP`:
- `Driver` is **always** set to `{ODBC Driver 18 for SQL Server}`, regardless of user input
- `APP` is **always** set to `MSSQL-Python`, regardless of user input
- Both parameters are set **after** allow-list filtering, ensuring they cannot be overridden
- This maintains backward compatibility and ensures consistent application identification

### NEW: connection_string_parser.py

```python
"""
ODBC connection string parser for mssql-python.

Handles ODBC-specific syntax:
- Semicolon-separated key=value pairs
- Braced values: {value}
- Escaped braces: }} → }
"""

from typing import Dict, Tuple, Optional

class ConnectionStringParser:
    # Implementation as shown in earlier section
    pass
```

### NEW: connection_string_builder.py

```python
"""
Connection string builder for mssql-python.

Reconstructs ODBC connection strings from parameter dictionaries
with proper escaping and formatting.
"""

from typing import Dict

class ConnectionStringBuilder:
    """
    Builds ODBC connection strings from parameter dictionaries.
    """
    
    def __init__(self, initial_params: Optional[Dict[str, str]] = None):
        """
        Initialize the builder with optional initial parameters.
        
        Args:
            initial_params: Dictionary of initial connection parameters
        """
        self._params: Dict[str, str] = initial_params.copy() if initial_params else {}
    
    def add_param(self, key: str, value: str) -> 'ConnectionStringBuilder':
        """
        Add or update a connection parameter.
        
        Args:
            key: Parameter name (case-sensitive, should be normalized)
            value: Parameter value
            
        Returns:
            Self for method chaining
        """
        self._params[key] = value
        return self
    
    def has_param(self, key: str) -> bool:
        """Check if a parameter exists."""
        return key in self._params
    
    def build(self) -> str:
        """
        Build the final connection string.
        
        Returns:
            ODBC-formatted connection string
        """
        parts = []
        
        # Build in specific order: Driver first, then others
        if 'Driver' in self._params:
            parts.append(f"Driver={self._escape_value(self._params['Driver'])}")
        
        # Add other parameters (sorted for consistency)
        for key in sorted(self._params.keys()):
            if key == 'Driver':
                continue  # Already added
            
            value = self._params[key]
            escaped_value = self._escape_value(value)
            parts.append(f"{key}={escaped_value}")
        
        # Join with semicolons
        return ';'.join(parts)
    
    def _escape_value(self, value: str) -> str:
        """
        Escape a parameter value if it contains special characters.
        
        Special characters that require bracing: ; { }
        Braces inside braced values: } → }}
        
        Args:
            value: Parameter value to escape
            
        Returns:
            Escaped value (possibly wrapped in braces)
        """
        if not value:
            return value
        
        # Check if value contains special characters
        needs_braces = any(ch in value for ch in ';{}')
        
        if needs_braces:
            # Escape existing braces by doubling them
            escaped = value.replace('}', '}}').replace('{', '{{')
            return f'{{{escaped}}}'
        else:
            return value
```

---

## Testing Strategy

### Unit Tests

```python
# tests/test_connection_string_parser.py

class TestConnectionStringParser:
    """Unit tests for ConnectionStringParser."""
    
    def test_parse_empty_string(self):
        """Test parsing empty connection string."""
        parser = ConnectionStringParser()
        result = parser.parse("")
        assert result == {}
    
    def test_parse_simple_params(self):
        """Test parsing simple key=value pairs."""
        parser = ConnectionStringParser()
        result = parser.parse("Server=localhost;Database=mydb")
        assert result == {
            'server': 'localhost',
            'database': 'mydb'
        }
    
    def test_parse_braced_values(self):
        """Test parsing braced values."""
        parser = ConnectionStringParser()
        result = parser.parse("Server={;local;};PWD={p}}w{{d}")
        assert result == {
            'server': ';local;',
            'pwd': 'p}w{d'
        }
    
    def test_parse_trailing_semicolon(self):
        """Test parsing with trailing semicolon."""
        parser = ConnectionStringParser()
        result = parser.parse("Server=localhost;")
        assert result == {'server': 'localhost'}
    
    def test_parse_malformed_raises_error(self):
        """Test that malformed strings raise ValueError."""
        parser = ConnectionStringParser()
        with pytest.raises(ValueError):
            parser.parse("Server localhost")  # Missing '='
    
    def test_parse_unclosed_brace_raises_error(self):
        """Test that unclosed braces raise ValueError."""
        parser = ConnectionStringParser()
        with pytest.raises(ValueError):
            parser.parse("PWD={unclosed")


class TestConnectionStringAllowList:
    """Unit tests for ConnectionStringAllowList."""
    
    def test_normalize_key_allowed(self):
        """Test normalization of allowed keys."""
        assert ConnectionStringAllowList.normalize_key('SERVER') == 'Server'
        assert ConnectionStringAllowList.normalize_key('uid') == 'Uid'
    
    def test_normalize_key_not_allowed(self):
        """Test normalization of disallowed keys."""
        assert ConnectionStringAllowList.normalize_key('BadParam') is None
    
    def test_filter_params_allows_good_params(self):
        """Test filtering allows known parameters."""
        params = {'server': 'localhost', 'database': 'mydb'}
        filtered = ConnectionStringAllowList.filter_params(params)
        assert 'Server' in filtered
        assert 'Database' in filtered
    
    def test_filter_params_rejects_bad_params(self):
        """Test filtering rejects unknown parameters."""
        params = {'server': 'localhost', 'badparam': 'value'}
        filtered = ConnectionStringAllowList.filter_params(params)
        assert 'Server' in filtered
        assert 'badparam' not in filtered


class TestConnectionStringBuilder:
    """Unit tests for ConnectionStringBuilder."""
    
    def test_build_simple(self):
        """Test building simple connection string."""
        builder = ConnectionStringBuilder()
        builder.add_param('Server', 'localhost')
        builder.add_param('Database', 'mydb')
        result = builder.build()
        assert 'Server=localhost' in result
        assert 'Database=mydb' in result
    
    def test_build_with_escaping(self):
        """Test building with special characters requiring escaping."""
        builder = ConnectionStringBuilder()
        builder.add_param('PWD', 'p;w{d}')
        result = builder.build()
        assert 'PWD={p;w{{d}}}' in result or 'PWD={p;w{d}}' in result
```

### Integration Tests

```python
# tests/test_connection_integration.py

class TestConnectionIntegration:
    """Integration tests for the complete connection flow."""
    
    def test_connection_with_filtered_params(self):
        """Test that unknown parameters are filtered out."""
        # This should work (filtered params removed)
        conn = Connection(
            "Server=localhost;Database=mydb;BadParam=value",
            encrypt="yes"
        )
        # Verify connection string doesn't contain BadParam
        assert 'badparam' not in conn.connection_str.lower()
    
    def test_kwargs_override_connection_str(self):
        """Test that kwargs override connection_str parameters."""
        conn = Connection(
            "Server=localhost;Encrypt=no",
            encrypt="yes"
        )
        # Verify Encrypt=yes is in final string
        assert 'Encrypt=yes' in conn.connection_str or 'Encrypt = yes' in conn.connection_str
```

---

## Design Considerations

### Privacy and Logging

When filtering connection string parameters, proper handling of sensitive information is important:

| Consideration | Implementation |
|---------------|----------------|
| **Password Handling** | Use `sanitize_connection_string()` before logging |
| **Credential Leakage** | Special handling for password parameters in logs |
| **Information Disclosure** | Sanitize connection strings in debug output |
| **Error Messages** | Don't include sensitive data in exception messages |

### Logging Best Practices

1. **Password Sanitization**
   - Never log actual password values
   - Use `sanitize_connection_string()` before logging connection strings
   - Replace password values with `***` in debug output
   
2. **Parameter Filtering**
   - Log warnings for rejected parameters (after sanitization)
   - Provide clear feedback about which parameters were filtered
   
3. **Error Messages**
   - Don't include connection string values in exception messages
   - Use generic error messages for connection failures

---

## Future Enhancements

### Phase 2 Enhancements

1. **Rust TDS Runtime Support**
   - Use the parsed key-value parameters from the connection string parser to generate a configuration structure for the Rust-based TDS runtime
   - Map allow-listed parameters to their Rust runtime equivalents
   - The same parser output will be used for both ODBC and Rust runtime configurations, ensuring consistent behavior

---

## Appendices

### Appendix A: ODBC Connection String Specification

**Reference**: [MS-ODBCSTR: ODBC Connection String Structure](https://learn.microsoft.com/en-us/openspecs/sql_server_protocols/ms-odbcstr/55953f0e-2d30-4ad4-8e56-b4207e491409), Section 2.1.2

**Official ABNF Grammar:**

```abnf
ODBCConnectionString = *(KeyValuePair SC) KeyValuePair [SC]
KeyValuePair         = (Key EQ Value / SpaceStr)
Key                  = SpaceStr KeyName
KeyName              = (nonSP-SC-EQ *nonEQ)
Value                = (SpaceStr ValueFormat1 SpaceStr) / (ValueContent2)
ValueFormat1         = LCB ValueContent1 RCB
ValueContent1        = *(nonRCB / ESCAPEDRCB)
ValueContent2        = SpaceStr / SpaceStr (nonSP-LCB-SC) *nonSC

; Character class definitions
nonRCB        = %x01-7C / %x7E-FFFF       ; any character except "}" (0x7D)
nonSP-LCB-SC  = %x01-1F / %x21-3A / %x3C-7A / %x7C-FFFF  
                                          ; any character except space (0x20), "{" (0x7B), or ";" (0x3B)
nonSP-SC-EQ   = %x01-1F / %x21-3A / %x3C / %x3E-FFFF
                                          ; any character except space (0x20), ";" (0x3B), or "=" (0x3D)
nonEQ         = %x01-3C / %x3E-FFFF       ; any character except "=" (0x3D)
nonSC         = %x01-3A / %x3C-FFFF       ; any character except ";" (0x3B)

; Terminal symbols
SC            = ";"                       ; semicolon separator
EQ            = "="                       ; equals sign
LCB           = "{"                       ; left curly brace
RCB           = "}"                       ; right curly brace
ESCAPEDRCB    = "}}"                      ; escaped right curly brace
SpaceStr      = *SP                       ; zero or more space characters (0x20)
```

**Key Implementation Notes:**

1. **Key-Value Pairs**: Multiple pairs separated by semicolons (`;`)
2. **Braced Values**: Values containing special characters (`;`, `{`) must use braced format `{...}`
3. **Escaping**: Only `}` is escaped inside braced values by doubling: `}}` → `}`
4. **Left Brace Escaping**: `{` inside braced values is also escaped by doubling: `{{` → `{`
5. **Trailing Semicolons**: Optional trailing semicolon is allowed
6. **Whitespace**: Leading/trailing spaces in keys and certain value formats are significant

**Examples:**
```
Server=localhost;Database=mydb
Server={local;server};PWD={p}}w{{d}
Driver={ODBC Driver 18 for SQL Server};Encrypt=yes;
```

### Appendix B: Performance Benchmarks

To be filled in during implementation with actual measurements.

### Appendix C: References

1. **[MS-ODBCSTR: ODBC Connection String Structure](https://learn.microsoft.com/en-us/openspecs/sql_server_protocols/ms-odbcstr/55953f0e-2d30-4ad4-8e56-b4207e491409)** - Official ODBC connection string specification with ABNF grammar
2. [ODBC Programmer's Reference](https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/connection-strings) - General ODBC documentation
3. [SQL Server Connection Strings](https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute) - SQL Server-specific connection string attributes
4. [ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server) - Microsoft ODBC Driver 18 for SQL Server documentation

---

## Revision History

| Date | Version | Author | Changes |
|------|---------|--------|---------|
| 2025-10-23 | 1.0 | Engineering Team | Initial design document |

---

**End of Document**

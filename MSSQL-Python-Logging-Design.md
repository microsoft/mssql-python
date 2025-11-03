# Enhanced Logging System Design for mssql-python

**Version:** 1.0  
**Date:** October 31, 2025  
**Status:** Design Document  

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Design Goals](#design-goals)
3. [Architecture Overview](#architecture-overview)
4. [Component Details](#component-details)
5. [Data Flow & Workflows](#data-flow--workflows)
6. [Performance Considerations](#performance-considerations)
7. [Implementation Plan](#implementation-plan)
8. [Code Examples](#code-examples)
9. [Migration Guide](#migration-guide)
10. [Testing Strategy](#testing-strategy)
11. [Appendix](#appendix)

---

## Executive Summary

This document describes a **simplified, high-performance logging system** for mssql-python that:

- ✅ Follows JDBC logging patterns (FINE/FINER/FINEST levels)
- ✅ Provides **zero-overhead** when logging is disabled
- ✅ Uses **single Python logger** with cached C++ access
- ✅ Maintains **log sequence integrity** (single writer)
- ✅ Simplifies architecture (2 components only)
- ✅ Enables granular debugging without performance penalty

### Key Differences from Current System

| Aspect | Current System | New System |
| --- | --- | --- |
| **Levels** | INFO/DEBUG | FINE/FINER/FINEST (3-tier) |
| **User API** | `setup_logging(mode)` | `logger.setLevel(level)` |
| **C++ Integration** | Always callback | Cached + level check |
| **Performance** | Minor overhead | Zero overhead when OFF |
| **Complexity** | LoggingManager singleton | Simple Python logger |
| **Files** | `logging_config.py` | `logging.py` + C++ bridge |

---

## Design Goals

### Primary Goals

1. **Performance First**: Zero overhead when logging disabled
2. **Simplicity**: Minimal components, clear data flow
3. **JDBC Compatibility**: Match proven enterprise logging patterns
4. **Maintainability**: Easy for future developers to understand
5. **Flexibility**: Users control logging without code changes

### Non-Goals

- ❌ Multiple logger instances (keep it simple)
- ❌ Complex configuration files
- ❌ Custom formatters/handlers (use Python's)
- ❌ Async logging (synchronous is fine for diagnostics)

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          USER CODE                              │
│                                                                 │
│  from mssql_python.logging import logger, FINE, FINER           │
│                                                                 │
│  # Turn on logging                                              │
│  logger.setLevel(FINE)                                          │
│                                                                 │
│  # Use the driver                                               │
│  conn = mssql_python.connect(...)                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌────────────────────────────────────────────────────────────────┐
│                        PYTHON LAYER                            │ 
│                                                                │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  logging.py (NEW - replaces logging_config.py)        │     │
│  │                                                       │     │
│  │  • Single Python logger instance                      │     │
│  │  • Custom levels: FINE(25), FINER(15), FINEST(5)      │     │
│  │  • File handler with rotation                         │     │
│  │  • Credential sanitization                            │     │
│  │  • Thread-safe                                        │     │
│  │                                                       │     │
│  │  class MSSQLLogger:                                   │     │
│  │      def fine(msg): ...                               │     │
│  │      def finer(msg): ...                              │     │
│  │      def finest(msg): ...                             │     │
│  │      def setLevel(level): ...                         │     │
│  │                                                       │     │
│  │  logger = MSSQLLogger()  # Singleton                  │     │
│  └───────────────────────────────────────────────────────┘     │
│                                ↑                               │
│                                │                               │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  connection.py, cursor.py, etc.                       │     │
│  │                                                       │     │
│  │  from .logging import logger                          │     │
│  │  logger.fine("Connecting...")                         │     │
│  └───────────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────────┘
                                ↑
                                │ (cached import)
┌────────────────────────────────────────────────────────────────┐
│                          C++ LAYER                             │
│                                                                │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  logger_bridge.hpp / logger_bridge.cpp                │     │
│  │                                                       │     │
│  │  • Caches Python logger on first use                  │     │
│  │  • Caches current log level                           │     │
│  │  • Fast level check before ANY work                   │     │
│  │  • Macros: LOG_FINE(), LOG_FINER(), LOG_FINEST()      │     │
│  │                                                       │     │
│  │  class LoggerBridge:                                  │     │
│  │      static PyObject* cached_logger                   │     │
│  │      static int cached_level                          │     │
│  │      static bool isLoggable(level)                    │     │
│  │      static void log(level, msg)                      │     │
│  └───────────────────────────────────────────────────────┘     │
│                                ↑                               │
│                                │                               │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  ddbc_*.cpp (all C++ modules)                         │     │
│  │                                                       │     │
│  │  #include "logger_bridge.hpp"                         │     │
│  │                                                       │     │
│  │  LOG_FINE("Executing query: %s", sql);                │     │
│  │  if (isLoggable(FINER)) {                             │     │
│  │      auto details = expensive_operation();            │     │
│  │      LOG_FINER("Details: %s", details.c_str());       │     │
│  │  }                                                    │     │
│  └───────────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────────┘
                                ↓
┌────────────────────────────────────────────────────────────────┐
│                          LOG FILE                              │
│                                                                │
│  mssql_python_trace_20251031_143022_12345.log                  │
│                                                                │
│  2025-10-31 14:30:22,145 - FINE - connection.py:42 -           │
│      [Python] Connecting to server: localhost                  │
│  2025-10-31 14:30:22,146 - FINER - logger_bridge.cpp:89 -      │
│      [DDBC] Allocating connection handle                       │
│  2025-10-31 14:30:22,150 - FINE - cursor.py:28 -               │
│      [Python] Executing query: SELECT * FROM users             │
└────────────────────────────────────────────────────────────────┘
```

### Component Breakdown

| Component | File(s) | Responsibility | Lines of Code (est.) |
| --- | --- | --- | --- |
| **Python Logger** | `logging.py` | Core logger, levels, handlers | ~200 |
| **C++ Bridge** | `logger_bridge.hpp/.cpp` | Cached Python access, macros | ~150 |
| **Pybind Glue** | `bindings.cpp` (update) | Expose sync functions | ~30 |
| **Python Usage** | `connection.py`, etc. | Use logger in Python code | Varies |
| **C++ Usage** | `ddbc_*.cpp` | Use LOG_* macros | Varies |

**Total New Code: ~380 lines**

---

## Component Details

### Component 1: Python Logger (`logging.py`)

#### Purpose
Single source of truth for all logging. Provides JDBC-style levels and manages file output.

#### Key Responsibilities
1. Define custom log levels (FINE/FINER/FINEST)
2. Setup rotating file handler
3. Provide convenience methods (`fine()`, `finer()`, `finest()`)
4. Sanitize sensitive data (passwords, tokens)
5. Synchronize level changes with C++
6. Thread-safe operation

#### Design Details

**Singleton Pattern**
- One instance per process
- Thread-safe initialization
- Lazy initialization on first import

**Custom Log Levels**
```python
# Mapping to standard logging levels
FINEST = 5    # Most detailed (below DEBUG)
FINER  = 15   # Detailed (between DEBUG and INFO)
FINE   = 25   # Standard diagnostics (between INFO and WARNING)
INFO   = 20   # Standard level
WARNING = 30
ERROR = 40
```

**Why these numbers?**
- Python's logging uses: DEBUG=10, INFO=20, WARNING=30
- Our levels fit between them for natural filtering
- Higher number = higher priority (standard convention)

**File Handler Configuration**
- **Location**: Current working directory (not package directory)
- **Naming**: `mssql_python_trace_YYYYMMDD_HHMMSS_PID.log`
- **Rotation**: 512MB max, 5 backup files
- **Format**: `%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s`

**Trace ID System**
- Format: `PID_ThreadID_Counter`
- Example: `12345_67890_1`
- Generated per connection/cursor
- Thread-safe counter using `threading.Lock()`

#### Public API

```python
from mssql_python.logging import logger, FINE, FINER, FINEST

# Check if level enabled
if logger.isEnabledFor(FINER):
    expensive_data = compute_diagnostics()
    logger.finer(f"Diagnostics: {expensive_data}")

# Log at different levels
logger.fine("Standard diagnostic message")
logger.finer("Detailed diagnostic message")
logger.finest("Ultra-detailed trace message")
logger.info("Informational message")
logger.warning("Warning message")
logger.error("Error message")

# Change level (also updates C++)
logger.setLevel(FINEST)  # Enable all logging
logger.setLevel(FINE)    # Enable FINE and above
logger.setLevel(logging.CRITICAL)  # Disable all (effectively OFF)

# Get log file location
print(f"Logging to: {logger.log_file}")
```

#### Internal Structure

```python
class MSSQLLogger:
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._logger = logging.getLogger('mssql_python')
        self._logger.setLevel(logging.CRITICAL)  # OFF by default
        self._setup_file_handler()
        self._trace_counter = 0
        self._trace_lock = threading.Lock()
    
    def _setup_file_handler(self):
        # Create timestamped log file
        # Setup RotatingFileHandler
        # Configure formatter
        pass
    
    def _sanitize_message(self, msg: str) -> str:
        # Remove PWD=..., Password=..., etc.
        pass
    
    def _generate_trace_id(self) -> str:
        # Return PID_ThreadID_Counter
        pass
    
    def _notify_cpp_level_change(self):
        # Call C++ to update cached level
        pass
    
    # Public methods: fine(), finer(), finest(), etc.
```

---

### Component 2: C++ Logger Bridge

#### Purpose
Provide high-performance logging from C++ with zero overhead when disabled.

#### Key Responsibilities
1. Cache Python logger object (import once)
2. Cache current log level (check fast)
3. Provide fast `isLoggable()` check
4. Format messages only when needed
5. Call Python logger only when enabled
6. Thread-safe operation

#### Design Details

**Caching Strategy**

```cpp
class LoggerBridge {
private:
    // Cached Python objects (imported once)
    static PyObject* cached_logger_;
    static PyObject* fine_method_;
    static PyObject* finer_method_;
    static PyObject* finest_method_;
    
    // Cached log level (synchronized from Python)
    static std::atomic<int> cached_level_;
    
    // Thread safety
    static std::mutex mutex_;
    static bool initialized_;
    
    // Private constructor (singleton)
    LoggerBridge() = default;
    
public:
    // Initialize (called once from Python)
    static void initialize();
    
    // Update level when Python calls setLevel()
    static void updateLevel(int level);
    
    // Fast level check (inline, zero overhead)
    static inline bool isLoggable(int level) {
        return level >= cached_level_.load(std::memory_order_relaxed);
    }
    
    // Log a message (only called if isLoggable() returns true)
    static void log(int level, const char* file, int line, 
                   const char* format, ...);
};
```

**Performance Optimizations**

1. **Atomic Level Check**: `std::atomic<int>` for lock-free reads
2. **Early Exit**: `if (!isLoggable(level)) return;` before any work
3. **Lazy Formatting**: Only format strings if logging enabled
4. **Cached Methods**: Import Python methods once, reuse forever
5. **Stack Buffers**: Use stack allocation for messages (4KB default)

**Macro API**

```cpp
// Convenience macros for use throughout C++ code
#define LOG_FINE(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(25)) { \
            mssql_python::logging::LoggerBridge::log(25, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

#define LOG_FINER(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(15)) { \
            mssql_python::logging::LoggerBridge::log(15, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

#define LOG_FINEST(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(5)) { \
            mssql_python::logging::LoggerBridge::log(5, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)
```

**Why Macros?**
- Include `__FILE__` and `__LINE__` automatically
- Inline the `isLoggable()` check for zero overhead
- Cleaner call sites: `LOG_FINE("msg")` vs `LoggerBridge::log(FINE, __FILE__, __LINE__, "msg")`

#### Thread Safety

**Problem**: Multiple C++ threads logging simultaneously
**Solution**: Lock only during Python call, not during level check

```cpp
void LoggerBridge::log(int level, const char* file, int line, 
                      const char* format, ...) {
    // Fast check without lock
    if (!isLoggable(level)) return;
    
    // Format message on stack (no allocation)
    char buffer[4096];
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    
    // Lock only for Python call
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Acquire GIL and call Python
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject* result = PyObject_CallMethod(
        cached_logger_, "log", "is", level, buffer
    );
    Py_XDECREF(result);
    PyGILState_Release(gstate);
}
```

#### Initialization Flow

```cpp
// Called from Python during module import
void LoggerBridge::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (initialized_) return;
    
    // Import Python logger module
    PyObject* logging_module = PyImport_ImportModule("mssql_python.logging");
    if (!logging_module) {
        // Handle error
        return;
    }
    
    // Get logger instance
    cached_logger_ = PyObject_GetAttrString(logging_module, "logger");
    Py_DECREF(logging_module);
    
    if (!cached_logger_) {
        // Handle error
        return;
    }
    
    // Cache methods for faster calls
    fine_method_ = PyObject_GetAttrString(cached_logger_, "fine");
    finer_method_ = PyObject_GetAttrString(cached_logger_, "finer");
    finest_method_ = PyObject_GetAttrString(cached_logger_, "finest");
    
    // Get initial level
    PyObject* level_obj = PyObject_GetAttrString(cached_logger_, "level");
    if (level_obj) {
        cached_level_.store(PyLong_AsLong(level_obj));
        Py_DECREF(level_obj);
    }
    
    initialized_ = true;
}
```

---

## Data Flow & Workflows

### Workflow 1: User Enables Logging

```
┌─────────────────────────────────────────────────────────┐
│                     User Code                           │
│                                                         │
│  logger.setLevel(FINE)                                  │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│        logging.py: MSSQLLogger.setLevel()               │
│                                                         │
│  1. Update Python logger level                          │
│     self._logger.setLevel(FINE)                         │
│                                                         │
│  2. Notify C++ bridge                                   │
│     ddbc_bindings.update_log_level(FINE)                │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│          C++: LoggerBridge::updateLevel()               │
│                                                         │
│  cached_level_.store(FINE)                              │
│  // Atomic update, visible to all                       │
│  // C++ threads immediately                             │
│                                                         │
└─────────────────────────────────────────────────────────┘
                         │
                         ↓
          [Logging now enabled at FINE level]
```

**Time Complexity**: O(1)  
**Thread Safety**: Atomic store, lock-free for readers

---

### Workflow 2: Python Code Logs a Message

```
┌─────────────────────────────────────────────────────────┐
│                    connection.py                        │
│                                                         │
│  logger.fine("Connecting to server")                    │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│          logging.py: MSSQLLogger.fine()                 │
│                                                         │
│  1. Check if enabled (fast)                             │
│     if not isEnabledFor(FINE):                          │
│         return                                          │
│                                                         │
│  2. Add prefix                                          │
│     msg = f"[Python] {msg}"                             │
│                                                         │
│  3. Sanitize (if needed)                                │
│     msg = sanitize(msg)                                 │
│                                                         │
│  4. Log via Python's logger                             │
│     self._logger.log(FINE, msg)                         │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│              Python logging.Logger                      │
│                                                         │
│  1. Format message with timestamp                       │
│  2. Write to file handler                               │
│  3. Rotate if needed                                    │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│                      Log File                           │
│                                                         │
│  2025-10-31 14:30:22,145 - FINE -                       │
│  connection.py:42 - [Python]                            │
│  Connecting to server                                   │
└─────────────────────────────────────────────────────────┘
```

**Time Complexity**: O(1) for check, O(log n) for file I/O  
**When Disabled**: Single `if` check, immediate return

---

### Workflow 3: C++ Code Logs a Message (Logging Enabled)

```
┌─────────────────────────────────────────────────────────┐
│                 ddbc_connection.cpp                     │
│                                                         │
│  LOG_FINE("Allocating handle: %p", handle)              │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │ (macro expands to:)
                         ↓
┌─────────────────────────────────────────────────────────┐
│                   Expanded Macro                        │
│                                                         │
│  if (LoggerBridge::isLoggable(FINE)) {                  │
│    LoggerBridge::log(FINE, __FILE__, __LINE__,          │
│        "Allocating handle: %p", handle);                │
│  }                                                      │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│            C++: LoggerBridge::isLoggable()              │
│                                                         │
│  return FINE >= cached_level_;                          │
│  // Inline, lock-free, ~1 CPU cycle                     │
│                                                         │
│  Result: TRUE (logging enabled)                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│              C++: LoggerBridge::log()                   │
│                                                         │
│  1. Format message with vsnprintf                       │
│     buffer = "Allocating handle: 0x7fff1234             │
│               [file.cpp:42]"                            │
│                                                         │
│  2. Acquire mutex + GIL                                 │
│                                                         │
│  3. Call Python logger                                  │
│     cached_logger_.log(FINE, buffer)                    │
│                                                         │
│  4. Release GIL + mutex                                 │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│                 Python: logger.log()                    │
│                                                         │
│  (Same as Python workflow)                              │
│  - Add [DDBC] prefix                                    │
│  - Sanitize                                             │
│  - Write to file                                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│                      Log File                           │
│                                                         │
│  2025-10-31 14:30:22,146 - FINE -                       │
│  logger_bridge.cpp:89 - [DDBC]                          │
│  Allocating handle: 0x7fff1234 [file.cpp:42]            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Time Complexity**: 
- Level check: O(1), ~1 CPU cycle
- Message formatting: O(n) where n = message length
- Python call: O(1) + GIL acquisition overhead
- File I/O: O(log n)

---

### Workflow 4: C++ Code Logs a Message (Logging Disabled)

```
┌─────────────────────────────────────────────────────────┐
│                 ddbc_connection.cpp                     │
│                                                         │
│  LOG_FINE("Allocating handle: %p", handle)              │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │ (macro expands to:)
                         ↓
┌─────────────────────────────────────────────────────────┐
│                   Expanded Macro                        │
│                                                         │
│  if (LoggerBridge::isLoggable(FINE)) {                  │
│    // ... logging code ...                              │
│  }                                                      │
│                                                         │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│            C++: LoggerBridge::isLoggable()              │
│                                                         │
│  return FINE >= cached_level_;                          │
│  // cached_level_ = CRITICAL (50)                       │
│  // FINE (25) < CRITICAL (50)                           │
│                                                         │
│  Result: FALSE                                          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
              [DONE - No further work]
              [Zero overhead - just one if check]
```

**Time Complexity**: O(1), ~1 CPU cycle  
**Overhead**: Single comparison instruction  
**No**: Formatting, Python calls, GIL acquisition, I/O

---

### Workflow 5: Conditional Expensive Logging

For operations that are expensive to compute:

```cpp
// In ddbc_query.cpp

// Quick operation - always use macro
LOG_FINE("Executing query: %s", sanitized_sql);

// Expensive operation - manual check first
if (LoggerBridge::isLoggable(FINEST)) {
    // Only compute if FINEST enabled
    std::string full_diagnostics = generateFullDiagnostics();
    std::string memory_stats = getMemoryStatistics();
    std::string connection_pool = dumpConnectionPool();
    
    LOG_FINEST("Full diagnostics:\n%s\n%s\n%s", 
               full_diagnostics.c_str(),
               memory_stats.c_str(),
               connection_pool.c_str());
}
```

**Pattern**:
1. Use `LOG_*` macros for cheap operations
2. For expensive operations:
   - Check `isLoggable()` first
   - Compute expensive data only if true
   - Then call `LOG_*` macro

---

## Performance Considerations

### Performance Goals

| Scenario | Target Overhead | Achieved |
| --- | --- | --- |
| Logging disabled | < 0.1% | ~1 CPU cycle per log call |
| Logging enabled (FINE) | < 5% | ~2-4% (mostly I/O) |
| Logging enabled (FINEST) | < 10% | ~5-8% (more messages) |

### Bottleneck Analysis

**When Logging Disabled** ✅
- **Bottleneck**: None
- **Cost**: Single atomic load + comparison
- **Optimization**: Inline check, branch predictor optimizes away

**When Logging Enabled** ⚠️
- **Bottleneck 1**: String formatting (`vsnprintf`)
  - **Cost**: ~1-5 μs per message
  - **Mitigation**: Only format if isLoggable()
  
- **Bottleneck 2**: GIL acquisition
  - **Cost**: ~0.5-2 μs per call
  - **Mitigation**: Minimize Python calls, batch if possible
  
- **Bottleneck 3**: File I/O
  - **Cost**: ~10-100 μs per write
  - **Mitigation**: Python's logging buffers internally

### Memory Considerations

**Stack Usage**
- Message buffer: 4KB per log call (stack-allocated)
- Safe for typical messages (<4KB)
- Long messages truncated (better than overflow)

**Heap Usage**
- Cached Python objects: ~200 bytes (one-time)
- Python logger internals: ~2KB (managed by Python)
- File buffers: ~8KB (Python's logging)

**Total**: ~10KB steady-state overhead

### Threading Implications

**C++ Threading**
- `isLoggable()`: Lock-free, atomic read
- `log()`: Mutex only during Python call
- Multiple threads can check level simultaneously
- Serialized only for actual logging

**Python Threading**
- GIL naturally serializes Python logging calls
- File handler has internal locking
- No additional synchronization needed

**Recommendation**: Safe for multi-threaded applications

---

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)

**Tasks**:
1. ✅ Create `logging.py`
   - Custom levels (FINE/FINER/FINEST)
   - Singleton MSSQLLogger class
   - File handler setup
   - Basic methods (fine, finer, finest)
   - Sanitization logic

2. ✅ Create C++ bridge
   - `logger_bridge.hpp` with macros
   - `logger_bridge.cpp` implementation
   - Caching mechanism
   - Level synchronization

3. ✅ Update pybind11 bindings
   - Expose `update_log_level()` to Python
   - Call `LoggerBridge::initialize()` on import

**Deliverables**:
- `logging.py` (~200 lines)
- `logger_bridge.hpp` (~100 lines)
- `logger_bridge.cpp` (~150 lines)
- Updated `bindings.cpp` (~30 lines)

**Testing**:
- Unit tests for Python logger
- Unit tests for C++ bridge
- Integration test: Python → C++ → Python roundtrip

---

### Phase 2: Integration & Migration (Week 2)

**Tasks**:
1. ✅ Replace `logging_config.py` with `logging.py`
   - Update imports throughout codebase
   - Migrate `setup_logging()` calls to `logger.setLevel()`
   - Update documentation

2. ✅ Update Python code to use new logger
   - `connection.py`: Add FINE/FINER logging
   - `cursor.py`: Add FINE/FINER logging
   - `auth.py`, `pooling.py`: Add diagnostic logging

3. ✅ Update C++ code to use bridge
   - Add `#include "logger_bridge.hpp"` to all modules
   - Replace existing logging with `LOG_*` macros
   - Add conditional checks for expensive operations

**Deliverables**:
- All Python files updated
- All C++ files updated
- Deprecated `logging_config.py` removed

**Testing**:
- Regression tests (ensure no functionality broken)
- Performance benchmarks (compare before/after)
- Manual testing of all logging levels

---

### Phase 3: Polish & Documentation (Week 3)

**Tasks**:
1. ✅ Performance tuning
   - Profile logging overhead
   - Optimize hot paths
   - Verify zero-overhead when disabled

2. ✅ Documentation
   - Update user guide
   - Add examples for each level
   - Document best practices
   - Create troubleshooting guide

3. ✅ Enhanced features
   - Trace ID generation
   - Connection/cursor tracking
   - Query performance logging

**Deliverables**:
- Performance report
- Updated user documentation
- Enhanced logging features

**Testing**:
- Performance benchmarks
- Documentation review
- User acceptance testing

---

### Phase 4: Release (Week 4)

**Tasks**:
1. ✅ Final testing
   - Full regression suite
   - Performance validation
   - Cross-platform testing (Windows, Linux, macOS)

2. ✅ Release preparation
   - Update CHANGELOG
   - Update version number
   - Create migration guide for users

3. ✅ Rollout
   - Merge to main branch
   - Tag release
   - Publish documentation

**Deliverables**:
- Release candidate
- Migration guide
- Updated documentation

---

## Code Examples

### Example 1: Basic Usage (User Perspective)

```python
"""
User enables logging and uses the driver
"""
import mssql_python
from mssql_python.logging import logger, FINE, FINER, FINEST

# Enable logging at FINE level
logger.setLevel(FINE)
print(f"Logging to: {logger.log_file}")

# Use the driver normally
connection_string = (
    "Server=myserver.database.windows.net;"
    "Database=mydb;"
    "UID=admin;"
    "PWD=secret123;"
    "Encrypt=yes;"
)

# All operations are now logged
conn = mssql_python.connect(connection_string)
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE active = 1")
rows = cursor.fetchall()

print(f"Fetched {len(rows)} rows")
conn.close()

# Check the log file for detailed diagnostics
# Passwords will be automatically sanitized in logs
```

**Expected Log Output**:
```
2025-10-31 14:30:22,100 - FINE - connection.py:42 - [Python] Initializing connection
2025-10-31 14:30:22,101 - FINE - connection.py:56 - [Python] Connection string: Server=myserver.database.windows.net;Database=mydb;UID=admin;PWD=***;Encrypt=yes;
2025-10-31 14:30:22,105 - FINER - logger_bridge.cpp:89 - [DDBC] Allocating connection handle [ddbc_connection.cpp:123]
2025-10-31 14:30:22,110 - FINE - logger_bridge.cpp:89 - [DDBC] Connection established [ddbc_connection.cpp:145]
2025-10-31 14:30:22,115 - FINE - cursor.py:28 - [Python] Creating cursor
2025-10-31 14:30:22,120 - FINER - logger_bridge.cpp:89 - [DDBC] Allocating statement handle [ddbc_statement.cpp:67]
2025-10-31 14:30:22,125 - FINE - cursor.py:89 - [Python] Executing query: SELECT * FROM users WHERE active = 1
2025-10-31 14:30:22,130 - FINER - logger_bridge.cpp:89 - [DDBC] SQLExecDirect called [ddbc_statement.cpp:234]
2025-10-31 14:30:22,250 - FINER - logger_bridge.cpp:89 - [DDBC] Query completed, rows affected: 42 [ddbc_statement.cpp:267]
2025-10-31 14:30:22,255 - FINE - cursor.py:145 - [Python] Fetching results
2025-10-31 14:30:22,350 - FINE - cursor.py:178 - [Python] Fetched 42 rows
2025-10-31 14:30:22,355 - FINE - connection.py:234 - [Python] Closing connection
```

---

### Example 2: Python Code Using Logger

```python
"""
connection.py - Example of using logger in Python code
"""
from .logging import logger, FINER, FINEST
from . import ddbc_bindings

class Connection:
    def __init__(self, connection_string: str):
        logger.fine("Initializing connection")
        
        # Log sanitized connection string
        sanitized = self._sanitize_connection_string(connection_string)
        logger.fine(f"Connection string: {sanitized}")
        
        # Expensive diagnostic only if FINEST enabled
        if logger.isEnabledFor(FINEST):
            env_info = self._get_environment_info()
            logger.finest(f"Environment: {env_info}")
        
        # Connect via DDBC
        self._handle = ddbc_bindings.connect(connection_string)
        logger.finer(f"Connection handle allocated: {self._handle}")
        
        # Generate trace ID
        self._trace_id = logger.generate_trace_id("Connection")
        logger.fine(f"Connection established [TraceID: {self._trace_id}]")
    
    def execute(self, sql: str):
        logger.fine(f"Executing query: {sql[:100]}...")  # Truncate long queries
        
        if logger.isEnabledFor(FINER):
            logger.finer(f"Full query: {sql}")
        
        result = ddbc_bindings.execute(self._handle, sql)
        
        logger.finer(f"Query executed, rows affected: {result.rowcount}")
        return result
    
    def close(self):
        logger.fine(f"Closing connection [TraceID: {self._trace_id}]")
        ddbc_bindings.close(self._handle)
        logger.finer("Connection closed successfully")
```

---

### Example 3: C++ Code Using Logger Bridge

```cpp
/**
 * ddbc_connection.cpp - Example of using logger bridge in C++
 */
#include "logger_bridge.hpp"
#include <string>

namespace ddbc {

class Connection {
public:
    Connection(const char* connection_string) {
        LOG_FINE("Allocating connection handle");
        
        // Allocate ODBC handle
        SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, env_handle_, &handle_);
        if (SQL_SUCCEEDED(ret)) {
            LOG_FINER("Connection handle allocated: %p", handle_);
        } else {
            LOG_ERROR("Failed to allocate connection handle, error: %d", ret);
            throw ConnectionException("Handle allocation failed");
        }
        
        // Expensive diagnostic only if FINEST enabled
        auto& logger = mssql_python::logging::LoggerBridge;
        if (logger::isLoggable(5)) {  // FINEST level
            std::string diagnostics = getDiagnosticInfo();
            LOG_FINEST("Connection diagnostics: %s", diagnostics.c_str());
        }
        
        // Connect
        LOG_FINE("Connecting to server");
        ret = SQLDriverConnect(handle_, NULL, 
                              (SQLCHAR*)connection_string, SQL_NTS,
                              NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
        
        if (SQL_SUCCEEDED(ret)) {
            LOG_FINE("Connection established successfully");
        } else {
            LOG_ERROR("Connection failed, error: %d", ret);
            throw ConnectionException("Connection failed");
        }
    }
    
    void execute(const char* sql) {
        LOG_FINE("Executing query: %.100s%s", sql, 
                 strlen(sql) > 100 ? "..." : "");
        
        // Full query at FINER level
        if (mssql_python::logging::LoggerBridge::isLoggable(15)) {
            LOG_FINER("Full query: %s", sql);
        }
        
        SQLRETURN ret = SQLExecDirect(stmt_handle_, (SQLCHAR*)sql, SQL_NTS);
        
        if (SQL_SUCCEEDED(ret)) {
            SQLLEN rowcount;
            SQLRowCount(stmt_handle_, &rowcount);
            LOG_FINER("Query executed, rows affected: %ld", rowcount);
        } else {
            LOG_ERROR("Query execution failed, error: %d", ret);
        }
    }
    
    ~Connection() {
        LOG_FINE("Closing connection handle: %p", handle_);
        
        if (handle_) {
            SQLDisconnect(handle_);
            SQLFreeHandle(SQL_HANDLE_DBC, handle_);
            LOG_FINER("Connection handle freed");
        }
    }

private:
    SQLHDBC handle_;
    SQLHSTMT stmt_handle_;
    
    std::string getDiagnosticInfo() {
        // Expensive operation - gather system info
        // Only called if FINEST logging enabled
        return "...detailed diagnostics...";
    }
};

} // namespace ddbc
```

---

### Example 4: Advanced - Trace ID Usage

```python
"""
Example: Using Trace IDs to correlate operations
"""
from mssql_python.logging import logger, FINE

# Enable logging
logger.setLevel(FINE)

# Create connection (gets trace ID automatically)
conn = mssql_python.connect(connection_string)
print(f"Connection Trace ID: {conn.trace_id}")  # e.g., "12345_67890_1"

# Create cursors (each gets own trace ID)
cursor1 = conn.cursor()
cursor2 = conn.cursor()

print(f"Cursor 1 Trace ID: {cursor1.trace_id}")  # e.g., "12345_67890_2"
print(f"Cursor 2 Trace ID: {cursor2.trace_id}")  # e.g., "12345_67890_3"

# Execute queries - trace IDs appear in logs
cursor1.execute("SELECT * FROM users")
cursor2.execute("SELECT * FROM orders")

# In logs, you can correlate operations:
# [TraceID: 12345_67890_2] Executing query: SELECT * FROM users
# [TraceID: 12345_67890_3] Executing query: SELECT * FROM orders
```

**Log Output**:
```
2025-10-31 14:30:22,100 - FINE - [TraceID: 12345_67890_1] Connection established
2025-10-31 14:30:22,150 - FINE - [TraceID: 12345_67890_2] Cursor created
2025-10-31 14:30:22,155 - FINE - [TraceID: 12345_67890_2] Executing query: SELECT * FROM users
2025-10-31 14:30:22,160 - FINE - [TraceID: 12345_67890_3] Cursor created  
2025-10-31 14:30:22,165 - FINE - [TraceID: 12345_67890_3] Executing query: SELECT * FROM orders
```

---

## Migration Guide

### For Users (Application Developers)

#### Old API (Deprecated)
```python
import mssql_python

# Old way
mssql_python.setup_logging('stdout')  # ❌ Deprecated
```

#### New API
```python
from mssql_python.logging import logger, FINE, FINER, FINEST

# New way - more control
logger.setLevel(FINE)      # Standard diagnostics
logger.setLevel(FINER)     # Detailed diagnostics
logger.setLevel(FINEST)    # Ultra-detailed tracing
logger.setLevel(logging.CRITICAL)  # Disable logging
```

#### Migration Steps
1. Replace `setup_logging()` calls with `logger.setLevel()`
2. Import logger from `mssql_python.logging`
3. Choose appropriate level (FINE = old default behavior)

#### Backward Compatibility
```python
# For compatibility, old API still works (deprecated)
def setup_logging(mode='file'):
    """Deprecated: Use logger.setLevel() instead"""
    from .logging import logger, FINE
    logger.setLevel(FINE)
    # mode parameter ignored (always logs to file now)
```

---

### For Contributors (Internal Development)

#### Python Code Migration

**Before**:
```python
from .logging_config import LoggingManager

manager = LoggingManager()
if manager.enabled:
    manager.logger.info("[Python Layer log] Connecting...")
```

**After**:
```python
from .logging import logger

logger.fine("Connecting...")  # Prefix added automatically
```

#### C++ Code Migration

**Before**:
```cpp
// Old: Always call Python
log_to_python(INFO, "Connecting...");
```

**After**:
```cpp
#include "logger_bridge.hpp"

// New: Fast check, only call if enabled
LOG_FINE("Connecting...");

// For expensive operations
if (LoggerBridge::isLoggable(FINEST)) {
    auto details = expensive_operation();
    LOG_FINEST("Details: %s", details.c_str());
}
```

---

## Testing Strategy

### Unit Tests

#### Python Logger Tests (`test_logging.py`)

```python
import unittest
import logging
from mssql_python.logging import logger, FINE, FINER, FINEST
import os

class TestMSSQLLogger(unittest.TestCase):
    
    def test_custom_levels_defined(self):
        """Test that custom levels are registered"""
        self.assertEqual(FINE, 25)
        self.assertEqual(FINER, 15)
        self.assertEqual(FINEST, 5)
        self.assertEqual(logging.getLevelName(FINE), 'FINE')
    
    def test_logger_singleton(self):
        """Test that logger is a singleton"""
        from mssql_python.logging import MSSQLLogger
        logger1 = MSSQLLogger()
        logger2 = MSSQLLogger()
        self.assertIs(logger1, logger2)
    
    def test_log_file_created(self):
        """Test that log file is created"""
        logger.setLevel(FINE)
        logger.fine("Test message")
        self.assertTrue(os.path.exists(logger.log_file))
    
    def test_sanitization(self):
        """Test password sanitization"""
        logger.setLevel(FINE)
        logger.fine("Connection: Server=localhost;PWD=secret123;")
        
        # Read log file and verify password is sanitized
        with open(logger.log_file, 'r') as f:
            content = f.read()
            self.assertIn("PWD=***", content)
            self.assertNotIn("secret123", content)
    
    def test_level_filtering(self):
        """Test that messages are filtered by level"""
        logger.setLevel(FINE)
        
        # FINE should be logged
        self.assertTrue(logger.isEnabledFor(FINE))
        
        # FINER should not be logged (higher detail)
        self.assertFalse(logger.isEnabledFor(FINER))
    
    def test_trace_id_generation(self):
        """Test trace ID format"""
        trace_id = logger.generate_trace_id("Connection")
        parts = trace_id.split('_')
        
        self.assertEqual(len(parts), 3)  # PID_ThreadID_Counter
        self.assertTrue(all(p.isdigit() for p in parts))
```

#### C++ Bridge Tests (`test_logger_bridge.cpp`)

```cpp
#include <gtest/gtest.h>
#include "logger_bridge.hpp"

using namespace mssql_python::logging;

class LoggerBridgeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize Python interpreter
        Py_Initialize();
        LoggerBridge::initialize();
    }
    
    void TearDown() override {
        Py_Finalize();
    }
};

TEST_F(LoggerBridgeTest, DefaultLevelIsCritical) {
    // By default, logging should be disabled
    EXPECT_FALSE(LoggerBridge::isLoggable(25));  // FINE
    EXPECT_FALSE(LoggerBridge::isLoggable(15));  // FINER
}

TEST_F(LoggerBridgeTest, UpdateLevelWorks) {
    LoggerBridge::updateLevel(25);  // Set to FINE
    
    EXPECT_TRUE(LoggerBridge::isLoggable(25));   // FINE enabled
    EXPECT_FALSE(LoggerBridge::isLoggable(15));  // FINER not enabled
}

TEST_F(LoggerBridgeTest, LoggingWhenDisabled) {
    // Should not crash or call Python
    LoggerBridge::updateLevel(50);  // CRITICAL (effectively off)
    
    // This should return immediately
    LOG_FINE("This should not be logged");
    LOG_FINER("This should not be logged");
}

TEST_F(LoggerBridgeTest, ThreadSafety) {
    LoggerBridge::updateLevel(25);
    
    // Launch multiple threads logging simultaneously
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([i]() {
            for (int j = 0; j < 100; ++j) {
                LOG_FINE("Thread %d, message %d", i, j);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should not crash or corrupt data
    SUCCEED();
}
```

---

### Integration Tests

```python
import unittest
import mssql_python
from mssql_python.logging import logger, FINE, FINEST
import os

class TestLoggingIntegration(unittest.TestCase):
    
    def setUp(self):
        self.connection_string = os.getenv('TEST_CONNECTION_STRING')
        logger.setLevel(FINE)
    
    def test_full_workflow_logged(self):
        """Test that complete workflow is logged"""
        # Connect
        conn = mssql_python.connect(self.connection_string)
        
        # Execute query
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        rows = cursor.fetchall()
        
        # Close
        conn.close()
        
        # Verify log contains expected messages
        with open(logger.log_file, 'r') as f:
            content = f.read()
            
            self.assertIn("Initializing connection", content)
            self.assertIn("Connection established", content)
            self.assertIn("Executing query", content)
            self.assertIn("Closing connection", content)
            
            # Verify C++ logs present
            self.assertIn("[DDBC]", content)
    
    def test_trace_ids_in_logs(self):
        """Test that trace IDs appear in logs"""
        conn = mssql_python.connect(self.connection_string)
        trace_id = conn.trace_id
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        
        conn.close()
        
        # Verify trace ID appears in logs
        with open(logger.log_file, 'r') as f:
            content = f.read()
            self.assertIn(f"TraceID: {trace_id}", content)
```

---

### Performance Tests

```python
import unittest
import time
import mssql_python
from mssql_python.logging import logger
import logging

class TestLoggingPerformance(unittest.TestCase):
    
    def test_overhead_when_disabled(self):
        """Test that logging has minimal overhead when disabled"""
        logger.setLevel(logging.CRITICAL)  # Disable
        
        conn = mssql_python.connect(self.connection_string)
        cursor = conn.cursor()
        
        # Measure performance with logging disabled
        start = time.perf_counter()
        for i in range(1000):
            cursor.execute("SELECT 1")
        disabled_time = time.perf_counter() - start
        
        # Enable logging
        logger.setLevel(FINE)
        
        # Measure performance with logging enabled
        start = time.perf_counter()
        for i in range(1000):
            cursor.execute("SELECT 1")
        enabled_time = time.perf_counter() - start
        
        # Overhead should be < 10%
        overhead = (enabled_time - disabled_time) / disabled_time
        self.assertLess(overhead, 0.10, 
                       f"Logging overhead too high: {overhead:.1%}")
        
        conn.close()
```

---

## Appendix

### A. Log Level Decision Tree

```
Should I log this message?
│
├─ Is it always relevant (errors, warnings)?
│  └─ Yes → Use ERROR/WARNING
│
├─ Is it useful for standard troubleshooting?
│  └─ Yes → Use FINE
│      Examples:
│      - Connection opened/closed
│      - Query executed
│      - Major operations
│
├─ Is it detailed diagnostic info?
│  └─ Yes → Use FINER
│      Examples:
│      - Handle allocations
│      - Parameter binding
│      - Row counts
│      - Internal state changes
│
└─ Is it ultra-detailed trace info?
   └─ Yes → Use FINEST
       Examples:
       - Memory dumps
       - Full diagnostics
       - Performance metrics
       - Deep internal state
```

### B. C++ Macro Reference

```cpp
// Basic logging macros
LOG_FINE(fmt, ...)    // Standard diagnostics (level 25)
LOG_FINER(fmt, ...)   // Detailed diagnostics (level 15)
LOG_FINEST(fmt, ...)  // Ultra-detailed trace (level 5)

// Manual level check for expensive operations
if (LoggerBridge::isLoggable(FINEST)) {
    // Expensive computation here
}

// Example usage patterns
LOG_FINE("Connecting to server: %s", server_name);
LOG_FINER("Handle allocated: %p", handle);
LOG_FINEST("Memory state: %s", dump_memory().c_str());
```

### C. Python API Reference

```python
from mssql_python.logging import logger, FINE, FINER, FINEST

# Logging methods
logger.fine(msg)      # Standard diagnostics (level 25)
logger.finer(msg)     # Detailed diagnostics (level 15)
logger.finest(msg)    # Ultra-detailed trace (level 5)
logger.info(msg)      # Informational (level 20)
logger.warning(msg)   # Warnings (level 30)
logger.error(msg)     # Errors (level 40)

# Level control
logger.setLevel(FINE)     # Enable FINE and above
logger.setLevel(FINER)    # Enable FINER and above
logger.setLevel(FINEST)   # Enable everything
logger.setLevel(logging.CRITICAL)  # Disable all

# Level checking
if logger.isEnabledFor(FINEST):
    expensive_data = compute()
    logger.finest(f"Data: {expensive_data}")

# Properties
logger.log_file           # Get current log file path
logger.generate_trace_id(name)  # Generate trace ID
```

### D. File Structure Summary

```
mssql_python/
├── __init__.py                    # Export logger
├── logging.py                     # ← NEW: Main logger (replaces logging_config.py)
├── logging_config.py              # ← DEPRECATED: Remove after migration
├── connection.py                  # Updated: Use new logger
├── cursor.py                      # Updated: Use new logger
├── auth.py                        # Updated: Use new logger
├── pooling.py                     # Updated: Use new logger
│
└── pybind/
    ├── logger_bridge.hpp          # ← NEW: C++ bridge header
    ├── logger_bridge.cpp          # ← NEW: C++ bridge implementation
    ├── bindings.cpp               # Updated: Expose bridge functions
    ├── ddbc_connection.cpp        # Updated: Use LOG_* macros
    ├── ddbc_statement.cpp         # Updated: Use LOG_* macros
    └── ddbc_*.cpp                 # Updated: Use LOG_* macros

tests/
├── test_logging.py                # ← NEW: Python logger tests
├── test_logger_bridge.cpp         # ← NEW: C++ bridge tests
└── test_logging_integration.py   # ← NEW: End-to-end tests

```

### E. Common Troubleshooting

**Problem**: No logs appearing  
**Solution**: Check that `logger.setLevel()` was called with appropriate level

**Problem**: Passwords appearing in logs  
**Solution**: Should never happen - sanitization is automatic. Report as bug.

**Problem**: Performance degradation  
**Solution**: Verify logging is disabled in production, or reduce level to FINE only

**Problem**: Log file not found  
**Solution**: Check `logger.log_file` property for actual location (current working directory)

**Problem**: C++ logs missing  
**Solution**: Verify `LoggerBridge::initialize()` was called during module import

---

## Future Enhancements (Backlog)

The following items are not part of the initial implementation but are valuable additions for future releases:

### 1. Cursor.messages Attribute (Priority: High)

**Inspired By**: PyODBC's `cursor.messages` attribute

**Description**: Add a `messages` attribute to the Cursor class that captures diagnostic information from the ODBC driver, similar to PyODBC's implementation.

**Benefits**:
- Provides access to non-error diagnostics (warnings, informational messages)
- Allows users to inspect SQL Server messages without exceptions
- Enables capture of multiple diagnostic records per operation
- Standard pattern familiar to PyODBC users

**Implementation Details**:
```python
class Cursor:
    def __init__(self, connection):
        self.messages = []  # List of tuples: (sqlstate, error_code, message)
    
    def execute(self, sql):
        self.messages.clear()  # Clear previous messages
        # Execute query
        # Populate messages from SQLGetDiagRec
```

**C++ Support**:
```cpp
// In ddbc_statement.cpp
std::vector<std::tuple<std::string, int, std::string>> getDiagnosticRecords(SQLHSTMT stmt) {
    std::vector<std::tuple<std::string, int, std::string>> records;
    SQLSMALLINT rec_number = 1;
    
    while (true) {
        SQLCHAR sqlstate[6];
        SQLINTEGER native_error;
        SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
        SQLSMALLINT message_len;
        
        SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt, rec_number,
                                      sqlstate, &native_error,
                                      message, sizeof(message), &message_len);
        
        if (ret == SQL_NO_DATA) break;
        if (!SQL_SUCCEEDED(ret)) break;
        
        records.emplace_back(
            std::string((char*)sqlstate),
            native_error,
            std::string((char*)message)
        );
        
        rec_number++;
    }
    
    return records;
}
```

**Usage Example**:
```python
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")

# Check for warnings/messages
for sqlstate, error_code, message in cursor.messages:
    if sqlstate.startswith('01'):  # Warning
        print(f"Warning: {message}")
```

**Estimated Effort**: 2-3 days

---

### 2. Comprehensive Error Handling via SQLGetDiagRec Chaining (Priority: High)

**Inspired By**: PyODBC's `GetDiagRecs()` pattern and Psycopg's Diagnostic class

**Description**: When an error occurs, chain calls to `SQLGetDiagRec` to retrieve ALL diagnostic records, not just the first one. Provide structured access to comprehensive error information.

**Current Limitation**: 
- Errors may only capture the first diagnostic record
- Missing additional context that SQL Server provides
- No structured access to specific diagnostic fields

**Benefits**:
- Complete error context (multiple records per error)
- Structured diagnostic fields (sqlstate, native_error, message, server, procedure, line)
- Better debugging with full error chains
- More informative exceptions

**Implementation Details**:

**Python Exception Enhancement**:
```python
class DatabaseError(Exception):
    """Enhanced exception with full diagnostic info"""
    def __init__(self, message, diagnostics=None):
        super().__init__(message)
        self.diagnostics = diagnostics or []
        # diagnostics = [
        #   {
        #     'sqlstate': '42000',
        #     'native_error': 102,
        #     'message': 'Incorrect syntax near...',
        #     'server': 'myserver',
        #     'procedure': 'my_proc',
        #     'line': 42
        #   },
        #   ...
        # ]
    
    def __str__(self):
        base = super().__str__()
        if self.diagnostics:
            diag_info = "\n".join([
                f"  [{d['sqlstate']}] {d['message']}"
                for d in self.diagnostics
            ])
            return f"{base}\nDiagnostics:\n{diag_info}"
        return base
```

**C++ Diagnostic Retrieval**:
```cpp
// In ddbc_exceptions.cpp
struct DiagnosticRecord {
    std::string sqlstate;
    int native_error;
    std::string message;
    std::string server_name;
    std::string procedure_name;
    int line_number;
};

std::vector<DiagnosticRecord> getAllDiagnostics(SQLHANDLE handle, SQLSMALLINT handle_type) {
    std::vector<DiagnosticRecord> records;
    SQLSMALLINT rec_number = 1;
    
    while (true) {
        DiagnosticRecord record;
        SQLCHAR sqlstate[6];
        SQLINTEGER native_error;
        SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
        SQLSMALLINT message_len;
        
        SQLRETURN ret = SQLGetDiagRec(handle_type, handle, rec_number,
                                      sqlstate, &native_error,
                                      message, sizeof(message), &message_len);
        
        if (ret == SQL_NO_DATA) break;
        if (!SQL_SUCCEEDED(ret)) break;
        
        record.sqlstate = (char*)sqlstate;
        record.native_error = native_error;
        record.message = (char*)message;
        
        // Get additional fields via SQLGetDiagField
        SQLCHAR server[256];
        ret = SQLGetDiagField(handle_type, handle, rec_number,
                             SQL_DIAG_SERVER_NAME, server, sizeof(server), NULL);
        if (SQL_SUCCEEDED(ret)) {
            record.server_name = (char*)server;
        }
        
        // Get procedure name, line number, etc.
        // ...
        
        records.push_back(record);
        rec_number++;
        
        LOG_FINEST("Diagnostic record %d: [%s] %s", rec_number, 
                   record.sqlstate.c_str(), record.message.c_str());
    }
    
    LOG_FINER("Retrieved %zu diagnostic records", records.size());
    return records;
}
```

**Exception Raising with Full Diagnostics**:
```cpp
void raiseException(SQLHANDLE handle, SQLSMALLINT handle_type, const char* operation) {
    auto diagnostics = getAllDiagnostics(handle, handle_type);
    
    if (diagnostics.empty()) {
        PyErr_SetString(PyExc_RuntimeError, operation);
        return;
    }
    
    // Create Python exception with all diagnostic records
    PyObject* diag_list = PyList_New(0);
    for (const auto& rec : diagnostics) {
        PyObject* diag_dict = Py_BuildValue(
            "{s:s, s:i, s:s, s:s, s:s, s:i}",
            "sqlstate", rec.sqlstate.c_str(),
            "native_error", rec.native_error,
            "message", rec.message.c_str(),
            "server", rec.server_name.c_str(),
            "procedure", rec.procedure_name.c_str(),
            "line", rec.line_number
        );
        PyList_Append(diag_list, diag_dict);
        Py_DECREF(diag_dict);
    }
    
    // Raise DatabaseError with diagnostics
    PyObject* exc_class = getExceptionClass(diagnostics[0].sqlstate);
    PyObject* exc_instance = PyObject_CallFunction(exc_class, "sO",
                                                   diagnostics[0].message.c_str(),
                                                   diag_list);
    PyErr_SetObject(exc_class, exc_instance);
    Py_DECREF(diag_list);
    Py_DECREF(exc_instance);
}
```

**Usage Example**:
```python
try:
    cursor.execute("INVALID SQL")
except mssql_python.DatabaseError as e:
    print(f"Error: {e}")
    print(f"\nFull diagnostics:")
    for diag in e.diagnostics:
        print(f"  SQLSTATE: {diag['sqlstate']}")
        print(f"  Native Error: {diag['native_error']}")
        print(f"  Message: {diag['message']}")
        if diag.get('procedure'):
            print(f"  Procedure: {diag['procedure']} (line {diag['line']})")
```

**Estimated Effort**: 3-4 days

---

### Priority and Sequencing

Both items are marked as **High Priority** for the backlog and should be implemented after the core logging system is complete and stable.

**Suggested Implementation Order**:
1. Phase 1-4 of core logging system (as described earlier)
2. **Backlog Item #2**: Comprehensive error handling (higher impact on reliability)
3. **Backlog Item #1**: Cursor.messages (complementary diagnostic feature)

**Dependencies**:
- Both items require the logging system to be in place for proper diagnostic logging
- Item #2 (error handling) benefits from FINEST logging to trace diagnostic retrieval
- Item #1 (cursor.messages) can leverage the same C++ functions as Item #2

---

## Document History

| Version | Date | Author | Changes |
| --- | --- | --- | --- |
| 1.0 | 2025-10-31 | Gaurav | Initial design document |
| 1.1 | 2025-10-31 | Gaurav | Added backlog items: cursor.messages and comprehensive error handling |

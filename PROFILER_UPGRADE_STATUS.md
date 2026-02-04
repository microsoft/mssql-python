# Profiler Upgrade Plan

## Status: In Progress

### ✅ Phase 1: Core Infrastructure (DONE)
- [x] Copy `performance_counter.hpp` to new branch
- [x] Add `#include "performance_counter.hpp"` to ddbc_bindings.cpp
- [x] Add profiling submodule to PYBIND11_MODULE
- [x] Copy `run_profiler.py` and `profiling_results.md`

### 🔄 Phase 2: Add PERF_TIMER Calls (43 locations)

**Critical Path Functions (High Value):**
1. FetchAll_wrap - Main fetch loop
2. FetchBatchData - Batch processing  
3. FetchBatchData::construct_rows - Python object creation
4. FetchBatchData::SQLFetchScroll_call - ODBC driver call
5. Connection::connect - Connection establishment

**Data Retrieval:**
6. FetchOne_wrap
7. SQLFetch_wrap
8. SQLGetData_wrap
9. FetchLobColumnData

**Metadata:**
10. SQLDescribeCol_wrap
11. SQLNumResultCols_wrap
12. SQLBindColums

**Connection/Driver:**
13. DriverLoader::loadDriver
14. Connection::Connection
15. Connection::allocateDbcHandle
16. Connection::setAutocommit

**Query Execution:**
17. SQLExecDirect_wrap
18. SQLExecDirect_wrap::configure_cursor
19. SQLExecDirect_wrap::SQLExecDirect_call

**Cleanup:**
20. SqlHandle::free
21. SQLFreeHandle_wrap

**Diagnostics:**
22. SQLCheckError_Wrap
23. SQLGetAllDiagRecords

**Result Processing:**
24. SQLMoreResults_wrap
25. SQLRowCount_wrap

### 📝 Phase 3: Enhanced Profiling (NEW - Your task #2)

**Add new detailed timers inside construct_rows:**
- Per-column type processing (INT, BIGINT, VARCHAR, etc.)
- Buffer read time vs Python object creation time
- String conversion overhead (Windows vs Linux)
- Row append time

**Add connection.cpp timers:**
- Transaction begin/commit/rollback
- Connection pool operations
- Attribute setting

**Add new profiling features:**
- Memory allocation tracking
- Cache hit/miss rates
- Batch size effectiveness metrics

### 🧪 Phase 4: New Benchmarks (Your task #3)

**Expand benchmark suite:**
1. **Transaction performance** - BEGIN/COMMIT overhead
2. **Parameter binding** - Prepared statements vs direct exec
3. **Concurrent connections** - Connection pool performance  
4. **LOB handling** - Large text/binary data
5. **Result set variations** - Wide vs tall tables
6. **Network latency simulation** - Local vs remote SQL Server
7. **Memory usage** - Peak memory, leak detection
8. **Different data types** - Date/time, decimals, JSON, XML

### 🚀 Phase 5: Testing & Documentation

**Test on all platforms:**
- [ ] Windows (your results already in profiling_results.md)
- [ ] Linux Ubuntu
- [ ] macOS

**Update documentation:**
- [ ] Profiling guide
- [ ] Benchmarking methodology
- [ ] Performance comparison with pyodbc
- [ ] Platform-specific optimizations guide

---

## Current File Status

- `performance_counter.hpp` ✅ Added
- `ddbc_bindings.cpp` ⚠️ Partial (includes + submodule, need PERF_TIMER calls)
- `connection.cpp` ❌ Not started
- `run_profiler.py` ✅ Added
- `profiling_results.md` ✅ Added

---

## Next Steps (Immediate)

1. Add remaining 40+ PERF_TIMER calls to ddbc_bindings.cpp
2. Add PERF_TIMER calls to connection/connection.cpp
3. Enable profiling by default (currently disabled via macro)
4. Build and test on local machine
5. Run profiler and compare with old results

---

## Tools for Automation

Created helper script to add PERF_TIMER calls systematically (see below).


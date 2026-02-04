# Profiler Integration - Complete TODO List

## Summary

This document lists all 43 PERF_TIMER locations that need to be added from the old profiler branch to the new main branch.

## How to Use This List

For each function below, add `PERF_TIMER("function_name");` as the **first line** inside the function body.

---

## 1. Driver & Initialization (2 locations)

### DriverLoader::loadDriver
**File:** `ddbc_bindings.cpp`
**Line:** ~1078 (old), find `void loadDriver()`
```cpp
void loadDriver() {
    PERF_TIMER("DriverLoader::loadDriver");
    // ... rest of function
}
```

### SqlHandle::free  
**File:** `ddbc_bindings.cpp`
**Line:** ~1111 (old), find `void SqlHandle::free()`
```cpp
void SqlHandle::free() {
    PERF_TIMER("SqlHandle::free");
    // ... rest of function
}
```

---

## 2. Error Handling & Diagnostics (2 locations)

### SQLCheckError_Wrap
**File:** `ddbc_bindings.cpp`  
**Line:** ~1376 (old)
```cpp
PERF_TIMER("SQLCheckError_Wrap");
```

### SQLGetAllDiagRecords
**File:** `ddbc_bindings.cpp`
**Line:** ~1416 (old)  
```cpp
PERF_TIMER("SQLGetAllDiagRecords");
```

---

## 3. Query Execution (3 locations)

### SQLExecDirect_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~1483 (old)
```cpp
SQLRETURN SQLExecDirect_wrap(...) {
    PERF_TIMER("SQLExecDirect_wrap");
```

### SQLExecDirect_wrap::configure_cursor
**File:** `ddbc_bindings.cpp`
**Line:** ~1493 (old), inside SQLExecDirect_wrap
```cpp
{
    PERF_TIMER("SQLExecDirect_wrap::configure_cursor");
    // cursor configuration code
}
```

### SQLExecDirect_wrap::SQLExecDirect_call
**File:** `ddbc_bindings.cpp`  
**Line:** ~1517 (old), inside SQLExecDirect_wrap
```cpp
{
    PERF_TIMER("SQLExecDirect_wrap::SQLExecDirect_call");
    ret = SQLExecDirect(...);
}
```

---

## 4. Column Metadata (3 locations)

### SQLNumResultCols_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~2307 (old)
```cpp
PERF_TIMER("SQLNumResultCols_wrap");
```

### SQLDescribeCol_wrap  
**File:** `ddbc_bindings.cpp`
**Line:** ~2322 (old)
```cpp
PERF_TIMER("SQLDescribeCol_wrap");
```

### SQLDescribeCol_wrap::per_column
**File:** `ddbc_bindings.cpp`
**Line:** ~2338 (old), inside loop
```cpp
for (SQLSMALLINT i = 1; i <= numCols; i++) {
    PERF_TIMER("SQLDescribeCol_wrap::per_column");
    // ... column description
}
```

---

## 5. Data Fetching - Basic (3 locations)

### SQLFetch_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~2418 (old)
```cpp
PERF_TIMER("SQLFetch_wrap");
```

### FetchLobColumnData
**File:** `ddbc_bindings.cpp`
**Line:** ~2434 (old)
```cpp
PERF_TIMER("FetchLobColumnData");
```

### SQLGetData_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~2543 (old)
```cpp
PERF_TIMER("SQLGetData_wrap");
```

---

## 6. Binding (1 location)

### SQLBindColums
**File:** `ddbc_bindings.cpp`
**Line:** ~3055 (old)
```cpp
PERF_TIMER("SQLBindColums");
```

---

## 7. Batch Fetching - CRITICAL SECTION (16 locations)

### FetchBatchData
**File:** `ddbc_bindings.cpp`
**Line:** ~3385 (old)
```cpp
SQLRETURN FetchBatchData(...) {
    PERF_TIMER("FetchBatchData");
```

### FetchBatchData::SQLFetchScroll_call
**File:** `ddbc_bindings.cpp`
**Line:** ~3391 (old)
```cpp
{
    PERF_TIMER("FetchBatchData::SQLFetchScroll_call");
    ret = SQLFetchScroll(...);
}
```

### FetchBatchData::cache_column_metadata
**File:** `ddbc_bindings.cpp`
**Line:** ~3407 (old)
```cpp
{
    PERF_TIMER("FetchBatchData::cache_column_metadata");
    // metadata caching
}
```

### FetchBatchData::batch_allocate_rows
**File:** `ddbc_bindings.cpp`
**Line:** ~3474 (old)
```cpp
{
    PERF_TIMER("FetchBatchData::batch_allocate_rows");
    rows.reserve(...);
}
```

### FetchBatchData::construct_rows
**File:** `ddbc_bindings.cpp`
**Line:** ~3484 (old) - **THIS IS THE BOTTLENECK**
```cpp
{
    PERF_TIMER("FetchBatchData::construct_rows");
    // Main row construction loop
}
```

### construct_rows::per_row_total
**File:** `ddbc_bindings.cpp`
**Line:** ~3487 (old), outer loop
```cpp
for (SQLULEN row = 0; row < actualRowsFetched; ++row) {
    PERF_TIMER("construct_rows::per_row_total");
```

### construct_rows::all_columns_processing  
**File:** `ddbc_bindings.cpp`
**Line:** ~3493 (old), inner column loop
```cpp
for (SQLUSMALLINT col = 1; col <= numCols; ++col) {
    PERF_TIMER("construct_rows::all_columns_processing");
```

**The following are INSIDE the column processing switch statement:**

### construct_rows::int_buffer_read
```cpp
case SQL_INTEGER:
    PERF_TIMER("construct_rows::int_buffer_read");
    // read from buffer
```

### construct_rows::int_c_api_assign
```cpp
    PERF_TIMER("construct_rows::int_c_api_assign");
    pyRow[col-1] = py::int_(...);
```

### construct_rows::bigint_buffer_read
```cpp
case SQL_BIGINT:
    PERF_TIMER("construct_rows::bigint_buffer_read");
```

### construct_rows::bigint_c_api_assign
```cpp
    PERF_TIMER("construct_rows::bigint_c_api_assign");
    pyRow[col-1] = py::int_(...);
```

### construct_rows::smallint_buffer_read
```cpp
case SQL_SMALLINT:
    PERF_TIMER("construct_rows::smallint_buffer_read");
```

### construct_rows::smallint_c_api_assign
```cpp
    PERF_TIMER("construct_rows::smallint_c_api_assign");
    pyRow[col-1] = py::int_(...);
```

### construct_rows::wstring_conversion (Linux only)
```cpp
#ifdef __linux__
    PERF_TIMER("construct_rows::wstring_conversion");
    // PyUnicode_DecodeUTF16 call
#endif
```

### construct_rows::pylist_creation
```cpp
{
    PERF_TIMER("construct_rows::pylist_creation");
    py::list pyRow(numCols);
}
```

### construct_rows::rows_append
```cpp
{
    PERF_TIMER("construct_rows::rows_append");
    rows.append(pyRow);
}
```

---

## 8. FetchAll Wrapper (1 location)

### FetchAll_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~3810 (old)
```cpp
SQLRETURN FetchAll_wrap(...) {
    PERF_TIMER("FetchAll_wrap");
```

---

## 9. Result Set Navigation (2 locations)

### SQLMoreResults_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~3951 (old)
```cpp
PERF_TIMER("SQLMoreResults_wrap");
```

### SQLRowCount_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~3980 (old)
```cpp
PERF_TIMER("SQLRowCount_wrap");
```

---

## 10. Cleanup (1 location)

### SQLFreeHandle_wrap
**File:** `ddbc_bindings.cpp`
**Line:** ~3963 (old)
```cpp
PERF_TIMER("SQLFreeHandle_wrap");
```

---

## 11. Connection Functions (connection.cpp) - ~10 locations

**Note:** These need to be added to `connection/connection.cpp`

### Connection::Connection
```cpp
Connection::Connection(...) {
    PERF_TIMER("Connection::Connection");
```

### Connection::allocateDbcHandle
```cpp
PERF_TIMER("Connection::allocateDbcHandle");
```

### Connection::connect
```cpp
PERF_TIMER("Connection::connect");
```

### Connection::connect::SQLDriverConnect_call (inside connect)
```cpp
{
    PERF_TIMER("Connection::connect::SQLDriverConnect_call");
    ret = SQLDriverConnect(...);
}
```

### Connection::setAutocommit
```cpp
PERF_TIMER("Connection::setAutocommit");
```

### Connection::disconnect
```cpp
PERF_TIMER("Connection::disconnect");
```

### Connection::commit
```cpp
PERF_TIMER("Connection::commit");
```

### Connection::rollback
```cpp
PERF_TIMER("Connection::rollback");
```

### Connection::allocStatementHandle
```cpp
PERF_TIMER("Connection::allocStatementHandle");
```

---

## 12. Additional Timers from Old Profiling

Check `profiling_results.md` for other functions that appear in stats:
- SQLWCHARToWString
- WStringToSQLWCHAR  
- construct_rows::row_store

---

## Total Count: 43+ PERF_TIMER locations

**Priority Order for Manual Addition:**
1. ✅ **DONE:** Infrastructure (includes, submodule)
2. **HIGH:** FetchAll_wrap, FetchBatchData, construct_rows (lines ~3385-3600)
3. **MEDIUM:** SQLExecDirect_wrap, Connection functions
4. **LOW:** Diagnostics, metadata functions

---

## Automation Script

To add these automatically, run:
```bash
# TODO: Create Python script to parse function signatures and insert PERF_TIMER
```

---

## Verification

After adding all timers:
1. Build: `cd mssql_python/pybind && ./build.sh`
2. Enable profiling in `performance_counter.hpp`:
   - Comment line: `#define PERF_TIMER(name) do {} while(0)`
   - Uncomment line: `#define PERF_TIMER(name) mssql_profiling::ScopedTimer ...`
3. Run: `python run_profiler.py`
4. Compare with `profiling_results.md`


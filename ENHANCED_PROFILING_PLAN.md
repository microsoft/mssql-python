# Enhanced Profiler - New Features Plan

## Task #2: Add New Profiling Points

### 1. **Granular Type Processing Timers**

Add per-data-type processing timers in construct_rows switch statement:

```cpp
case SQL_WVARCHAR:
case SQL_WCHAR:
case SQL_WLONGVARCHAR:
    PERF_TIMER("construct_rows::string_type_total");
    {
        PERF_TIMER("construct_rows::string_buffer_read");
        // read from indicatorArray, buffers
    }
    {
        PERF_TIMER("construct_rows::string_decode");
        // PyUnicode_Decode...
    }
    {
        PERF_TIMER("construct_rows::string_assign");
        // pyRow[col-1] = pyStr
    }
    break;

case SQL_TYPE_DATE:
case SQL_TYPE_TIME:
case SQL_TYPE_TIMESTAMP:
    PERF_TIMER("construct_rows::datetime_type_total");
    {
        PERF_TIMER("construct_rows::datetime_buffer_read");
        // read SQL_TIMESTAMP_STRUCT
    }
    {
        PERF_TIMER("construct_rows::datetime_python_create");
        // py::module::import("datetime").attr("datetime")(...)
    }
    break;

case SQL_DECIMAL:
case SQL_NUMERIC:
    PERF_TIMER("construct_rows::decimal_type_total");
    {
        PERF_TIMER("construct_rows::decimal_buffer_read");
        // read SQL_NUMERIC_STRUCT
    }
    {
        PERF_TIMER("construct_rows::decimal_string_convert");
        // Convert to string representation
    }
    {
        PERF_TIMER("construct_rows::decimal_python_create");
        // py::module::import("decimal").attr("Decimal")(str)
    }
    break;

case SQL_REAL:
case SQL_FLOAT:
case SQL_DOUBLE:
    PERF_TIMER("construct_rows::float_type_total");
    {
        PERF_TIMER("construct_rows::float_buffer_read");
        // read double
    }
    {
        PERF_TIMER("construct_rows::float_assign");
        // pyRow[col-1] = py::float_(...)
    }
    break;
```

### 2. **Memory Operations Tracking**

```cpp
// Add to FetchBatchData before buffer allocation
{
    PERF_TIMER("FetchBatchData::memory_allocation");
    // malloc/new for buffers
    // indicatorArrays allocation
}

// After fetch complete
{
    PERF_TIMER("FetchBatchData::memory_deallocation");
    // free/delete buffers
}
```

### 3. **Connection Pool Profiling**

**File:** `connection/connection_pool.cpp`

```cpp
Connection* getConnection() {
    PERF_TIMER("ConnectionPool::getConnection");
    {
        PERF_TIMER("ConnectionPool::lock_acquire");
        std::lock_guard<std::mutex> lock(mutex_);
    }
    {
        PERF_TIMER("ConnectionPool::find_idle_connection");
        // search for available connection
    }
    {
        PERF_TIMER("ConnectionPool::create_new_connection");
        // if no idle, create new
    }
}

void releaseConnection(Connection* conn) {
    PERF_TIMER("ConnectionPool::releaseConnection");
    {
        PERF_TIMER("ConnectionPool::validate_connection");
        // check if connection still valid
    }
    {
        PERF_TIMER("ConnectionPool::return_to_pool");
        // add back to available pool
    }
}
```

### 4. **Transaction Profiling**

**File:** `connection/connection.cpp`

```cpp
void Connection::begin() {
    PERF_TIMER("Connection::begin_transaction");
    {
        PERF_TIMER("Connection::begin::odbc_call");
        // ODBC transaction begin
    }
}

void Connection::commit() {
    PERF_TIMER("Connection::commit_transaction");
    {
        PERF_TIMER("Connection::commit::odbc_call");
        ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
    }
}

void Connection::rollback() {
    PERF_TIMER("Connection::rollback_transaction");
    {
        PERF_TIMER("Connection::rollback::odbc_call");
        ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
    }
}
```

### 5. **Parameter Binding Profiling**

```cpp
SQLRETURN SQLBindParameter_wrap(...) {
    PERF_TIMER("SQLBindParameter_wrap");
    {
        PERF_TIMER("SQLBindParameter::type_inference");
        // determine SQL type from Python type
    }
    {
        PERF_TIMER("SQLBindParameter::buffer_prepare");
        // allocate and fill buffer
    }
    {
        PERF_TIMER("SQLBindParameter::odbc_bind_call");
        ret = SQLBindParameter(...);
    }
}
```

### 6. **Batch Size Effectiveness Metrics**

Add custom metrics (not just timers):

```cpp
// In performance_counter.hpp, add:
struct BatchMetrics {
    size_t total_batches = 0;
    size_t total_rows = 0;
    size_t rows_per_batch_histogram[10] = {0}; // 0-100, 101-500, 501-1000, etc.
};

// In FetchBatchData:
void record_batch_metrics(size_t rows_fetched) {
    auto& metrics = PerformanceCounter::instance().get_batch_metrics();
    metrics.total_batches++;
    metrics.total_rows += rows_fetched;
    
    // Histogram bucket
    if (rows_fetched <= 100) metrics.rows_per_batch_histogram[0]++;
    else if (rows_fetched <= 500) metrics.rows_per_batch_histogram[1]++;
    // ... etc
}
```

### 7. **Network I/O Tracking**

```cpp
// Wrap SQLFetch/SQLFetchScroll to track network calls
{
    PERF_TIMER("ODBC::network_io");
    ret = SQLFetchScroll(StatementHandle, SQL_FETCH_NEXT, 0);
}
```

### 8. **Platform-Specific String Conversion**

```cpp
#ifdef _WIN32
    PERF_TIMER("construct_rows::wstring_native_copy");
    // Windows: direct copy, no conversion
#elif defined(__linux__)
    PERF_TIMER("construct_rows::wstring_utf32_to_utf16");
    // Linux: wchar_t is UTF-32, need conversion
    {
        PERF_TIMER("construct_rows::wstring_utf32_decode");
        // PyUnicode_DecodeUTF32
    }
#elif defined(__APPLE__)
    PERF_TIMER("construct_rows::wstring_macos_utf32");
    // macOS: wchar_t is UTF-32 like Linux
#endif
```

---

## Task #3: New Benchmarks

### Benchmark Suite Expansion

**File:** `benchmarks/comprehensive_benchmarks.py`

```python
import mssql_python
import pyodbc
import time
import statistics
from contextlib import contextmanager

ITERATIONS = 5

@contextmanager
def timer(name):
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"{name}: {elapsed:.4f}s")

class BenchmarkSuite:
    def __init__(self, conn_str):
        self.conn_str = conn_str
    
    # 1. Transaction Performance
    def benchmark_transactions(self):
        """Test BEGIN/COMMIT overhead with varying transaction sizes"""
        for driver in ["mssql-python", "pyodbc"]:
            times = []
            for _ in range(ITERATIONS):
                conn = self.connect(driver)
                cursor = conn.cursor()
                
                start = time.perf_counter()
                
                # 100 small transactions
                for i in range(100):
                    cursor.execute("BEGIN TRANSACTION")
                    cursor.execute("UPDATE test_table SET value = value + 1 WHERE id = 1")
                    cursor.execute("COMMIT")
                
                elapsed = time.perf_counter() - start
                times.append(elapsed)
                
                conn.close()
            
            print(f"{driver} - 100 transactions: avg={statistics.mean(times):.4f}s")
    
    # 2. Prepared Statement vs Direct Execution
    def benchmark_prepared_statements(self):
        """Compare executemany with parameters vs individual executes"""
        params = [(i, f"name_{i}") for i in range(1000)]
        
        for driver in ["mssql-python", "pyodbc"]:
            conn = self.connect(driver)
            cursor = conn.cursor()
            
            # Direct execution (1000 separate queries)
            with timer(f"{driver} - Direct execution (1000 INSERTs)"):
                for id, name in params:
                    cursor.execute(f"INSERT INTO test_table VALUES ({id}, '{name}')")
            
            cursor.execute("TRUNCATE TABLE test_table")
            
            # Prepared statement (executemany)
            with timer(f"{driver} - Prepared statement (executemany 1000)"):
                cursor.executemany("INSERT INTO test_table VALUES (?, ?)", params)
            
            conn.close()
    
    # 3. Connection Pool Performance
    def benchmark_connection_pool(self):
        """Test concurrent connection acquisition"""
        import concurrent.futures
        
        def get_and_query(driver):
            conn = self.connect(driver)
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            cursor.fetchone()
            conn.close()
        
        for driver in ["mssql-python", "pyodbc"]:
            with timer(f"{driver} - 100 concurrent connections"):
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    list(executor.map(lambda _: get_and_query(driver), range(100)))
    
    # 4. LOB Handling (Large Binary/Text)
    def benchmark_lob_handling(self):
        """Test performance with large text and binary data"""
        large_text = "A" * (1024 * 1024)  # 1MB text
        large_binary = b"\\x00" * (1024 * 1024)  # 1MB binary
        
        for driver in ["mssql-python", "pyodbc"]:
            conn = self.connect(driver)
            cursor = conn.cursor()
            
            with timer(f"{driver} - Insert 1MB TEXT"):
                cursor.execute("INSERT INTO lob_table (text_col) VALUES (?)", (large_text,))
            
            with timer(f"{driver} - Fetch 1MB TEXT"):
                cursor.execute("SELECT text_col FROM lob_table WHERE id = 1")
                row = cursor.fetchone()
            
            with timer(f"{driver} - Insert 1MB VARBINARY"):
                cursor.execute("INSERT INTO lob_table (binary_col) VALUES (?)", (large_binary,))
            
            with timer(f"{driver} - Fetch 1MB VARBINARY"):
                cursor.execute("SELECT binary_col FROM lob_table WHERE id = 2")
                row = cursor.fetchone()
            
            conn.close()
    
    # 5. Wide vs Tall Tables
    def benchmark_table_shapes(self):
        """Compare performance on wide (many columns) vs tall (many rows) tables"""
        for driver in ["mssql-python", "pyodbc"]:
            conn = self.connect(driver)
            cursor = conn.cursor()
            
            # Wide table: 100 columns, 1000 rows
            with timer(f"{driver} - Wide table (100 cols, 1K rows)"):
                cursor.execute("SELECT * FROM wide_table")  # 100 columns
                rows = cursor.fetchall()
            
            # Tall table: 10 columns, 100K rows
            with timer(f"{driver} - Tall table (10 cols, 100K rows)"):
                cursor.execute("SELECT * FROM tall_table")  # 100K rows
                rows = cursor.fetchall()
            
            conn.close()
    
    # 6. Different Data Types
    def benchmark_data_types(self):
        """Test fetch performance with different SQL data types"""
        queries = {
            "INT": "SELECT id FROM numbers_table",  # 100K integers
            "BIGINT": "SELECT big_id FROM numbers_table",
            "DECIMAL": "SELECT price FROM numbers_table",  # DECIMAL(18,2)
            "VARCHAR": "SELECT name FROM strings_table",  # VARCHAR(100)
            "NVARCHAR": "SELECT description FROM strings_table",  # NVARCHAR(500)
            "DATE": "SELECT birth_date FROM dates_table",
            "DATETIME": "SELECT created_at FROM dates_table",
            "DATETIME2": "SELECT updated_at FROM dates_table",
            "DATETIMEOFFSET": "SELECT synced_at FROM dates_table",
            "UNIQUEIDENTIFIER": "SELECT guid FROM guids_table",
            "BIT": "SELECT is_active FROM flags_table",
        }
        
        for data_type, query in queries.items():
            for driver in ["mssql-python", "pyodbc"]:
                conn = self.connect(driver)
                cursor = conn.cursor()
                
                with timer(f"{driver} - {data_type}"):
                    cursor.execute(query)
                    rows = cursor.fetchall()
                
                conn.close()
    
    # 7. Network Latency Simulation
    def benchmark_network_latency(self):
        """Test with local vs remote SQL Server"""
        local_conn_str = self.conn_str  # localhost
        remote_conn_str = self.conn_str.replace("localhost", "remote-server")
        
        for location, conn_str in [("Local", local_conn_str), ("Remote", remote_conn_str)]:
            for driver in ["mssql-python", "pyodbc"]:
                conn = self.connect_with_str(driver, conn_str)
                cursor = conn.cursor()
                
                with timer(f"{driver} - {location} - Small query (1 row)"):
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                
                with timer(f"{driver} - {location} - Medium query (10K rows)"):
                    cursor.execute("SELECT TOP 10000 * FROM large_table")
                    cursor.fetchall()
                
                conn.close()
    
    # 8. Memory Usage
    def benchmark_memory_usage(self):
        """Track memory consumption during large result set fetch"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        
        for driver in ["mssql-python", "pyodbc"]:
            conn = self.connect(driver)
            cursor = conn.cursor()
            
            mem_before = process.memory_info().rss / (1024 * 1024)  # MB
            
            cursor.execute("SELECT * FROM huge_table")  # 1M rows
            rows = cursor.fetchall()
            
            mem_after = process.memory_info().rss / (1024 * 1024)  # MB
            mem_used = mem_after - mem_before
            
            print(f"{driver} - Memory used for 1M rows: {mem_used:.2f} MB")
            print(f"  Per-row overhead: {(mem_used * 1024) / len(rows):.2f} KB")
            
            conn.close()

if __name__ == "__main__":
    suite = BenchmarkSuite(os.getenv("DB_CONNECTION_STRING"))
    
    print("=== TRANSACTION BENCHMARKS ===")
    suite.benchmark_transactions()
    
    print("\\n=== PREPARED STATEMENT BENCHMARKS ===")
    suite.benchmark_prepared_statements()
    
    print("\\n=== CONNECTION POOL BENCHMARKS ===")
    suite.benchmark_connection_pool()
    
    print("\\n=== LOB BENCHMARKS ===")
    suite.benchmark_lob_handling()
    
    print("\\n=== TABLE SHAPE BENCHMARKS ===")
    suite.benchmark_table_shapes()
    
    print("\\n=== DATA TYPE BENCHMARKS ===")
    suite.benchmark_data_types()
    
    print("\\n=== NETWORK LATENCY BENCHMARKS ===")
    suite.benchmark_network_latency()
    
    print("\\n=== MEMORY USAGE BENCHMARKS ===")
    suite.benchmark_memory_usage()
```

---

## Summary

### Files to Create/Modify:

1. ✅ `performance_counter.hpp` - Core profiling (DONE)
2. ⏳ `ddbc_bindings.cpp` - Add 43 PERF_TIMER calls (see PERF_TIMER_LOCATIONS.md)
3. ⏳ `connection/connection.cpp` - Add transaction/pool timers
4. ✅ `benchmarks/comprehensive_benchmarks.py` - New benchmark suite (ABOVE)
5. ✅ Documentation (DONE)

### Next Steps:
1. Review PERF_TIMER_LOCATIONS.md and add timers
2. Build and test
3. Run benchmarks
4. Compare results on Windows/Linux/macOS


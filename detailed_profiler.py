"""
Detailed Performance Profiler for mssql-python
Profiles Unix vs Windows performance with granular C++ and Python metrics
Run this on both macOS/Linux and Windows to compare bottlenecks
"""
import os
import sys
import time
import cProfile
import pstats
import io
from collections import defaultdict
import mssql_python

# Connection string from environment
CONN_STR = os.getenv("DB_CONNECTION_STRING")
if not CONN_STR:
    print("Error: DB_CONNECTION_STRING environment variable not set")
    sys.exit(1)

# Test queries with different characteristics
TEST_QUERIES = {
    "small_strings": ("SELECT TOP 1000 FirstName, LastName, Title FROM Person.Person", 1000),
    "medium_strings": ("SELECT TOP 5000 FirstName, LastName, Title FROM Person.Person", 5000),
    "large_strings": ("SELECT TOP 20000 FirstName, LastName, Title FROM Person.Person", 20000),
    "wide_row": ("SELECT TOP 1000 * FROM Person.Person", 1000),
    "numeric_heavy": ("SELECT TOP 5000 BusinessEntityID, EmailPromotion, ModifiedDate FROM Person.Person", 5000),
    "mixed_types": ("SELECT TOP 10000 BusinessEntityID, FirstName, LastName, ModifiedDate FROM Person.Person", 10000),
}

def profile_query(cursor, query_name, sql, expected_rows):
    """Profile a single query with detailed timing"""
    print(f"\n{'='*80}")
    print(f"Profiling: {query_name}")
    print(f"Expected rows: {expected_rows}")
    print(f"{'='*80}")
    
    timings = {}
    
    # Execute timing
    start = time.perf_counter()
    cursor.execute(sql)
    timings['execute'] = time.perf_counter() - start
    
    # Fetch timing with row count
    start = time.perf_counter()
    rows = cursor.fetchall()
    timings['fetchall'] = time.perf_counter() - start
    
    actual_rows = len(rows)
    timings['total'] = timings['execute'] + timings['fetchall']
    
    # Per-row metrics
    if actual_rows > 0:
        timings['us_per_row'] = (timings['fetchall'] * 1_000_000) / actual_rows
        timings['rows_per_sec'] = actual_rows / timings['fetchall'] if timings['fetchall'] > 0 else 0
    
    # Row size estimation (first row)
    if rows:
        first_row_size = sum(
            len(str(val).encode('utf-8')) if val is not None else 0 
            for val in rows[0]
        )
        timings['first_row_bytes'] = first_row_size
        timings['est_total_kb'] = (first_row_size * actual_rows) / 1024
    
    print(f"\nResults:")
    print(f"  Rows fetched: {actual_rows} (expected: {expected_rows})")
    print(f"  Execute time: {timings['execute']*1000:.2f} ms")
    print(f"  Fetchall time: {timings['fetchall']*1000:.2f} ms")
    print(f"  Total time: {timings['total']*1000:.2f} ms")
    if actual_rows > 0:
        print(f"  Time per row: {timings['us_per_row']:.2f} µs")
        print(f"  Throughput: {timings['rows_per_sec']:,.0f} rows/sec")
        if 'first_row_bytes' in timings:
            print(f"  First row size: {timings['first_row_bytes']} bytes")
            print(f"  Est. total data: {timings['est_total_kb']:.1f} KB")
    
    return timings, actual_rows

def run_cprofile_analysis(conn_str):
    """Run cProfile on a representative workload"""
    print("\n" + "="*80)
    print("DETAILED cProfile ANALYSIS")
    print("="*80)
    
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Representative workload
    conn = mssql_python.connect(conn_str)
    cursor = conn.cursor()
    
    # Mix of operations
    cursor.execute("SELECT TOP 10000 * FROM Person.Person")
    rows = cursor.fetchall()
    
    cursor.execute("SELECT TOP 5000 FirstName, LastName FROM Person.Person")
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    profiler.disable()
    
    # Print detailed stats
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    
    print("\n--- Top 30 functions by cumulative time ---")
    ps.sort_stats('cumulative')
    ps.print_stats(30)
    print(s.getvalue())
    
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    print("\n--- Top 30 functions by total time ---")
    ps.sort_stats('tottime')
    ps.print_stats(30)
    print(s.getvalue())
    
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    print("\n--- Most called functions (top 30) ---")
    ps.sort_stats('ncalls')
    ps.print_stats(30)
    print(s.getvalue())

def profile_string_conversion_overhead():
    """Profile string conversion specifically"""
    print("\n" + "="*80)
    print("STRING CONVERSION OVERHEAD ANALYSIS")
    print("="*80)
    
    conn = mssql_python.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Test with pure string columns
    cursor.execute("SELECT TOP 5000 FirstName, LastName, Title FROM Person.Person")
    
    start = time.perf_counter()
    rows = cursor.fetchall()
    fetch_time = time.perf_counter() - start
    
    row_count = len(rows)
    col_count = len(rows[0]) if rows else 0
    total_strings = row_count * col_count
    
    # Measure access time
    start = time.perf_counter()
    for row in rows:
        _ = row[0]  # Access first column
        _ = row[1]  # Access second column
        _ = row[2]  # Access third column
    access_time = time.perf_counter() - start
    
    print(f"\nString Data:")
    print(f"  Rows: {row_count}")
    print(f"  Columns per row: {col_count}")
    print(f"  Total string values: {total_strings}")
    print(f"  Fetch time: {fetch_time*1000:.2f} ms")
    print(f"  Time per string: {(fetch_time*1_000_000)/total_strings:.2f} µs")
    print(f"  Row access time: {access_time*1000:.2f} ms")
    print(f"  Access per string: {(access_time*1_000_000)/total_strings:.2f} µs")
    
    cursor.close()
    conn.close()

def profile_row_object_creation():
    """Profile Row object creation overhead"""
    print("\n" + "="*80)
    print("ROW OBJECT CREATION OVERHEAD")
    print("="*80)
    
    conn = mssql_python.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Execute but don't fetch yet
    cursor.execute("SELECT TOP 10000 BusinessEntityID, FirstName, LastName FROM Person.Person")
    
    # Time the fetchall (which includes Row creation)
    start = time.perf_counter()
    rows = cursor.fetchall()
    total_time = time.perf_counter() - start
    
    row_count = len(rows)
    
    # Time accessing rows (to measure Row.__getitem__ overhead)
    start = time.perf_counter()
    for row in rows:
        _ = row[0]
        _ = row[1]
        _ = row[2]
    indexing_time = time.perf_counter() - start
    
    # Time attribute access
    start = time.perf_counter()
    for row in rows:
        _ = row.BusinessEntityID
        _ = row.FirstName
        _ = row.LastName
    attribute_time = time.perf_counter() - start
    
    print(f"\nRow Object Metrics:")
    print(f"  Rows created: {row_count}")
    print(f"  Total fetch time: {total_time*1000:.2f} ms")
    print(f"  Time per Row: {(total_time*1_000_000)/row_count:.2f} µs")
    print(f"  Index access (row[i]): {indexing_time*1000:.2f} ms ({(indexing_time*1_000_000)/(row_count*3):.2f} µs per access)")
    print(f"  Attribute access (row.col): {attribute_time*1000:.2f} ms ({(attribute_time*1_000_000)/(row_count*3):.2f} µs per access)")
    print(f"  Attribute vs Index overhead: {((attribute_time/indexing_time - 1)*100):.1f}%")
    
    cursor.close()
    conn.close()

def profile_c_api_calls():
    """Profile C++ API call overhead"""
    print("\n" + "="*80)
    print("C++ API CALL OVERHEAD")
    print("="*80)
    
    conn = mssql_python.connect(CONN_STR)
    cursor = conn.cursor()
    
    iterations = 100
    
    # Profile execute overhead (lightweight query)
    start = time.perf_counter()
    for _ in range(iterations):
        cursor.execute("SELECT 1")
        _ = cursor.fetchall()
    execute_time = time.perf_counter() - start
    
    print(f"\nC++ API Metrics ({iterations} iterations):")
    print(f"  Total time: {execute_time*1000:.2f} ms")
    print(f"  Per iteration: {(execute_time*1000)/iterations:.2f} ms")
    print(f"  Per execute: {(execute_time*1_000_000)/iterations:.2f} µs")
    
    cursor.close()
    conn.close()

def main():
    """Run all profiling tests"""
    print("="*80)
    print("MSSQL-PYTHON DETAILED PERFORMANCE PROFILER")
    print("="*80)
    print(f"Platform: {sys.platform}")
    print(f"Python: {sys.version}")
    print(f"mssql-python: {mssql_python.__version__ if hasattr(mssql_python, '__version__') else 'unknown'}")
    
    # Check if C++ profiling is enabled
    try:
        from mssql_python import ddbc_bindings
        if hasattr(ddbc_bindings, 'reset_profiling_stats'):
            print("C++ Profiling: ENABLED")
            ddbc_bindings.reset_profiling_stats()
        else:
            print("C++ Profiling: DISABLED (rebuild with: ./build_with_profiling.sh)")
    except:
        print("C++ Profiling: DISABLED")
    
    print("="*80)
    
    # 1. Profile all test queries
    conn = mssql_python.connect(CONN_STR)
    cursor = conn.cursor()
    
    all_timings = {}
    for query_name, (sql, expected_rows) in TEST_QUERIES.items():
        timings, actual_rows = profile_query(cursor, query_name, sql, expected_rows)
        all_timings[query_name] = timings
    
    cursor.close()
    conn.close()
    
    # 2. Summary comparison
    print("\n" + "="*80)
    print("SUMMARY TABLE")
    print("="*80)
    print(f"{'Query':<20} {'Rows':<8} {'Total(ms)':<12} {'Fetch(ms)':<12} {'µs/row':<10} {'rows/sec':<12}")
    print("-"*80)
    for query_name, timings in all_timings.items():
        rows = TEST_QUERIES[query_name][1]
        print(f"{query_name:<20} {rows:<8} "
              f"{timings['total']*1000:<12.2f} "
              f"{timings['fetchall']*1000:<12.2f} "
              f"{timings.get('us_per_row', 0):<10.2f} "
              f"{timings.get('rows_per_sec', 0):<12,.0f}")
    
    # 3. Detailed profiling
    profile_string_conversion_overhead()
    profile_row_object_creation()
    profile_c_api_calls()
    run_cprofile_analysis(CONN_STR)
    
    # 4. Print C++ profiling stats if available
    try:
        from mssql_python import ddbc_bindings
        if hasattr(ddbc_bindings, 'print_profiling_stats'):
            print("\n")
            ddbc_bindings.print_profiling_stats()
    except:
        pass
    
    print("\n" + "="*80)
    print("PROFILING COMPLETE")
    print("="*80)
    print("\nTo compare with pyodbc, run: python compare_with_pyodbc.py")
    print("To enable C++ profiling, rebuild with: ENABLE_PROFILING=1 python setup.py build_ext --inplace")

if __name__ == "__main__":
    main()

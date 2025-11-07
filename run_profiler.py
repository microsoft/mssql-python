"""
Minimal profiler to identify performance bottlenecks with granular C++ timing.
"""
import os
import sys
import cProfile
import pstats
import io

sys.path.insert(0, os.path.abspath('.'))

# Simple query - ~120k rows instead of 1.2M
SIMPLE_QUERY = """
SELECT
    sod.SalesOrderID,
    sod.SalesOrderDetailID,
    sod.ProductID,
    sod.OrderQty,
    sod.UnitPrice,
    sod.LineTotal,
    p.Name AS ProductName,
    p.ProductNumber,
    p.Color,
    p.ListPrice,
    n1.number AS RowMultiplier1
FROM Sales.SalesOrderDetail sod
CROSS JOIN (SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
            FROM Sales.SalesOrderDetail) n1
INNER JOIN Production.Product p ON sod.ProductID = p.ProductID;
"""

CONN_STR = os.getenv("DB_CONNECTION_STRING")
if not CONN_STR:
    print("Error: Set DB_CONNECTION_STRING environment variable")
    sys.exit(1)

def run_query():
    """Execute query with C++ profiling enabled"""
    from mssql_python import connect, ddbc_bindings
    
    # Enable C++ profiling
    ddbc_bindings.profiling.enable()
    
    # Execute query
    conn = connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute(SIMPLE_QUERY)
    rows = cursor.fetchall()
    
    # Get results
    cpp_stats = ddbc_bindings.profiling.get_stats()
    
    cursor.close()
    conn.close()
    
    return rows, cpp_stats

if __name__ == "__main__":
    import platform
    
    print("="*80)
    print("PROFILING: Simple Query (~120K rows)")
    print("="*80)
    print(f"Python Platform: {platform.system()} {platform.release()}")
    print(f"Python Version: {platform.python_version()}")
    print()
    
    # Python-level profiling
    pr = cProfile.Profile()
    pr.enable()
    
    rows, cpp_stats = run_query()
    
    pr.disable()
    
    print(f"\nRows fetched: {len(rows):,}")
    
    # Python stats (top 15)
    print("\n" + "="*80)
    print("PYTHON LAYER (cProfile - Top 15)")
    print("="*80)
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(15)
    print(s.getvalue())
    
    # C++ stats - sequential order
    print("\n" + "="*80)
    print("C++ LAYER (Sequential Execution Order)")
    print("="*80)
    if cpp_stats:
        # Detect platform from stats (all functions should have same platform)
        platform = next(iter(cpp_stats.values()))['platform'] if cpp_stats else 'unknown'
        print(f"\nPlatform: {platform.upper()}")
        
        # Group by execution phase
        phases = {
            'Driver & Connection': ['DriverLoader::loadDriver', 'Connection::Connection', 'Connection::allocateDbcHandle', 'Connection::connect', 'Connection::setAutocommit'],
            'Statement Preparation': ['Connection::allocStatementHandle'],
            'Query Execution': ['SQLExecDirect_wrap', 'SQLExecDirect_wrap::configure_cursor', 'SQLExecDirect_wrap::SQLExecDirect_call'],
            'Column Metadata': ['SQLNumResultCols_wrap', 'SQLDescribeCol_wrap', 'SQLBindColums'],
            'Data Fetching': ['FetchAll_wrap', 'FetchBatchData', 'FetchBatchData::SQLFetchScroll_call', 'FetchBatchData::cache_column_metadata', 'FetchBatchData::construct_rows', 'FetchOne_wrap', 'SQLFetch_wrap', 'SQLGetData_wrap', 'FetchLobColumnData'],
            'Result Processing': ['SQLMoreResults_wrap', 'SQLRowCount_wrap'],
            'Cleanup': ['SQLFreeHandle_wrap', 'Connection::disconnect', 'Connection::commit', 'Connection::rollback', 'SqlHandle::free']
        }
        
        print(f"\n{'Function':<50} {'Calls':>8} {'Total(ms)':>12} {'Avg(μs)':>12} {'Min(μs)':>12} {'Max(μs)':>12}")
        print("-" * 116)
        
        for phase, funcs in phases.items():
            print(f"\n{phase}:")
            for func_name in funcs:
                if func_name in cpp_stats:
                    stats = cpp_stats[func_name]
                    total_ms = stats['total_us'] / 1000.0
                    avg_us = stats['total_us'] / stats['calls'] if stats['calls'] > 0 else 0
                    print(f"  {func_name:<48} {stats['calls']:>8} {total_ms:>12.3f} {avg_us:>12.1f} {stats['min_us']:>12.1f} {stats['max_us']:>12.1f}")
        
        # Any functions not in phases
        all_phase_funcs = set(f for funcs in phases.values() for f in funcs)
        other_funcs = set(cpp_stats.keys()) - all_phase_funcs
        if other_funcs:
            print(f"\nOther:")
            for func_name in sorted(other_funcs):
                stats = cpp_stats[func_name]
                total_ms = stats['total_us'] / 1000.0
                avg_us = stats['total_us'] / stats['calls'] if stats['calls'] > 0 else 0
                print(f"  {func_name:<48} {stats['calls']:>8} {total_ms:>12.3f} {avg_us:>12.1f} {stats['min_us']:>12.1f} {stats['max_us']:>12.1f}")
    else:
        print("No C++ profiling data collected")
    
    print("\n" + "="*80)

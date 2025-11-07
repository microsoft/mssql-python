# Profiling Usage Guide

## Quick Start

### Enable C++ Profiling
```python
from mssql_python import ddbc_bindings

# Enable profiling
ddbc_bindings.profiling.enable()

# Run your queries
conn = connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT * FROM table")
rows = cursor.fetchall()
cursor.close()
conn.close()

# Get stats
stats = ddbc_bindings.profiling.get_stats()

# Display results
for name, data in sorted(stats.items(), key=lambda x: x[1]['total_us'], reverse=True):
    print(f"{name}: {data['total_us']/1000:.2f}ms, {data['calls']} calls")
```

### Run Profiler Script
```bash
# Set connection string
export DB_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=AdventureWorks2022;UID=sa;Pwd=YourPassword;TrustServerCertificate=yes;"

# Run profiler on Very Large Dataset query
python run_profiler.py
```

## API Reference

### Methods
- `profiling.enable()` - Start collecting stats
- `profiling.disable()` - Stop collecting stats
- `profiling.is_enabled()` - Check if enabled
- `profiling.get_stats()` - Get dict of stats
- `profiling.reset()` - Clear all stats

### Stats Dictionary Format
```python
{
    'FunctionName': {
        'total_us': 105230,    # Total microseconds
        'calls': 1,             # Number of calls
        'min_us': 105230,       # Minimum time
        'max_us': 105230,       # Maximum time
        'platform': 'windows'   # Platform: windows/linux/macos
    }
}
```

## Instrumented Functions

| Function | Description |
|----------|-------------|
| `FetchAll_wrap` | Overall fetch orchestration |
| `FetchBatchData` | Batch data retrieval loop |
| `SQLExecDirect_wrap` | Query execution |
| `Connection::Construction` | Connection object creation |
| `Connection::connect` | SQLDriverConnect call |

## Output Example

```
================================================================================
PROFILING: Simple Query (~120K rows)
================================================================================
Python Platform: Windows 10
Python Version: 3.13.0

...Python profiling output...

C++ LAYER (Sequential Execution Order)
====================================================================================
Platform: WINDOWS

Function                       Total(ms)    Calls     Avg(us)     Min(us)     Max(us)
--------------------------------------------------------------------------------------
FetchAll_wrap                     105.23        1   105230.00   105230.00   105230.00
FetchBatchData                     80.15     1000       80.15       65.20      120.45
SQLExecDirect_wrap                 23.12        1    23120.00    23120.00    23120.00
Connection::connect                42.35        1    42350.00    42350.00    42350.00
Connection::Construction            5.18        1     5180.00     5180.00     5180.00
```

## Platform-Specific Performance Analysis

The profiler automatically detects the platform and includes it in the stats. This is useful for comparing:
- **Windows vs Linux**: ODBC driver differences
- **Windows vs macOS**: TLS/memory manager differences
- **Linux variations**: Different distros, kernel versions

Example workflow:
1. Run profiler on Windows: `python run_profiler.py > windows_profile.txt`
2. Run profiler on Linux: `python run_profiler.py > linux_profile.txt`
3. Compare the two files to identify platform-specific bottlenecks

## Tips

1. **Always disable in production**: Profiling has ~1-2% overhead
2. **Use `reset()` between tests**: Clear stats for clean measurements
3. **Combine with Python cProfile**: See both Python and C++ layers
4. **Focus on high total_us**: These are the bottlenecks
5. **Check call counts**: High counts with low avg might indicate optimization opportunities

## Troubleshooting

**Profiling not working?**
- Check `is_enabled()` returns `True`
- Verify you called `enable()` before running queries
- Make sure you rebuilt the C++ extension after adding instrumentation

**No stats returned?**
- Profiling might be disabled
- Functions might not have been called (check your query)
- Check for exceptions during query execution

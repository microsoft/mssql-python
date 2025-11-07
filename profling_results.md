# on main branch

```bash
================================================================================
PROFILING: Simple Query (~120K rows)
================================================================================
Python Platform: Windows 11
Python Version: 3.13.9


Rows fetched: 121,317

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         527661 function calls (526639 primitive calls) in 1.139 seconds

   Ordered by: cumulative time
   List reduced from 569 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.141    0.141    0.936    0.936 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\cursor.py:2065(fetchall)
        1    0.680    0.680    0.680    0.680 {built-in method ddbc_bindings.DDBCSQLFetchAll}
        1    0.000    0.000    0.202    0.202 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\db_connection.py:11(connect)
     44/1    0.001    0.000    0.170    0.170 <frozen importlib._bootstrap>:1349(_find_and_load)
     44/1    0.000    0.000    0.170    0.170 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
     44/1    0.000    0.000    0.169    0.169 <frozen importlib._bootstrap>:911(_load_unlocked)
     33/1    0.000    0.000    0.169    0.169 <frozen importlib._bootstrap_external>:1021(exec_module)
     94/2    0.000    0.000    0.160    0.080 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
     34/1    0.000    0.000    0.160    0.160 {built-in method builtins.exec}
        1    0.000    0.000    0.160    0.160 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\__init__.py:1(<module>)
        1    0.000    0.000    0.136    0.136 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\helpers.py:1(<module>)
   121317    0.076    0.000    0.114    0.000 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\row.py:26(__init__)
      9/6    0.000    0.000    0.113    0.019 <frozen importlib._bootstrap>:1390(_handle_fromlist)
        1    0.000    0.000    0.113    0.113 {built-in method builtins.__import__}
        1    0.000    0.000    0.110    0.110 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\ddbc_bindings.py:1(<module>)




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: WINDOWS

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1       10.297      10297.0      10297.0      10297.0
  Connection::allocateDbcHandle                           1       10.293      10293.0      10293.0      10293.0
  Connection::connect                                     1       20.180      20180.0      20180.0      20180.0
  Connection::setAutocommit                               1        0.309        309.0        309.0        309.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.020         10.0          3.0         17.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.003          3.0          3.0          3.0
  SQLDescribeCol_wrap                                     3        0.086         28.7         17.0         49.0
  SQLBindColums                                           1        0.201        201.0        201.0        201.0

Data Fetching:
  FetchAll_wrap                                           1      680.190     680190.0     680190.0     680190.0
  FetchBatchData                                        123      679.661       5525.7         26.0       9695.0
  FetchBatchData::SQLFetchScroll_call                   123      408.548       3321.5          2.0       7568.0
  FetchBatchData::cache_column_metadata                 122        0.827          6.8          4.0         32.0
  FetchBatchData::construct_rows                        122      221.447       1815.1        570.0       5548.0

Result Processing:
  SQLRowCount_wrap                                        1        0.008          8.0          8.0          8.0

Cleanup:
  SqlHandle::free                                         1        0.005          5.0          5.0          5.0

Other:
  Connection::connect::SQLDriverConnect_call              1       20.168      20168.0      20168.0      20168.0
  SQLDescribeCol_wrap::per_column                        30        0.052          1.7          0.0         13.0
  SQLGetAllDiagRecords                                    2        0.016          8.0          4.0         12.0

================================================================================

(myvenv) azureuser@python-perftest:~/mssql-python$ python run_profiler.py 
================================================================================
PROFILING: Simple Query (~120K rows)
================================================================================
Python Platform: Linux 6.8.0-1041-azure
Python Version: 3.10.12


Rows fetched: 121,317

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         520869 function calls (520286 primitive calls) in 2.165 seconds

   Ordered by: cumulative time
   List reduced from 556 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.003    0.003    2.165    2.165 /home/azureuser/mssql-python/run_profiler.py:34(run_query)
        1    0.000    0.000    1.957    1.957 /home/azureuser/mssql-python/mssql_python/cursor.py:2065(fetchall)
        1    1.415    1.415    1.416    1.416 {built-in method ddbc_bindings.DDBCSQLFetchAll}
        1    0.359    0.359    0.540    0.540 /home/azureuser/mssql-python/mssql_python/cursor.py:2099(<listcomp>)
   121317    0.141    0.000    0.181    0.000 /home/azureuser/mssql-python/mssql_python/row.py:26(__init__)
        1    0.000    0.000    0.142    0.142 /home/azureuser/mssql-python/mssql_python/db_connection.py:11(connect)
        1    0.139    0.139    0.142    0.142 /home/azureuser/mssql-python/mssql_python/connection.py:133(__init__)
     44/1    0.000    0.000    0.052    0.052 <frozen importlib._bootstrap>:1022(_find_and_load)
     44/1    0.000    0.000    0.052    0.052 <frozen importlib._bootstrap>:987(_find_and_load_unlocked)
     41/1    0.000    0.000    0.052    0.052 <frozen importlib._bootstrap>:664(_load_unlocked)
     30/1    0.000    0.000    0.052    0.052 <frozen importlib._bootstrap_external>:877(exec_module)
     57/1    0.000    0.000    0.051    0.051 <frozen importlib._bootstrap>:233(_call_with_frames_removed)
     30/1    0.000    0.000    0.051    0.051 {built-in method builtins.exec}
        1    0.000    0.000    0.051    0.051 /home/azureuser/mssql-python/mssql_python/__init__.py:1(<module>)
        1    0.000    0.000    0.045    0.045 /home/azureuser/mssql-python/mssql_python/helpers.py:1(<module>)




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: LINUX

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1        1.157       1157.0       1157.0       1157.0
  Connection::allocateDbcHandle                           1        1.139       1139.0       1139.0       1139.0
  Connection::connect                                     1      137.946     137946.0     137946.0     137946.0
  Connection::setAutocommit                               1        0.468        468.0        468.0        468.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.038         19.0         12.0         26.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.012         12.0         12.0         12.0
  SQLDescribeCol_wrap                                     3        0.372        124.0        100.0        165.0
  SQLBindColums                                           1        0.782        782.0        782.0        782.0

Data Fetching:
  FetchAll_wrap                                           1     1416.450    1416450.0    1416450.0    1416450.0
  FetchBatchData                                        123     1414.999      11504.1         44.0      56552.0
  FetchBatchData::SQLFetchScroll_call                   123      180.215       1465.2          3.0       2388.0
  FetchBatchData::cache_column_metadata                 122        3.817         31.3         22.0         47.0
  FetchBatchData::construct_rows                        122     1208.030       9901.9       2860.0      54918.0

Result Processing:
  SQLRowCount_wrap                                        1        0.027         27.0         27.0         27.0

Cleanup:
  SqlHandle::free                                         1        0.008          8.0          8.0          8.0

Other:
  Connection::connect::SQLDriverConnect_call              1      137.803     137803.0     137803.0     137803.0
  SQLDescribeCol_wrap::per_column                        30        0.308         10.3          7.0         59.0
  SQLGetAllDiagRecords                                    2        0.417        208.5        130.0        287.0
  SQLWCHARToWString                                  329836        0.948          0.0          0.0         83.0
  WStringToSQLWCHAR                                       2        0.072         36.0         30.0         42.0
  construct_rows::wstring_conversion                 329806      228.338          0.7          0.0       1250.0

================================================================================
```

# After FIX 1 - PyUnicode_Decode change - String coversion to PyStr at one go instead of char by char 
```bash
(myvenv) azureuser@python-perftest:~/mssql-python$ python run_profiler.py 
================================================================================
PROFILING: Simple Query (~120K rows)
================================================================================
Python Platform: Linux 6.8.0-1041-azure
Python Version: 3.10.12


Rows fetched: 121,317

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         520869 function calls (520286 primitive calls) in 1.940 seconds

   Ordered by: cumulative time
   List reduced from 556 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.003    0.003    1.940    1.940 /home/azureuser/mssql-python/run_profiler.py:34(run_query)
        1    0.000    0.000    1.696    1.696 /home/azureuser/mssql-python/mssql_python/cursor.py:2065(fetchall)
        1    1.144    1.144    1.145    1.145 {built-in method ddbc_bindings.DDBCSQLFetchAll}
        1    0.368    0.368    0.551    0.551 /home/azureuser/mssql-python/mssql_python/cursor.py:2099(<listcomp>)
   121317    0.143    0.000    0.183    0.000 /home/azureuser/mssql-python/mssql_python/row.py:26(__init__)
        1    0.000    0.000    0.180    0.180 /home/azureuser/mssql-python/mssql_python/db_connection.py:11(connect)
        1    0.177    0.177    0.180    0.180 /home/azureuser/mssql-python/mssql_python/connection.py:133(__init__)
     44/1    0.000    0.000    0.055    0.055 <frozen importlib._bootstrap>:1022(_find_and_load)
     44/1    0.000    0.000    0.055    0.055 <frozen importlib._bootstrap>:987(_find_and_load_unlocked)
     41/1    0.000    0.000    0.055    0.055 <frozen importlib._bootstrap>:664(_load_unlocked)
     30/1    0.000    0.000    0.055    0.055 <frozen importlib._bootstrap_external>:877(exec_module)
     57/1    0.000    0.000    0.055    0.055 <frozen importlib._bootstrap>:233(_call_with_frames_removed)
     30/1    0.000    0.000    0.055    0.055 {built-in method builtins.exec}
        1    0.000    0.000    0.055    0.055 /home/azureuser/mssql-python/mssql_python/__init__.py:1(<module>)
        1    0.000    0.000    0.048    0.048 /home/azureuser/mssql-python/mssql_python/helpers.py:1(<module>)




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: LINUX

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1        1.172       1172.0       1172.0       1172.0
  Connection::allocateDbcHandle                           1        1.155       1155.0       1155.0       1155.0
  Connection::connect                                     1      175.511     175511.0     175511.0     175511.0
  Connection::setAutocommit                               1        0.430        430.0        430.0        430.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.048         24.0         12.0         36.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.011         11.0         11.0         11.0
  SQLDescribeCol_wrap                                     3        0.384        128.0        114.0        152.0
  SQLBindColums                                           1        0.774        774.0        774.0        774.0

Data Fetching:
  FetchAll_wrap                                           1     1145.263    1145263.0    1145263.0    1145263.0
  FetchBatchData                                        123     1143.798       9299.2         36.0      54242.0
  FetchBatchData::SQLFetchScroll_call                   123      178.698       1452.8          2.0       2475.0
  FetchBatchData::cache_column_metadata                 122        3.220         26.4         19.0         57.0
  FetchBatchData::construct_rows                        122      939.276       7699.0       2178.0      52596.0

Result Processing:
  SQLRowCount_wrap                                        1        0.019         19.0         19.0         19.0

Cleanup:
  SqlHandle::free                                         1        0.008          8.0          8.0          8.0

Other:
  Connection::connect::SQLDriverConnect_call              1      175.405     175405.0     175405.0     175405.0
  SQLDescribeCol_wrap::per_column                        30        0.314         10.5          7.0         42.0
  SQLGetAllDiagRecords                                    2        0.357        178.5        127.0        230.0
  SQLWCHARToWString                                      30        0.001          0.0          0.0          1.0
  WStringToSQLWCHAR                                       2        0.049         24.5         19.0         30.0
  construct_rows::wstring_conversion                 329806       14.924          0.0          0.0        127.0
```

# Profiling for 1.2M rows on ubuntu after FIX 1
```bash
(myvenv) azureuser@python-perftest:~/mssql-python$ python run_profiler.py 
================================================================================
PROFILING: Very Large Dataset Query (1.2M rows)
================================================================================
Python Platform: Linux 6.8.0-1041-azure
Python Version: 3.13.8


Rows fetched: 1,213,170

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         4901347 function calls (4900288 primitive calls) in 19.550 seconds

   Ordered by: cumulative time
   List reduced from 599 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    2.370    2.370   19.324   19.324 /home/azureuser/mssql-python/mssql_python/cursor.py:2065(fetchall)
        1   15.339   15.339   15.345   15.345 {built-in method ddbc_bindings.DDBCSQLFetchAll}
  1213170    1.061    0.000    1.608    0.000 /home/azureuser/mssql-python/mssql_python/row.py:26(__init__)
  2426346    0.317    0.000    0.317    0.000 /home/azureuser/mssql-python/mssql_python/cursor.py:932(connection)
  1214615    0.231    0.000    0.231    0.000 {built-in method builtins.hasattr}
        1    0.000    0.000    0.218    0.218 /home/azureuser/mssql-python/mssql_python/db_connection.py:11(connect)
        1    0.000    0.000    0.139    0.139 /home/azureuser/mssql-python/mssql_python/connection.py:133(__init__)
        1    0.138    0.138    0.139    0.139 /home/azureuser/mssql-python/mssql_python/connection.py:334(setautocommit)
     50/1    0.001    0.000    0.080    0.080 <frozen importlib._bootstrap>:1349(_find_and_load)
     50/1    0.000    0.000    0.079    0.079 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
     50/1    0.000    0.000    0.079    0.079 <frozen importlib._bootstrap>:911(_load_unlocked)
     35/1    0.000    0.000    0.079    0.079 <frozen importlib._bootstrap_external>:1021(exec_module)
    106/2    0.000    0.000    0.079    0.039 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
     36/1    0.000    0.000    0.079    0.079 {built-in method builtins.exec}
        1    0.000    0.000    0.079    0.079 /home/azureuser/mssql-python/mssql_python/__init__.py:1(<module>)




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: LINUX

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1        1.223       1223.0       1223.0       1223.0
  Connection::allocateDbcHandle                           1        1.207       1207.0       1207.0       1207.0
  Connection::connect                                     1      135.758     135758.0     135758.0     135758.0
  Connection::setAutocommit                               1        0.467        467.0        467.0        467.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.041         20.5         12.0         29.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.011         11.0         11.0         11.0
  SQLDescribeCol_wrap                                     3        0.542        180.7        157.0        223.0
  SQLBindColums                                           1        1.049       1049.0       1049.0       1049.0

Data Fetching:
  FetchAll_wrap                                           1    15344.941   15344941.0   15344941.0   15344941.0
  FetchBatchData                                       1215    15341.280      12626.6         20.0     843888.0
  FetchBatchData::SQLFetchScroll_call                  1215     1783.295       1467.7          1.0       2650.0
  FetchBatchData::cache_column_metadata                1214       26.901         22.2         16.0         55.0
  FetchBatchData::construct_rows                       1214     9624.970       7928.3       1313.0      13595.0

Result Processing:
  SQLRowCount_wrap                                        1        0.024         24.0         24.0         24.0

Cleanup:
  SqlHandle::free                                         1        0.006          6.0          6.0          6.0

Other:
  Connection::connect::SQLDriverConnect_call              1      135.663     135663.0     135663.0     135663.0
  SQLDescribeCol_wrap::per_column                        33        0.464         14.1         11.0         53.0
  SQLGetAllDiagRecords                                    2        0.398        199.0        118.0        280.0
  SQLWCHARToWString                                      33        0.002          0.1          0.0          1.0
  WStringToSQLWCHAR                                       2        0.076         38.0         19.0         57.0
  construct_rows::row_store                         1213170        2.082          0.0          0.0         89.0
  construct_rows::wstring_conversion                3298060       79.578          0.0          0.0        340.0

================================================================================
```
# Profiling for 1.2M rows on windows (FIX 1 doesnt apply to windows since the code is not executed there)
```bash
(myvenv) PS C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python> python .\run_profiler.py                     
================================================================================
PROFILING: Very Large Dataset Query (1.2M rows)
================================================================================
Python Platform: Windows 11
Python Version: 3.13.9


Rows fetched: 1,213,170

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         4898443 function calls (4897421 primitive calls) in 9.090 seconds

   Ordered by: cumulative time
   List reduced from 569 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    1.335    1.335    8.831    8.831 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\cursor.py:2065(fetchall)
        1    6.455    6.455    6.459    6.459 {built-in method ddbc_bindings.DDBCSQLFetchAll}
  1213170    0.693    0.000    1.037    0.000 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\row.py:26(__init__)
        1    0.000    0.000    0.254    0.254 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\db_connection.py:11(connect)
     44/1    0.001    0.000    0.223    0.223 <frozen importlib._bootstrap>:1349(_find_and_load)
     44/1    0.000    0.000    0.223    0.223 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
     44/1    0.000    0.000    0.222    0.222 <frozen importlib._bootstrap>:911(_load_unlocked)
     33/1    0.000    0.000    0.222    0.222 <frozen importlib._bootstrap_external>:1021(exec_module)
     94/2    0.000    0.000    0.219    0.109 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
     34/1    0.000    0.000    0.219    0.219 {built-in method builtins.exec}
        1    0.000    0.000    0.219    0.219 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\__init__.py:1(<module>)
  2426346    0.203    0.000    0.203    0.000 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\cursor.py:932(connection)
        1    0.000    0.000    0.197    0.197 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\helpers.py:1(<module>)
      9/6    0.000    0.000    0.175    0.029 <frozen importlib._bootstrap>:1390(_handle_fromlist)
        1    0.000    0.000    0.175    0.175 {built-in method builtins.__import__}




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: WINDOWS

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1       12.951      12951.0      12951.0      12951.0
  Connection::allocateDbcHandle                           1       12.947      12947.0      12947.0      12947.0
  Connection::connect                                     1       17.434      17434.0      17434.0      17434.0
  Connection::setAutocommit                               1        0.236        236.0        236.0        236.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.023         11.5          5.0         18.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.004          4.0          4.0          4.0
  SQLDescribeCol_wrap                                     3        0.094         31.3         22.0         43.0
  SQLBindColums                                           1        0.235        235.0        235.0        235.0

Data Fetching:
  FetchAll_wrap                                           1     6458.527    6458527.0    6458527.0    6458527.0
  FetchBatchData                                       1215     6457.036       5314.4         18.0     388008.0
  FetchBatchData::SQLFetchScroll_call                  1215     2361.493       1943.6          2.0       7384.0
  FetchBatchData::cache_column_metadata                1214        6.840          5.6          4.0         26.0
  FetchBatchData::construct_rows                       1214     2244.624       1848.9        323.0       7475.0

Result Processing:
  SQLRowCount_wrap                                        1        0.009          9.0          9.0          9.0

Cleanup:
  SqlHandle::free                                         1        0.005          5.0          5.0          5.0

Other:
  Connection::connect::SQLDriverConnect_call              1       17.426      17426.0      17426.0      17426.0
  SQLDescribeCol_wrap::per_column                        33        0.056          1.7          1.0         10.0
  SQLGetAllDiagRecords                                    2        0.016          8.0          4.0         12.0
  construct_rows::row_store                         1213170        1.105          0.0          0.0        160.0

================================================================================
```

# Analysis Till Now

## Final Analysis - 1.2M Rows (Python 3.13 on both platforms)

### Overall Performance (C++ Layer - FetchAll_wrap):
- **Linux**: 15.3 seconds
- **Windows**: 6.5 seconds
- **Gap**: **2.4x slower on Linux**

### Breakdown of the 8.9 second gap:

#### 1. **SQLFetchScroll (ODBC Driver)**
- **Linux**: 1,783ms
- **Windows**: 2,361ms
- **Winner**: Linux is **578ms faster** ✅

#### 2. **construct_rows (Python object creation)**
- **Linux**: 9,625ms (63% of total time)
- **Windows**: 2,245ms (35% of total time)
- **Gap**: **7,380ms slower on Linux** ❌ **THIS IS THE PROBLEM**

#### 3. **String conversion (your optimization)**
- **Linux**: 80ms (construct_rows::wstring_conversion)
- **Windows**: 0ms (not measured, but negligible)
- **Impact**: Minimal - **your fix worked!** ✅

#### 4. **Row storage (py::list assignment)**
- **Linux**: 2.1ms (construct_rows::row_store)
- **Windows**: 1.1ms
- **Impact**: Negligible

---

## Key Observations:

### ✅ **What's Working:**
1. String conversion is **no longer the bottleneck** (80ms is negligible)
2. Linux ODBC driver is actually **faster** than Windows
3. Both using Python 3.13, so Python version is **not the issue**

### ❌ **The Real Problem: construct_rows is 4.3x slower**

**Unaccounted overhead in construct_rows:**
- Linux: 9,625ms - 80ms (string) - 2ms (row_store) = **9,543ms** for other operations
- Windows: 2,245ms - 1ms (row_store) = **2,244ms** for other operations
- **Gap: 7,299ms** (4.3x slower on Linux)

This overhead is in the **switch statement** processing integers, floats, timestamps, decimals, etc. - **not string conversion**.

## Root Cause Hypothesis:

The 7.3 second gap is likely due to:

1. **pybind11 `py::list` assignment overhead** - Every `row[col - 1] = value` creates Python objects
   - 1.2M rows × 3 columns = **3.6M assignments**
   - If each assignment is 2μs slower on Linux: 3.6M × 2μs = **7.2 seconds** ✅ **This matches!**

2. **Why is assignment slower on Linux?**
   - Different memory allocator (glibc malloc vs Windows heap)
   - Different CPU cache behavior
   - Compiler differences (GCC vs MSVC optimization of pybind11 code)

## Recommended Next Steps:

1. **Profile with `perf` on Linux** to see CPU cache misses, memory stalls
2. **Try batch assignment** - Build `py::tuple` instead of assigning to `py::list` element by element
3. **Pre-allocate with actual values** instead of `py::none()` placeholders
4. **Test with tcmalloc/jemalloc** instead of glibc malloc

**Bottom line**: Your string conversion fix was successful. The remaining gap is fundamental platform/allocator differences in how pybind11 creates Python objects, not something easily fixable in application code.



# Much more detailed profiling

```bash
(myvenv) PS C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python> python .\run_profiler.py                     
================================================================================
PROFILING: Very Large Dataset Query (1.2M rows)
================================================================================
Python Platform: Windows 11
Python Version: 3.13.9


Rows fetched: 1,213,170

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         4898443 function calls (4897421 primitive calls) in 12.227 seconds

   Ordered by: cumulative time
   List reduced from 569 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    1.283    1.283   11.970   11.970 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\cursor.py:2065(fetchall)
        1    9.663    9.663    9.668    9.668 {built-in method ddbc_bindings.DDBCSQLFetchAll}
  1213170    0.682    0.000    1.019    0.000 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\row.py:26(__init__)
        1    0.000    0.000    0.253    0.253 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\db_connection.py:11(connect)
     44/1    0.001    0.000    0.222    0.222 <frozen importlib._bootstrap>:1349(_find_and_load)
     44/1    0.000    0.000    0.222    0.222 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
     44/1    0.000    0.000    0.221    0.221 <frozen importlib._bootstrap>:911(_load_unlocked)
     33/1    0.000    0.000    0.221    0.221 <frozen importlib._bootstrap_external>:1021(exec_module)
     94/2    0.000    0.000    0.219    0.109 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
     34/1    0.000    0.000    0.219    0.219 {built-in method builtins.exec}
        1    0.000    0.000    0.219    0.219 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\__init__.py:1(<module>)
  2426346    0.196    0.000    0.196    0.000 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\cursor.py:932(connection)
        1    0.000    0.000    0.196    0.196 C:\Users\sharmag\OneDrive - Microsoft\Desktop\gh-mssql-python\mssql_python\helpers.py:1(<module>)
      9/6    0.000    0.000    0.176    0.029 <frozen importlib._bootstrap>:1390(_handle_fromlist)
        1    0.000    0.000    0.176    0.176 {built-in method builtins.__import__}




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: WINDOWS

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1       13.727      13727.0      13727.0      13727.0
  Connection::allocateDbcHandle                           1       13.724      13724.0      13724.0      13724.0
  Connection::connect                                     1       16.058      16058.0      16058.0      16058.0
  Connection::setAutocommit                               1        0.199        199.0        199.0        199.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.019          9.5          6.0         13.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.003          3.0          3.0          3.0
  SQLDescribeCol_wrap                                     3        0.069         23.0         17.0         35.0
  SQLBindColums                                           1        0.226        226.0        226.0        226.0

Data Fetching:
  FetchAll_wrap                                           1     9668.422    9668422.0    9668422.0    9668422.0
  FetchBatchData                                       1215     9666.767       7956.2         25.0     395330.0
  FetchBatchData::SQLFetchScroll_call                  1215     2758.888       2270.7          3.0       9071.0
  FetchBatchData::cache_column_metadata                1214       10.530          8.7          4.0        257.0
  FetchBatchData::construct_rows                       1214     5009.733       4126.6        852.0      11047.0

Result Processing:
  SQLRowCount_wrap                                        1        0.008          8.0          8.0          8.0

Cleanup:
  SqlHandle::free                                         1        0.006          6.0          6.0          6.0

Other:
  Connection::connect::SQLDriverConnect_call              1       16.046      16046.0      16046.0      16046.0
  SQLDescribeCol_wrap::per_column                        33        0.038          1.2          0.0          8.0
  SQLGetAllDiagRecords                                    2        0.024         12.0          6.0         18.0
  construct_rows::all_columns_processing            1213170     3344.022          2.8          2.0       2636.0
  construct_rows::bigint_buffer_read                1213170        1.039          0.0          0.0        109.0
  construct_rows::bigint_c_api_assign               1213170        0.943          0.0          0.0        110.0
  construct_rows::int_buffer_read                   3639510        2.848          0.0          0.0        143.0
  construct_rows::int_c_api_assign                  3639510       41.500          0.0          0.0        493.0
  construct_rows::per_row_total                     1213170     4448.547          3.7          2.0       2638.0
  construct_rows::pylist_creation                   1213170       54.651          0.0          0.0        310.0
  construct_rows::rows_append                       1213170        1.008          0.0          0.0         71.0
  construct_rows::smallint_buffer_read              1213170        0.628          0.0          0.0        141.0
  construct_rows::smallint_c_api_assign             1213170        0.861          0.0          0.0         76.0

================================================================================


(myvenv) azureuser@python-perftest:~/mssql-python$ python run_profiler.py 
================================================================================
PROFILING: Very Large Dataset Query (1.2M rows)
================================================================================
Python Platform: Linux 6.8.0-1041-azure
Python Version: 3.13.8


Rows fetched: 1,213,170

================================================================================
PYTHON LAYER (cProfile - Top 15)
================================================================================
         4901347 function calls (4900288 primitive calls) in 26.733 seconds

   Ordered by: cumulative time
   List reduced from 599 to 15 due to restriction <15>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    2.325    2.325   26.504   26.504 /home/azureuser/mssql-python/mssql_python/cursor.py:2065(fetchall)
        1   22.652   22.652   22.659   22.659 {built-in method ddbc_bindings.DDBCSQLFetchAll}
  1213170    1.001    0.000    1.520    0.000 /home/azureuser/mssql-python/mssql_python/row.py:26(__init__)
  2426346    0.305    0.000    0.305    0.000 /home/azureuser/mssql-python/mssql_python/cursor.py:932(connection)
        1    0.000    0.000    0.221    0.221 /home/azureuser/mssql-python/mssql_python/db_connection.py:11(connect)
  1214615    0.215    0.000    0.215    0.000 {built-in method builtins.hasattr}
        1    0.000    0.000    0.145    0.145 /home/azureuser/mssql-python/mssql_python/connection.py:133(__init__)
        1    0.144    0.144    0.145    0.145 /home/azureuser/mssql-python/mssql_python/connection.py:334(setautocommit)
     50/1    0.000    0.000    0.076    0.076 <frozen importlib._bootstrap>:1349(_find_and_load)
     50/1    0.000    0.000    0.076    0.076 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
     50/1    0.000    0.000    0.076    0.076 <frozen importlib._bootstrap>:911(_load_unlocked)
     35/1    0.000    0.000    0.076    0.076 <frozen importlib._bootstrap_external>:1021(exec_module)
    106/2    0.000    0.000    0.076    0.038 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
     36/1    0.000    0.000    0.076    0.076 {built-in method builtins.exec}
        1    0.000    0.000    0.076    0.076 /home/azureuser/mssql-python/mssql_python/__init__.py:1(<module>)




================================================================================
C++ LAYER (Sequential Execution Order)
================================================================================

Platform: LINUX

Function                                              Calls    Total(ms)      Avg(μs)      Min(μs)      Max(μs)
--------------------------------------------------------------------------------------------------------------------

Driver & Connection:
  Connection::Connection                                  1        1.153       1153.0       1153.0       1153.0
  Connection::allocateDbcHandle                           1        1.138       1138.0       1138.0       1138.0
  Connection::connect                                     1      141.850     141850.0     141850.0     141850.0
  Connection::setAutocommit                               1        0.492        492.0        492.0        492.0

Statement Preparation:
  Connection::allocStatementHandle                        2        0.042         21.0         12.0         30.0

Query Execution:

Column Metadata:
  SQLNumResultCols_wrap                                   1        0.007          7.0          7.0          7.0
  SQLDescribeCol_wrap                                     3        0.497        165.7        105.0        236.0
  SQLBindColums                                           1        0.774        774.0        774.0        774.0

Data Fetching:
  FetchAll_wrap                                           1    22658.544   22658544.0   22658544.0   22658544.0
  FetchBatchData                                       1215    22655.679      18646.6         29.0     870990.0
  FetchBatchData::SQLFetchScroll_call                  1215     1773.554       1459.7          2.0       3051.0
  FetchBatchData::cache_column_metadata                1214       29.237         24.1         14.0         60.0
  FetchBatchData::construct_rows                       1214    16828.442      13862.0       2302.0      19747.0

Result Processing:
  SQLRowCount_wrap                                        1        0.018         18.0         18.0         18.0

Cleanup:
  SqlHandle::free                                         1        0.007          7.0          7.0          7.0

Other:
  Connection::connect::SQLDriverConnect_call              1      141.764     141764.0     141764.0     141764.0
  SQLDescribeCol_wrap::per_column                        33        0.349         10.6          7.0         41.0
  SQLGetAllDiagRecords                                    2        0.322        161.0        116.0        206.0
  SQLWCHARToWString                                      33        0.001          0.0          0.0          1.0
  WStringToSQLWCHAR                                       2        0.072         36.0         18.0         54.0
  construct_rows::all_columns_processing            1213170    13183.675         10.9          6.0       5705.0
  construct_rows::bigint_buffer_read                1213170        2.335          0.0          0.0        552.0
  construct_rows::bigint_c_api_assign               1213170        2.705          0.0          0.0        122.0
  construct_rows::int_buffer_read                   3639510        8.890          0.0          0.0       2039.0
  construct_rows::int_c_api_assign                  3639510      102.380          0.0          0.0        649.0
  construct_rows::per_row_total                     1213170    15654.802         12.9          8.0       5708.0
  construct_rows::pylist_creation                   1213170      140.972          0.1          0.0        407.0
  construct_rows::rows_append                       1213170        6.608          0.0          0.0        133.0
  construct_rows::smallint_buffer_read              1213170        1.627          0.0          0.0        130.0
  construct_rows::smallint_c_api_assign             1213170        2.019          0.0          0.0        279.0
  construct_rows::wstring_conversion                3298060       99.929          0.0          0.0        565.0

================================================================================
```

**🔥 THIS IS THE KEY INSIGHT!**

## **Windows vs Linux Performance - MASSIVE Difference:**

| Metric | Windows | Linux | Difference |
|--------|---------|-------|------------|
| **Total FetchAll** | **9.7s** | **22.7s** | **2.3x slower on Linux!** |
| **construct_rows** | **5.0s** | **16.8s** | **3.4x slower on Linux!** |
| **all_columns_processing** | **3.3s** | **13.2s** | **4.0x slower on Linux!** |
| **SQLFetchScroll** | 2.8s | 1.8s | Faster on Linux |

## **The Smoking Gun - Detailed Breakdown:**

### **Windows (Fast):**
```
construct_rows:                    5,010 ms (100%)
├─ per_row_total:                  4,449 ms (89%)
│  ├─ all_columns_processing:      3,344 ms (67%)
│  │  ├─ int_c_api_assign:            42 ms (0.8%)
│  │  ├─ int_buffer_read:              3 ms (0.1%)
│  │  ├─ bigint_c_api_assign:          1 ms (0.0%)
│  │  ├─ smallint_c_api_assign:        1 ms (0.0%)
│  │  └─ Missing:                  3,297 ms (98.6%) ← Still mystery, but MUCH smaller
│  ├─ pylist_creation:                55 ms (1.1%)
│  └─ rows_append:                     1 ms (0.0%)
```

### **Linux (Slow):**
```
construct_rows:                   16,828 ms (100%)
├─ per_row_total:                 15,655 ms (93%)
│  ├─ all_columns_processing:     13,184 ms (78%)
│  │  ├─ int_c_api_assign:           102 ms (0.6%)
│  │  ├─ wstring_conversion:         100 ms (0.6%) ← STRING OVERHEAD!
│  │  ├─ int_buffer_read:              9 ms (0.1%)
│  │  └─ Missing:                 12,973 ms (98.4%) ← 4x bigger than Windows!
│  ├─ pylist_creation:               141 ms (0.8%)
│  └─ rows_append:                     7 ms (0.0%)
```

## **Root Causes Identified:**

### **1. String Conversion Overhead (Linux-specific):**
- Windows: Native UTF-16, no conversion needed
- Linux: wchar_t is UTF-32, requires conversion
- Even with our PyUnicode_DecodeUTF16 optimization, strings still take 100ms on Linux vs ~0ms on Windows

### **2. The 13-second Mystery on Linux:**

The gap is **3x larger on Linux**:
- Windows gap: 3.3s (in all_columns_processing)
- Linux gap: 13.2s (in all_columns_processing)
- **Difference: ~10 seconds!**

**Possible reasons for Linux-specific slowness:**

1. **Memory access pattern penalty** - Linux/GCC may have worse cache behavior with our column-major buffers
2. **Branch prediction** - The switch(dataType) statement might be predicted poorly on Linux
3. **UnixODBC overhead** - Additional abstraction layer
4. **Compiler differences** - MSVC (Windows) vs GCC/Clang (Linux) optimization differences
5. **wchar_t size mismatch** - 2 bytes (Windows) vs 4 bytes (Linux) causing alignment issues

## **Next Steps to Close the Gap:**

**The data shows the bottleneck is platform-specific!** We should focus on Linux-specific optimizations:

1. **Profile Linux with perf or valgrind** - See cache misses, branch mispredictions
2. **Try row-major buffer layout** - Better cache locality for row-by-row processing
3. **Reduce switch statement overhead** - Use function pointers or template dispatch
4. **Pre-compute column offsets** - Avoid repeated pointer arithmetic

**Want me to try any of these approaches?**
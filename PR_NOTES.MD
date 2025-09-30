Looking at that Python GIL error during finalization, this is a classic C++ extension cleanup issue. Let me analyze what's likely happening:

## **GIL Error Analysis**

The error `"_Py_GetConfig: the function must be called with the GIL held"` during finalization means:
- Python is shutting down 
- Some C++ code is trying to access Python objects/functions
- But the GIL (Global Interpreter Lock) is already released
- This happens during module cleanup/destruction

## **Root Cause: Module Destruction Order**

When pytest exits, Python destroys modules in a specific order. The problem is likely:

1. **Python starts finalizing**
2. **Some modules get destroyed first** (including Python objects our C++ code references)
3. **Our C++ extension tries to clean up** (destructors, finalizers)
4. **C++ code calls Python functions** (like logging, object destruction)
5. **But GIL is already released** → CRASH

## **Likely Culprits in Our Codebase**

Based on our recent work, these are the most likely causes:

### 1. **Connection Pool Cleanup** (HIGH PROBABILITY)
- We discovered the pooling disable bug
- Pooling likely has background threads
- **During Python shutdown, these threads try to access Python objects**
- **But Python is already finalizing** → GIL error

### 2. **Connection Cleanup in Destructors**
- Connection objects have C++ destructors
- **Destructors might call Python logging functions**
- During shutdown, logging might already be destroyed

### 3. **Static Objects with Python References**
- C++ static objects holding Python object references
- **These destroy AFTER Python starts finalizing**
- When they try to release Python references → GIL error

## **Why This Happens Specifically in pytest**

pytest is more likely to trigger this because:
- **Runs many tests** → more connections created
- **Uses threading** for test execution
- **Complex shutdown sequence** with fixtures, plugins
- **Connection pooling** gets used across multiple tests
- **More C++ objects accumulate** before shutdown

## **Evidence From Our Testing**

This supports the theory:
- **Individual scripts complete fine** (simple shutdown)
- **pytest hangs/crashes** (complex shutdown with pooling)
- **The pooling disable bug we found** (background threads)
- **Exit code 134 (SIGABRT)** from Python runtime

## **Most Likely Sequence**

1. pytest runs multiple tests
2. Connection pooling creates background threads
3. Tests complete, pytest starts shutting down
4. **Python begins module finalization**
5. **Background pool threads are still running**
6. **Pool threads try to log/cleanup Python objects**  
7. **But Python's GIL is already released** → CRASH

## **The Smoking Gun**

The connection pooling bug we discovered (disable hanging) is likely the root cause. The hanging suggests **background threads that don't terminate properly**, and these same threads probably cause the GIL error during shutdown.

**Recommendation**: Fix the pooling cleanup bug first - it's likely causing both the hang AND the GIL finalization crash.


Logs:

(myvenv) gaurav@Gauravs-MacBook-Pro mssql-python % python -m pytest -v                                              
=========================================================== test session starts ===========================================================
platform darwin -- Python 3.13.3, pytest-8.4.1, pluggy-1.6.0 -- /Users/gaurav/Desktop/mssql-python/myvenv/bin/python
cachedir: .pytest_cache
rootdir: /Users/gaurav/Desktop/mssql-python
plugins: anyio-4.10.0, cov-6.2.1
collected 583 items                                                                                                                       

tests/test_000_dependencies.py::TestPlatformDetection::test_platform_detection PASSED                                               [  0%]
tests/test_000_dependencies.py::TestPlatformDetection::test_architecture_detection PASSED                                           [  0%]
tests/test_000_dependencies.py::TestPlatformDetection::test_module_directory_exists PASSED                                          [  0%]
tests/test_000_dependencies.py::TestDependencyFiles::test_platform_specific_dependencies PASSED                                     [  0%]
tests/test_000_dependencies.py::TestDependencyFiles::test_python_extension_exists PASSED                                            [  0%]
tests/test_000_dependencies.py::TestDependencyFiles::test_python_extension_loadable PASSED                                          [  1%]
tests/test_000_dependencies.py::TestArchitectureSpecificDependencies::test_windows_vcredist_dependency SKIPPED (Windows-specifi...) [  1%]
tests/test_000_dependencies.py::TestArchitectureSpecificDependencies::test_windows_auth_dependency SKIPPED (Windows-specific test)  [  1%]
tests/test_000_dependencies.py::TestArchitectureSpecificDependencies::test_macos_universal_dependencies PASSED                      [  1%]
tests/test_000_dependencies.py::TestArchitectureSpecificDependencies::test_linux_distribution_dependencies SKIPPED (Linux-speci...) [  1%]
tests/test_000_dependencies.py::TestDependencyContent::test_dependency_file_sizes PASSED                                            [  1%]
tests/test_000_dependencies.py::TestDependencyContent::test_python_extension_file_size PASSED                                       [  2%]
tests/test_000_dependencies.py::TestRuntimeCompatibility::test_python_extension_imports PASSED                                      [  2%]
tests/test_000_dependencies.py::test_ddbc_bindings_import PASSED                                                                    [  2%]
tests/test_000_dependencies.py::test_get_driver_path_from_ddbc_bindings PASSED                                                      [  2%]
tests/test_001_globals.py::test_apilevel PASSED                                                                                     [  2%]
tests/test_001_globals.py::test_threadsafety PASSED                                                                                 [  2%]
tests/test_001_globals.py::test_paramstyle PASSED                                                                                   [  3%]
tests/test_001_globals.py::test_lowercase PASSED                                                                                    [  3%]
tests/test_001_globals.py::test_decimal_separator PASSED                                                                            [  3%]
tests/test_001_globals.py::test_lowercase_thread_safety_no_db PASSED                                                                [  3%]
tests/test_001_globals.py::test_lowercase_concurrent_access_with_db PASSED                                                          [  3%]
tests/test_001_globals.py::test_decimal_separator_edge_cases PASSED                                                                 [  3%]
tests/test_001_globals.py::test_decimal_separator_with_db_operations PASSED                                                         [  4%]
tests/test_001_globals.py::test_decimal_separator_batch_operations PASSED                                                           [  4%]
tests/test_001_globals.py::test_decimal_separator_thread_safety PASSED                                                              [  4%]
tests/test_001_globals.py::test_decimal_separator_concurrent_db_operations PASSED                                                   [  4%]
tests/test_002_types.py::test_string_type PASSED                                                                                    [  4%]
tests/test_002_types.py::test_binary_type PASSED                                                                                    [  4%]
tests/test_002_types.py::test_number_type PASSED                                                                                    [  5%]
tests/test_002_types.py::test_datetime_type PASSED                                                                                  [  5%]
tests/test_002_types.py::test_rowid_type PASSED                                                                                     [  5%]
tests/test_002_types.py::test_date_constructor PASSED                                                                               [  5%]
tests/test_002_types.py::test_time_constructor PASSED                                                                               [  5%]
tests/test_002_types.py::test_timestamp_constructor PASSED                                                                          [  6%]
tests/test_002_types.py::test_date_from_ticks PASSED                                                                                [  6%]
tests/test_002_types.py::test_time_from_ticks PASSED                                                                                [  6%]
tests/test_002_types.py::test_timestamp_from_ticks PASSED                                                                           [  6%]
tests/test_002_types.py::test_binary_constructor PASSED                                                                             [  6%]
tests/test_003_connection.py::test_connection_string PASSED                                                                         [  6%]
tests/test_003_connection.py::test_connection PASSED                                                                                [  7%]
tests/test_003_connection.py::test_construct_connection_string PASSED                                                               [  7%]
tests/test_003_connection.py::test_connection_string_with_attrs_before PASSED                                                       [  7%]
tests/test_003_connection.py::test_connection_string_with_odbc_param PASSED                                                         [  7%]
tests/test_003_connection.py::test_autocommit_default PASSED                                                                        [  7%]
tests/test_003_connection.py::test_autocommit_setter PASSED                                                                         [  7%]
tests/test_003_connection.py::test_set_autocommit PASSED                                                                            [  8%]
tests/test_003_connection.py::test_commit PASSED                                                                                    [  8%]
tests/test_003_connection.py::test_rollback_on_close PASSED                                                                         [  8%]
tests/test_003_connection.py::test_rollback PASSED                                                                                  [  8%]
tests/test_003_connection.py::test_invalid_connection_string PASSED                                                                 [  8%]
tests/test_003_connection.py::test_connection_close PASSED                                                                          [  8%]
tests/test_003_connection.py::test_connection_pooling_speed PASSED                                                                  [  9%]
tests/test_003_connection.py::test_connection_pooling_reuse_spid PASSED                                                             [  9%]
tests/test_003_connection.py::test_pool_exhaustion_max_size_1 PASSED                                                                [  9%]
tests/test_003_connection.py::test_pool_idle_timeout_removes_connections PASSED                                                     [  9%]
tests/test_003_connection.py::test_connection_timeout_invalid_password PASSED                                                       [  9%]
tests/test_003_connection.py::test_connection_timeout_invalid_host PASSED                                                           [  9%]
tests/test_003_connection.py::test_pool_removes_invalid_connections PASSED                                                          [ 10%]
tests/test_003_connection.py::test_pool_recovery_after_failed_connection PASSED                                                     [ 10%]
tests/test_003_connection.py::test_pool_capacity_limit_and_overflow PASSED                                                          [ 10%]
tests/test_003_connection.py::test_connection_pooling_basic PASSED                                                                  [ 10%]
tests/test_003_connection.py::test_context_manager_commit PASSED                                                                    [ 10%]
tests/test_003_connection.py::test_context_manager_connection_closes PASSED                                                         [ 10%]
tests/test_003_connection.py::test_close_with_autocommit_true PASSED                                                                [ 11%]
tests/test_003_connection.py::test_setencoding_default_settings PASSED                                                              [ 11%]
tests/test_003_connection.py::test_setencoding_basic_functionality PASSED                                                           [ 11%]
tests/test_003_connection.py::test_setencoding_automatic_ctype_detection PASSED                                                     [ 11%]
tests/test_003_connection.py::test_setencoding_explicit_ctype_override PASSED                                                       [ 11%]
tests/test_003_connection.py::test_setencoding_none_parameters PASSED                                                               [ 12%]
tests/test_003_connection.py::test_setencoding_invalid_encoding PASSED                                                              [ 12%]
tests/test_003_connection.py::test_setencoding_invalid_ctype PASSED                                                                 [ 12%]
tests/test_003_connection.py::test_setencoding_closed_connection PASSED                                                             [ 12%]
tests/test_003_connection.py::test_setencoding_constants_access PASSED                                                              [ 12%]
tests/test_003_connection.py::test_setencoding_with_constants PASSED                                                                [ 12%]
tests/test_003_connection.py::test_setencoding_common_encodings PASSED                                                              [ 13%]
tests/test_003_connection.py::test_setencoding_persistence_across_cursors PASSED                                                    [ 13%]
tests/test_003_connection.py::test_setencoding_with_unicode_data SKIPPED (Skipping Unicode data tests till we have support for ...) [ 13%]
tests/test_003_connection.py::test_setencoding_before_and_after_operations PASSED                                                   [ 13%]
tests/test_003_connection.py::test_getencoding_default PASSED                                                                       [ 13%]
tests/test_003_connection.py::test_getencoding_returns_copy PASSED                                                                  [ 13%]
tests/test_003_connection.py::test_getencoding_closed_connection PASSED                                                             [ 14%]
tests/test_003_connection.py::test_setencoding_getencoding_consistency PASSED                                                       [ 14%]
tests/test_003_connection.py::test_setencoding_default_encoding PASSED                                                              [ 14%]
tests/test_003_connection.py::test_setencoding_utf8 PASSED                                                                          [ 14%]
tests/test_003_connection.py::test_setencoding_latin1 PASSED                                                                        [ 14%]
tests/test_003_connection.py::test_setencoding_with_explicit_ctype_sql_char PASSED                                                  [ 14%]
tests/test_003_connection.py::test_setencoding_with_explicit_ctype_sql_wchar PASSED                                                 [ 15%]
tests/test_003_connection.py::test_setencoding_invalid_ctype_error PASSED                                                           [ 15%]
tests/test_003_connection.py::test_setencoding_case_insensitive_encoding PASSED                                                     [ 15%]
tests/test_003_connection.py::test_setencoding_none_encoding_default PASSED                                                         [ 15%]
tests/test_003_connection.py::test_setencoding_override_previous PASSED                                                             [ 15%]
tests/test_003_connection.py::test_setencoding_ascii PASSED                                                                         [ 15%]
tests/test_003_connection.py::test_setencoding_cp1252 PASSED                                                                        [ 16%]
tests/test_003_connection.py::test_setdecoding_default_settings PASSED                                                              [ 16%]
tests/test_003_connection.py::test_setdecoding_basic_functionality PASSED                                                           [ 16%]
tests/test_003_connection.py::test_setdecoding_automatic_ctype_detection PASSED                                                     [ 16%]
tests/test_003_connection.py::test_setdecoding_explicit_ctype_override PASSED                                                       [ 16%]
tests/test_003_connection.py::test_setdecoding_none_parameters PASSED                                                               [ 16%]
tests/test_003_connection.py::test_setdecoding_invalid_sqltype PASSED                                                               [ 17%]
tests/test_003_connection.py::test_setdecoding_invalid_encoding PASSED                                                              [ 17%]
tests/test_003_connection.py::test_setdecoding_invalid_ctype PASSED                                                                 [ 17%]
tests/test_003_connection.py::test_setdecoding_closed_connection PASSED                                                             [ 17%]
tests/test_003_connection.py::test_setdecoding_constants_access PASSED                                                              [ 17%]
tests/test_003_connection.py::test_setdecoding_with_constants PASSED                                                                [ 18%]
tests/test_003_connection.py::test_setdecoding_common_encodings PASSED                                                              [ 18%]
tests/test_003_connection.py::test_setdecoding_case_insensitive_encoding PASSED                                                     [ 18%]
tests/test_003_connection.py::test_setdecoding_independent_sql_types PASSED                                                         [ 18%]
tests/test_003_connection.py::test_setdecoding_override_previous PASSED                                                             [ 18%]
tests/test_003_connection.py::test_getdecoding_invalid_sqltype PASSED                                                               [ 18%]
tests/test_003_connection.py::test_getdecoding_closed_connection PASSED                                                             [ 19%]
tests/test_003_connection.py::test_getdecoding_returns_copy PASSED                                                                  [ 19%]
tests/test_003_connection.py::test_setdecoding_getdecoding_consistency PASSED                                                       [ 19%]
tests/test_003_connection.py::test_setdecoding_persistence_across_cursors PASSED                                                    [ 19%]
tests/test_003_connection.py::test_setdecoding_before_and_after_operations PASSED                                                   [ 19%]
tests/test_003_connection.py::test_setdecoding_all_sql_types_independently PASSED                                                   [ 19%]
tests/test_003_connection.py::test_setdecoding_security_logging PASSED                                                              [ 20%]
tests/test_003_connection.py::test_setdecoding_with_unicode_data SKIPPED (Skipping Unicode data tests till we have support for ...) [ 20%]
tests/test_003_connection.py::test_connection_exception_attributes_exist PASSED                                                     [ 20%]
tests/test_003_connection.py::test_connection_exception_attributes_are_classes PASSED                                               [ 20%]
tests/test_003_connection.py::test_connection_exception_inheritance PASSED                                                          [ 20%]
tests/test_003_connection.py::test_connection_exception_instantiation PASSED                                                        [ 20%]
tests/test_003_connection.py::test_connection_exception_catching_with_connection_attributes PASSED                                  [ 21%]
tests/test_003_connection.py::test_connection_exception_error_handling_example PASSED                                               [ 21%]
tests/test_003_connection.py::test_connection_exception_multi_connection_scenario PASSED                                            [ 21%]
tests/test_003_connection.py::test_connection_exception_attributes_consistency PASSED                                               [ 21%]
tests/test_003_connection.py::test_connection_exception_attributes_comprehensive_list PASSED                                        [ 21%]
tests/test_003_connection.py::test_connection_execute PASSED                                                                        [ 21%]
tests/test_003_connection.py::test_connection_execute_error_handling PASSED                                                         [ 22%]
tests/test_003_connection.py::test_connection_execute_empty_result PASSED                                                           [ 22%]
tests/test_003_connection.py::test_connection_execute_different_parameter_types PASSED                                              [ 22%]
tests/test_003_connection.py::test_connection_execute_with_transaction PASSED                                                       [ 22%]
tests/test_003_connection.py::test_connection_execute_vs_cursor_execute PASSED                                                      [ 22%]
tests/test_003_connection.py::test_connection_execute_many_parameters PASSED                                                        [ 22%]
tests/test_003_connection.py::test_execute_after_connection_close PASSED                                                            [ 23%]
tests/test_003_connection.py::test_execute_multiple_simultaneous_cursors PASSED                                                     [ 23%]
tests/test_003_connection.py::test_execute_with_large_parameters PASSED                                                             [ 23%]
tests/test_003_connection.py::test_connection_execute_cursor_lifecycle PASSED                                                       [ 23%]
tests/test_003_connection.py::test_batch_execute_basic PASSED                                                                       [ 23%]
tests/test_003_connection.py::test_batch_execute_with_parameters PASSED                                                             [ 24%]
tests/test_003_connection.py::test_batch_execute_dml_statements PASSED                                                              [ 24%]
tests/test_003_connection.py::test_batch_execute_reuse_cursor PASSED                                                                [ 24%]
tests/test_003_connection.py::test_batch_execute_auto_close PASSED                                                                  [ 24%]
tests/test_003_connection.py::test_batch_execute_transaction PASSED                                                                 [ 24%]
tests/test_003_connection.py::test_batch_execute_error_handling PASSED                                                              [ 24%]
tests/test_003_connection.py::test_batch_execute_input_validation PASSED                                                            [ 25%]
tests/test_003_connection.py::test_batch_execute_large_batch PASSED                                                                 [ 25%]
tests/test_003_connection.py::test_add_output_converter PASSED                                                                      [ 25%]
tests/test_003_connection.py::test_get_output_converter PASSED                                                                      [ 25%]
tests/test_003_connection.py::test_remove_output_converter PASSED                                                                   [ 25%]
tests/test_003_connection.py::test_clear_output_converters PASSED                                                                   [ 25%]
tests/test_003_connection.py::test_converter_integration PASSED                                                                     [ 26%]
tests/test_003_connection.py::test_output_converter_with_null_values PASSED                                                         [ 26%]
tests/test_003_connection.py::test_chaining_output_converters PASSED                                                                [ 26%]
tests/test_003_connection.py::test_temporary_converter_replacement PASSED                                                           [ 26%]
tests/test_003_connection.py::test_multiple_output_converters PASSED                                                                [ 26%]
tests/test_003_connection.py::test_output_converter_exception_handling PASSED                                                       [ 26%]
tests/test_003_connection.py::test_timeout_default PASSED                                                                           [ 27%]
tests/test_003_connection.py::test_timeout_setter PASSED                                                                            [ 27%]
tests/test_003_connection.py::test_timeout_from_constructor PASSED                                                                  [ 27%]
tests/test_003_connection.py::test_timeout_long_query PASSED                                                                        [ 27%]
tests/test_003_connection.py::test_timeout_affects_all_cursors PASSED                                                               [ 27%]
tests/test_003_connection.py::test_getinfo_basic_driver_info PASSED                                                                 [ 27%]
tests/test_003_connection.py::test_getinfo_sql_support PASSED                                                                       [ 28%]
tests/test_003_connection.py::test_getinfo_numeric_limits PASSED                                                                    [ 28%]
tests/test_003_connection.py::test_getinfo_catalog_support PASSED                                                                   [ 28%]
tests/test_003_connection.py::test_getinfo_transaction_support PASSED                                                               [ 28%]
tests/test_003_connection.py::test_getinfo_data_types PASSED                                                                        [ 28%]
tests/test_003_connection.py::test_getinfo_invalid_info_type PASSED                                                                 [ 28%]
tests/test_003_connection.py::test_getinfo_type_consistency PASSED                                                                  [ 29%]
tests/test_003_connection.py::test_getinfo_standard_types PASSED                                                                    [ 29%]
tests/test_003_connection.py::test_getinfo_invalid_binary_data PASSED                                                               [ 29%]
tests/test_003_connection.py::test_getinfo_zero_length_return PASSED                                                                [ 29%]
tests/test_003_connection.py::test_getinfo_non_standard_types PASSED                                                                [ 29%]
tests/test_003_connection.py::test_getinfo_yes_no_bytes_handling PASSED                                                             [ 30%]
tests/test_003_connection.py::test_getinfo_numeric_bytes_conversion PASSED                                                          [ 30%]
tests/test_003_connection.py::test_connection_searchescape_basic PASSED                                                             [ 30%]
tests/test_003_connection.py::test_connection_searchescape_with_percent PASSED                                                      [ 30%]
tests/test_003_connection.py::test_connection_searchescape_with_underscore PASSED                                                   [ 30%]
tests/test_003_connection.py::test_connection_searchescape_with_brackets PASSED                                                     [ 30%]
tests/test_003_connection.py::test_connection_searchescape_multiple_escapes PASSED                                                  [ 31%]
tests/test_003_connection.py::test_connection_searchescape_consistency PASSED                                                       [ 31%]
tests/test_003_connection.py::test_attrs_before_access_token_attribute FAILED                                                       [ 31%]
tests/test_003_connection.py::test_attrs_before_integer_valued_attribute_unsupported PASSED                                         [ 31%]
tests/test_003_connection.py::test_attrs_before_bytearray_instead_of_bytes FAILED                                                   [ 31%]
tests/test_003_connection.py::test_attrs_before_unsupported_value_type PASSED                                                       [ 31%]
tests/test_003_connection.py::test_attrs_before_invalid_attribute_key PASSED                                                        [ 32%]
tests/test_003_connection.py::test_attrs_before_non_integer_key PASSED                                                              [ 32%]
tests/test_003_connection.py::test_attrs_before_empty_dict PASSED                                                                   [ 32%]
tests/test_003_connection.py::test_attrs_before_multiple_attributes PASSED                                                          [ 32%]
tests/test_003_connection.py::test_attrs_before_memory_safety_binary_data PASSED                                                    [ 32%]
tests/test_003_connection.py::test_attrs_before_with_autocommit_compatibility PASSED                                                [ 32%]
tests/test_004_cursor.py::test_cursor PASSED                                                                                        [ 33%]
tests/test_004_cursor.py::test_empty_string_handling PASSED                                                                         [ 33%]
tests/test_004_cursor.py::test_empty_binary_handling PASSED                                                                         [ 33%]
tests/test_004_cursor.py::test_mixed_empty_and_null_values PASSED                                                                   [ 33%]
tests/test_004_cursor.py::test_empty_string_edge_cases PASSED                                                                       [ 33%]
tests/test_004_cursor.py::test_insert_id_column PASSED                                                                              [ 33%]
tests/test_004_cursor.py::test_insert_bit_column PASSED                                                                             [ 34%]
tests/test_004_cursor.py::test_insert_nvarchar_column PASSED                                                                        [ 34%]
tests/test_004_cursor.py::test_insert_time_column PASSED                                                                            [ 34%]
tests/test_004_cursor.py::test_insert_datetime_column PASSED                                                                        [ 34%]
tests/test_004_cursor.py::test_insert_datetime2_column PASSED                                                                       [ 34%]
tests/test_004_cursor.py::test_insert_smalldatetime_column PASSED                                                                   [ 34%]
tests/test_004_cursor.py::test_insert_date_column PASSED                                                                            [ 35%]
tests/test_004_cursor.py::test_insert_real_column PASSED                                                                            [ 35%]
tests/test_004_cursor.py::test_insert_decimal_column PASSED                                                                         [ 35%]
tests/test_004_cursor.py::test_insert_tinyint_column PASSED                                                                         [ 35%]
tests/test_004_cursor.py::test_insert_smallint_column PASSED                                                                        [ 35%]
tests/test_004_cursor.py::test_insert_bigint_column PASSED                                                                          [ 36%]
tests/test_004_cursor.py::test_insert_integer_column PASSED                                                                         [ 36%]
tests/test_004_cursor.py::test_insert_float_column PASSED                                                                           [ 36%]
tests/test_004_cursor.py::test_varchar_full_capacity PASSED                                                                         [ 36%]
tests/test_004_cursor.py::test_wvarchar_full_capacity PASSED                                                                        [ 36%]
tests/test_004_cursor.py::test_varbinary_full_capacity PASSED                                                                       [ 36%]
tests/test_004_cursor.py::test_varbinary_max PASSED                                                                                 [ 37%]
tests/test_004_cursor.py::test_longvarchar PASSED                                                                                   [ 37%]
tests/test_004_cursor.py::test_longwvarchar PASSED                                                                                  [ 37%]
tests/test_004_cursor.py::test_longvarbinary PASSED                                                                                 [ 37%]
tests/test_004_cursor.py::test_create_table PASSED                                                                                  [ 37%]
tests/test_004_cursor.py::test_insert_args PASSED                                                                                   [ 37%]
tests/test_004_cursor.py::test_parametrized_insert[data0] PASSED                                                                    [ 38%]
tests/test_004_cursor.py::test_parametrized_insert[data1] PASSED                                                                    [ 38%]
tests/test_004_cursor.py::test_parametrized_insert[data2] PASSED                                                                    [ 38%]
tests/test_004_cursor.py::test_parametrized_insert[data3] PASSED                                                                    [ 38%]
tests/test_004_cursor.py::test_rowcount PASSED                                                                                      [ 38%]
tests/test_004_cursor.py::test_rowcount_executemany PASSED                                                                          [ 38%]
tests/test_004_cursor.py::test_fetchone PASSED                                                                                      [ 39%]
tests/test_004_cursor.py::test_fetchmany PASSED                                                                                     [ 39%]
tests/test_004_cursor.py::test_fetchmany_with_arraysize PASSED                                                                      [ 39%]
tests/test_004_cursor.py::test_fetchall PASSED                                                                                      [ 39%]
tests/test_004_cursor.py::test_execute_invalid_query PASSED                                                                         [ 39%]
tests/test_004_cursor.py::test_arraysize PASSED                                                                                     [ 39%]
tests/test_004_cursor.py::test_description PASSED                                                                                   [ 40%]
tests/test_004_cursor.py::test_execute_many PASSED                                                                                  [ 40%]
tests/test_004_cursor.py::test_executemany_empty_strings PASSED                                                                     [ 40%]
tests/test_004_cursor.py::test_executemany_empty_strings_various_types PASSED                                                       [ 40%]
tests/test_004_cursor.py::test_executemany_unicode_and_empty_strings PASSED                                                         [ 40%]
tests/test_004_cursor.py::test_executemany_large_batch_with_empty_strings PASSED                                                    [ 40%]
tests/test_004_cursor.py::test_executemany_compare_with_execute PASSED                                                              [ 41%]
tests/test_004_cursor.py::test_executemany_edge_cases_empty_strings PASSED                                                          [ 41%]
tests/test_004_cursor.py::test_executemany_null_vs_empty_string PASSED                                                              [ 41%]
tests/test_004_cursor.py::test_executemany_binary_data_edge_cases PASSED                                                            [ 41%]
tests/test_004_cursor.py::test_nextset PASSED                                                                                       [ 41%]
tests/test_004_cursor.py::test_delete_table PASSED                                                                                  [ 42%]
tests/test_004_cursor.py::test_create_tables_for_join PASSED                                                                        [ 42%]
tests/test_004_cursor.py::test_insert_data_for_join PASSED                                                                          [ 42%]
tests/test_004_cursor.py::test_join_operations PASSED                                                                               [ 42%]
tests/test_004_cursor.py::test_join_operations_with_parameters PASSED                                                               [ 42%]
tests/test_004_cursor.py::test_create_stored_procedure PASSED                                                                       [ 42%]
tests/test_004_cursor.py::test_execute_stored_procedure_with_parameters PASSED                                                      [ 43%]
tests/test_004_cursor.py::test_execute_stored_procedure_without_parameters PASSED                                                   [ 43%]
tests/test_004_cursor.py::test_drop_stored_procedure PASSED                                                                         [ 43%]
tests/test_004_cursor.py::test_drop_tables_for_join PASSED                                                                          [ 43%]
tests/test_004_cursor.py::test_cursor_description PASSED                                                                            [ 43%]
tests/test_004_cursor.py::test_parse_datetime PASSED                                                                                [ 43%]
tests/test_004_cursor.py::test_parse_date PASSED                                                                                    [ 44%]
tests/test_004_cursor.py::test_parse_time PASSED                                                                                    [ 44%]
tests/test_004_cursor.py::test_parse_smalldatetime PASSED                                                                           [ 44%]
tests/test_004_cursor.py::test_parse_datetime2 PASSED                                                                               [ 44%]
tests/test_004_cursor.py::test_get_numeric_data PASSED                                                                              [ 44%]
tests/test_004_cursor.py::test_none PASSED                                                                                          [ 44%]
tests/test_004_cursor.py::test_boolean PASSED                                                                                       [ 45%]
tests/test_004_cursor.py::test_sql_wvarchar PASSED                                                                                  [ 45%]
tests/test_004_cursor.py::test_sql_varchar PASSED                                                                                   [ 45%]
tests/test_004_cursor.py::test_numeric_precision_scale_positive_exponent PASSED                                                     [ 45%]
tests/test_004_cursor.py::test_numeric_precision_scale_negative_exponent PASSED                                                     [ 45%]
tests/test_004_cursor.py::test_row_attribute_access PASSED                                                                          [ 45%]
tests/test_004_cursor.py::test_row_comparison_with_list PASSED                                                                      [ 46%]
tests/test_004_cursor.py::test_row_string_representation PASSED                                                                     [ 46%]
tests/test_004_cursor.py::test_row_column_mapping PASSED                                                                            [ 46%]
tests/test_004_cursor.py::test_lowercase_setting_after_cursor_creation PASSED                                                       [ 46%]
tests/test_004_cursor.py::test_concurrent_cursors_different_lowercase_settings SKIPPED (Future work: relevant if per-cursor low...) [ 46%]
tests/test_004_cursor.py::test_cursor_context_manager_basic PASSED                                                                  [ 46%]
tests/test_004_cursor.py::test_cursor_context_manager_autocommit_true PASSED                                                        [ 47%]
tests/test_004_cursor.py::test_cursor_context_manager_closes_cursor PASSED                                                          [ 47%]
tests/test_004_cursor.py::test_cursor_context_manager_no_auto_commit PASSED                                                         [ 47%]
tests/test_004_cursor.py::test_cursor_context_manager_exception_handling PASSED                                                     [ 47%]
tests/test_004_cursor.py::test_cursor_context_manager_transaction_behavior PASSED                                                   [ 47%]
tests/test_004_cursor.py::test_cursor_context_manager_nested PASSED                                                                 [ 48%]
tests/test_004_cursor.py::test_cursor_context_manager_multiple_operations PASSED                                                    [ 48%]
tests/test_004_cursor.py::test_cursor_with_contextlib_closing PASSED                                                                [ 48%]
tests/test_004_cursor.py::test_cursor_context_manager_enter_returns_self PASSED                                                     [ 48%]
tests/test_004_cursor.py::test_execute_returns_self PASSED                                                                          [ 48%]
tests/test_004_cursor.py::test_execute_fetchone_chaining PASSED                                                                     [ 48%]
tests/test_004_cursor.py::test_execute_fetchall_chaining PASSED                                                                     [ 49%]
tests/test_004_cursor.py::test_execute_fetchmany_chaining PASSED                                                                    [ 49%]
tests/test_004_cursor.py::test_execute_rowcount_chaining PASSED                                                                     [ 49%]
tests/test_004_cursor.py::test_execute_description_chaining PASSED                                                                  [ 49%]
tests/test_004_cursor.py::test_multiple_chaining_operations PASSED                                                                  [ 49%]
tests/test_004_cursor.py::test_chaining_with_parameters PASSED                                                                      [ 49%]
tests/test_004_cursor.py::test_chaining_with_iteration PASSED                                                                       [ 50%]
tests/test_004_cursor.py::test_cursor_next_functionality PASSED                                                                     [ 50%]
tests/test_004_cursor.py::test_cursor_next_with_different_data_types PASSED                                                         [ 50%]
tests/test_004_cursor.py::test_cursor_next_error_conditions PASSED                                                                  [ 50%]
tests/test_004_cursor.py::test_future_iterator_protocol_compatibility PASSED                                                        [ 50%]
tests/test_004_cursor.py::test_chaining_error_handling PASSED                                                                       [ 50%]
tests/test_004_cursor.py::test_chaining_performance_statement_reuse PASSED                                                          [ 51%]
tests/test_004_cursor.py::test_execute_chaining_compatibility_examples PASSED                                                       [ 51%]
tests/test_004_cursor.py::test_rownumber_basic_functionality PASSED                                                                 [ 51%]
tests/test_004_cursor.py::test_cursor_rownumber_mixed_fetches PASSED                                                                [ 51%]
tests/test_004_cursor.py::test_cursor_rownumber_empty_results PASSED                                                                [ 51%]
tests/test_004_cursor.py::test_rownumber_warning_logged PASSED                                                                      [ 51%]
tests/test_004_cursor.py::test_rownumber_closed_cursor PASSED                                                                       [ 52%]
tests/test_004_cursor.py::test_cursor_rownumber_fetchall PASSED                                                                     [ 52%]
tests/test_004_cursor.py::test_nextset_with_different_result_sizes_safe PASSED                                                      [ 52%]
tests/test_004_cursor.py::test_nextset_basic_functionality_only PASSED                                                              [ 52%]
tests/test_004_cursor.py::test_nextset_memory_safety_check PASSED                                                                   [ 52%]
tests/test_004_cursor.py::test_nextset_error_conditions_safe PASSED                                                                 [ 53%]
tests/test_004_cursor.py::test_nextset_diagnostics PASSED                                                                           [ 53%]
tests/test_004_cursor.py::test_fetchval_basic_functionality PASSED                                                                  [ 53%]
tests/test_004_cursor.py::test_fetchval_different_data_types PASSED                                                                 [ 53%]
tests/test_004_cursor.py::test_fetchval_null_values PASSED                                                                          [ 53%]
tests/test_004_cursor.py::test_fetchval_no_results PASSED                                                                           [ 53%]
tests/test_004_cursor.py::test_fetchval_multiple_columns PASSED                                                                     [ 54%]
tests/test_004_cursor.py::test_fetchval_multiple_rows PASSED                                                                        [ 54%]
tests/test_004_cursor.py::test_fetchval_method_chaining PASSED                                                                      [ 54%]
tests/test_004_cursor.py::test_fetchval_closed_cursor PASSED                                                                        [ 54%]
tests/test_004_cursor.py::test_fetchval_rownumber_tracking PASSED                                                                   [ 54%]
tests/test_004_cursor.py::test_fetchval_aggregate_functions PASSED                                                                  [ 54%]
tests/test_004_cursor.py::test_fetchval_empty_result_set_edge_cases PASSED                                                          [ 55%]
tests/test_004_cursor.py::test_fetchval_error_scenarios PASSED                                                                      [ 55%]
tests/test_004_cursor.py::test_fetchval_performance_common_patterns PASSED                                                          [ 55%]
tests/test_004_cursor.py::test_cursor_commit_basic PASSED                                                                           [ 55%]
tests/test_004_cursor.py::test_cursor_rollback_basic PASSED                                                                         [ 55%]
tests/test_004_cursor.py::test_cursor_commit_affects_all_cursors PASSED                                                             [ 55%]
tests/test_004_cursor.py::test_cursor_rollback_affects_all_cursors PASSED                                                           [ 56%]
tests/test_004_cursor.py::test_cursor_commit_closed_cursor PASSED                                                                   [ 56%]
tests/test_004_cursor.py::test_cursor_rollback_closed_cursor PASSED                                                                 [ 56%]
tests/test_004_cursor.py::test_cursor_commit_equivalent_to_connection_commit PASSED                                                 [ 56%]
tests/test_004_cursor.py::test_cursor_transaction_boundary_behavior PASSED                                                          [ 56%]
tests/test_004_cursor.py::test_cursor_commit_with_method_chaining PASSED                                                            [ 56%]
tests/test_004_cursor.py::test_cursor_commit_error_scenarios PASSED                                                                 [ 57%]
tests/test_004_cursor.py::test_cursor_commit_performance_patterns PASSED                                                            [ 57%]
tests/test_004_cursor.py::test_cursor_rollback_error_scenarios PASSED                                                               [ 57%]
tests/test_004_cursor.py::test_cursor_rollback_with_method_chaining PASSED                                                          [ 57%]
tests/test_004_cursor.py::test_cursor_rollback_savepoints_simulation PASSED                                                         [ 57%]
tests/test_004_cursor.py::test_cursor_rollback_performance_patterns PASSED                                                          [ 57%]
tests/test_004_cursor.py::test_cursor_rollback_equivalent_to_connection_rollback PASSED                                             [ 58%]
tests/test_004_cursor.py::test_cursor_rollback_nested_transactions_simulation PASSED                                                [ 58%]
tests/test_004_cursor.py::test_cursor_rollback_data_consistency PASSED                                                              [ 58%]
tests/test_004_cursor.py::test_cursor_rollback_large_transaction PASSED                                                             [ 58%]
tests/test_004_cursor.py::test_scroll_relative_basic PASSED                                                                         [ 58%]
tests/test_004_cursor.py::test_scroll_absolute_basic PASSED                                                                         [ 59%]
tests/test_004_cursor.py::test_scroll_backward_not_supported PASSED                                                                 [ 59%]
tests/test_004_cursor.py::test_scroll_on_empty_result_set_raises PASSED                                                             [ 59%]
tests/test_004_cursor.py::test_scroll_mixed_fetches_consume_correctly PASSED                                                        [ 59%]
tests/test_004_cursor.py::test_scroll_edge_cases_and_validation PASSED                                                              [ 59%]
tests/test_004_cursor.py::test_cursor_skip_basic_functionality PASSED                                                               [ 59%]
tests/test_004_cursor.py::test_cursor_skip_zero_is_noop PASSED                                                                      [ 60%]
tests/test_004_cursor.py::test_cursor_skip_empty_result_set PASSED                                                                  [ 60%]
tests/test_004_cursor.py::test_cursor_skip_past_end PASSED                                                                          [ 60%]
tests/test_004_cursor.py::test_cursor_skip_invalid_arguments PASSED                                                                 [ 60%]
tests/test_004_cursor.py::test_cursor_skip_closed_cursor PASSED                                                                     [ 60%]
tests/test_004_cursor.py::test_cursor_skip_integration_with_fetch_methods PASSED                                                    [ 60%]
tests/test_004_cursor.py::test_cursor_messages_basic PASSED                                                                         [ 61%]
tests/test_004_cursor.py::test_cursor_messages_clearing PASSED                                                                      [ 61%]
tests/test_004_cursor.py::test_cursor_messages_preservation_across_fetches PASSED                                                   [ 61%]
tests/test_004_cursor.py::test_cursor_messages_multiple PASSED                                                                      [ 61%]
tests/test_004_cursor.py::test_cursor_messages_format PASSED                                                                        [ 61%]
tests/test_004_cursor.py::test_cursor_messages_with_warnings PASSED                                                                 [ 61%]
tests/test_004_cursor.py::test_cursor_messages_manual_clearing PASSED                                                               [ 62%]
tests/test_004_cursor.py::test_cursor_messages_executemany PASSED                                                                   [ 62%]
tests/test_004_cursor.py::test_cursor_messages_with_error PASSED                                                                    [ 62%]
tests/test_004_cursor.py::test_tables_setup PASSED                                                                                  [ 62%]
tests/test_004_cursor.py::test_tables_all PASSED                                                                                    [ 62%]
tests/test_004_cursor.py::test_tables_specific_table PASSED                                                                         [ 62%]
tests/test_004_cursor.py::test_tables_with_table_pattern PASSED                                                                     [ 63%]
tests/test_004_cursor.py::test_tables_with_schema_pattern PASSED                                                                    [ 63%]
tests/test_004_cursor.py::test_tables_with_type_filter PASSED                                                                       [ 63%]
tests/test_004_cursor.py::test_tables_with_multiple_types PASSED                                                                    [ 63%]
tests/test_004_cursor.py::test_tables_catalog_filter PASSED                                                                         [ 63%]
tests/test_004_cursor.py::test_tables_nonexistent PASSED                                                                            [ 63%]
tests/test_004_cursor.py::test_tables_combined_filters PASSED                                                                       [ 64%]
tests/test_004_cursor.py::test_tables_result_processing PASSED                                                                      [ 64%]
tests/test_004_cursor.py::test_tables_method_chaining PASSED                                                                        [ 64%]
tests/test_004_cursor.py::test_tables_cleanup PASSED                                                                                [ 64%]
tests/test_004_cursor.py::test_emoji_round_trip PASSED                                                                              [ 64%]
tests/test_004_cursor.py::test_varcharmax_transaction_rollback PASSED                                                               [ 65%]
tests/test_004_cursor.py::test_nvarcharmax_transaction_rollback PASSED                                                              [ 65%]
tests/test_004_cursor.py::test_empty_char_single_and_batch_fetch PASSED                                                             [ 65%]
tests/test_004_cursor.py::test_empty_varbinary_batch_fetch PASSED                                                                   [ 65%]
tests/test_004_cursor.py::test_empty_values_fetchmany PASSED                                                                        [ 65%]
tests/test_004_cursor.py::test_sql_no_total_large_data_scenario PASSED                                                              [ 65%]
tests/test_004_cursor.py::test_batch_fetch_empty_values_no_assertion_failure PASSED                                                 [ 66%]
tests/test_004_cursor.py::test_executemany_utf16_length_validation PASSED                                                           [ 66%]
tests/test_004_cursor.py::test_binary_data_over_8000_bytes PASSED                                                                   [ 66%]
tests/test_004_cursor.py::test_varbinarymax_insert_fetch PASSED                                                                     [ 66%]
tests/test_004_cursor.py::test_all_empty_binaries PASSED                                                                            [ 66%]
tests/test_004_cursor.py::test_mixed_bytes_and_bytearray_types PASSED                                                               [ 66%]
tests/test_004_cursor.py::test_binary_mostly_small_one_large PASSED                                                                 [ 67%]
tests/test_004_cursor.py::test_varbinarymax_insert_fetch_null PASSED                                                                [ 67%]
tests/test_004_cursor.py::test_only_null_and_empty_binary PASSED                                                                    [ 67%]
tests/test_004_cursor.py::test_varcharmax_short_fetch PASSED                                                                        [ 67%]
tests/test_004_cursor.py::test_varcharmax_empty_string PASSED                                                                       [ 67%]
tests/test_004_cursor.py::test_varcharmax_null PASSED                                                                               [ 67%]
tests/test_004_cursor.py::test_varcharmax_boundary PASSED                                                                           [ 68%]
tests/test_004_cursor.py::test_varcharmax_streaming PASSED                                                                          [ 68%]
tests/test_004_cursor.py::test_varcharmax_large PASSED                                                                              [ 68%]
tests/test_004_cursor.py::test_nvarcharmax_short_fetch PASSED                                                                       [ 68%]
tests/test_004_cursor.py::test_nvarcharmax_empty_string PASSED                                                                      [ 68%]
tests/test_004_cursor.py::test_nvarcharmax_null PASSED                                                                              [ 68%]
tests/test_004_cursor.py::test_nvarcharmax_boundary PASSED                                                                          [ 69%]
tests/test_004_cursor.py::test_nvarcharmax_streaming PASSED                                                                         [ 69%]
tests/test_004_cursor.py::test_nvarcharmax_large PASSED                                                                             [ 69%]
tests/test_004_cursor.py::test_money_smallmoney_insert_fetch PASSED                                                                 [ 69%]
tests/test_004_cursor.py::test_money_smallmoney_null_handling PASSED                                                                [ 69%]
tests/test_004_cursor.py::test_money_smallmoney_roundtrip PASSED                                                                    [ 69%]
tests/test_004_cursor.py::test_money_smallmoney_boundaries PASSED                                                                   [ 70%]
tests/test_004_cursor.py::test_money_smallmoney_invalid_values PASSED                                                               [ 70%]
tests/test_004_cursor.py::test_money_smallmoney_roundtrip_executemany PASSED                                                        [ 70%]
tests/test_004_cursor.py::test_money_smallmoney_executemany_null_handling PASSED                                                    [ 70%]
tests/test_004_cursor.py::test_money_smallmoney_out_of_range_low PASSED                                                             [ 70%]
tests/test_004_cursor.py::test_uuid_insert_and_select_none PASSED                                                                   [ 71%]
tests/test_004_cursor.py::test_insert_multiple_uuids PASSED                                                                         [ 71%]
tests/test_004_cursor.py::test_fetchmany_uuids PASSED                                                                               [ 71%]
tests/test_004_cursor.py::test_uuid_insert_with_none PASSED                                                                         [ 71%]
tests/test_004_cursor.py::test_invalid_uuid_inserts PASSED                                                                          [ 71%]
tests/test_004_cursor.py::test_duplicate_uuid_inserts PASSED                                                                        [ 71%]
tests/test_004_cursor.py::test_extreme_uuids PASSED                                                                                 [ 72%]
tests/test_004_cursor.py::test_executemany_uuid_insert_and_select PASSED                                                            [ 72%]
tests/test_004_cursor.py::test_executemany_uuid_roundtrip_fixed_value PASSED                                                        [ 72%]
tests/test_004_cursor.py::test_decimal_separator_with_multiple_values PASSED                                                        [ 72%]
tests/test_004_cursor.py::test_decimal_separator_calculations PASSED                                                                [ 72%]
tests/test_004_cursor.py::test_decimal_separator_function PASSED                                                                    [ 72%]
tests/test_004_cursor.py::test_decimal_separator_basic_functionality PASSED                                                         [ 73%]
tests/test_004_cursor.py::test_lowercase_attribute PASSED                                                                           [ 73%]
tests/test_004_cursor.py::test_datetimeoffset_read_write PASSED                                                                     [ 73%]
tests/test_004_cursor.py::test_datetimeoffset_max_min_offsets PASSED                                                                [ 73%]
tests/test_004_cursor.py::test_datetimeoffset_invalid_offsets PASSED                                                                [ 73%]
tests/test_004_cursor.py::test_datetimeoffset_dst_transitions PASSED                                                                [ 73%]
tests/test_004_cursor.py::test_datetimeoffset_leap_second PASSED                                                                    [ 74%]
tests/test_004_cursor.py::test_datetimeoffset_malformed_input PASSED                                                                [ 74%]
tests/test_004_cursor.py::test_datetimeoffset_executemany PASSED                                                                    [ 74%]
tests/test_004_cursor.py::test_datetimeoffset_execute_vs_executemany_consistency PASSED                                             [ 74%]
tests/test_004_cursor.py::test_datetimeoffset_extreme_offsets PASSED                                                                [ 74%]
tests/test_004_cursor.py::test_cursor_setinputsizes_basic PASSED                                                                    [ 74%]
tests/test_004_cursor.py::test_cursor_setinputsizes_with_executemany_float PASSED                                                   [ 75%]
tests/test_004_cursor.py::test_cursor_setinputsizes_reset PASSED                                                                    [ 75%]
tests/test_004_cursor.py::test_cursor_setinputsizes_override_inference PASSED                                                       [ 75%]
tests/test_004_cursor.py::test_setinputsizes_parameter_count_mismatch_fewer PASSED                                                  [ 75%]
tests/test_004_cursor.py::test_setinputsizes_parameter_count_mismatch_more PASSED                                                   [ 75%]
tests/test_004_cursor.py::test_setinputsizes_with_null_values PASSED                                                                [ 75%]
tests/test_004_cursor.py::test_setinputsizes_sql_injection_protection PASSED                                                        [ 76%]
tests/test_004_cursor.py::test_gettypeinfo_all_types PASSED                                                                         [ 76%]
tests/test_004_cursor.py::test_gettypeinfo_specific_type PASSED                                                                     [ 76%]
tests/test_004_cursor.py::test_gettypeinfo_result_structure PASSED                                                                  [ 76%]
tests/test_004_cursor.py::test_gettypeinfo_numeric_type PASSED                                                                      [ 76%]
tests/test_004_cursor.py::test_gettypeinfo_datetime_types PASSED                                                                    [ 77%]
tests/test_004_cursor.py::test_gettypeinfo_multiple_calls PASSED                                                                    [ 77%]
tests/test_004_cursor.py::test_gettypeinfo_binary_types PASSED                                                                      [ 77%]
tests/test_004_cursor.py::test_gettypeinfo_cached_results PASSED                                                                    [ 77%]
tests/test_004_cursor.py::test_procedures_setup PASSED                                                                              [ 77%]
tests/test_004_cursor.py::test_procedures_all PASSED                                                                                [ 77%]
tests/test_004_cursor.py::test_procedures_specific PASSED                                                                           [ 78%]
tests/test_004_cursor.py::test_procedures_with_schema PASSED                                                                        [ 78%]
tests/test_004_cursor.py::test_procedures_nonexistent PASSED                                                                        [ 78%]
tests/test_004_cursor.py::test_procedures_catalog_filter PASSED                                                                     [ 78%]
tests/test_004_cursor.py::test_procedures_with_parameters PASSED                                                                    [ 78%]
tests/test_004_cursor.py::test_procedures_result_set_info PASSED                                                                    [ 78%]
tests/test_004_cursor.py::test_procedures_cleanup PASSED                                                                            [ 79%]
tests/test_004_cursor.py::test_foreignkeys_setup PASSED                                                                             [ 79%]
tests/test_004_cursor.py::test_foreignkeys_all PASSED                                                                               [ 79%]
tests/test_004_cursor.py::test_foreignkeys_specific_table PASSED                                                                    [ 79%]
tests/test_004_cursor.py::test_foreignkeys_specific_foreign_table PASSED                                                            [ 79%]
tests/test_004_cursor.py::test_foreignkeys_both_tables PASSED                                                                       [ 79%]
tests/test_004_cursor.py::test_foreignkeys_nonexistent PASSED                                                                       [ 80%]
tests/test_004_cursor.py::test_foreignkeys_catalog_schema PASSED                                                                    [ 80%]
tests/test_004_cursor.py::test_foreignkeys_result_structure PASSED                                                                  [ 80%]
tests/test_004_cursor.py::test_foreignkeys_multiple_column_fk PASSED                                                                [ 80%]
tests/test_004_cursor.py::test_cleanup_schema PASSED                                                                                [ 80%]
tests/test_004_cursor.py::test_primarykeys_setup PASSED                                                                             [ 80%]
tests/test_004_cursor.py::test_primarykeys_simple PASSED                                                                            [ 81%]
tests/test_004_cursor.py::test_primarykeys_composite PASSED                                                                         [ 81%]
tests/test_004_cursor.py::test_primarykeys_column_info PASSED                                                                       [ 81%]
tests/test_004_cursor.py::test_primarykeys_nonexistent PASSED                                                                       [ 81%]
tests/test_004_cursor.py::test_primarykeys_catalog_filter PASSED                                                                    [ 81%]
tests/test_004_cursor.py::test_primarykeys_cleanup PASSED                                                                           [ 81%]
tests/test_004_cursor.py::test_specialcolumns_setup PASSED                                                                          [ 82%]
tests/test_004_cursor.py::test_rowid_columns_basic PASSED                                                                           [ 82%]
tests/test_004_cursor.py::test_rowid_columns_identity PASSED                                                                        [ 82%]
tests/test_004_cursor.py::test_rowid_columns_composite PASSED                                                                       [ 82%]
tests/test_004_cursor.py::test_rowid_columns_nonexistent PASSED                                                                     [ 82%]
tests/test_004_cursor.py::test_rowid_columns_nullable PASSED                                                                        [ 83%]
tests/test_004_cursor.py::test_rowver_columns_basic PASSED                                                                          [ 83%]
tests/test_004_cursor.py::test_rowver_columns_nonexistent PASSED                                                                    [ 83%]
tests/test_004_cursor.py::test_rowver_columns_nullable PASSED                                                                       [ 83%]
tests/test_004_cursor.py::test_specialcolumns_catalog_filter PASSED                                                                 [ 83%]
tests/test_004_cursor.py::test_specialcolumns_cleanup PASSED                                                                        [ 83%]
tests/test_004_cursor.py::test_statistics_setup PASSED                                                                              [ 84%]
tests/test_004_cursor.py::test_statistics_basic PASSED                                                                              [ 84%]
tests/test_004_cursor.py::test_statistics_unique_only PASSED                                                                        [ 84%]
tests/test_004_cursor.py::test_statistics_empty_table PASSED                                                                        [ 84%]
tests/test_004_cursor.py::test_statistics_nonexistent PASSED                                                                        [ 84%]
tests/test_004_cursor.py::test_statistics_result_structure PASSED                                                                   [ 84%]
tests/test_004_cursor.py::test_statistics_catalog_filter PASSED                                                                     [ 85%]
tests/test_004_cursor.py::test_statistics_with_quick_parameter PASSED                                                               [ 85%]
tests/test_004_cursor.py::test_statistics_cleanup PASSED                                                                            [ 85%]
tests/test_004_cursor.py::test_columns_setup PASSED                                                                                 [ 85%]
tests/test_004_cursor.py::test_columns_all PASSED                                                                                   [ 85%]
tests/test_004_cursor.py::test_columns_specific_table PASSED                                                                        [ 85%]
tests/test_004_cursor.py::test_columns_special_chars PASSED                                                                         [ 86%]
tests/test_004_cursor.py::test_columns_specific_column PASSED                                                                       [ 86%]
tests/test_004_cursor.py::test_columns_with_underscore_pattern PASSED                                                               [ 86%]
tests/test_004_cursor.py::test_columns_nonexistent PASSED                                                                           [ 86%]
tests/test_004_cursor.py::test_columns_data_types PASSED                                                                            [ 86%]
tests/test_004_cursor.py::test_columns_catalog_filter PASSED                                                                        [ 86%]
tests/test_004_cursor.py::test_columns_schema_pattern PASSED                                                                        [ 87%]
tests/test_004_cursor.py::test_columns_table_pattern PASSED                                                                         [ 87%]
tests/test_004_cursor.py::test_columns_ordinal_position PASSED                                                                      [ 87%]
tests/test_004_cursor.py::test_columns_cleanup PASSED                                                                               [ 87%]
tests/test_004_cursor.py::test_executemany_with_uuids PASSED                                                                        [ 87%]
tests/test_004_cursor.py::test_nvarcharmax_executemany_streaming PASSED                                                             [ 87%]
tests/test_004_cursor.py::test_varcharmax_executemany_streaming PASSED                                                              [ 88%]
tests/test_004_cursor.py::test_varbinarymax_executemany_streaming PASSED                                                            [ 88%]
tests/test_004_cursor.py::test_date_string_parameter_binding PASSED                                                                 [ 88%]
tests/test_004_cursor.py::test_time_string_parameter_binding PASSED                                                                 [ 88%]
tests/test_004_cursor.py::test_datetime_string_parameter_binding PASSED                                                             [ 88%]
tests/test_004_cursor.py::test_close PASSED                                                                                         [ 89%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_cleanup_on_connection_close PASSED                                       [ 89%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_cleanup_without_close PASSED                                             [ 89%]
tests/test_005_connection_cursor_lifecycle.py::test_no_segfault_on_gc PASSED                                                        [ 89%]
tests/test_005_connection_cursor_lifecycle.py::test_multiple_connections_interleaved_cursors PASSED                                 [ 89%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_outlives_connection PASSED                                               [ 89%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_weakref_cleanup PASSED                                                   [ 90%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_cleanup_order_no_segfault PASSED                                         [ 90%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_close_removes_from_connection PASSED                                     [ 90%]
tests/test_005_connection_cursor_lifecycle.py::test_connection_close_idempotent PASSED                                              [ 90%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_after_connection_close PASSED                                            [ 90%]
tests/test_005_connection_cursor_lifecycle.py::test_multiple_cursor_operations_cleanup PASSED                                       [ 90%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_close_raises_on_double_close PASSED                                      [ 91%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_del_no_logging_during_shutdown PASSED                                    [ 91%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_del_on_closed_cursor_no_errors PASSED                                    [ 91%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_del_unclosed_cursor_cleanup PASSED                                       [ 91%]
tests/test_005_connection_cursor_lifecycle.py::test_cursor_operations_after_close_raise_errors PASSED                               [ 91%]
tests/test_005_connection_cursor_lifecycle.py::test_mixed_cursor_cleanup_scenarios PASSED                                           [ 91%]
tests/test_005_connection_cursor_lifecycle.py::test_sql_syntax_error_no_segfault_on_shutdown PASSED                                 [ 92%]
tests/test_005_connection_cursor_lifecycle.py::test_multiple_sql_syntax_errors_no_segfault PASSED                                   [ 92%]
tests/test_005_connection_cursor_lifecycle.py::test_connection_close_during_active_query_no_segfault PASSED                         [ 92%]
tests/test_005_connection_cursor_lifecycle.py::test_concurrent_cursor_operations_no_segfault PASSED                                 [ 92%]
tests/test_005_connection_cursor_lifecycle.py::test_aggressive_threading_abrupt_exit_no_segfault PASSED                             [ 92%]
tests/test_006_exceptions.py::test_truncate_error_message PASSED                                                                    [ 92%]
tests/test_006_exceptions.py::test_raise_exception PASSED                                                                           [ 93%]
tests/test_006_exceptions.py::test_warning_exception PASSED                                                                         [ 93%]
tests/test_006_exceptions.py::test_data_error_exception PASSED                                                                      [ 93%]
tests/test_006_exceptions.py::test_operational_error_exception PASSED                                                               [ 93%]
tests/test_006_exceptions.py::test_integrity_error_exception PASSED                                                                 [ 93%]
tests/test_006_exceptions.py::test_internal_error_exception PASSED                                                                  [ 93%]
tests/test_006_exceptions.py::test_programming_error_exception PASSED                                                               [ 94%]
tests/test_006_exceptions.py::test_not_supported_error_exception PASSED                                                             [ 94%]
tests/test_006_exceptions.py::test_unknown_error_exception PASSED                                                                   [ 94%]
tests/test_006_exceptions.py::test_syntax_error PASSED                                                                              [ 94%]
tests/test_006_exceptions.py::test_table_not_found_error PASSED                                                                     [ 94%]
tests/test_006_exceptions.py::test_data_truncation_error PASSED                                                                     [ 95%]
tests/test_006_exceptions.py::test_unique_constraint_error PASSED                                                                   [ 95%]
tests/test_006_exceptions.py::test_foreign_key_constraint_error PASSED                                                              [ 95%]
tests/test_006_exceptions.py::test_connection_error PASSED                                                                          [ 95%]
tests/test_007_logging.py::test_no_logging PASSED                                                                                   [ 95%]
tests/test_007_logging.py::test_setup_logging PASSED                                                                                [ 95%]
tests/test_007_logging.py::test_logging_in_file_mode PASSED                                                                         [ 96%]
tests/test_007_logging.py::test_logging_in_stdout_mode PASSED                                                                       [ 96%]
tests/test_007_logging.py::test_python_layer_prefix PASSED                                                                          [ 96%]
tests/test_007_logging.py::test_different_log_levels PASSED                                                                         [ 96%]
tests/test_007_logging.py::test_singleton_behavior PASSED                                                                           [ 96%]
tests/test_007_logging.py::test_timestamp_in_log_filename PASSED                                                                    [ 96%]
tests/test_008_auth.py::TestAuthType::test_auth_type_constants PASSED                                                               [ 97%]
tests/test_008_auth.py::TestAADAuth::test_get_token_struct PASSED                                                                   [ 97%]
tests/test_008_auth.py::TestAADAuth::test_get_token_default PASSED                                                                  [ 97%]
tests/test_008_auth.py::TestAADAuth::test_get_token_device_code PASSED                                                              [ 97%]
tests/test_008_auth.py::TestAADAuth::test_get_token_interactive PASSED                                                              [ 97%]
tests/test_008_auth.py::TestAADAuth::test_get_token_credential_mapping PASSED                                                       [ 97%]
tests/test_008_auth.py::TestAADAuth::test_get_token_client_authentication_error PASSED                                              [ 98%]
tests/test_008_auth.py::TestProcessAuthParameters::test_empty_parameters PASSED                                                     [ 98%]
tests/test_008_auth.py::TestProcessAuthParameters::test_interactive_auth_windows PASSED                                             [ 98%]
tests/test_008_auth.py::TestProcessAuthParameters::test_interactive_auth_non_windows PASSED                                         [ 98%]
tests/test_008_auth.py::TestProcessAuthParameters::test_device_code_auth PASSED                                                     [ 98%]
tests/test_008_auth.py::TestProcessAuthParameters::test_default_auth PASSED                                                         [ 98%]
tests/test_008_auth.py::TestRemoveSensitiveParams::test_remove_sensitive_parameters PASSED                                          [ 99%]
tests/test_008_auth.py::TestProcessConnectionString::test_process_connection_string_with_default_auth PASSED                        [ 99%]
tests/test_008_auth.py::TestProcessConnectionString::test_process_connection_string_no_auth PASSED                                  [ 99%]
tests/test_008_auth.py::TestProcessConnectionString::test_process_connection_string_interactive_non_windows PASSED                  [ 99%]
tests/test_008_auth.py::test_error_handling PASSED                                                                                  [ 99%]
tests/test_008_auth.py::test_short_access_token_protection PASSED                                                                   [100%]

================================================================ FAILURES =================================================================
________________________________________________ test_attrs_before_access_token_attribute _________________________________________________


    def test_attrs_before_access_token_attribute(conn_str):
        """
        Test setting binary-valued ODBC attributes before connection (SQL_COPT_SS_ACCESS_TOKEN).
    
        SQL_COPT_SS_ACCESS_TOKEN (1256) is a SQL Server-specific ODBC attribute that allows
        passing an Azure AD access token for authentication instead of username/password.
        This is the ONLY attribute currently processed by Connection::applyAttrsBefore().
    
        Expected behavior: When both access token and UID/Pwd are provided, ODBC driver
        enforces security by rejecting the combination with a specific error message.
        This tests:
        1. Binary data handling in setAttribute() (no crashes/memory errors)
        2. ODBC security enforcement for conflicting authentication methods
        """
        # Create a fake access token (binary data) to test the code path
        fake_token = b"fake_access_token_for_testing_purposes_only"
        attrs_before = {1256: fake_token}  # SQL_COPT_SS_ACCESS_TOKEN = 1256
    
        # Should fail: ODBC driver rejects access token + UID/Pwd combination
>       with pytest.raises(Exception) as exc_info:
             ^^^^^^^^^^^^^^^^^^^^^^^^
E       Failed: DID NOT RAISE <class 'Exception'>

tests/test_003_connection.py:5305: Failed
______________________________________________ test_attrs_before_bytearray_instead_of_bytes _______________________________________________


    def test_attrs_before_bytearray_instead_of_bytes(conn_str):
        """
        Test that bytearray (mutable bytes) is handled the same as bytes (immutable).
    
        Python has two binary data types:
        - bytes: immutable sequence of bytes (b"data")
        - bytearray: mutable sequence of bytes (bytearray(b"data"))
    
        The C++ setAttribute() method should handle both types correctly by converting
        them to std::string and passing to SQLSetConnectAttr.
    
        Expected behavior: When both access token and UID/Pwd are provided, ODBC driver
        enforces security by rejecting the combination with a specific error message.
        """
        # Test with bytearray instead of bytes
        fake_data = bytearray(b"test_bytearray_data_for_coverage")
        attrs_before = {1256: fake_data}  # SQL_COPT_SS_ACCESS_TOKEN = 1256
    
        # Should fail: ODBC driver rejects access token + UID/Pwd combination
>       with pytest.raises(Exception) as exc_info:
             ^^^^^^^^^^^^^^^^^^^^^^^^
E       Failed: DID NOT RAISE <class 'Exception'>

tests/test_003_connection.py:5371: Failed
========================================================= short test summary info =========================================================
FAILED tests/test_003_connection.py::test_attrs_before_access_token_attribute - Failed: DID NOT RAISE <class 'Exception'>
FAILED tests/test_003_connection.py::test_attrs_before_bytearray_instead_of_bytes - Failed: DID NOT RAISE <class 'Exception'>
========================================== 2 failed, 575 passed, 6 skipped in 686.91s (0:11:26) ===========================================
Fatal Python error: _Py_GetConfig: the function must be called with the GIL held, after Python initialization and before Python finalization, but the GIL is released (the current Python thread state is NULL)
Python runtime state: finalizing (tstate=0x0000000105424548)

zsh: abort      python -m pytest -v
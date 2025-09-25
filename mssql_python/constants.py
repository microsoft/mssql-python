"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the constants used in the DDBC module.
"""

from enum import Enum


class ConstantsDDBC(Enum):
    """
    Constants used in the DDBC module.
    """
    SQL_HANDLE_ENV = 1
    SQL_HANDLE_DBC = 2
    SQL_HANDLE_STMT = 3
    SQL_SUCCESS = 0
    SQL_SUCCESS_WITH_INFO = 1
    SQL_NO_DATA = 100
    SQL_STILL_EXECUTING = 2
    SQL_NTS = -3
    SQL_DRIVER_NOPROMPT = 0
    SQL_ATTR_ASYNC_DBC_EVENT = 119
    SQL_IS_INTEGER = -6
    SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE = 117
    SQL_OV_DDBC3_80 = 380
    SQL_ATTR_DDBC_VERSION = 200
    SQL_ATTR_ASYNC_ENABLE = 4
    SQL_ATTR_ASYNC_STMT_EVENT = 29
    SQL_ERROR = -1
    SQL_INVALID_HANDLE = -2
    SQL_NULL_HANDLE = 0
    SQL_OV_DDBC3 = 3
    SQL_COMMIT = 0
    SQL_ROLLBACK = 1
    SQL_ATTR_AUTOCOMMIT = 102
    SQL_SMALLINT = 5
    SQL_CHAR = 1
    SQL_WCHAR = -8
    SQL_WVARCHAR = -9
    SQL_BIT = -7
    SQL_TINYINT = -6
    SQL_BIGINT = -5
    SQL_BINARY = -2
    SQL_VARBINARY = -3
    SQL_LONGVARBINARY = -4
    SQL_LONGVARCHAR = -1
    SQL_UNKNOWN_TYPE = 0
    SQL_NUMERIC = 2
    SQL_DECIMAL = 3
    SQL_INTEGER = 4
    SQL_FLOAT = 6
    SQL_REAL = 7
    SQL_DOUBLE = 8
    SQL_DATETIME = 9
    SQL_INTERVAL = 10
    SQL_TIMESTAMP = 11
    SQL_DATE = 9
    SQL_TIME = 10
    SQL_VARCHAR = 12
    SQL_TYPE_DATE = 91
    SQL_TYPE_TIME = 92
    SQL_TYPE_TIMESTAMP = 93
    SQL_TYPE_TIMESTAMP_WITH_TIMEZONE = 95
    SQL_GUID = -11
    SQL_XML = 241
    SQL_SMALLDATETIME = 58
    SQL_TIMESTAMPOFFSET = 43
    SQL_DATETIME2 = 42
    SQL_SMALLMONEY = 122
    SQL_MONEY = 60
    SQL_WLONGVARCHAR = -10
    SQL_C_BIT = -7
    SQL_C_TINYINT = -6
    SQL_C_SBIGINT = -25
    SQL_C_BINARY = -2
    SQL_AUTOCOMMIT_ON = 1
    SQL_AUTOCOMMIT_OFF = 0
    SQL_C_VARBINARY = -3
    SQL_C_LONGVARBINARY = -4
    SQL_C_LONGVARCHAR = -1
    SQL_C_CHAR = -8
    SQL_C_NUMERIC = 2
    SQL_C_DECIMAL = 3
    SQL_C_LONG = 4
    SQL_C_SHORT = 5
    SQL_C_FLOAT = 7
    SQL_C_DOUBLE = 8
    SQL_C_TYPE_DATE = 91
    SQL_C_TYPE_TIME = 92
    SQL_C_TYPE_TIMESTAMP = 93
    SQL_C_TYPE_TIMESTAMP_WITH_TIMEZONE = 95
    SQL_C_GUID = -11
    SQL_DESC_TYPE = 2
    SQL_DESC_LENGTH = 3
    SQL_DESC_NAME = 4
    SQL_ATTR_ROW_ARRAY_SIZE = 27
    SQL_ATTR_ROWS_FETCHED_PTR = 26
    SQL_ATTR_ROW_STATUS_PTR = 25
    SQL_ROW_SUCCESS = 0
    SQL_ROW_SUCCESS_WITH_INFO = 1
    SQL_ROW_NOROW = 100
    SQL_ATTR_CURSOR_TYPE = 6
    SQL_CURSOR_FORWARD_ONLY = 0
    SQL_CURSOR_STATIC = 3
    SQL_CURSOR_KEYSET_DRIVEN = 2
    SQL_CURSOR_DYNAMIC = 3
    SQL_NULL_DATA = -1
    SQL_C_DEFAULT = 99
    SQL_ATTR_ROW_BIND_TYPE = 5
    SQL_BIND_BY_COLUMN = 0
    SQL_PARAM_INPUT = 1
    SQL_PARAM_OUTPUT = 2
    SQL_PARAM_INPUT_OUTPUT = 3
    SQL_C_WCHAR = -8
    SQL_NULLABLE = 1
    SQL_MAX_NUMERIC_LEN = 16
    SQL_ATTR_QUERY_TIMEOUT = 2

    SQL_FETCH_NEXT = 1
    SQL_FETCH_FIRST = 2
    SQL_FETCH_LAST = 3
    SQL_FETCH_PRIOR = 4
    SQL_FETCH_ABSOLUTE = 5
    SQL_FETCH_RELATIVE = 6
    SQL_FETCH_BOOKMARK = 8
    SQL_DATETIMEOFFSET = -155
    SQL_C_SS_TIMESTAMPOFFSET = 0x4001
    SQL_SCOPE_CURROW = 0
    SQL_BEST_ROWID = 1
    SQL_ROWVER = 2
    SQL_NO_NULLS = 0
    SQL_NULLABLE_UNKNOWN = 2
    SQL_INDEX_UNIQUE = 0
    SQL_INDEX_ALL = 1
    SQL_QUICK = 0
    SQL_ENSURE = 1

class GetInfoConstants(Enum):
    """
    These constants are used with various methods like getinfo().
    """
    
    # Driver and database information
    SQL_DRIVER_NAME = 6
    SQL_DRIVER_VER = 7
    SQL_DRIVER_ODBC_VER = 77
    SQL_DRIVER_HLIB = 76
    SQL_DRIVER_HENV = 75
    SQL_DRIVER_HDBC = 74
    SQL_DATA_SOURCE_NAME = 2
    SQL_DATABASE_NAME = 16
    SQL_SERVER_NAME = 13
    SQL_USER_NAME = 47

    # SQL conformance and support
    SQL_SQL_CONFORMANCE = 118
    SQL_KEYWORDS = 89
    SQL_IDENTIFIER_CASE = 28
    SQL_IDENTIFIER_QUOTE_CHAR = 29
    SQL_SPECIAL_CHARACTERS = 94
    SQL_SQL92_ENTRY_SQL = 127
    SQL_SQL92_INTERMEDIATE_SQL = 128
    SQL_SQL92_FULL_SQL = 129
    SQL_SUBQUERIES = 95
    SQL_EXPRESSIONS_IN_ORDERBY = 27
    SQL_CORRELATION_NAME = 74
    SQL_SEARCH_PATTERN_ESCAPE = 14

    # Catalog and schema support
    SQL_CATALOG_TERM = 42
    SQL_CATALOG_NAME_SEPARATOR = 41
    SQL_SCHEMA_TERM = 39
    SQL_TABLE_TERM = 45
    SQL_PROCEDURES = 21
    SQL_ACCESSIBLE_TABLES = 19
    SQL_ACCESSIBLE_PROCEDURES = 20
    SQL_CATALOG_NAME = 10002
    SQL_CATALOG_USAGE = 92
    SQL_SCHEMA_USAGE = 91
    SQL_COLUMN_ALIAS = 87
    SQL_DESCRIBE_PARAMETER = 10003

    # Transaction support
    SQL_TXN_CAPABLE = 46
    SQL_TXN_ISOLATION_OPTION = 72
    SQL_DEFAULT_TXN_ISOLATION = 26
    SQL_MULTIPLE_ACTIVE_TXN = 37
    SQL_TXN_ISOLATION_LEVEL = 108

    # Data type support
    SQL_NUMERIC_FUNCTIONS = 49
    SQL_STRING_FUNCTIONS = 50
    SQL_DATETIME_FUNCTIONS = 51
    SQL_SYSTEM_FUNCTIONS = 58
    SQL_CONVERT_FUNCTIONS = 48
    SQL_LIKE_ESCAPE_CLAUSE = 113

    # Numeric limits
    SQL_MAX_COLUMN_NAME_LEN = 30
    SQL_MAX_TABLE_NAME_LEN = 35
    SQL_MAX_SCHEMA_NAME_LEN = 32
    SQL_MAX_CATALOG_NAME_LEN = 34
    SQL_MAX_IDENTIFIER_LEN = 10005
    SQL_MAX_STATEMENT_LEN = 105
    SQL_MAX_CHAR_LITERAL_LEN = 108
    SQL_MAX_BINARY_LITERAL_LEN = 112
    SQL_MAX_COLUMNS_IN_TABLE = 101
    SQL_MAX_COLUMNS_IN_SELECT = 100
    SQL_MAX_COLUMNS_IN_GROUP_BY = 97
    SQL_MAX_COLUMNS_IN_ORDER_BY = 99
    SQL_MAX_COLUMNS_IN_INDEX = 98
    SQL_MAX_TABLES_IN_SELECT = 106
    SQL_MAX_CONCURRENT_ACTIVITIES = 1
    SQL_MAX_DRIVER_CONNECTIONS = 0
    SQL_MAX_ROW_SIZE = 104
    SQL_MAX_USER_NAME_LEN = 107

    # Connection attributes
    SQL_ACTIVE_CONNECTIONS = 0
    SQL_ACTIVE_STATEMENTS = 1
    SQL_DATA_SOURCE_READ_ONLY = 25
    SQL_NEED_LONG_DATA_LEN = 111
    SQL_GETDATA_EXTENSIONS = 81

    # Result set and cursor attributes
    SQL_CURSOR_COMMIT_BEHAVIOR = 23
    SQL_CURSOR_ROLLBACK_BEHAVIOR = 24
    SQL_CURSOR_SENSITIVITY = 10001
    SQL_BOOKMARK_PERSISTENCE = 82
    SQL_DYNAMIC_CURSOR_ATTRIBUTES1 = 144
    SQL_DYNAMIC_CURSOR_ATTRIBUTES2 = 145
    SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1 = 146
    SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2 = 147
    SQL_STATIC_CURSOR_ATTRIBUTES1 = 150
    SQL_STATIC_CURSOR_ATTRIBUTES2 = 151
    SQL_KEYSET_CURSOR_ATTRIBUTES1 = 148
    SQL_KEYSET_CURSOR_ATTRIBUTES2 = 149
    SQL_SCROLL_OPTIONS = 44
    SQL_SCROLL_CONCURRENCY = 43
    SQL_FETCH_DIRECTION = 8
    SQL_ROWSET_SIZE = 9
    SQL_CONCURRENCY = 7
    SQL_ROW_NUMBER = 14
    SQL_STATIC_SENSITIVITY = 83
    SQL_BATCH_SUPPORT = 121
    SQL_BATCH_ROW_COUNT = 120
    SQL_PARAM_ARRAY_ROW_COUNTS = 153
    SQL_PARAM_ARRAY_SELECTS = 154
    SQL_PROCEDURE_TERM = 40

    # Positioned statement support
    SQL_POSITIONED_STATEMENTS = 80

    # Other constants
    SQL_GROUP_BY = 88
    SQL_OJ_CAPABILITIES = 65
    SQL_ORDER_BY_COLUMNS_IN_SELECT = 90
    SQL_OUTER_JOINS = 38
    SQL_QUOTED_IDENTIFIER_CASE = 93
    SQL_CONCAT_NULL_BEHAVIOR = 22
    SQL_NULL_COLLATION = 85
    SQL_ALTER_TABLE = 86
    SQL_UNION = 96
    SQL_DDL_INDEX = 170
    SQL_MULT_RESULT_SETS = 36
    SQL_OWNER_USAGE = 91
    SQL_QUALIFIER_USAGE = 92
    SQL_TIMEDATE_ADD_INTERVALS = 109
    SQL_TIMEDATE_DIFF_INTERVALS = 110

    # Return values for some getinfo functions
    SQL_IC_UPPER = 1
    SQL_IC_LOWER = 2
    SQL_IC_SENSITIVE = 3
    SQL_IC_MIXED = 4

class AuthType(Enum):
    """Constants for authentication types"""
    INTERACTIVE = "activedirectoryinteractive"
    DEVICE_CODE = "activedirectorydevicecode"
    DEFAULT = "activedirectorydefault"

class SQLTypes:
    """Constants for valid SQL data types to use with setinputsizes"""
    
    @classmethod
    def get_valid_types(cls) -> set:
        """Returns a set of all valid SQL type constants"""
        
        return {
            ConstantsDDBC.SQL_CHAR.value, ConstantsDDBC.SQL_VARCHAR.value, 
            ConstantsDDBC.SQL_LONGVARCHAR.value, ConstantsDDBC.SQL_WCHAR.value,
            ConstantsDDBC.SQL_WVARCHAR.value, ConstantsDDBC.SQL_WLONGVARCHAR.value,
            ConstantsDDBC.SQL_DECIMAL.value, ConstantsDDBC.SQL_NUMERIC.value, 
            ConstantsDDBC.SQL_BIT.value, ConstantsDDBC.SQL_TINYINT.value,
            ConstantsDDBC.SQL_SMALLINT.value, ConstantsDDBC.SQL_INTEGER.value, 
            ConstantsDDBC.SQL_BIGINT.value, ConstantsDDBC.SQL_REAL.value,
            ConstantsDDBC.SQL_FLOAT.value, ConstantsDDBC.SQL_DOUBLE.value, 
            ConstantsDDBC.SQL_BINARY.value, ConstantsDDBC.SQL_VARBINARY.value,
            ConstantsDDBC.SQL_LONGVARBINARY.value, ConstantsDDBC.SQL_DATE.value, 
            ConstantsDDBC.SQL_TIME.value, ConstantsDDBC.SQL_TIMESTAMP.value,
            ConstantsDDBC.SQL_GUID.value
        }
    
    # Could also add category methods for convenience
    @classmethod
    def get_string_types(cls) -> set:
        """Returns a set of string SQL type constants"""
        
        return {
            ConstantsDDBC.SQL_CHAR.value, ConstantsDDBC.SQL_VARCHAR.value, 
            ConstantsDDBC.SQL_LONGVARCHAR.value, ConstantsDDBC.SQL_WCHAR.value,
            ConstantsDDBC.SQL_WVARCHAR.value, ConstantsDDBC.SQL_WLONGVARCHAR.value
        }
    
    @classmethod
    def get_numeric_types(cls) -> set:
        """Returns a set of numeric SQL type constants"""
        
        return {
            ConstantsDDBC.SQL_DECIMAL.value, ConstantsDDBC.SQL_NUMERIC.value,
            ConstantsDDBC.SQL_BIT.value, ConstantsDDBC.SQL_TINYINT.value,
            ConstantsDDBC.SQL_SMALLINT.value, ConstantsDDBC.SQL_INTEGER.value,
            ConstantsDDBC.SQL_BIGINT.value, ConstantsDDBC.SQL_REAL.value,
            ConstantsDDBC.SQL_FLOAT.value, ConstantsDDBC.SQL_DOUBLE.value
        }
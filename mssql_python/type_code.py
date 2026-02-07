"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
"""

import decimal
import uuid
import datetime
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const


class SqlTypeCode:
    """
    A dual-compatible type code that compares equal to both SQL type integers and Python types.

    This class maintains backwards compatibility with code that checks
    `cursor.description[i][1] == str` while also supporting DB-API 2.0
    compliant code that checks `cursor.description[i][1] == -9`.

    Examples:
        >>> type_code = SqlTypeCode(-9)
        >>> type_code == str  # Backwards compatible with pandas, etc.
        True
        >>> type_code == -9   # DB-API 2.0 compliant
        True
        >>> int(type_code)    # Get the raw SQL type code
        -9
    """

    # SQL type code to Python type mapping (class-level cache)
    _type_map = None

    def __init__(self, type_code: int):
        self.type_code = type_code
        self.python_type = self._get_python_type(type_code)

    @classmethod
    def _get_type_map(cls):
        """Lazily build the SQL to Python type mapping."""
        if cls._type_map is None:
            cls._type_map = {
                ddbc_sql_const.SQL_CHAR.value: str,
                ddbc_sql_const.SQL_VARCHAR.value: str,
                ddbc_sql_const.SQL_LONGVARCHAR.value: str,
                ddbc_sql_const.SQL_WCHAR.value: str,
                ddbc_sql_const.SQL_WVARCHAR.value: str,
                ddbc_sql_const.SQL_WLONGVARCHAR.value: str,
                ddbc_sql_const.SQL_INTEGER.value: int,
                ddbc_sql_const.SQL_REAL.value: float,
                ddbc_sql_const.SQL_FLOAT.value: float,
                ddbc_sql_const.SQL_DOUBLE.value: float,
                ddbc_sql_const.SQL_DECIMAL.value: decimal.Decimal,
                ddbc_sql_const.SQL_NUMERIC.value: decimal.Decimal,
                ddbc_sql_const.SQL_DATE.value: datetime.date,
                ddbc_sql_const.SQL_TIMESTAMP.value: datetime.datetime,
                ddbc_sql_const.SQL_TIME.value: datetime.time,
                ddbc_sql_const.SQL_SS_TIME2.value: datetime.time,
                ddbc_sql_const.SQL_TYPE_DATE.value: datetime.date,
                ddbc_sql_const.SQL_TYPE_TIME.value: datetime.time,
                ddbc_sql_const.SQL_TYPE_TIMESTAMP.value: datetime.datetime,
                ddbc_sql_const.SQL_TYPE_TIMESTAMP_WITH_TIMEZONE.value: datetime.datetime,
                ddbc_sql_const.SQL_BIT.value: bool,
                ddbc_sql_const.SQL_TINYINT.value: int,
                ddbc_sql_const.SQL_SMALLINT.value: int,
                ddbc_sql_const.SQL_BIGINT.value: int,
                ddbc_sql_const.SQL_BINARY.value: bytes,
                ddbc_sql_const.SQL_VARBINARY.value: bytes,
                ddbc_sql_const.SQL_LONGVARBINARY.value: bytes,
                ddbc_sql_const.SQL_GUID.value: uuid.UUID,
                ddbc_sql_const.SQL_SS_UDT.value: bytes,
                ddbc_sql_const.SQL_SS_XML.value: str,
                ddbc_sql_const.SQL_DATETIME2.value: datetime.datetime,
                ddbc_sql_const.SQL_SMALLDATETIME.value: datetime.datetime,
                ddbc_sql_const.SQL_DATETIMEOFFSET.value: datetime.datetime,
            }
        return cls._type_map

    @classmethod
    def _get_python_type(cls, sql_code: int) -> type:
        """Get the Python type for a SQL type code."""
        return cls._get_type_map().get(sql_code, str)

    def __eq__(self, other):
        """Compare equal to both Python types and SQL integer codes."""
        if isinstance(other, type):
            return self.python_type == other
        if isinstance(other, int):
            return self.type_code == other
        if isinstance(other, SqlTypeCode):
            return self.type_code == other.type_code
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        """
        SqlTypeCode is intentionally unhashable because __eq__ allows
        comparisons to both Python types and integer SQL codes, and
        there is no single hash value that can be consistent with both.
        """
        raise TypeError(
            "SqlTypeCode is unhashable. Use int(type_code) or type_code.type_code "
            "as a dict key instead. Example: {int(desc[1]): handler}"
        )

    def __int__(self):
        return self.type_code

    def __repr__(self):
        type_name = self.python_type.__name__ if self.python_type else "Unknown"
        return f"SqlTypeCode({self.type_code}, {type_name})"

    def __str__(self):
        return str(self.type_code)

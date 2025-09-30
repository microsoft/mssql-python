"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains type objects and constructors for the mssql_python package.
"""

import datetime
import time


# Type Objects
class STRING(str):
    """
    This type object is used to describe columns in a database that are string-based (e.g. CHAR).
    """

    def __new__(cls):
        return str.__new__(cls, "")


class BINARY(bytearray):
    """
    This type object is used to describe (long)
    binary columns in a database (e.g. LONG, RAW, BLOBs).
    """

    def __new__(cls):
        return bytearray.__new__(cls)


class NUMBER(float):
    """
    This type object is used to describe numeric columns in a database.
    """

    def __new__(cls):
        return float.__new__(cls, 0.0)


class DATETIME(datetime.datetime):
    """
    This type object is used to describe date/time columns in a database.
    """

    def __new__(cls, year: int = 1, month: int = 1, day: int = 1):
        return datetime.datetime.__new__(cls, year, month, day)


class ROWID(int):
    """
    This type object is used to describe the "Row ID" column in a database.
    """

    def __new__(cls):
        return int.__new__(cls, 0)


# Type Constructors
def Date(year: int, month: int, day: int) -> datetime.date:
    """
    Generates a date object.
    """
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:
    """
    Generates a time object.
    """
    return datetime.time(hour, minute, second)


def Timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int, microsecond: int
) -> datetime.datetime:
    """
    Generates a timestamp object.
    """
    return datetime.datetime(year, month, day, hour, minute, second, microsecond)


def DateFromTicks(ticks: int) -> datetime.date:
    """
    Generates a date object from ticks.
    """
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: int) -> datetime.time:
    """
    Generates a time object from ticks.
    """
    return datetime.time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> datetime.datetime:
    """
    Generates a timestamp object from ticks.
    """
    return datetime.datetime.fromtimestamp(ticks)


def Binary(value) -> bytes:
    """
    Converts a string or bytes to bytes for use with binary database columns.
    
    This function follows the DB-API 2.0 specification.
    It accepts only str and bytes/bytearray types to ensure type safety.
    
    Args:
        value: A string (str) or bytes-like object (bytes, bytearray)
        
    Returns:
        bytes: The input converted to bytes
        
    Raises:
        TypeError: If the input type is not supported
        
    Examples:
        Binary("hello")           # Returns b"hello"
        Binary(b"hello")          # Returns b"hello"  
        Binary(bytearray(b"hi"))  # Returns b"hi"
    """
    if isinstance(value, bytes):
        return value
    elif isinstance(value, bytearray):
        return bytes(value)
    elif isinstance(value, str):
        return value.encode("utf-8")
    else:
        # Raise TypeError for unsupported types to improve type safety
        raise TypeError(f"Cannot convert type {type(value).__name__} to bytes. "
                       f"Binary() only accepts str, bytes, or bytearray objects.")

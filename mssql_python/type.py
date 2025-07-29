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
    Converts a string or bytes to bytes using UTF-8 encoding.
    """
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return bytes(value, "utf-8")
    else:
        # Handle other types by converting to string first
        return bytes(str(value), "utf-8")

"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains type objects and constructors for the mssql_python package.
"""

import datetime
import time


# Type Objects
class STRING:
    """
    This type object is used to describe columns in a database that are string-based (e.g. CHAR).
    """

    def __init__(self) -> None:
        self.type = "STRING"


class BINARY:
    """
    This type object is used to describe (long)
    binary columns in a database (e.g. LONG, RAW, BLOBs).
    """

    def __init__(self) -> None:
        self.type = "BINARY"


class NUMBER:
    """
    This type object is used to describe numeric columns in a database.
    """

    def __init__(self) -> None:
        self.type = "NUMBER"


class DATETIME:
    """
    This type object is used to describe date/time columns in a database.
    """

    def __init__(self) -> None:
        self.type = "DATETIME"


class ROWID:
    """
    This type object is used to describe the “Row ID” column in a database.
    """

    def __init__(self) -> None:
        self.type = "ROWID"


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
    return datetime.time(*time.gmtime(ticks)[3:6])


def TimestampFromTicks(ticks: int) -> datetime.datetime:
    """
    Generates a timestamp object from ticks.
    """
    return datetime.datetime.fromtimestamp(ticks, datetime.timezone.utc)


def Binary(string: str) -> bytes:
    """
    Converts a string to bytes using UTF-8 encoding.
    """
    return bytes(string, "utf-8")

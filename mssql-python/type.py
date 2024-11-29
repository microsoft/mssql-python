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
    This type object is used to describe (long) binary columns in a database (e.g. LONG, RAW, BLOBs).
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

    Args:
        year (int): The year component of the date.
        month (int): The month component of the date.
        day (int): The day component of the date.

    Returns:
        datetime.date: A date object representing the date.
    """
    return datetime.date(year, month, day)

def Time(hour: int, minute: int, second: int) -> datetime.time:
    """
    Generates a time object.

    Args:
        hour (int): The hour component of the time.
        minute (int): The minute component of the time.
        second (int): The second component of the time.

    Returns:
        datetime.time: A time object representing the time.
    """
    return datetime.time(hour, minute, second)

def Timestamp(year: int, month: int, day: int, hour: int, minute: int, second: int) -> datetime.datetime:
    """
    Generates a timestamp object.

    Args:
        year (int): The year component of the timestamp.
        month (int): The month component of the timestamp.
        day (int): The day component of the timestamp.
        hour (int): The hour component of the timestamp.
        minute (int): The minute component of the timestamp.
        second (int): The second component of the timestamp.

    Returns:
        datetime.datetime: A datetime object representing the timestamp.
    """
    return datetime.datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks: int) -> datetime.date:
    """
    Generates a date object from ticks.

    Args:
        ticks (int): The number of ticks since the epoch.

    Returns:
        datetime.date: A date object representing the date.
    """
    return datetime.date.fromtimestamp(ticks)

def TimeFromTicks(ticks: int) -> datetime.time:
    """
    Generates a time object from ticks.

    Args:
        ticks (int): The number of ticks since the epoch.

    Returns:
        datetime.time: A time object representing the time.
    """
    return datetime.time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks: int) -> datetime.datetime:
    """
    Generates a timestamp object from ticks.

    Args:
        ticks (int): The number of ticks since the epoch.

    Returns:
        datetime.datetime: A datetime object representing the timestamp.
    """
    return datetime.datetime.fromtimestamp(ticks)

def Binary(string: str) -> bytes:
    """
    Converts a string to bytes using UTF-8 encoding.

    Args:
        string (str): The string to be converted.

    Returns:
        bytes: The byte representation of the string.
    """
    return bytes(string, 'utf-8')
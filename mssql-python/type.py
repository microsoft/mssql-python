import time

# Type Objects
STRING = "STRING"
BINARY = "BINARY"
NUMBER = "NUMBER"
DATETIME = "DATETIME"
ROWID = "ROWID"

# Type Constructors
def Date(year, month, day):
    """
    Generates a date string in the format YYYY-MM-DD.

    Args:
        year (int): The year component of the date.
        month (int): The month component of the date.
        day (int): The day component of the date.

    Returns:
        str: A string representing the date in the format YYYY-MM-DD.
    """
    return f"{year}-{month}-{day}"

def Time(hour, minute, second):
    """
    Generates a time string in the format HH:MM:SS.

    Args:
        hour (int): The hour component of the time.
        minute (int): The minute component of the time.
        second (int): The second component of the time.

    Returns:
        str: A string representing the time in the format HH:MM:SS.
    """
    return f"{hour}:{minute}:{second}"

def Timestamp(year, month, day, hour, minute, second):
    """
    Generates a timestamp string in the format YYYY-MM-DD HH:MM:SS.

    Args:
        year (int): The year component of the timestamp.
        month (int): The month component of the timestamp.
        day (int): The day component of the timestamp.
        hour (int): The hour component of the timestamp.
        minute (int): The minute component of the timestamp.
        second (int): The second component of the timestamp.

    Returns:
        str: A string representing the timestamp in the format YYYY-MM-DD HH:MM:SS.
    """
    return f"{year}-{month}-{day} {hour}:{minute}:{second}"

def DateFromTicks(ticks):
    """
    Generates a date string from ticks.

    Args:
        ticks (int): The number of ticks since the epoch.

    Returns:
        str: A string representing the date in the format YYYY-MM-DD.
    """
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    """
    Generates a time string from ticks.

    Args:
        ticks (int): The number of ticks since the epoch.

    Returns:
        str: A string representing the time in the format HH:MM:SS.
    """
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    """
    Generates a timestamp string from ticks.

    Args:
        ticks (int): The number of ticks since the epoch.

    Returns:
        str: A string representing the timestamp in the format YYYY-MM-DD HH:MM:SS.
    """
    return Timestamp(*time.localtime(ticks)[:6])

def Binary(string):
    """
    Converts a string to bytes using UTF-8 encoding.

    Args:
        string (str): The string to be converted.

    Returns:
        bytes: The byte representation of the string.
    """
    return bytes(string, 'utf-8')
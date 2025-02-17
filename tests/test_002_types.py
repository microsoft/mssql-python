import pytest
import datetime
from mssql_python.type import STRING, BINARY, NUMBER, DATETIME, ROWID, Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary

def test_string_type():
    assert STRING().type == "STRING", "STRING type mismatch"

def test_binary_type():
    assert BINARY().type == "BINARY", "BINARY type mismatch"

def test_number_type():
    assert NUMBER().type == "NUMBER", "NUMBER type mismatch"

def test_datetime_type():
    assert DATETIME().type == "DATETIME", "DATETIME type mismatch"

def test_rowid_type():
    assert ROWID().type == "ROWID", "ROWID type mismatch"

def test_date_constructor():
    date = Date(2023, 10, 5)
    assert isinstance(date, datetime.date), "Date constructor did not return a date object"
    assert date.year == 2023 and date.month == 10 and date.day == 5, "Date constructor returned incorrect date"

def test_time_constructor():
    time = Time(12, 30, 45)
    assert isinstance(time, datetime.time), "Time constructor did not return a time object"
    assert time.hour == 12 and time.minute == 30 and time.second == 45, "Time constructor returned incorrect time"

def test_timestamp_constructor():
    timestamp = Timestamp(2023, 10, 5, 12, 30, 45)
    assert isinstance(timestamp, datetime.datetime), "Timestamp constructor did not return a datetime object"
    assert timestamp.year == 2023 and timestamp.month == 10 and timestamp.day == 5, "Timestamp constructor returned incorrect date"
    assert timestamp.hour == 12 and timestamp.minute == 30 and timestamp.second == 45, "Timestamp constructor returned incorrect time"

def test_date_from_ticks():
    ticks = 1696500000  # Corresponds to 2023-10-05
    date = DateFromTicks(ticks)
    assert isinstance(date, datetime.date), "DateFromTicks did not return a date object"
    assert date == datetime.date(2023, 10, 5), "DateFromTicks returned incorrect date"

def test_time_from_ticks():
    ticks = 1696500000  # Corresponds to 10:00:00
    time = TimeFromTicks(ticks)
    assert isinstance(time, datetime.time), "TimeFromTicks did not return a time object"
    assert time == datetime.time(10, 0, 0), "TimeFromTicks returned incorrect time"

def test_timestamp_from_ticks():
    ticks = 1696500000  # Corresponds to 2023-10-05 10:00:00
    timestamp = TimestampFromTicks(ticks)
    assert isinstance(timestamp, datetime.datetime), "TimestampFromTicks did not return a datetime object"
    assert timestamp == datetime.datetime(2023, 10, 5, 10, 0, 0, tzinfo=datetime.timezone.utc), "TimestampFromTicks returned incorrect timestamp"

def test_binary_constructor():
    binary = Binary("test")
    assert isinstance(binary, bytes), "Binary constructor did not return a bytes object"
    assert binary == b"test", "Binary constructor returned incorrect bytes"

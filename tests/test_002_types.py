import pytest
import datetime
import time
from mssql_python.type import (
    STRING,
    BINARY,
    NUMBER,
    DATETIME,
    ROWID,
    Date,
    Time,
    Timestamp,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
    Binary,
)


def test_string_type():
    assert STRING() == str(), "STRING type mismatch"


def test_binary_type():
    assert BINARY() == bytearray(), "BINARY type mismatch"


def test_number_type():
    assert NUMBER() == float(), "NUMBER type mismatch"


def test_datetime_type():
    assert DATETIME(2025, 1, 1) == datetime.datetime(
        2025, 1, 1
    ), "DATETIME type mismatch"


def test_rowid_type():
    assert ROWID() == int(), "ROWID type mismatch"


def test_date_constructor():
    date = Date(2023, 10, 5)
    assert isinstance(
        date, datetime.date
    ), "Date constructor did not return a date object"
    assert (
        date.year == 2023 and date.month == 10 and date.day == 5
    ), "Date constructor returned incorrect date"


def test_time_constructor():
    time = Time(12, 30, 45)
    assert isinstance(
        time, datetime.time
    ), "Time constructor did not return a time object"
    assert (
        time.hour == 12 and time.minute == 30 and time.second == 45
    ), "Time constructor returned incorrect time"


def test_timestamp_constructor():
    timestamp = Timestamp(2023, 10, 5, 12, 30, 45, 123456)
    assert isinstance(
        timestamp, datetime.datetime
    ), "Timestamp constructor did not return a datetime object"
    assert (
        timestamp.year == 2023 and timestamp.month == 10 and timestamp.day == 5
    ), "Timestamp constructor returned incorrect date"
    assert (
        timestamp.hour == 12 and timestamp.minute == 30 and timestamp.second == 45
    ), "Timestamp constructor returned incorrect time"
    assert (
        timestamp.microsecond == 123456
    ), "Timestamp constructor returned incorrect fraction"


def test_date_from_ticks():
    ticks = 1696500000  # Corresponds to 2023-10-05
    date = DateFromTicks(ticks)
    assert isinstance(date, datetime.date), "DateFromTicks did not return a date object"
    assert date == datetime.date(2023, 10, 5), "DateFromTicks returned incorrect date"


def test_time_from_ticks():
    ticks = 1696500000  # Corresponds to local
    time_var = TimeFromTicks(ticks)
    assert isinstance(
        time_var, datetime.time
    ), "TimeFromTicks did not return a time object"
    assert time_var == datetime.time(
        *time.localtime(ticks)[3:6]
    ), "TimeFromTicks returned incorrect time"


def test_timestamp_from_ticks():
    ticks = 1696500000  # Corresponds to 2023-10-05 local time
    timestamp = TimestampFromTicks(ticks)
    assert isinstance(
        timestamp, datetime.datetime
    ), "TimestampFromTicks did not return a datetime object"
    assert timestamp == datetime.datetime.fromtimestamp(
        ticks
    ), "TimestampFromTicks returned incorrect timestamp"


def test_binary_constructor():
    binary = Binary("test".encode("utf-8"))
    assert isinstance(
        binary, (bytes, bytearray)
    ), "Binary constructor did not return a bytes object"
    assert binary == b"test", "Binary constructor returned incorrect bytes"


def test_binary_string_encoding():
    """Test Binary() string encoding (Lines 134-135)."""
    # Test basic string encoding
    result = Binary("hello")
    assert result == b"hello", "String should be encoded to UTF-8 bytes"
    
    # Test string with UTF-8 characters
    result = Binary("café")
    assert result == "café".encode("utf-8"), "UTF-8 string should be properly encoded"
    
    # Test empty string
    result = Binary("")
    assert result == b"", "Empty string should encode to empty bytes"
    
    # Test string with special characters
    result = Binary("Hello\nWorld\t!")
    assert result == b"Hello\nWorld\t!", "String with special characters should encode properly"


def test_binary_unsupported_types_error():
    """Test Binary() TypeError for unsupported types (Lines 138-141)."""
    # Test integer type
    with pytest.raises(TypeError) as exc_info:
        Binary(123)
    assert "Cannot convert type int to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, or bytearray objects" in str(exc_info.value)
    
    # Test float type
    with pytest.raises(TypeError) as exc_info:
        Binary(3.14)
    assert "Cannot convert type float to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, or bytearray objects" in str(exc_info.value)
    
    # Test list type
    with pytest.raises(TypeError) as exc_info:
        Binary([1, 2, 3])
    assert "Cannot convert type list to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, or bytearray objects" in str(exc_info.value)
    
    # Test dict type
    with pytest.raises(TypeError) as exc_info:
        Binary({"key": "value"})
    assert "Cannot convert type dict to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, or bytearray objects" in str(exc_info.value)
    
    # Test None type
    with pytest.raises(TypeError) as exc_info:
        Binary(None)
    assert "Cannot convert type NoneType to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, or bytearray objects" in str(exc_info.value)
    
    # Test custom object type
    class CustomObject:
        pass
    
    with pytest.raises(TypeError) as exc_info:
        Binary(CustomObject())
    assert "Cannot convert type CustomObject to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, or bytearray objects" in str(exc_info.value)


def test_binary_comprehensive_coverage():
    """Test Binary() function comprehensive coverage including all paths."""
    # Test bytes input (should return as-is)
    bytes_input = b"hello bytes"
    result = Binary(bytes_input)
    assert result is bytes_input, "Bytes input should be returned as-is"
    assert result == b"hello bytes", "Bytes content should be unchanged"
    
    # Test bytearray input (should convert to bytes)
    bytearray_input = bytearray(b"hello bytearray")
    result = Binary(bytearray_input)
    assert isinstance(result, bytes), "Bytearray should be converted to bytes"
    assert result == b"hello bytearray", "Bytearray content should be preserved in bytes"
    
    # Test string input with various encodings (Lines 134-135)
    # ASCII string
    result = Binary("hello world")
    assert result == b"hello world", "ASCII string should encode properly"
    
    # Unicode string
    result = Binary("héllo wørld")
    assert result == "héllo wørld".encode("utf-8"), "Unicode string should encode to UTF-8"
    
    # String with emojis
    result = Binary("Hello 🌍")
    assert result == "Hello 🌍".encode("utf-8"), "Emoji string should encode to UTF-8"
    
    # Empty inputs
    assert Binary("") == b"", "Empty string should encode to empty bytes"
    assert Binary(b"") == b"", "Empty bytes should remain empty bytes"
    assert Binary(bytearray()) == b"", "Empty bytearray should convert to empty bytes"

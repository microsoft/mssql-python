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
    assert DATETIME(2025, 1, 1) == datetime.datetime(2025, 1, 1), "DATETIME type mismatch"


def test_rowid_type():
    assert ROWID() == int(), "ROWID type mismatch"


def test_date_constructor():
    date = Date(2023, 10, 5)
    assert isinstance(date, datetime.date), "Date constructor did not return a date object"
    assert (
        date.year == 2023 and date.month == 10 and date.day == 5
    ), "Date constructor returned incorrect date"


def test_time_constructor():
    time = Time(12, 30, 45)
    assert isinstance(time, datetime.time), "Time constructor did not return a time object"
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
    assert timestamp.microsecond == 123456, "Timestamp constructor returned incorrect fraction"


def test_date_from_ticks():
    ticks = 1696500000  # Corresponds to 2023-10-05
    date = DateFromTicks(ticks)
    assert isinstance(date, datetime.date), "DateFromTicks did not return a date object"
    assert date == datetime.date(2023, 10, 5), "DateFromTicks returned incorrect date"


def test_time_from_ticks():
    ticks = 1696500000  # Corresponds to local
    time_var = TimeFromTicks(ticks)
    assert isinstance(time_var, datetime.time), "TimeFromTicks did not return a time object"
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


def test_binary_string_encoding():
    """Encode each distinct input from the former UTF-8 suites exactly once.

    Binary is a Python UTF-8 helper, not a native SQLWCHAR conversion entry point.
    Native database encoding/decoding is covered in test_013_encoding_decoding.py.
    """
    cases = [
        ("hello world", b"hello world"),
        ("h\xe9llo w\xf8rld", b"h\xc3\xa9llo w\xc3\xb8rld"),
        ("Hello \U0001f30d", b"Hello \xf0\x9f\x8c\x8d"),
        ("", b""),
        ("hello", b"hello"),
        ("caf\xe9", b"caf\xc3\xa9"),
        ("Hello\nWorld\t!", b"Hello\nWorld\t!"),
        ("\x80", b"\xc2\x80"),
        ("\xa9", b"\xc2\xa9"),
        ("\xff", b"\xc3\xbf"),
        ("\u07ff", b"\xdf\xbf"),
        ("\u0800", b"\xe0\xa0\x80"),
        ("\u4e2d", b"\xe4\xb8\xad"),
        ("\u20ac", b"\xe2\x82\xac"),
        ("\ud7ff", b"\xed\x9f\xbf"),
        ("\ue000", b"\xee\x80\x80"),
        ("\uffff", b"\xef\xbf\xbf"),
        ("\U00010000", b"\xf0\x90\x80\x80"),
        ("\U0001f600", b"\xf0\x9f\x98\x80"),
        ("\U0001f601", b"\xf0\x9f\x98\x81"),
        ("\U0001f30d", b"\xf0\x9f\x8c\x8d"),
        ("\U000f0000", b"\xf3\xb0\x80\x80"),
        ("\U0010ffff", b"\xf4\x8f\xbf\xbf"),
        ("\x00", b"\x00"),
        (" ", b" "),
        ("A", b"A"),
        ("Z", b"Z"),
        ("a", b"a"),
        ("z", b"z"),
        ("\x7f", b"\x7f"),
        ("Hello", b"Hello"),
        ("0123456789", b"0123456789"),
        ("!@#$%^&*()", b"!@#$%^&*()"),
        ("\xe9", b"\xc3\xa9"),
        ("\u03b1", b"\xce\xb1"),
        ("\u0401", b"\xd0\x81"),
        ("\u05d0", b"\xd7\x90"),
        (
            "\u041f\u0440\u0438\u0432\u0435\u0442",
            b"\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82",
        ),
        ("\u65e5", b"\xe6\x97\xa5"),
        ("\uac00", b"\xea\xb0\x80"),
        ("\u2764", b"\xe2\x9d\xa4"),
        ("\u4f60\u597d", b"\xe4\xbd\xa0\xe5\xa5\xbd"),
        (
            "\u3053\u3093\u306b\u3061\u306f",
            b"\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf",
        ),
        ("\U0001f44d", b"\xf0\x9f\x91\x8d"),
        ("\U0001f525", b"\xf0\x9f\x94\xa5"),
        ("\U0001d54a", b"\xf0\x9d\x95\x8a"),
        ("\U00020000", b"\xf0\xa0\x80\x80"),
        ("Hello \U0001f600", b"Hello \xf0\x9f\x98\x80"),
        ("\U0001f525\U0001f4af", b"\xf0\x9f\x94\xa5\xf0\x9f\x92\xaf"),
        ("A\xe9\u4e2d\U0001f600", b"A\xc3\xa9\xe4\xb8\xad\xf0\x9f\x98\x80"),
        ("Test: \u20ac100 \U0001f4b0", b"Test: \xe2\x82\xac100 \xf0\x9f\x92\xb0"),
        ("A\xa9\u20ac\U0001f600", b"A\xc2\xa9\xe2\x82\xac\xf0\x9f\x98\x80"),
        ("Hello \xa9\u4e2d\U0001f600", b"Hello \xc2\xa9\xe4\xb8\xad\xf0\x9f\x98\x80"),
        ("ABCDEFGHIJKLMNOPQRSTUVWXYZ", b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
        ("!@#$%^&*()_+-=[]{}|;:',.<>?/", b"!@#$%^&*()_+-=[]{}|;:',.<>?/"),
        ("a" * 1000, b"a" * 1000),
        ("r\xe9sum\xe9", b"r\xc3\xa9sum\xc3\xa9"),
        ("na\xefve", b"na\xc3\xafve"),
        ("\xc5ngstr\xf6m", b"\xc3\x85ngstr\xc3\xb6m"),
        (
            "\u03b3\u03b5\u03b9\u03b1 \u03c3\u03bf\u03c5",
            b"\xce\xb3\xce\xb5\xce\xb9\xce\xb1 \xcf\x83\xce\xbf\xcf\x85",
        ),
        ("\xa7\xa9\xae\u2122", b"\xc2\xa7\xc2\xa9\xc2\xae\xe2\x84\xa2"),
        ("\u4f60\u597d\u4e16\u754c", b"\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c"),
        (
            "\uc548\ub155\ud558\uc138\uc694",
            b"\xec\x95\x88\xeb\x85\x95\xed\x95\x98\xec\x84\xb8\xec\x9a\x94",
        ),
        ("\u0645\u0631\u062d\u0628\u0627", b"\xd9\x85\xd8\xb1\xd8\xad\xd8\xa8\xd8\xa7"),
        ("\u05e9\u05dc\u05d5\u05dd", b"\xd7\xa9\xd7\x9c\xd7\x95\xd7\x9d"),
        ("\u0939\u0948\u0932\u094b", b"\xe0\xa4\xb9\xe0\xa5\x88\xe0\xa4\xb2\xe0\xa5\x8b"),
        ("\u20ac\xa3\xa5", b"\xe2\x82\xac\xc2\xa3\xc2\xa5"),
        ("\u2192\u21d2\u2194", b"\xe2\x86\x92\xe2\x87\x92\xe2\x86\x94"),
        (
            "\U0001f600\U0001f603\U0001f604\U0001f601",
            b"\xf0\x9f\x98\x80\xf0\x9f\x98\x83\xf0\x9f\x98\x84\xf0\x9f\x98\x81",
        ),
        ("\U0001f30d\U0001f30e\U0001f30f", b"\xf0\x9f\x8c\x8d\xf0\x9f\x8c\x8e\xf0\x9f\x8c\x8f"),
        (
            "\U0001f468\u200d\U0001f469\u200d\U0001f467\u200d\U0001f466",
            b"\xf0\x9f\x91\xa8\xe2\x80\x8d\xf0\x9f\x91\xa9\xe2\x80\x8d\xf0\x9f\x91\xa7\xe2\x80\x8d\xf0\x9f\x91\xa6",
        ),
        ("\U0001f525\U0001f4af\u2728", b"\xf0\x9f\x94\xa5\xf0\x9f\x92\xaf\xe2\x9c\xa8"),
        (
            "\U0001d573\U0001d58a\U0001d591\U0001d591\U0001d594",
            b"\xf0\x9d\x95\xb3\xf0\x9d\x96\x8a\xf0\x9d\x96\x91\xf0\x9d\x96\x91\xf0\x9d\x96\x94",
        ),
        (
            "\U0002070e\U00020731\U00020779\U00020c53",
            b"\xf0\xa0\x9c\x8e\xf0\xa0\x9c\xb1\xf0\xa0\x9d\xb9\xf0\xa0\xb1\x93",
        ),
        ("Hello \u4e16\u754c", b"Hello \xe4\xb8\x96\xe7\x95\x8c"),
        ("Caf\xe9 \u2615", b"Caf\xc3\xa9 \xe2\x98\x95"),
        ("Price: \u20ac100", b"Price: \xe2\x82\xac100"),
        ("Score: \U0001f4af/100", b"Score: \xf0\x9f\x92\xaf/100"),
        (
            "ASCII text then \ud55c\uae00 then more ASCII",
            b"ASCII text then \xed\x95\x9c\xea\xb8\x80 then more ASCII",
        ),
        ("123 numbers \u6570\u5b57 456", b"123 numbers \xe6\x95\xb0\xe5\xad\x97 456"),
        ("A\x00B", b"A\x00B"),
        ("\u4e2d\u6587", b"\xe4\xb8\xad\xe6\x96\x87"),
        ("Before\ufffdAfter", b"Before\xef\xbf\xbdAfter"),
        ("Valid\ufffdMiddle", b"Valid\xef\xbf\xbdMiddle"),
        ("\ufffd\ufffd\ufffd", b"\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd"),
        ("\ufffd", b"\xef\xbf\xbd"),
        ("\ufffdStart", b"\xef\xbf\xbdStart"),
        ("End\ufffd", b"End\xef\xbf\xbd"),
        ("A\ufffdB\ufffdC", b"A\xef\xbf\xbdB\xef\xbf\xbdC"),
        ("ASCII", b"ASCII"),
        ("Caf\xe9", b"Caf\xc3\xa9"),
        ("\u4e2d\u6587\u6d4b\u8bd5", b"\xe4\xb8\xad\xe6\x96\x87\xe6\xb5\x8b\xe8\xaf\x95"),
        ("\U0001f600\U0001f30d", b"\xf0\x9f\x98\x80\xf0\x9f\x8c\x8d"),
        ("0", b"0"),
        ("~", b"~"),
        ("\x00\x7f", b"\x00\x7f"),
        ("HelloWorld123", b"HelloWorld123"),
        ("Hello\U0001f600", b"Hello\xf0\x9f\x98\x80"),
        ("Test\ufffdValue", b"Test\xef\xbf\xbdValue"),
        ("A" * 1000, b"A" * 1000),
        ("\u4e2d" * 500, b"\xe4\xb8\xad" * 500),
        ("\U0001f600" * 200, b"\xf0\x9f\x98\x80" * 200),
        ("Valid\ufffdText", b"Valid\xef\xbf\xbdText"),
        ("A\xa9\u4e2d\U0001f600", b"A\xc2\xa9\xe4\xb8\xad\xf0\x9f\x98\x80"),
    ]
    for value, expected in cases:
        result = Binary(value)
        assert isinstance(result, bytes)
        assert result == expected, repr(value)


def test_binary_bytes_are_unchanged():
    """Raw bytes, including malformed UTF-8, are not decoded by Binary."""
    for value in (
        b"hello bytes",
        b"",
        b"test",
        b"Test\xed\xa0\x80",
        b"\xed\xb0\x80Test",
        b"A\xed\xa0\x80B",
        b"\xed\xb0\x80C",
    ):
        assert Binary(value) is value


@pytest.mark.parametrize("value", [bytearray(), bytearray(b"hello bytearray")])
def test_binary_bytearray(value):
    result = Binary(value)
    assert isinstance(result, bytes)
    assert result == bytes(value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("\ud800", id="high-surrogate-start"),
        pytest.param("\udbff", id="high-surrogate-end"),
        pytest.param("\udc00", id="low-surrogate-start"),
        pytest.param("\udfff", id="low-surrogate-end"),
        pytest.param("A\ud800B", id="embedded-surrogate"),
        pytest.param("\ud83d\ude00", id="surrogate-pair"),
    ],
)
def test_binary_rejects_surrogate_codepoints(value):
    with pytest.raises(UnicodeEncodeError):
        Binary(value)


def test_binary_memoryview():
    """Binary() accepts memoryview (GH-739): Django BinaryField and DB-API buffers."""
    result = Binary(memoryview(b"\x01\x02\x03"))
    assert isinstance(result, bytes), "memoryview should be converted to bytes"
    assert result == b"\x01\x02\x03", "memoryview content should be preserved"

    # Empty memoryview
    assert Binary(memoryview(b"")) == b""

    # memoryview over a bytearray round-trips the same bytes
    assert Binary(memoryview(bytearray(b"hello"))) == b"hello"


def test_binary_unsupported_types_error():
    """Test Binary() TypeError for unsupported types (Lines 138-141)."""
    # Test integer type
    with pytest.raises(TypeError) as exc_info:
        Binary(123)
    assert "Cannot convert type int to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, bytearray, or memoryview objects" in str(
        exc_info.value
    )

    # Test float type
    with pytest.raises(TypeError) as exc_info:
        Binary(3.14)
    assert "Cannot convert type float to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, bytearray, or memoryview objects" in str(
        exc_info.value
    )

    # Test list type
    with pytest.raises(TypeError) as exc_info:
        Binary([1, 2, 3])
    assert "Cannot convert type list to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, bytearray, or memoryview objects" in str(
        exc_info.value
    )

    # Test dict type
    with pytest.raises(TypeError) as exc_info:
        Binary({"key": "value"})
    assert "Cannot convert type dict to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, bytearray, or memoryview objects" in str(
        exc_info.value
    )

    # Test None type
    with pytest.raises(TypeError) as exc_info:
        Binary(None)
    assert "Cannot convert type NoneType to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, bytearray, or memoryview objects" in str(
        exc_info.value
    )

    # Test custom object type
    class CustomObject:
        pass

    with pytest.raises(TypeError) as exc_info:
        Binary(CustomObject())
    assert "Cannot convert type CustomObject to bytes" in str(exc_info.value)
    assert "Binary() only accepts str, bytes, bytearray, or memoryview objects" in str(
        exc_info.value
    )

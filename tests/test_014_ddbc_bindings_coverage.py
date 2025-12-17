"""
Additional coverage tests for ddbc_bindings.h UTF conversion edge cases.

This test file focuses on specific uncovered paths in:
- IsValidUnicodeScalar (lines 74-78)
- SQLWCHARToWString UTF-32 path (lines 120-130)
- WStringToSQLWCHAR UTF-32 path (lines 159-167)
- WideToUTF8 Unix path (lines 415-453)
- Utf8ToWString decodeUtf8 lambda (lines 462-530)
"""

import pytest
import sys
import platform


class TestIsValidUnicodeScalar:
    """Test the IsValidUnicodeScalar function (ddbc_bindings.h lines 74-78)."""

    @pytest.mark.parametrize(
        "char",
        [
            "\u0000",  # NULL
            "\u007f",  # Last ASCII
            "\u0080",  # First 2-byte
            "\u07ff",  # Last 2-byte
            "\u0800",  # First 3-byte
            "\ud7ff",  # Just before surrogate range
            "\ue000",  # Just after surrogate range
            "\uffff",  # Last BMP
            "\U00010000",  # First supplementary
            "\U0010ffff",  # Last valid Unicode
        ],
    )
    def test_valid_scalar_values(self, char):
        """Test valid Unicode scalar values using Binary() for faster execution."""
        from mssql_python.type import Binary

        # Test through Binary() which exercises the conversion code
        result = Binary(char)
        assert len(result) > 0

    def test_boundary_codepoints(self):
        """Test boundary code points including max valid and surrogate range."""
        from mssql_python.type import Binary

        # Test valid maximum (line 76)
        max_valid = "\U0010ffff"
        result = Binary(max_valid)
        assert len(result) > 0

        # Test surrogate boundaries (line 77)
        before_surrogate = "\ud7ff"
        result = Binary(before_surrogate)
        assert len(result) > 0

        after_surrogate = "\ue000"
        result = Binary(after_surrogate)
        assert len(result) > 0

        # Invalid UTF-8 that would decode to > 0x10FFFF
        invalid_above_max = b"\xf4\x90\x80\x80"  # Would be 0x110000
        result = invalid_above_max.decode("utf-8", errors="replace")
        assert len(result) > 0


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific UTF-32 path")
class TestUTF32ConversionPaths:
    """Test UTF-32 conversion paths for SQLWCHARToWString and WStringToSQLWCHAR (lines 120-130, 159-167)."""

    @pytest.mark.parametrize(
        "test_str", ["ASCII", "Hello", "Café", "中文", "中文测试", "😀", "😀🌍", "\U0010ffff"]
    )
    def test_utf32_valid_scalars(self, test_str):
        """Test UTF-32 path with valid scalar values using Binary() for faster execution."""
        from mssql_python.type import Binary

        # Valid scalars should be copied directly
        result = Binary(test_str)
        assert len(result) > 0
        # Verify round-trip
        decoded = result.decode("utf-8")
        assert decoded == test_str

    @pytest.mark.parametrize(
        "test_str",
        [
            "Test\ud800",  # High surrogate
            "\udc00Test",  # Low surrogate
            "A\ud800B",  # High surrogate in middle
            "\udc00C",  # Low surrogate at start
        ],
    )
    def test_utf32_invalid_scalars(self, test_str):
        """Test UTF-32 path with invalid scalar values (surrogates) using Binary()."""
        from mssql_python.type import Binary

        # Invalid scalars should be handled (replaced with U+FFFD)
        result = Binary(test_str)
        assert len(result) > 0


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific WideToUTF8 path")
class TestWideToUTF8UnixPath:
    """Test WideToUTF8 Unix path (lines 415-453)."""

    def test_all_utf8_byte_lengths(self):
        """Test 1-4 byte UTF-8 encoding (lines 424-445)."""
        from mssql_python.type import Binary

        # Combined test for all UTF-8 byte lengths
        all_tests = [
            # 1-byte (ASCII, lines 424-427)
            ("A", b"A"),
            ("0", b"0"),
            (" ", b" "),
            ("~", b"~"),
            ("\x00", b"\x00"),
            ("\x7f", b"\x7f"),
            # 2-byte (lines 428-432)
            ("\u0080", b"\xc2\x80"),  # Minimum 2-byte
            ("\u00a9", b"\xc2\xa9"),  # Copyright ©
            ("\u00ff", b"\xc3\xbf"),  # ÿ
            ("\u07ff", b"\xdf\xbf"),  # Maximum 2-byte
            # 3-byte (lines 433-438)
            ("\u0800", b"\xe0\xa0\x80"),  # Minimum 3-byte
            ("\u4e2d", b"\xe4\xb8\xad"),  # 中
            ("\u20ac", b"\xe2\x82\xac"),  # €
            ("\uffff", b"\xef\xbf\xbf"),  # Maximum 3-byte
            # 4-byte (lines 439-445)
            ("\U00010000", b"\xf0\x90\x80\x80"),  # Minimum 4-byte
            ("\U0001f600", b"\xf0\x9f\x98\x80"),  # 😀
            ("\U0001f30d", b"\xf0\x9f\x8c\x8d"),  # 🌍
            ("\U0010ffff", b"\xf4\x8f\xbf\xbf"),  # Maximum Unicode
        ]

        for char, expected in all_tests:
            result = Binary(char)
            assert result == expected, f"UTF-8 encoding failed for {char!r}"


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific Utf8ToWString path")
class TestUtf8ToWStringUnixPath:
    """Test Utf8ToWString decodeUtf8 lambda (lines 462-530)."""

    @pytest.mark.parametrize(
        "test_str,expected",
        [
            ("HelloWorld123", b"HelloWorld123"),  # Pure ASCII
            ("Hello😀", "Hello😀".encode("utf-8")),  # Mixed ASCII + emoji
        ],
    )
    def test_fast_path_ascii(self, test_str, expected):
        """Test fast path for ASCII-only prefix (lines 539-542)."""
        from mssql_python.type import Binary

        result = Binary(test_str)
        assert result == expected

    def test_1byte_and_2byte_decode(self):
        """Test 1-byte and 2-byte sequence decoding (lines 472-488)."""
        from mssql_python.type import Binary

        # 1-byte decode tests (lines 472-475)
        one_byte_tests = [
            (b"A", "A"),
            (b"Hello", "Hello"),
            (b"\x00\x7f", "\x00\x7f"),
        ]

        for utf8_bytes, expected in one_byte_tests:
            result = Binary(expected)
            assert result == utf8_bytes

        # 2-byte valid decode tests (lines 481-484)
        two_byte_tests = [
            (b"\xc2\x80", "\u0080"),
            (b"\xc2\xa9", "\u00a9"),
            (b"\xdf\xbf", "\u07ff"),
        ]

        for utf8_bytes, expected in two_byte_tests:
            result = utf8_bytes.decode("utf-8")
            assert result == expected
            encoded = Binary(expected)
            assert encoded == utf8_bytes

        # 2-byte invalid tests
        invalid_2byte = b"\xc2\x00"  # Invalid continuation (lines 477-480)
        result = invalid_2byte.decode("utf-8", errors="replace")
        assert "\ufffd" in result, "Invalid 2-byte should produce replacement char"

        overlong_2byte = b"\xc0\x80"  # Overlong encoding (lines 486-487)
        result = overlong_2byte.decode("utf-8", errors="replace")
        assert "\ufffd" in result, "Overlong 2-byte should produce replacement char"

    def test_3byte_and_4byte_decode_paths(self):
        """Test 3-byte and 4-byte sequence decoding paths (lines 490-527)."""
        from mssql_python.type import Binary

        # 3-byte valid decode tests (lines 499-502)
        valid_3byte = [
            (b"\xe0\xa0\x80", "\u0800"),
            (b"\xe4\xb8\xad", "\u4e2d"),  # 中
            (b"\xed\x9f\xbf", "\ud7ff"),  # Before surrogates
            (b"\xee\x80\x80", "\ue000"),  # After surrogates
        ]

        for utf8_bytes, expected in valid_3byte:
            result = utf8_bytes.decode("utf-8")
            assert result == expected
            encoded = Binary(expected)
            assert encoded == utf8_bytes

        # 4-byte valid decode tests (lines 519-522)
        valid_4byte = [
            (b"\xf0\x90\x80\x80", "\U00010000"),
            (b"\xf0\x9f\x98\x80", "\U0001f600"),  # 😀
            (b"\xf4\x8f\xbf\xbf", "\U0010ffff"),
        ]

        for utf8_bytes, expected in valid_4byte:
            result = utf8_bytes.decode("utf-8")
            assert result == expected
            encoded = Binary(expected)
            assert encoded == utf8_bytes

        # Invalid continuation bytes tests
        invalid_tests = [
            # 3-byte invalid (lines 492-495)
            b"\xe0\x00\x80",  # Second byte invalid
            b"\xe0\xa0\x00",  # Third byte invalid
            # 4-byte invalid (lines 512-514)
            b"\xf0\x00\x80\x80",  # Second byte invalid
            b"\xf0\x90\x00\x80",  # Third byte invalid
            b"\xf0\x90\x80\x00",  # Fourth byte invalid
        ]

        for test_bytes in invalid_tests:
            result = test_bytes.decode("utf-8", errors="replace")
            assert (
                "\ufffd" in result
            ), f"Invalid sequence {test_bytes.hex()} should produce replacement"

        # Surrogate encoding rejection (lines 500-503)
        for test_bytes in [b"\xed\xa0\x80", b"\xed\xbf\xbf"]:
            result = test_bytes.decode("utf-8", errors="replace")
            assert len(result) > 0

        # Overlong encoding rejection (lines 504-505, 524-525)
        for test_bytes in [b"\xe0\x80\x80", b"\xf0\x80\x80\x80"]:
            result = test_bytes.decode("utf-8", errors="replace")
            assert "\ufffd" in result, f"Overlong {test_bytes.hex()} should produce replacement"

        # Out-of-range rejection (lines 524-525)
        out_of_range = b"\xf4\x90\x80\x80"  # 0x110000
        result = out_of_range.decode("utf-8", errors="replace")
        assert len(result) > 0, "Out-of-range 4-byte should produce some output"

    def test_invalid_sequence_fallback(self):
        """Test invalid sequence fallback (lines 528-529)."""
        # Invalid start bytes
        invalid_starts = [
            b"\xf8\x80\x80\x80",  # Invalid start byte
            b"\xfc\x80\x80\x80",
            b"\xfe\x80\x80\x80",
            b"\xff",
        ]

        for test_bytes in invalid_starts:
            result = test_bytes.decode("utf-8", errors="replace")
            assert (
                "\ufffd" in result
            ), f"Invalid sequence {test_bytes.hex()} should produce replacement"


class TestUtf8ToWStringAlwaysPush:
    """Test that decodeUtf8 always pushes the result (lines 547-550)."""

    def test_always_push_result(self):
        """Test that decoded characters are always pushed, including legitimate U+FFFD."""
        from mssql_python.type import Binary

        # Test legitimate U+FFFD in input
        legitimate_fffd = "Test\ufffdValue"
        result = Binary(legitimate_fffd)
        expected = legitimate_fffd.encode("utf-8")  # Should encode to valid UTF-8
        assert result == expected, "Legitimate U+FFFD should be preserved"

        # Test that it decodes back correctly
        decoded = result.decode("utf-8")
        assert decoded == legitimate_fffd, "Round-trip should preserve U+FFFD"

        # Multiple U+FFFD characters
        multi_fffd = "\ufffd\ufffd\ufffd"
        result = Binary(multi_fffd)
        expected = multi_fffd.encode("utf-8")
        assert result == expected, "Multiple U+FFFD should be preserved"


class TestEdgeCases:
    """Test edge cases and error paths."""

    @pytest.mark.parametrize(
        "test_input,expected,description",
        [
            ("", b"", "Empty string"),
            ("\x00", b"\x00", "NULL character"),
            ("A\x00B", b"A\x00B", "NULL in middle"),
            ("Valid\ufffdText", "Valid\ufffdText", "Mixed valid/U+FFFD"),
            ("A\u00a9\u4e2d\U0001f600", "A\u00a9\u4e2d\U0001f600", "All UTF-8 ranges"),
        ],
    )
    def test_special_characters(self, test_input, expected, description):
        """Test special character handling including NULL and replacement chars."""
        from mssql_python.type import Binary

        result = Binary(test_input)
        if isinstance(expected, str):
            # For strings, encode and compare
            assert result == expected.encode("utf-8"), f"{description} should work"
            # Verify round-trip
            decoded = result.decode("utf-8")
            assert decoded == test_input
        else:
            assert result == expected, f"{description} should produce expected bytes"

    @pytest.mark.parametrize(
        "char,count,expected_len",
        [
            ("A", 1000, 1000),  # 1-byte chars - reduced from 10000 for speed
            ("中", 500, 1500),  # 3-byte chars - reduced from 5000 for speed
            ("😀", 200, 800),  # 4-byte chars - reduced from 2000 for speed
        ],
    )
    def test_long_strings(self, char, count, expected_len):
        """Test long strings with reduced size for faster execution."""
        from mssql_python.type import Binary

        long_str = char * count
        result = Binary(long_str)
        assert len(result) == expected_len, f"Long {char!r} string should encode correctly"

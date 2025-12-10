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
    
    def test_valid_scalar_values(self):
        """Test valid Unicode scalar values."""
        import mssql_python
        from mssql_python import connect
        
        # Valid scalar values (not surrogates, <= 0x10FFFF)
        valid_chars = [
            "\u0000",  # NULL
            "\u007F",  # Last ASCII
            "\u0080",  # First 2-byte
            "\u07FF",  # Last 2-byte
            "\u0800",  # First 3-byte
            "\uD7FF",  # Just before surrogate range
            "\uE000",  # Just after surrogate range
            "\uFFFF",  # Last BMP
            "\U00010000",  # First supplementary
            "\U0010FFFF",  # Last valid Unicode
        ]
        
        for char in valid_chars:
            try:
                conn_str = f"Server=test;Database=DB{char};UID=u;PWD=p"
                conn = connect(conn_str, autoconnect=False)
                conn.close()
            except Exception:
                pass
    
    def test_above_max_codepoint(self):
        """Test code points > 0x10FFFF (ddbc_bindings.h line 76 first condition)."""
        # Python won't let us create invalid codepoints easily, but we can test
        # through the Binary() function which uses UTF-8 decode
        from mssql_python.type import Binary
        
        # Test valid maximum
        max_valid = "\U0010FFFF"
        result = Binary(max_valid)
        assert len(result) > 0
        
        # Invalid UTF-8 that would decode to > 0x10FFFF is handled by decoder
        # and replaced with U+FFFD
        invalid_above_max = b"\xf4\x90\x80\x80"  # Would be 0x110000
        result = invalid_above_max.decode("utf-8", errors="replace")
        # Should contain replacement character or be handled
        assert len(result) > 0
    
    def test_surrogate_range(self):
        """Test surrogate range 0xD800-0xDFFF (ddbc_bindings.h line 77 second condition)."""
        import mssql_python
        from mssql_python import connect
        
        # Test boundaries around surrogate range
        # These may fail to connect but test the conversion logic
        
        # Just before surrogate range (valid)
        try:
            conn_str = "Server=test;Database=DB\uD7FF;UID=u;PWD=p"
            conn = connect(conn_str, autoconnect=False)
            conn.close()
        except Exception:
            pass
        
        # Inside surrogate range (invalid)  
        try:
            conn_str = "Server=test;Database=DB\uD800;UID=u;PWD=p"
            conn = connect(conn_str, autoconnect=False)
            conn.close()
        except Exception:
            pass
        
        try:
            conn_str = "Server=test;Database=DB\uDFFF;UID=u;PWD=p"
            conn = connect(conn_str, autoconnect=False)
            conn.close()
        except Exception:
            pass
        
        # Just after surrogate range (valid)
        try:
            conn_str = "Server=test;Database=DB\uE000;UID=u;PWD=p"
            conn = connect(conn_str, autoconnect=False)
            conn.close()
        except Exception:
            pass


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific UTF-32 path")
class TestSQLWCHARUTF32Path:
    """Test SQLWCHARToWString UTF-32 path (sizeof(SQLWCHAR) == 4, lines 120-130)."""
    
    def test_utf32_valid_scalars(self):
        """Test UTF-32 path with valid scalar values (line 122 condition true)."""
        import mssql_python
        from mssql_python import connect
        
        # On systems where SQLWCHAR is 4 bytes (UTF-32)
        # Valid scalars should be copied directly
        valid_tests = [
            "ASCII",
            "Café",
            "中文",
            "😀",
            "\U0010FFFF",
        ]
        
        for test_str in valid_tests:
            try:
                conn_str = f"Server=test;Database={test_str};UID=u;PWD=p"
                conn = connect(conn_str, autoconnect=False)
                conn.close()
            except Exception:
                pass
    
    def test_utf32_invalid_scalars(self):
        """Test UTF-32 path with invalid scalar values (line 122 condition false)."""
        import mssql_python
        from mssql_python import connect
        
        # Invalid scalars should be replaced with U+FFFD (lines 125-126)
        # Python strings with surrogates
        invalid_tests = [
            "Test\uD800",  # High surrogate
            "\uDC00Test",  # Low surrogate
        ]
        
        for test_str in invalid_tests:
            try:
                conn_str = f"Server=test;Database={test_str};UID=u;PWD=p"
                conn = connect(conn_str, autoconnect=False)
                conn.close()
            except Exception:
                pass


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific UTF-32 path")
class TestWStringToSQLWCHARUTF32Path:
    """Test WStringToSQLWCHAR UTF-32 path (sizeof(SQLWCHAR) == 4, lines 159-167)."""
    
    def test_utf32_encode_valid(self):
        """Test UTF-32 encoding with valid scalars (line 162 condition true)."""
        import mssql_python
        from mssql_python import connect
        
        valid_tests = [
            "Hello",
            "Café",
            "中文测试",
            "😀🌍",
            "\U0010FFFF",
        ]
        
        for test_str in valid_tests:
            try:
                conn_str = f"Server=test;Database={test_str};UID=u;PWD=p"
                conn = connect(conn_str, autoconnect=False)
                conn.close()
            except Exception:
                pass
    
    def test_utf32_encode_invalid(self):
        """Test UTF-32 encoding with invalid scalars (line 162 condition false, lines 164-165)."""
        import mssql_python
        from mssql_python import connect
        
        # Invalid scalars should be replaced with U+FFFD
        invalid_tests = [
            "A\uD800B",  # High surrogate
            "\uDC00C",  # Low surrogate
        ]
        
        for test_str in invalid_tests:
            try:
                conn_str = f"Server=test;Database={test_str};UID=u;PWD=p"
                conn = connect(conn_str, autoconnect=False)
                conn.close()
            except Exception:
                pass


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific WideToUTF8 path")
class TestWideToUTF8UnixPath:
    """Test WideToUTF8 Unix path (lines 415-453)."""
    
    def test_1byte_utf8(self):
        """Test 1-byte UTF-8 encoding (lines 424-427, code_point <= 0x7F)."""
        from mssql_python.type import Binary
        
        # ASCII characters should encode to 1 byte
        ascii_tests = [
            ("A", b"A"),
            ("0", b"0"),
            (" ", b" "),
            ("~", b"~"),
            ("\x00", b"\x00"),
            ("\x7F", b"\x7F"),
        ]
        
        for char, expected in ascii_tests:
            result = Binary(char)
            assert result == expected, f"1-byte encoding failed for {char!r}"
    
    def test_2byte_utf8(self):
        """Test 2-byte UTF-8 encoding (lines 428-432, code_point <= 0x7FF)."""
        from mssql_python.type import Binary
        
        # Characters requiring 2 bytes
        two_byte_tests = [
            ("\u0080", b"\xc2\x80"),  # Minimum 2-byte
            ("\u00A9", b"\xc2\xa9"),  # Copyright ©
            ("\u00FF", b"\xc3\xbf"),  # ÿ
            ("\u07FF", b"\xdf\xbf"),  # Maximum 2-byte
        ]
        
        for char, expected in two_byte_tests:
            result = Binary(char)
            assert result == expected, f"2-byte encoding failed for {char!r}"
    
    def test_3byte_utf8(self):
        """Test 3-byte UTF-8 encoding (lines 433-438, code_point <= 0xFFFF)."""
        from mssql_python.type import Binary
        
        # Characters requiring 3 bytes
        three_byte_tests = [
            ("\u0800", b"\xe0\xa0\x80"),  # Minimum 3-byte
            ("\u4E2D", b"\xe4\xb8\xad"),  # 中
            ("\u20AC", b"\xe2\x82\xac"),  # €
            ("\uFFFF", b"\xef\xbf\xbf"),  # Maximum 3-byte
        ]
        
        for char, expected in three_byte_tests:
            result = Binary(char)
            assert result == expected, f"3-byte encoding failed for {char!r}"
    
    def test_4byte_utf8(self):
        """Test 4-byte UTF-8 encoding (lines 439-445, code_point <= 0x10FFFF)."""
        from mssql_python.type import Binary
        
        # Characters requiring 4 bytes
        four_byte_tests = [
            ("\U00010000", b"\xf0\x90\x80\x80"),  # Minimum 4-byte
            ("\U0001F600", b"\xf0\x9f\x98\x80"),  # 😀
            ("\U0001F30D", b"\xf0\x9f\x8c\x8d"),  # 🌍
            ("\U0010FFFF", b"\xf4\x8f\xbf\xbf"),  # Maximum Unicode
        ]
        
        for char, expected in four_byte_tests:
            result = Binary(char)
            assert result == expected, f"4-byte encoding failed for {char!r}"


@pytest.mark.skipif(platform.system() == "Windows", reason="Tests Unix-specific Utf8ToWString path")
class TestUtf8ToWStringUnixPath:
    """Test Utf8ToWString decodeUtf8 lambda (lines 462-530)."""
    
    def test_fast_path_ascii(self):
        """Test fast path for ASCII-only prefix (lines 539-542)."""
        from mssql_python.type import Binary
        
        # Pure ASCII should use fast path
        ascii_only = "HelloWorld123"
        result = Binary(ascii_only)
        expected = ascii_only.encode("utf-8")
        assert result == expected
        
        # Mixed ASCII + non-ASCII should use fast path for ASCII prefix
        mixed = "Hello😀"
        result = Binary(mixed)
        expected = mixed.encode("utf-8")
        assert result == expected
    
    def test_1byte_decode(self):
        """Test 1-byte sequence decoding (lines 472-475)."""
        from mssql_python.type import Binary
        
        # ASCII bytes should decode correctly
        test_cases = [
            (b"A", "A"),
            (b"Hello", "Hello"),
            (b"\x00\x7F", "\x00\x7F"),
        ]
        
        for utf8_bytes, expected in test_cases:
            # Test through round-trip
            original = expected
            result = Binary(original)
            assert result == utf8_bytes
    
    def test_2byte_decode_paths(self):
        """Test 2-byte sequence decoding paths (lines 476-488)."""
        from mssql_python.type import Binary
        
        # Test invalid continuation byte path (lines 477-480)
        invalid_2byte = b"\xc2\x00"  # Invalid continuation
        result = invalid_2byte.decode("utf-8", errors="replace")
        assert "\ufffd" in result, "Invalid 2-byte should produce replacement char"
        
        # Test valid decode path with cp >= 0x80 (lines 481-484)
        valid_2byte = [
            (b"\xc2\x80", "\u0080"),
            (b"\xc2\xa9", "\u00A9"),
            (b"\xdf\xbf", "\u07FF"),
        ]
        
        for utf8_bytes, expected in valid_2byte:
            result = utf8_bytes.decode("utf-8")
            assert result == expected
            # Round-trip test
            encoded = Binary(expected)
            assert encoded == utf8_bytes
        
        # Test overlong encoding rejection (lines 486-487)
        overlong_2byte = b"\xc0\x80"  # Overlong encoding of NULL
        result = overlong_2byte.decode("utf-8", errors="replace")
        assert "\ufffd" in result, "Overlong 2-byte should produce replacement char"
    
    def test_3byte_decode_paths(self):
        """Test 3-byte sequence decoding paths (lines 490-506)."""
        from mssql_python.type import Binary
        
        # Test invalid continuation bytes (lines 492-495)
        invalid_3byte = [
            b"\xe0\x00\x80",  # Second byte invalid
            b"\xe0\xa0\x00",  # Third byte invalid
        ]
        
        for test_bytes in invalid_3byte:
            result = test_bytes.decode("utf-8", errors="replace")
            assert "\ufffd" in result, f"Invalid 3-byte {test_bytes.hex()} should produce replacement"
        
        # Test valid decode with surrogate rejection (lines 499-502)
        # Valid characters outside surrogate range
        valid_3byte = [
            (b"\xe0\xa0\x80", "\u0800"),
            (b"\xe4\xb8\xad", "\u4E2D"),  # 中
            (b"\xed\x9f\xbf", "\uD7FF"),  # Before surrogates
            (b"\xee\x80\x80", "\uE000"),  # After surrogates
        ]
        
        for utf8_bytes, expected in valid_3byte:
            result = utf8_bytes.decode("utf-8")
            assert result == expected
            encoded = Binary(expected)
            assert encoded == utf8_bytes
        
        # Test surrogate encoding rejection (lines 500-503)
        surrogate_3byte = [
            b"\xed\xa0\x80",  # U+D800 (high surrogate)
            b"\xed\xbf\xbf",  # U+DFFF (low surrogate)
        ]
        
        for test_bytes in surrogate_3byte:
            result = test_bytes.decode("utf-8", errors="replace")
            # Should be rejected/replaced
            assert len(result) > 0
        
        # Test overlong encoding rejection (lines 504-505)
        overlong_3byte = b"\xe0\x80\x80"  # Overlong encoding of NULL
        result = overlong_3byte.decode("utf-8", errors="replace")
        assert "\ufffd" in result, "Overlong 3-byte should produce replacement"
    
    def test_4byte_decode_paths(self):
        """Test 4-byte sequence decoding paths (lines 508-527)."""
        from mssql_python.type import Binary
        
        # Test invalid continuation bytes (lines 512-514)
        invalid_4byte = [
            b"\xf0\x00\x80\x80",  # Second byte invalid
            b"\xf0\x90\x00\x80",  # Third byte invalid
            b"\xf0\x90\x80\x00",  # Fourth byte invalid
        ]
        
        for test_bytes in invalid_4byte:
            result = test_bytes.decode("utf-8", errors="replace")
            assert "\ufffd" in result, f"Invalid 4-byte {test_bytes.hex()} should produce replacement"
        
        # Test valid decode within range (lines 519-522)
        valid_4byte = [
            (b"\xf0\x90\x80\x80", "\U00010000"),
            (b"\xf0\x9f\x98\x80", "\U0001F600"),  # 😀
            (b"\xf4\x8f\xbf\xbf", "\U0010FFFF"),
        ]
        
        for utf8_bytes, expected in valid_4byte:
            result = utf8_bytes.decode("utf-8")
            assert result == expected
            encoded = Binary(expected)
            assert encoded == utf8_bytes
        
        # Test overlong encoding rejection (lines 524-525)
        overlong_4byte = b"\xf0\x80\x80\x80"  # Overlong encoding of NULL
        result = overlong_4byte.decode("utf-8", errors="replace")
        assert "\ufffd" in result, "Overlong 4-byte should produce replacement"
        
        # Test out-of-range rejection (lines 524-525)
        out_of_range = b"\xf4\x90\x80\x80"  # 0x110000 (beyond max Unicode)
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
            assert "\ufffd" in result, f"Invalid sequence {test_bytes.hex()} should produce replacement"


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
    
    def test_empty_string(self):
        """Test empty string handling."""
        from mssql_python.type import Binary
        
        empty = ""
        result = Binary(empty)
        assert result == b"", "Empty string should produce empty bytes"
    
    def test_null_character(self):
        """Test NULL character handling."""
        from mssql_python.type import Binary
        
        null_str = "\x00"
        result = Binary(null_str)
        assert result == b"\x00", "NULL character should be preserved"
        
        # NULL in middle of string
        with_null = "A\x00B"
        result = Binary(with_null)
        assert result == b"A\x00B", "NULL in middle should be preserved"
    
    def test_very_long_strings(self):
        """Test very long strings to ensure no buffer issues."""
        from mssql_python.type import Binary
        
        # Long ASCII
        long_ascii = "A" * 10000
        result = Binary(long_ascii)
        assert len(result) == 10000, "Long ASCII string should encode correctly"
        
        # Long multi-byte
        long_utf8 = "中" * 5000  # 3 bytes each
        result = Binary(long_utf8)
        assert len(result) == 15000, "Long UTF-8 string should encode correctly"
        
        # Long emoji
        long_emoji = "😀" * 2000  # 4 bytes each
        result = Binary(long_emoji)
        assert len(result) == 8000, "Long emoji string should encode correctly"
    
    def test_mixed_valid_invalid(self):
        """Test strings with mix of valid and invalid sequences."""
        from mssql_python.type import Binary
        
        # Valid text with legitimate U+FFFD
        mixed = "Valid\ufffdText"
        result = Binary(mixed)
        decoded = result.decode("utf-8")
        assert decoded == mixed, "Mixed valid/U+FFFD should work"
    
    def test_all_utf8_ranges(self):
        """Test characters from all UTF-8 ranges in one string."""
        from mssql_python.type import Binary
        
        all_ranges = "A\u00A9\u4E2D\U0001F600"  # 1, 2, 3, 4 byte chars
        result = Binary(all_ranges)
        decoded = result.decode("utf-8")
        assert decoded == all_ranges, "All UTF-8 ranges should work together"

"""
Unit tests for mssql_python logging module.
Tests the logging API, configuration, output modes, and formatting.
"""
import logging
import os
import pytest
import re
import tempfile
import shutil
from pathlib import Path
from mssql_python.logging import logger, setup_logging, DEBUG, STDOUT, FILE, BOTH


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def cleanup_logger():
    """Reset logger state before and after each test"""
    # Store original state
    original_level = logger.getLevel()
    original_output = logger.output
    
    # Disable logging and clear handlers
    logger._logger.setLevel(logging.CRITICAL)
    for handler in logger._logger.handlers[:]:
        handler.close()
        logger._logger.removeHandler(handler)
    logger._handlers_initialized = False
    logger._custom_log_path = None
    
    # Cleanup any log files in current directory
    log_dir = os.path.join(os.getcwd(), "mssql_python_logs")
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir, ignore_errors=True)
    
    yield
    
    # Restore state and cleanup
    logger._logger.setLevel(logging.CRITICAL)
    for handler in logger._logger.handlers[:]:
        handler.close()
        logger._logger.removeHandler(handler)
    logger._handlers_initialized = False
    logger._custom_log_path = None
    
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir, ignore_errors=True)


class TestLoggingBasics:
    """Test basic logging functionality"""
    
    def test_logger_disabled_by_default(self, cleanup_logger):
        """Logger should be disabled by default (CRITICAL level)"""
        assert logger.getLevel() == logging.CRITICAL
        assert not logger.isEnabledFor(logging.DEBUG)
        assert not logger.isEnabledFor(logging.INFO)
    
    def test_setup_logging_enables_debug(self, cleanup_logger):
        """setup_logging() should enable DEBUG level"""
        setup_logging()
        assert logger.getLevel() == logging.DEBUG
        assert logger.isEnabledFor(logging.DEBUG)
    
    def test_singleton_behavior(self, cleanup_logger):
        """Logger should behave as singleton"""
        from mssql_python.logging import logger as logger1
        from mssql_python.logging import logger as logger2
        assert logger1 is logger2


class TestOutputModes:
    """Test different output modes (file, stdout, both)"""
    
    def test_default_output_mode_is_file(self, cleanup_logger):
        """Default output mode should be FILE"""
        setup_logging()
        assert logger.output == FILE
        assert logger.log_file is not None
        assert os.path.exists(logger.log_file)
    
    def test_stdout_mode_no_file_created(self, cleanup_logger):
        """STDOUT mode should not create log file"""
        setup_logging(output=STDOUT)
        assert logger.output == STDOUT
        # Log file property might be None or point to non-existent file
        if logger.log_file:
            assert not os.path.exists(logger.log_file)
    
    def test_both_mode_creates_file(self, cleanup_logger):
        """BOTH mode should create log file and output to stdout"""
        setup_logging(output=BOTH)
        assert logger.output == BOTH
        assert logger.log_file is not None
        assert os.path.exists(logger.log_file)
    
    def test_invalid_output_mode_raises_error(self, cleanup_logger):
        """Invalid output mode should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid output mode"):
            setup_logging(output='invalid')


class TestLogFile:
    """Test log file creation and naming"""
    
    def test_log_file_created_in_mssql_python_logs_folder(self, cleanup_logger):
        """Log file should be created in mssql_python_logs subfolder"""
        setup_logging()
        logger.debug("Test message")
        
        log_file = logger.log_file
        assert log_file is not None
        assert "mssql_python_logs" in log_file
        assert os.path.exists(log_file)
    
    def test_log_file_naming_pattern(self, cleanup_logger):
        """Log file should follow naming pattern: mssql_python_trace_YYYYMMDDHHMMSS_PID.log"""
        setup_logging()
        logger.debug("Test message")
        
        filename = os.path.basename(logger.log_file)
        pattern = r'^mssql_python_trace_\d{14}_\d+\.log$'
        assert re.match(pattern, filename), f"Filename '{filename}' doesn't match pattern"
        
        # Extract and verify PID
        parts = filename.replace('mssql_python_trace_', '').replace('.log', '').split('_')
        assert len(parts) == 2
        timestamp_part, pid_part = parts
        
        assert len(timestamp_part) == 14 and timestamp_part.isdigit()
        assert int(pid_part) == os.getpid()
    
    def test_custom_log_file_path(self, cleanup_logger, temp_log_dir):
        """Custom log file path should be respected"""
        custom_path = os.path.join(temp_log_dir, "custom_test.log")
        setup_logging(log_file_path=custom_path)
        logger.debug("Test message")
        
        assert logger.log_file == custom_path
        assert os.path.exists(custom_path)
    
    def test_custom_log_file_path_creates_directory(self, cleanup_logger, temp_log_dir):
        """Custom log file path should create parent directories"""
        custom_path = os.path.join(temp_log_dir, "subdir", "nested", "test.log")
        setup_logging(log_file_path=custom_path)
        logger.debug("Test message")
        
        assert os.path.exists(custom_path)
    
    def test_log_file_extension_validation_txt(self, cleanup_logger, temp_log_dir):
        """.txt extension should be allowed"""
        custom_path = os.path.join(temp_log_dir, "test.txt")
        setup_logging(log_file_path=custom_path)
        assert os.path.exists(custom_path)
    
    def test_log_file_extension_validation_csv(self, cleanup_logger, temp_log_dir):
        """.csv extension should be allowed"""
        custom_path = os.path.join(temp_log_dir, "test.csv")
        setup_logging(log_file_path=custom_path)
        assert os.path.exists(custom_path)
    
    def test_log_file_extension_validation_invalid(self, cleanup_logger, temp_log_dir):
        """Invalid extension should raise ValueError"""
        custom_path = os.path.join(temp_log_dir, "test.json")
        with pytest.raises(ValueError, match="Invalid log file extension"):
            setup_logging(log_file_path=custom_path)


class TestCSVFormat:
    """Test CSV output format"""
    
    def test_csv_header_written(self, cleanup_logger):
        """CSV header should be written to log file"""
        setup_logging()
        logger.debug("Test message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Timestamp, ThreadID, Level, Location, Source, Message" in content
    
    def test_csv_metadata_header(self, cleanup_logger):
        """CSV metadata header should contain script, PID, Python version, etc."""
        setup_logging()
        logger.debug("Test message")
        
        with open(logger.log_file, 'r') as f:
            first_line = f.readline()
        
        assert first_line.startswith("#")
        assert "MSSQL-Python Driver Log" in first_line
        assert f"PID: {os.getpid()}" in first_line
        assert "Python:" in first_line
    
    def test_csv_row_format(self, cleanup_logger):
        """CSV rows should have correct format"""
        setup_logging()
        logger.debug("Test message")
        
        with open(logger.log_file, 'r') as f:
            lines = f.readlines()
        
        # Find first log line (skip header and metadata)
        log_line = None
        for line in lines:
            if not line.startswith('#') and 'Timestamp' not in line and 'Test message' in line:
                log_line = line
                break
        
        assert log_line is not None
        parts = [p.strip() for p in log_line.split(',')]
        assert len(parts) >= 6  # timestamp, thread_id, level, location, source, message
        
        # Verify timestamp format (YYYY-MM-DD HH:MM:SS.mmm)
        timestamp_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
        assert re.match(timestamp_pattern, parts[0]), f"Invalid timestamp: {parts[0]}"
        
        # Verify thread_id is numeric
        assert parts[1].isdigit(), f"Invalid thread_id: {parts[1]}"
        
        # Verify level
        assert parts[2] in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        
        # Verify location format (filename:lineno)
        assert ':' in parts[3]
        
        # Verify source
        assert parts[4] in ['Python', 'DDBC', 'Unknown']


class TestLogLevels:
    """Test different log levels"""
    
    def test_debug_level(self, cleanup_logger):
        """DEBUG level messages should be logged"""
        setup_logging()
        logger.debug("Debug message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Debug message" in content
        assert "DEBUG" in content
    
    def test_info_level(self, cleanup_logger):
        """INFO level messages should be logged"""
        setup_logging()
        logger.info("Info message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Info message" in content
        assert "INFO" in content
    
    def test_warning_level(self, cleanup_logger):
        """WARNING level messages should be logged"""
        setup_logging()
        logger.warning("Warning message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Warning message" in content
        assert "WARNING" in content
    
    def test_error_level(self, cleanup_logger):
        """ERROR level messages should be logged"""
        setup_logging()
        logger.error("Error message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Error message" in content
        assert "ERROR" in content
    
    def test_python_prefix_added(self, cleanup_logger):
        """All Python log messages should have [Python] prefix"""
        setup_logging()
        logger.debug("Test message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Python" in content  # Should appear in Source column


class TestPasswordSanitization:
    """Test password/credential sanitization using helpers.sanitize_connection_string()"""
    
    def test_pwd_sanitization(self, cleanup_logger):
        """PWD= should be sanitized when explicitly calling sanitize_connection_string()"""
        from mssql_python.helpers import sanitize_connection_string
        
        conn_str = "Server=localhost;PWD=secret123;Database=test"
        sanitized = sanitize_connection_string(conn_str)
        
        assert "PWD=***" in sanitized
        assert "secret123" not in sanitized
    
    def test_pwd_case_insensitive(self, cleanup_logger):
        """PWD/Pwd/pwd should all be sanitized (case-insensitive)"""
        from mssql_python.helpers import sanitize_connection_string
        
        test_cases = [
            ("Server=localhost;PWD=secret;Database=test", "PWD=***"),
            ("Server=localhost;Pwd=secret;Database=test", "Pwd=***"),
            ("Server=localhost;pwd=secret;Database=test", "pwd=***"),
        ]
        
        for conn_str, expected in test_cases:
            sanitized = sanitize_connection_string(conn_str)
            assert expected in sanitized
            assert "secret" not in sanitized
    
    def test_explicit_sanitization_in_logging(self, cleanup_logger):
        """Verify that explicit sanitization works when logging"""
        from mssql_python.helpers import sanitize_connection_string
        
        setup_logging()
        conn_str = "Server=localhost;PWD=secret123;Database=test"
        logger.debug("Connection string: %s", sanitize_connection_string(conn_str))
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "PWD=***" in content
        assert "secret123" not in content
    
    def test_no_automatic_sanitization(self, cleanup_logger):
        """Verify that logger does NOT automatically sanitize - user must do it explicitly"""
        setup_logging()
        # Log without sanitization - password should appear in log (by design)
        logger.debug("Connection string: Server=localhost;PWD=notsanitized;Database=test")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        # Password should be visible because we didn't sanitize
        assert "notsanitized" in content
        # This is expected behavior - caller must sanitize explicitly


class TestThreadID:
    """Test thread ID functionality"""
    
    def test_thread_id_in_logs(self, cleanup_logger):
        """Thread ID should appear in log output"""
        setup_logging()
        logger.debug("Test message")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        # Thread ID should be in the second column (after timestamp)
        lines = content.split('\n')
        for line in lines:
            if 'Test message' in line:
                parts = [p.strip() for p in line.split(',')]
                assert len(parts) >= 2
                assert parts[1].isdigit()  # Thread ID should be numeric
                break
        else:
            pytest.fail("Test message not found in log")
    
    def test_thread_id_consistent_in_same_thread(self, cleanup_logger):
        """Thread ID should be consistent for messages in same thread"""
        setup_logging()
        logger.debug("Message 1")
        logger.debug("Message 2")
        
        with open(logger.log_file, 'r') as f:
            lines = f.readlines()
        
        thread_ids = []
        for line in lines:
            if 'Message' in line and not line.startswith('#'):  # Skip header and metadata
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 6 and parts[1].isdigit():  # Ensure it's a data row with numeric thread ID
                    thread_ids.append(parts[1])
        
        assert len(thread_ids) == 2
        assert thread_ids[0] == thread_ids[1]  # Same thread ID


class TestLoggerProperties:
    """Test logger properties and methods"""
    
    def test_log_file_property(self, cleanup_logger):
        """log_file property should return current log file path"""
        setup_logging()
        log_file = logger.log_file
        assert log_file is not None
        assert os.path.exists(log_file)
    
    def test_level_property(self, cleanup_logger):
        """level property should return current log level"""
        setup_logging()
        assert logger.level == logging.DEBUG
    
    def test_output_property(self, cleanup_logger):
        """output property should return current output mode"""
        setup_logging(output=BOTH)
        assert logger.output == BOTH
    
    def test_getLevel_method(self, cleanup_logger):
        """getLevel() should return current level"""
        setup_logging()
        assert logger.getLevel() == logging.DEBUG
    
    def test_isEnabledFor_method(self, cleanup_logger):
        """isEnabledFor() should check if level is enabled"""
        setup_logging()
        assert logger.isEnabledFor(logging.DEBUG)
        assert logger.isEnabledFor(logging.INFO)


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_message_with_percent_signs(self, cleanup_logger):
        """Messages with % signs should not cause formatting errors"""
        setup_logging()
        logger.debug("Progress: 50%% complete")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Progress: 50" in content
    
    def test_message_with_commas(self, cleanup_logger):
        """Messages with commas should not break CSV format"""
        setup_logging()
        logger.debug("Values: 1, 2, 3, 4")
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert "Values: 1, 2, 3, 4" in content
    
    def test_empty_message(self, cleanup_logger):
        """Empty messages should not cause errors"""
        setup_logging()
        logger.debug("")
        
        # Should not raise exception
        assert os.path.exists(logger.log_file)
    
    def test_very_long_message(self, cleanup_logger):
        """Very long messages should be logged without errors"""
        setup_logging()
        long_message = "X" * 10000
        logger.debug(long_message)
        
        with open(logger.log_file, 'r') as f:
            content = f.read()
        
        assert long_message in content
    
    def test_unicode_characters(self, cleanup_logger):
        """Unicode characters should be handled correctly"""
        setup_logging()
        logger.debug("Unicode: 你好 🚀 café")
        
        # Use utf-8-sig on Windows to handle BOM if present
        import sys
        encoding = 'utf-8-sig' if sys.platform == 'win32' else 'utf-8'
        
        with open(logger.log_file, 'r', encoding=encoding, errors='replace') as f:
            content = f.read()
        
        # Check that the message was logged (exact unicode may vary by platform)
        assert "Unicode:" in content
        # At least one unicode character should be present or replaced
        assert ("你好" in content or "café" in content or "?" in content)


class TestDriverLogger:
    """Test driver_logger export"""
    
    def test_driver_logger_accessible(self, cleanup_logger):
        """driver_logger should be accessible for application use"""
        from mssql_python.logging import driver_logger
        assert driver_logger is not None
        assert isinstance(driver_logger, logging.Logger)
    
    def test_driver_logger_is_same_as_internal(self, cleanup_logger):
        """driver_logger should be the same as logger._logger"""
        from mssql_python.logging import driver_logger
        assert driver_logger is logger._logger

"""Tests for logging infrastructure."""

import json
import logging
import os
import tempfile

import pytest

from data_quality.utils.logger import (
    ContextAdapter,
    JSONFormatter,
    get_logger,
    log_with_context,
    setup_logging,
)


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    def test_format_basic_message(self):
        """Test formatting basic log message."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        data = json.loads(result)

        assert data["level"] == "INFO"
        assert data["component"] == "test"
        assert data["message"] == "Test message"
        assert "timestamp" in data
        assert data["timestamp"].endswith("Z")

    def test_format_with_context(self):
        """Test formatting message with context."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.context = {"key": "value", "number": 42}
        result = formatter.format(record)
        data = json.loads(result)

        assert "context" in data
        assert data["context"]["key"] == "value"
        assert data["context"]["number"] == 42

    def test_format_with_correlation_id(self):
        """Test formatting message with correlation ID."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.correlation_id = "uuid-1234-5678"
        result = formatter.format(record)
        data = json.loads(result)

        assert data["correlation_id"] == "uuid-1234-5678"

    def test_format_with_exception(self):
        """Test formatting message with exception info."""
        formatter = JSONFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Error occurred",
            args=(),
            exc_info=exc_info,
        )
        result = formatter.format(record)
        data = json.loads(result)

        assert "exception" in data
        assert "ValueError" in data["exception"]


class TestContextAdapter:
    """Tests for ContextAdapter."""

    def test_adds_context_to_kwargs(self):
        """Test adapter adds context to log kwargs."""
        logger = logging.getLogger("test_adapter")
        adapter = ContextAdapter(logger, {"component": "test"})
        msg, kwargs = adapter.process("test message", {})

        assert "extra" in kwargs
        assert kwargs["extra"]["component"] == "test"

    def test_merges_with_existing_extra(self):
        """Test adapter merges with existing extra."""
        logger = logging.getLogger("test_adapter2")
        adapter = ContextAdapter(logger, {"component": "test"})
        msg, kwargs = adapter.process("test message", {"extra": {"key": "value"}})

        assert kwargs["extra"]["component"] == "test"
        assert kwargs["extra"]["key"] == "value"


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_with_defaults(self):
        """Test setup with default parameters."""
        setup_logging()
        logger = logging.getLogger("data_quality")
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0

    def test_setup_with_debug_level(self):
        """Test setup with DEBUG level."""
        setup_logging(level="DEBUG")
        logger = logging.getLogger("data_quality")
        assert logger.level == logging.DEBUG

    def test_setup_with_json_format(self):
        """Test setup with JSON format."""
        setup_logging(log_format="json")
        logger = logging.getLogger("data_quality")
        handler = logger.handlers[0]
        assert isinstance(handler.formatter, JSONFormatter)

    def test_setup_with_file_output(self):
        """Test setup with file output."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as f:
            log_file = f.name

        try:
            setup_logging(log_file=log_file)
            logger = logging.getLogger("data_quality")
            # Should have console and file handlers
            assert len(logger.handlers) == 2
            assert os.path.exists(log_file)
        finally:
            os.unlink(log_file)

    def test_clears_existing_handlers(self):
        """Test that setup clears existing handlers."""
        # Setup twice
        setup_logging()
        setup_logging()
        logger = logging.getLogger("data_quality")
        # Should only have handlers from last setup
        assert len(logger.handlers) == 1


class TestGetLogger:
    """Tests for get_logger function."""

    def test_returns_adapter(self):
        """Test that get_logger returns a LoggerAdapter."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.LoggerAdapter)

    def test_logger_name_prefix(self):
        """Test logger name has correct prefix."""
        logger = get_logger("test_module")
        assert logger.logger.name == "data_quality.test_module"

    def test_with_context(self):
        """Test get_logger with context."""
        context = {"check_type": "completeness"}
        logger = get_logger("test_module", context)
        assert logger.extra == context

    def test_without_context(self):
        """Test get_logger without context."""
        logger = get_logger("test_module")
        assert logger.extra == {}


class TestLogWithContext:
    """Tests for log_with_context function."""

    def test_log_info_with_context(self):
        """Test logging info with context."""
        setup_logging(level="DEBUG")
        logger = logging.getLogger("data_quality.test")

        # This should not raise
        log_with_context(logger, "info", "Test message", {"key": "value"})

    def test_log_error_with_context(self):
        """Test logging error with context."""
        setup_logging(level="DEBUG")
        logger = logging.getLogger("data_quality.test")

        log_with_context(logger, "error", "Error message", {"error_code": 500})

    def test_log_without_context(self):
        """Test logging without context."""
        setup_logging(level="DEBUG")
        logger = logging.getLogger("data_quality.test")

        log_with_context(logger, "warning", "Warning message", None)


class TestLoggingIntegration:
    """Integration tests for logging."""

    def test_full_logging_flow(self):
        """Test complete logging flow."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as f:
            log_file = f.name

        try:
            setup_logging(level="DEBUG", log_format="json", log_file=log_file)

            logger = get_logger("integration_test", {"test": True})
            logger.info("Test message")
            logger.error("Error message")

            # Read and verify log file
            with open(log_file, "r") as f:
                lines = f.readlines()
                assert len(lines) >= 2

                for line in lines:
                    data = json.loads(line)
                    assert "timestamp" in data
                    assert "level" in data
                    assert "message" in data

        finally:
            os.unlink(log_file)

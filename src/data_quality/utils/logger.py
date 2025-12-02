"""Logging configuration for the Data Quality Framework."""

import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }

        # Add extra context if available
        if hasattr(record, "context"):
            log_entry["context"] = record.context

        # Add correlation ID if available
        if hasattr(record, "correlation_id"):
            log_entry["correlation_id"] = record.correlation_id

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


class ContextAdapter(logging.LoggerAdapter):
    """Logger adapter that adds context to log messages."""

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        extra = kwargs.get("extra", {})
        if self.extra:
            extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def setup_logging(
    level: str = "INFO",
    log_format: str = "text",
    log_file: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> None:
    """
    Set up logging configuration for the framework.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format type ('text' or 'json')
        log_file: Optional file path for log output
        correlation_id: Optional correlation ID for tracking
    """
    root_logger = logging.getLogger("data_quality")
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))

    if log_format == "json":
        console_handler.setFormatter(JSONFormatter())
    else:
        text_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(text_formatter)

    root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # File gets all logs

        if log_format == "json":
            file_handler.setFormatter(JSONFormatter())
        else:
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )

        root_logger.addHandler(file_handler)

    # Store correlation ID
    if correlation_id:
        root_logger.correlation_id = correlation_id


def get_logger(
    name: str,
    context: Optional[Dict[str, Any]] = None
) -> logging.LoggerAdapter:
    """
    Get a logger with optional context.

    Args:
        name: Logger name (usually module name)
        context: Optional context to include in all log messages

    Returns:
        LoggerAdapter with context support
    """
    logger = logging.getLogger(f"data_quality.{name}")
    return ContextAdapter(logger, context or {})


def log_with_context(
    logger: logging.Logger,
    level: str,
    message: str,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log a message with additional context.

    Args:
        logger: Logger instance
        level: Log level
        message: Log message
        context: Additional context dictionary
    """
    extra = {"context": context} if context else {}
    log_method = getattr(logger, level.lower())
    log_method(message, extra=extra)

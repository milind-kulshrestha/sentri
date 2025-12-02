"""Custom exceptions for the Sentri data quality framework."""

from typing import Any, Dict, Optional


class DQFrameworkError(Exception):
    """Base exception for all Sentri framework errors."""

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} [{context_str}]"
        return self.message


class ConfigurationError(DQFrameworkError):
    """Raised when configuration is invalid or missing."""

    def __init__(
        self,
        message: str,
        field_path: Optional[str] = None,
        line_number: Optional[int] = None,
        suggestion: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.field_path = field_path
        self.line_number = line_number
        self.suggestion = suggestion
        context = context or {}
        if field_path:
            context["field_path"] = field_path
        if line_number:
            context["line_number"] = line_number
        if suggestion:
            context["suggestion"] = suggestion
        super().__init__(message, context)


class ConnectionError(DQFrameworkError):
    """Raised when connection to a data source fails."""

    def __init__(
        self,
        message: str,
        connector_type: Optional[str] = None,
        retry_count: int = 0,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.connector_type = connector_type
        self.retry_count = retry_count
        context = context or {}
        if connector_type:
            context["connector_type"] = connector_type
        context["retry_count"] = retry_count
        super().__init__(message, context)


class DataRetrievalError(DQFrameworkError):
    """Raised when data retrieval from a source fails."""

    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.query = query
        context = context or {}
        if query:
            # Truncate long queries
            context["query"] = query[:200] + "..." if len(query) > 200 else query
        super().__init__(message, context)


class CheckError(DQFrameworkError):
    """Base exception for check-related errors."""

    def __init__(
        self,
        message: str,
        check_type: Optional[str] = None,
        column: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.check_type = check_type
        self.column = column
        context = context or {}
        if check_type:
            context["check_type"] = check_type
        if column:
            context["column"] = column
        super().__init__(message, context)


class InvalidThresholdError(CheckError):
    """Raised when threshold configuration is invalid."""
    pass


class MissingColumnError(CheckError):
    """Raised when a required column is missing from the data."""
    pass


class FilterError(CheckError):
    """Raised when a filter condition is invalid."""

    def __init__(
        self,
        message: str,
        filter_condition: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.filter_condition = filter_condition
        context = context or {}
        if filter_condition:
            context["filter_condition"] = filter_condition
        super().__init__(message, context=context)


class ExecutionError(CheckError):
    """Raised when check execution fails."""
    pass


class DataTypeError(ExecutionError):
    """Raised when data type is incompatible with the check."""
    pass


class CalculationError(ExecutionError):
    """Raised when a calculation fails during check execution."""
    pass


class InsufficientDataError(ExecutionError):
    """Raised when there is insufficient data to perform a check."""
    pass


class ValidationError(DQFrameworkError):
    """Raised when validation fails."""
    pass


class ThresholdViolationError(ValidationError):
    """Raised when a threshold is violated."""

    def __init__(
        self,
        message: str,
        threshold_type: Optional[str] = None,
        threshold_value: Optional[float] = None,
        actual_value: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.threshold_type = threshold_type
        self.threshold_value = threshold_value
        self.actual_value = actual_value
        context = context or {}
        if threshold_type:
            context["threshold_type"] = threshold_type
        if threshold_value is not None:
            context["threshold_value"] = threshold_value
        if actual_value is not None:
            context["actual_value"] = actual_value
        super().__init__(message, context)


class OutputError(DQFrameworkError):
    """Raised when output generation fails."""
    pass


class AlertingError(DQFrameworkError):
    """Raised when alerting fails."""
    pass

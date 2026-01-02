"""Tests for custom exceptions."""

import pytest

from data_quality.core.exceptions import (
    AlertingError,
    CalculationError,
    CheckError,
    ConfigurationError,
    ConnectionError,
    DataRetrievalError,
    DataTypeError,
    DQFrameworkError,
    ExecutionError,
    FilterError,
    InsufficientDataError,
    InvalidThresholdError,
    MissingColumnError,
    OutputError,
    ThresholdViolationError,
    ValidationError,
)


class TestDQFrameworkError:
    """Tests for base DQFrameworkError."""

    def test_basic_creation(self):
        """Test creating error with just message."""
        error = DQFrameworkError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.context == {}

    def test_with_context(self):
        """Test creating error with context."""
        context = {"key": "value", "number": 42}
        error = DQFrameworkError("Test error", context=context)
        assert "key=value" in str(error)
        assert "number=42" in str(error)
        assert error.context == context

    def test_is_exception(self):
        """Test that it's a proper exception."""
        with pytest.raises(DQFrameworkError) as exc_info:
            raise DQFrameworkError("Test")
        assert "Test" in str(exc_info.value)


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_basic_creation(self):
        """Test creating configuration error."""
        error = ConfigurationError("Invalid config")
        assert "Invalid config" in str(error)

    def test_with_field_path(self):
        """Test with field path specified."""
        error = ConfigurationError(
            "Invalid value", field_path="checks.completeness.column1"
        )
        assert error.field_path == "checks.completeness.column1"
        assert "field_path=checks.completeness.column1" in str(error)

    def test_with_line_number(self):
        """Test with line number specified."""
        error = ConfigurationError("Syntax error", line_number=45)
        assert error.line_number == 45
        assert "line_number=45" in str(error)

    def test_with_suggestion(self):
        """Test with suggestion for fix."""
        error = ConfigurationError(
            "Type error", suggestion="Remove quotes around numeric values"
        )
        assert error.suggestion == "Remove quotes around numeric values"
        assert "suggestion=" in str(error)

    def test_all_fields(self):
        """Test with all optional fields."""
        error = ConfigurationError(
            "Invalid threshold",
            field_path="checks.completeness.col1.thresholds",
            line_number=50,
            suggestion="Use float instead of string",
        )
        error_str = str(error)
        assert "field_path=" in error_str
        assert "line_number=50" in error_str
        assert "suggestion=" in error_str


class TestConnectionError:
    """Tests for ConnectionError."""

    def test_basic_creation(self):
        """Test creating connection error."""
        error = ConnectionError("Connection failed")
        assert error.retry_count == 0

    def test_with_connector_type(self):
        """Test with connector type."""
        error = ConnectionError("Auth failed", connector_type="oracle")
        assert error.connector_type == "oracle"
        assert "connector_type=oracle" in str(error)

    def test_with_retry_count(self):
        """Test with retry count."""
        error = ConnectionError("Timeout", retry_count=3)
        assert error.retry_count == 3
        assert "retry_count=3" in str(error)


class TestDataRetrievalError:
    """Tests for DataRetrievalError."""

    def test_basic_creation(self):
        """Test creating data retrieval error."""
        error = DataRetrievalError("Query failed")
        assert error.query is None

    def test_with_short_query(self):
        """Test with short query."""
        query = "SELECT * FROM table"
        error = DataRetrievalError("Failed", query=query)
        assert error.query == query
        assert query in str(error)

    def test_with_long_query_truncation(self):
        """Test that long queries are truncated."""
        long_query = "SELECT " + "a, " * 100 + "b FROM table"
        error = DataRetrievalError("Failed", query=long_query)
        error_str = str(error)
        assert "..." in error_str
        assert len(error.context.get("query", "")) <= 203  # 200 + "..."


class TestCheckError:
    """Tests for CheckError and subclasses."""

    def test_basic_creation(self):
        """Test creating check error."""
        error = CheckError("Check failed")
        assert error.check_type is None
        assert error.column is None

    def test_with_check_type(self):
        """Test with check type."""
        error = CheckError("Failed", check_type="completeness")
        assert error.check_type == "completeness"
        assert "check_type=completeness" in str(error)

    def test_with_column(self):
        """Test with column name."""
        error = CheckError("Failed", column="carbon_em")
        assert error.column == "carbon_em"
        assert "column=carbon_em" in str(error)

    def test_invalid_threshold_error(self):
        """Test InvalidThresholdError is a CheckError."""
        error = InvalidThresholdError("Invalid threshold")
        assert isinstance(error, CheckError)

    def test_missing_column_error(self):
        """Test MissingColumnError is a CheckError."""
        error = MissingColumnError("Column not found", column="missing_col")
        assert isinstance(error, CheckError)
        assert error.column == "missing_col"


class TestFilterError:
    """Tests for FilterError."""

    def test_basic_creation(self):
        """Test creating filter error."""
        error = FilterError("Invalid filter")
        assert error.filter_condition is None

    def test_with_filter_condition(self):
        """Test with filter condition."""
        error = FilterError("Syntax error", filter_condition="universe == 'US'")
        assert error.filter_condition == "universe == 'US'"
        assert "filter_condition=" in str(error)


class TestExecutionError:
    """Tests for ExecutionError and subclasses."""

    def test_execution_error(self):
        """Test ExecutionError is a CheckError."""
        error = ExecutionError("Execution failed")
        assert isinstance(error, CheckError)

    def test_data_type_error(self):
        """Test DataTypeError."""
        error = DataTypeError("Wrong type", column="col1")
        assert isinstance(error, ExecutionError)
        assert error.column == "col1"

    def test_calculation_error(self):
        """Test CalculationError."""
        error = CalculationError("Division by zero")
        assert isinstance(error, ExecutionError)

    def test_insufficient_data_error(self):
        """Test InsufficientDataError."""
        error = InsufficientDataError("Not enough data")
        assert isinstance(error, ExecutionError)


class TestValidationError:
    """Tests for ValidationError and subclasses."""

    def test_validation_error(self):
        """Test basic ValidationError."""
        error = ValidationError("Validation failed")
        assert isinstance(error, DQFrameworkError)

    def test_threshold_violation_error(self):
        """Test ThresholdViolationError."""
        error = ThresholdViolationError(
            "Threshold exceeded",
            threshold_type="absolute_critical",
            threshold_value=0.05,
            actual_value=0.08,
        )
        assert error.threshold_type == "absolute_critical"
        assert error.threshold_value == 0.05
        assert error.actual_value == 0.08
        error_str = str(error)
        assert "threshold_type=" in error_str
        assert "threshold_value=0.05" in error_str
        assert "actual_value=0.08" in error_str


class TestOutputAndAlertingErrors:
    """Tests for OutputError and AlertingError."""

    def test_output_error(self):
        """Test OutputError."""
        error = OutputError("Output generation failed")
        assert isinstance(error, DQFrameworkError)

    def test_alerting_error(self):
        """Test AlertingError."""
        error = AlertingError("Email sending failed")
        assert isinstance(error, DQFrameworkError)


class TestExceptionInheritance:
    """Tests for exception hierarchy."""

    def test_all_inherit_from_base(self):
        """Test all exceptions inherit from DQFrameworkError."""
        exceptions = [
            ConfigurationError("test"),
            ConnectionError("test"),
            DataRetrievalError("test"),
            CheckError("test"),
            FilterError("test"),
            ValidationError("test"),
            OutputError("test"),
            AlertingError("test"),
        ]
        for exc in exceptions:
            assert isinstance(exc, DQFrameworkError)

    def test_can_catch_with_base(self):
        """Test catching all exceptions with base class."""
        with pytest.raises(DQFrameworkError):
            raise ConfigurationError("test")

        with pytest.raises(DQFrameworkError):
            raise CheckError("test")

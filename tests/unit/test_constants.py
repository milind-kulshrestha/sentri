"""Tests for constants and enumerations."""

import pytest

from data_quality.utils.constants import (
    CORRELATION_MODERATE,
    CORRELATION_STRONG,
    CORRELATION_WEAK,
    DEFAULT_BACKOFF_FACTOR,
    DEFAULT_MAX_RETRIES,
    DEFAULT_THRESHOLDS,
    MAX_SAMPLE_SIZE,
    PSI_MODERATE_DRIFT,
    PSI_NO_DRIFT,
    CheckStatus,
    CheckType,
    ConnectorType,
    CorrelationType,
    DriftMethod,
    OutputFormat,
    Severity,
)


class TestCheckStatus:
    """Tests for CheckStatus enum."""

    def test_values(self):
        """Test all expected values exist."""
        assert CheckStatus.PASS == "PASS"
        assert CheckStatus.WARNING == "WARNING"
        assert CheckStatus.FAIL == "FAIL"
        assert CheckStatus.ERROR == "ERROR"

    def test_is_string(self):
        """Test enum values are strings."""
        assert isinstance(CheckStatus.PASS.value, str)
        assert CheckStatus.PASS == "PASS"

    def test_all_statuses(self):
        """Test we have exactly 4 statuses."""
        assert len(CheckStatus) == 4


class TestSeverity:
    """Tests for Severity enum."""

    def test_values(self):
        """Test all expected values exist."""
        assert Severity.INFO == "INFO"
        assert Severity.WARNING == "WARNING"
        assert Severity.CRITICAL == "CRITICAL"
        assert Severity.ERROR == "ERROR"

    def test_all_severities(self):
        """Test we have exactly 4 severities."""
        assert len(Severity) == 4


class TestOutputFormat:
    """Tests for OutputFormat enum."""

    def test_values(self):
        """Test all expected values exist."""
        assert OutputFormat.JSON == "json"
        assert OutputFormat.HTML == "html"
        assert OutputFormat.CSV == "csv"
        assert OutputFormat.DATAFRAME == "dataframe"

    def test_all_formats(self):
        """Test we have exactly 4 formats."""
        assert len(OutputFormat) == 4


class TestConnectorType:
    """Tests for ConnectorType enum."""

    def test_values(self):
        """Test all expected values exist."""
        assert ConnectorType.ORACLE == "oracle"
        assert ConnectorType.SNOWFLAKE == "snowflake"
        assert ConnectorType.CSV == "csv"
        assert ConnectorType.CUSTOM == "custom"

    def test_all_connectors(self):
        """Test we have exactly 4 connector types."""
        assert len(ConnectorType) == 4


class TestCheckType:
    """Tests for CheckType enum."""

    def test_all_check_types(self):
        """Test all 10 check types exist."""
        expected = [
            "completeness",
            "turnover",
            "uniqueness",
            "value_spike",
            "frequency",
            "correlation",
            "range",
            "statistical",
            "distribution",
            "drift",
        ]
        assert len(CheckType) == 10
        for check in expected:
            assert check in [ct.value for ct in CheckType]

    def test_specific_values(self):
        """Test specific check type values."""
        assert CheckType.COMPLETENESS == "completeness"
        assert CheckType.TURNOVER == "turnover"
        assert CheckType.VALUE_SPIKE == "value_spike"
        assert CheckType.STATISTICAL == "statistical"


class TestCorrelationType:
    """Tests for CorrelationType enum."""

    def test_values(self):
        """Test correlation types."""
        assert CorrelationType.TEMPORAL == "temporal"
        assert CorrelationType.CROSS_COLUMN == "cross_column"

    def test_count(self):
        """Test we have exactly 2 correlation types."""
        assert len(CorrelationType) == 2


class TestDriftMethod:
    """Tests for DriftMethod enum."""

    def test_values(self):
        """Test drift methods."""
        assert DriftMethod.PSI == "psi"
        assert DriftMethod.KS == "ks"
        assert DriftMethod.JENSEN_SHANNON == "jensen_shannon"

    def test_count(self):
        """Test we have exactly 3 drift methods."""
        assert len(DriftMethod) == 3


class TestDefaultThresholds:
    """Tests for DEFAULT_THRESHOLDS dictionary."""

    def test_has_all_check_types(self):
        """Test defaults exist for main check types."""
        expected_checks = [
            "completeness",
            "turnover",
            "uniqueness",
            "value_spike",
            "frequency",
            "correlation",
            "distribution",
            "drift",
        ]
        for check in expected_checks:
            assert check in DEFAULT_THRESHOLDS

    def test_completeness_defaults(self):
        """Test completeness default thresholds."""
        completeness = DEFAULT_THRESHOLDS["completeness"]
        assert "absolute_critical" in completeness
        assert "absolute_warning" in completeness
        assert completeness["absolute_critical"] == 0.05
        assert completeness["absolute_warning"] == 0.02

    def test_uniqueness_defaults(self):
        """Test uniqueness default thresholds."""
        uniqueness = DEFAULT_THRESHOLDS["uniqueness"]
        assert uniqueness["absolute_critical"] == 0

    def test_drift_defaults(self):
        """Test drift default thresholds match PSI interpretation."""
        drift = DEFAULT_THRESHOLDS["drift"]
        assert drift["absolute_warning"] == PSI_NO_DRIFT
        assert drift["absolute_critical"] == PSI_MODERATE_DRIFT


class TestPSIThresholds:
    """Tests for PSI interpretation thresholds."""

    def test_psi_values(self):
        """Test PSI threshold values."""
        assert PSI_NO_DRIFT == 0.1
        assert PSI_MODERATE_DRIFT == 0.25

    def test_psi_ordering(self):
        """Test PSI thresholds are in correct order."""
        assert PSI_NO_DRIFT < PSI_MODERATE_DRIFT


class TestCorrelationThresholds:
    """Tests for correlation strength thresholds."""

    def test_correlation_values(self):
        """Test correlation strength values."""
        assert CORRELATION_WEAK == 0.3
        assert CORRELATION_MODERATE == 0.7
        assert CORRELATION_STRONG == 0.9

    def test_correlation_ordering(self):
        """Test correlation thresholds are in correct order."""
        assert CORRELATION_WEAK < CORRELATION_MODERATE < CORRELATION_STRONG


class TestRetryConstants:
    """Tests for retry configuration constants."""

    def test_default_retries(self):
        """Test default retry values."""
        assert DEFAULT_MAX_RETRIES == 3
        assert DEFAULT_BACKOFF_FACTOR == 2.0

    def test_reasonable_values(self):
        """Test values are reasonable."""
        assert 1 <= DEFAULT_MAX_RETRIES <= 10
        assert 1.0 <= DEFAULT_BACKOFF_FACTOR <= 5.0


class TestLimitConstants:
    """Tests for limit constants."""

    def test_max_sample_size(self):
        """Test max sample size is reasonable."""
        assert MAX_SAMPLE_SIZE == 100
        assert MAX_SAMPLE_SIZE > 0

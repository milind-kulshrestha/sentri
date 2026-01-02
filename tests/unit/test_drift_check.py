"""Test cases for drift check."""

import numpy as np
import pandas as pd
import pytest

from data_quality.checks.drift import DriftCheck
from data_quality.utils.constants import PSI_MODERATE_DRIFT, PSI_NO_DRIFT, CheckStatus


class TestDriftCheck:
    """Test cases for DriftCheck class."""

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        np.random.seed(42)
        data = []

        # Baseline period
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-01"),
                    "entity_id": f"id_{i}",
                    "value": np.random.normal(100, 10),
                }
            )

        # Current period - slightly different distribution
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-02"),
                    "entity_id": f"id_{i}",
                    "value": np.random.normal(105, 12),
                }
            )

        return pd.DataFrame(data)

    @pytest.fixture
    def drift_config(self):
        """Drift check configuration."""
        return {
            "value": {
                "thresholds": {
                    "absolute_critical": PSI_MODERATE_DRIFT,
                    "absolute_warning": PSI_NO_DRIFT,
                },
                "description": "Value drift check",
            }
        }

    def test_drift_check_basic(self, sample_data, drift_config):
        """Test basic drift check functionality."""
        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["check_type"] == "drift"
        assert result.iloc[0]["column"] == "value"
        assert "psi" in result.iloc[0]["additional_metrics"]
        assert "interpretation" in result.iloc[0]["additional_metrics"]

    def test_drift_check_significant_drift(self, drift_config):
        """Test drift check with significant drift."""
        np.random.seed(42)
        data = []

        # Baseline - normal(0, 1)
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-01"),
                    "entity_id": f"id_{i}",
                    "value": np.random.normal(0, 1),
                }
            )

        # Current - exponential distribution (very different)
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-02"),
                    "entity_id": f"id_{i}",
                    "value": np.random.exponential(2),
                }
            )

        df = pd.DataFrame(data)

        check = DriftCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] in [CheckStatus.WARNING, CheckStatus.FAIL]
        assert result.iloc[0]["additional_metrics"]["psi"] > 0

    def test_drift_check_no_drift(self, drift_config):
        """Test drift check with no drift."""
        np.random.seed(42)
        data = []

        # Both periods - identical distribution
        for period, date in enumerate(
            [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")]
        ):
            np.random.seed(42)  # Same seed for identical distributions
            for i in range(100):
                data.append(
                    {
                        "effective_date": date,
                        "entity_id": f"id_{i}",
                        "value": np.random.normal(100, 10),
                    }
                )

        df = pd.DataFrame(data)

        check = DriftCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert (
            result.iloc[0]["additional_metrics"]["interpretation"]
            == "No significant drift"
        )

    def test_drift_check_insufficient_dates(self, drift_config):
        """Test drift check with insufficient dates."""
        data = []
        for i in range(20):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-01"),
                    "entity_id": f"id_{i}",
                    "value": i,
                }
            )

        df = pd.DataFrame(data)

        check = DriftCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert "Insufficient dates" in result.iloc[0]["additional_metrics"]["message"]

    def test_drift_check_insufficient_data(self, drift_config):
        """Test drift check with insufficient data points."""
        data = []
        for period, date in enumerate(
            [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")]
        ):
            for i in range(5):  # Only 5 points per period
                data.append(
                    {"effective_date": date, "entity_id": f"id_{i}", "value": i}
                )

        df = pd.DataFrame(data)

        check = DriftCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert (
            "Insufficient data for PSI"
            in result.iloc[0]["additional_metrics"]["message"]
        )

    def test_calculate_psi_identical_distributions(self, sample_data, drift_config):
        """Test PSI calculation with identical distributions."""
        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        # Create identical series
        baseline = pd.Series([1, 2, 3, 4, 5] * 20)
        current = pd.Series([1, 2, 3, 4, 5] * 20)

        psi = check._calculate_psi(baseline, current)

        assert psi < 0.1  # Should be very low for identical distributions

    def test_calculate_psi_different_distributions(self, sample_data, drift_config):
        """Test PSI calculation with different distributions."""
        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        # Create different distributions
        baseline = pd.Series(np.random.normal(0, 1, 100))
        current = pd.Series(np.random.normal(5, 1, 100))

        psi = check._calculate_psi(baseline, current)

        assert psi > 0.1  # Should be higher for different distributions

    def test_calculate_psi_constant_values(self, sample_data, drift_config):
        """Test PSI calculation with constant values."""
        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        # All same values
        baseline = pd.Series([5] * 100)
        current = pd.Series([5] * 100)

        psi = check._calculate_psi(baseline, current)

        assert psi == 0.0  # Should be 0 for constant values

    def test_drift_check_with_filter(self, sample_data):
        """Test drift check with filter condition."""
        config = {
            "value": {
                "thresholds": {
                    "absolute_critical": PSI_MODERATE_DRIFT,
                    "absolute_warning": PSI_NO_DRIFT,
                },
                "filter_condition": 'entity_id.str.contains("1")',
                "description": "Filtered drift check",
            }
        }

        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["filter_applied"] == 'entity_id.str.contains("1")'

    def test_drift_check_missing_column(self, sample_data, drift_config):
        """Test drift check with missing column."""
        config = {
            "missing_column": {"thresholds": {"absolute_critical": PSI_MODERATE_DRIFT}}
        }

        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == "ERROR"
        assert "not found" in result.iloc[0]["error_message"]

    def test_drift_check_disabled_column(self, sample_data):
        """Test drift check with disabled column."""
        config = {
            "value": {
                "enabled": False,
                "thresholds": {"absolute_critical": PSI_MODERATE_DRIFT},
            }
        }

        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert result.empty

    def test_drift_check_with_nulls(self, drift_config):
        """Test drift check with null values."""
        data = []

        # Add data with nulls
        for period, date in enumerate(
            [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")]
        ):
            for i in range(50):
                data.append(
                    {
                        "effective_date": date,
                        "entity_id": f"id_{i}",
                        "value": i if i % 5 != 0 else None,  # Every 5th value is null
                    }
                )

        df = pd.DataFrame(data)

        check = DriftCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        # Should handle nulls gracefully by dropping them

    def test_drift_check_error_handling(self, sample_data):
        """Test drift check error handling."""
        config = {
            "value": {
                "thresholds": {"absolute_critical": PSI_MODERATE_DRIFT},
                "filter_condition": "invalid_syntax ==",  # Invalid filter
            }
        }

        check = DriftCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == "ERROR"

    def test_drift_check_empty_dataframe(self, drift_config):
        """Test drift check with empty DataFrame."""
        df = pd.DataFrame(columns=["effective_date", "entity_id", "value"])

        check = DriftCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=drift_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert "Insufficient dates" in result.iloc[0]["additional_metrics"]["message"]

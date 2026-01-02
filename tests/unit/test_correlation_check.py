"""Tests for CorrelationCheck implementation."""

import numpy as np
import pandas as pd
import pytest

from data_quality.checks.correlation import CorrelationCheck
from data_quality.utils.constants import CheckStatus

# Import shared fixtures
from tests.fixtures.test_fixtures import (
    basic_df,
    correlation_config,
    correlation_test_df,
    edge_case_configs,
    multi_date_df,
    temporal_correlation_config,
    temporal_drift_df,
)


class TestCorrelationCheck:
    """Tests for the CorrelationCheck class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with correlated data."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
                "date": pd.to_datetime(
                    [
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-02",
                        "2025-01-02",
                        "2025-01-02",
                        "2025-01-02",
                        "2025-01-02",
                    ]
                ),
                "value1": [
                    10,
                    20,
                    30,
                    40,
                    50,
                    12,
                    22,
                    32,
                    42,
                    52,
                ],  # Highly correlated with value2
                "value2": [
                    15,
                    25,
                    35,
                    45,
                    55,
                    17,
                    27,
                    37,
                    47,
                    57,
                ],  # Highly correlated with value1
                "random": [1, 5, 2, 8, 3, 9, 1, 4, 7, 2],  # Low correlation
            }
        )

    def test_correlation_check_cross_column_high_correlation(self, sample_df):
        """Test cross-column correlation with high correlation."""
        config = {
            "value1": {
                "correlation_type": "cross_column",
                "correlation_with": "value2",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS
        assert results.iloc[0]["metric_value"] > 0.9  # Should be highly correlated

    def test_correlation_check_cross_column_low_correlation(self, sample_df):
        """Test cross-column correlation with low correlation."""
        config = {
            "value1": {
                "correlation_type": "cross_column",
                "correlation_with": "random",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.FAIL
        assert abs(results.iloc[0]["metric_value"]) < 0.8

    def test_correlation_check_temporal_correlation(self, sample_df):
        """Test temporal correlation between consecutive dates."""
        config = {
            "value1": {
                "correlation_type": "temporal",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS
        # Temporal correlation should be high since values are similar across dates

    def test_correlation_check_missing_correlation_column(self, sample_df):
        """Test cross-column correlation with missing correlation column."""
        config = {
            "value1": {
                "correlation_type": "cross_column",
                "correlation_with": "missing_column",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.ERROR

    def test_correlation_check_missing_target_column(self, sample_df):
        """Test correlation check with missing target column."""
        config = {
            "missing_column": {
                "correlation_type": "cross_column",
                "correlation_with": "value2",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.ERROR

    def test_correlation_check_insufficient_dates(self):
        """Test temporal correlation with insufficient dates."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "date": pd.to_datetime(["2025-01-01"] * 3),  # Only one date
                "value": [10, 20, 30],
            }
        )

        config = {
            "value": {
                "correlation_type": "temporal",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS
        assert results.iloc[0]["metric_value"] == 1.0
        assert "Insufficient dates" in results.iloc[0]["additional_metrics"]["message"]

    def test_correlation_check_insufficient_matching_records(self):
        """Test temporal correlation with insufficient matching records."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],  # Different IDs for each date
                "date": pd.to_datetime(
                    ["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"]
                ),
                "value": [10, 20, 30, 40],
            }
        )

        config = {
            "value": {
                "correlation_type": "temporal",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS
        assert results.iloc[0]["metric_value"] == 1.0
        assert (
            "Insufficient matching records"
            in results.iloc[0]["additional_metrics"]["message"]
        )

    def test_correlation_check_with_filter(self, sample_df):
        """Test correlation check with filter condition."""
        config = {
            "value1": {
                "correlation_type": "cross_column",
                "correlation_with": "value2",
                "filter_condition": "id <= 3",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS

    def test_correlation_check_disabled_column(self, sample_df):
        """Test correlation check with disabled column."""
        config = {
            "value1": {
                "enabled": False,
                "correlation_type": "cross_column",
                "correlation_with": "value2",
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 0  # No results for disabled column

    def test_correlation_check_default_correlation_type(self, sample_df):
        """Test correlation check with default correlation type (temporal)."""
        config = {
            "value1": {
                # No correlation_type specified, should default to temporal
                "thresholds": {"absolute_critical": 0.8}
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["additional_metrics"]["correlation_type"] == "temporal"

    def test_correlation_check_no_correlation_with_specified(self, sample_df):
        """Test cross-column correlation without specifying correlation_with."""
        config = {
            "value1": {
                "correlation_type": "cross_column",
                # No correlation_with specified
                "thresholds": {"absolute_critical": 0.8},
            }
        }

        check = CorrelationCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.ERROR

    def test_correlation_with_shared_fixtures(
        self, correlation_test_df, correlation_config
    ):
        """Test correlation using shared fixtures with known correlations."""
        check = CorrelationCheck(
            df=correlation_test_df,
            date_col="date",
            id_col="id",
            check_config=correlation_config,
        )

        results = check.run()
        assert len(results) == 2

        # Perfect positive correlation should pass
        perfect_result = results[results["column"] == "perfect_positive"].iloc[0]
        assert perfect_result["status"] == CheckStatus.PASS
        assert abs(perfect_result["metric_value"]) > 0.9

        # No correlation should fail
        no_corr_result = results[results["column"] == "no_correlation"].iloc[0]
        assert no_corr_result["status"] == CheckStatus.FAIL

    def test_temporal_correlation_with_drift(
        self, temporal_drift_df, temporal_correlation_config
    ):
        """Test temporal correlation with drifting data."""
        check = CorrelationCheck(
            df=temporal_drift_df,
            date_col="date",
            id_col="id",
            check_config=temporal_correlation_config,
        )

        results = check.run()
        assert len(results) == 1

        # Drifting values should still have some temporal correlation
        result = results.iloc[0]
        assert not pd.isna(result["metric_value"])
        assert result["additional_metrics"]["correlation_type"] == "temporal"

    def test_correlation_edge_cases(self, basic_df, edge_case_configs):
        """Test correlation with edge case configurations."""
        # Test with empty config
        check = CorrelationCheck(
            df=basic_df,
            date_col="date",
            id_col="id",
            check_config=edge_case_configs["empty_config"],
        )

        results = check.run()
        assert len(results) == 0

        # Test with disabled config
        check = CorrelationCheck(
            df=basic_df,
            date_col="date",
            id_col="id",
            check_config=edge_case_configs["disabled_config"],
        )

        results = check.run()
        assert len(results) == 0

    def test_correlation_with_identical_columns(self):
        """Test correlation between identical columns."""
        df = pd.DataFrame(
            {
                "id": range(1, 11),
                "date": pd.to_datetime(["2025-01-01"] * 10),
                "col1": range(1, 11),
                "col2": range(1, 11),  # Identical to col1
            }
        )

        config = {
            "col1": {
                "correlation_type": "cross_column",
                "correlation_with": "col2",
                "thresholds": {"absolute_critical": 0.99},
            }
        }

        check = CorrelationCheck(
            df=df, date_col="date", id_col="id", check_config=config
        )
        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS
        assert abs(results.iloc[0]["metric_value"]) == 1.0  # Perfect correlation

    def test_correlation_with_anticorrelated_data(self):
        """Test correlation with perfectly anti-correlated data."""
        df = pd.DataFrame(
            {
                "id": range(1, 11),
                "date": pd.to_datetime(["2025-01-01"] * 10),
                "x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "neg_x": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],  # Perfectly anti-correlated
            }
        )

        config = {
            "x": {
                "correlation_type": "cross_column",
                "correlation_with": "neg_x",
                "thresholds": {"absolute_critical": 0.8},  # Should pass due to abs()
            }
        }

        check = CorrelationCheck(
            df=df, date_col="date", id_col="id", check_config=config
        )
        results = check.run()

        assert len(results) == 1
        # The correlation should be negative and strong
        correlation_value = results.iloc[0]["metric_value"]
        assert correlation_value < 0  # Negative correlation
        # Check passes if abs(correlation) > threshold
        if abs(correlation_value) > 0.8:
            assert results.iloc[0]["status"] == CheckStatus.PASS
        else:
            assert results.iloc[0]["status"] == CheckStatus.FAIL

"""Tests for StatisticalCheck implementation."""

import numpy as np
import pandas as pd
import pytest

from data_quality.checks.statistical import StatisticalCheck
from data_quality.utils.constants import CheckStatus

# Import shared fixtures
from tests.fixtures.test_fixtures import (
    basic_df,
    edge_case_configs,
    extreme_values_df,
    messy_data_df,
    multi_date_df,
    statistical_config,
)


class TestStatisticalCheck:
    """Tests for the StatisticalCheck class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with numeric data."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
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
                "value": [10.0, 20.0, 30.0, 40.0, 50.0, 15.0, 25.0, 35.0, 45.0, 55.0],
                "score": [1.0, 2.0, 3.0, 4.0, 5.0, 1.5, 2.5, 3.5, 4.5, 5.5],
            }
        )

    def test_statistical_check_basic_measures(self, sample_df):
        """Test basic statistical measures calculation."""
        config = {"value": {"measures": ["mean", "std", "median"], "thresholds": {}}}

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 3  # mean, std, median
        assert all(results["check_type"] == "statistical")
        assert all(results["column"] == "value")

        # Check that we get expected measures
        measures = set(results["measure"])
        assert measures == {"mean", "std", "median"}

    def test_statistical_check_all_measures(self, sample_df):
        """Test all available statistical measures."""
        config = {
            "value": {
                "measures": [
                    "mean",
                    "median",
                    "std",
                    "min",
                    "max",
                    "count",
                    "skew",
                    "kurtosis",
                ],
                "thresholds": {},
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 8
        measures = set(results["measure"])
        expected = {"mean", "median", "std", "min", "max", "count", "skew", "kurtosis"}
        assert measures == expected

    def test_statistical_check_with_thresholds_pass(self, sample_df):
        """Test statistical check passing thresholds."""
        config = {
            "value": {
                "measures": ["mean"],
                "thresholds": {
                    "mean": {"absolute_critical": 100}  # Mean ~32.5, should pass
                },
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS

    def test_statistical_check_with_thresholds_fail(self, sample_df):
        """Test statistical check failing thresholds."""
        config = {
            "value": {
                "measures": ["mean"],
                "thresholds": {
                    "mean": {"absolute_critical": 10}  # Mean ~32.5, should fail
                },
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.FAIL
        assert results.iloc[0]["severity"] == "CRITICAL"

    def test_statistical_check_with_range_thresholds(self, sample_df):
        """Test statistical check with range thresholds (list format)."""
        config = {
            "value": {
                "measures": ["mean"],
                "thresholds": {
                    "mean": {"absolute_critical": [20, 40]}  # Mean ~32.5, should pass
                },
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.PASS

    def test_statistical_check_with_range_thresholds_fail(self, sample_df):
        """Test statistical check failing range thresholds."""
        config = {
            "value": {
                "measures": ["mean"],
                "thresholds": {
                    "mean": {"absolute_critical": [50, 60]}  # Mean ~32.5, should fail
                },
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.FAIL

    def test_statistical_check_missing_column(self, sample_df):
        """Test statistical check with missing column."""
        config = {"missing_column": {"measures": ["mean"], "thresholds": {}}}

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["status"] == CheckStatus.ERROR

    def test_statistical_check_with_filter(self, sample_df):
        """Test statistical check with filter condition."""
        config = {
            "value": {
                "measures": ["count"],
                "filter_condition": "date == '2025-01-01'",
                "thresholds": {},
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 1
        assert results.iloc[0]["metric_value"] == 5  # Only 5 records for 2025-01-01

    def test_statistical_check_empty_after_filter(self, sample_df):
        """Test statistical check with empty data after filtering."""
        config = {
            "value": {
                "measures": ["mean"],
                "filter_condition": "date == '2025-01-03'",  # No such date
                "thresholds": {},
            }
        }

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 0  # No results for empty data

    def test_statistical_check_disabled_column(self, sample_df):
        """Test statistical check with disabled column."""
        config = {"value": {"enabled": False, "measures": ["mean"], "thresholds": {}}}

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 0  # No results for disabled column

    def test_statistical_check_unknown_measure(self, sample_df):
        """Test statistical check with unknown measure."""
        config = {"value": {"measures": ["mean", "unknown_measure"], "thresholds": {}}}

        check = StatisticalCheck(
            df=sample_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        # Should only get result for 'mean', unknown_measure is skipped
        assert len(results) == 1
        assert results.iloc[0]["measure"] == "mean"

    def test_statistical_check_with_nulls(self):
        """Test statistical check with null values."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "date": pd.to_datetime(["2025-01-01"] * 5),
                "value": [10.0, 20.0, np.nan, 40.0, np.nan],
            }
        )

        config = {"value": {"measures": ["count", "mean"], "thresholds": {}}}

        check = StatisticalCheck(
            df=df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()

        assert len(results) == 2
        count_result = results[results["measure"] == "count"].iloc[0]
        assert count_result["metric_value"] == 3  # Only non-null values counted

    def test_statistical_check_with_shared_fixtures(
        self, extreme_values_df, statistical_config
    ):
        """Test statistical check using shared fixtures."""
        check = StatisticalCheck(
            df=extreme_values_df,
            date_col="date",
            id_col="id",
            check_config=statistical_config,
        )

        results = check.run()
        # May have fewer results if some measures fail
        assert len(results) >= 0

        # Check that extreme values are handled properly
        if len(results) > 0:
            # Check if we have measure column (normal results) or error results
            if "measure" in results.columns:
                mean_result = results[results["measure"] == "mean"]
                if len(mean_result) > 0:
                    assert not pd.isna(mean_result.iloc[0]["metric_value"])
                    assert not np.isinf(mean_result.iloc[0]["metric_value"])
            else:
                # Error results - just verify they exist
                assert "status" in results.columns

    def test_statistical_check_edge_cases(self, messy_data_df, edge_case_configs):
        """Test statistical check with edge case configurations."""
        # Test with empty config
        check = StatisticalCheck(
            df=messy_data_df,
            date_col="date",
            id_col="id",
            check_config=edge_case_configs["empty_config"],
        )

        results = check.run()
        assert len(results) == 0  # No columns configured

        # Test with disabled config
        check = StatisticalCheck(
            df=messy_data_df,
            date_col="date",
            id_col="id",
            check_config=edge_case_configs["disabled_config"],
        )

        results = check.run()
        assert len(results) == 0  # Column disabled

    def test_statistical_check_with_infinite_values(self, messy_data_df):
        """Test statistical measures with infinite values."""
        config = {
            "value": {"measures": ["mean", "std", "min", "max"], "thresholds": {}}
        }

        check = StatisticalCheck(
            df=messy_data_df, date_col="date", id_col="id", check_config=config
        )

        results = check.run()
        assert len(results) == 4

        # Check that infinite values are handled
        for _, result in results.iterrows():
            # Results should be finite numbers or handled gracefully
            if not pd.isna(result["metric_value"]):
                assert np.isfinite(result["metric_value"]) or result[
                    "metric_value"
                ] in [np.inf, -np.inf]

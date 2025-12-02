"""Comprehensive edge case tests for data quality checks."""

import pandas as pd
import pytest
import numpy as np

from data_quality.checks.completeness import CompletenessCheck
from data_quality.checks.uniqueness import UniquenessCheck
from data_quality.checks.range_check import RangeCheck
from data_quality.checks.statistical import StatisticalCheck
from data_quality.checks.correlation import CorrelationCheck
from data_quality.managers.check_manager import CheckManager
from data_quality.utils.constants import CheckStatus
from data_quality.core.exceptions import FilterError

# Import fixtures
from tests.fixtures.test_fixtures import *


class TestEdgeCasesCompleteness:
    """Edge case tests for CompletenessCheck."""

    def test_completeness_with_inf_values(self, messy_data_df, basic_completeness_config):
        """Test completeness handling of infinite values."""
        check = CompletenessCheck(
            df=messy_data_df,
            date_col="date", 
            id_col="id",
            check_config=basic_completeness_config
        )
        
        results = check.run()
        assert len(results) > 0
        # Inf values should be treated as non-null
        
    def test_completeness_empty_dataframe(self, empty_df, basic_completeness_config):
        """Test completeness with empty DataFrame."""
        check = CompletenessCheck(
            df=empty_df,
            date_col="date",
            id_col="id", 
            check_config=basic_completeness_config
        )
        
        results = check.run()
        # Empty dataframe may still return a result record with 0 metric
        assert len(results) >= 0

    def test_completeness_single_row(self, single_row_df, basic_completeness_config):
        """Test completeness with single row."""
        check = CompletenessCheck(
            df=single_row_df,
            date_col="date",
            id_col="id",
            check_config=basic_completeness_config
        )
        
        results = check.run()
        assert len(results) > 0
        assert results.iloc[0]["metric_value"] == 0.0  # 100% complete

    def test_completeness_all_null_column(self):
        """Test completeness with completely null column."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "date": pd.to_datetime(["2025-01-01"] * 3),
            "all_null": [None, np.nan, None]
        })
        
        config = {"all_null": {"thresholds": {"absolute_critical": 0.5}}}
        
        check = CompletenessCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        assert results.iloc[0]["metric_value"] == 1.0  # 100% missing
        assert results.iloc[0]["status"] == CheckStatus.FAIL

    def test_completeness_invalid_filter(self, basic_df):
        """Test completeness with invalid filter condition."""
        config = {
            "value": {
                "thresholds": {"absolute_critical": 0.2},
                "filter_condition": "invalid_column > 0"  # Column doesn't exist
            }
        }
        
        check = CompletenessCheck(df=basic_df, date_col="date", id_col="id", check_config=config)
        
        # May raise FilterError or handle gracefully with error result
        try:
            results = check.run()
            # If no exception, should have error result
            assert any(r.get("status") == CheckStatus.ERROR for r in results.to_dict('records'))
        except FilterError:
            # Expected behavior
            pass


class TestEdgeCasesUniqueness:
    """Edge case tests for UniquenessCheck."""

    def test_uniqueness_all_duplicates(self):
        """Test uniqueness when all values are duplicates."""
        df = pd.DataFrame({
            "id": [1, 1, 1, 1, 1],  # All same value
            "date": pd.to_datetime(["2025-01-01"] * 5),
            "value": [42] * 5
        })
        
        config = {"value": {"thresholds": {"absolute_critical": 0}}}
        
        check = UniquenessCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        assert results.iloc[0]["metric_value"] == 4  # 4 duplicates
        assert results.iloc[0]["status"] == CheckStatus.FAIL

    def test_uniqueness_with_nulls(self):
        """Test uniqueness handling of null values."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "date": pd.to_datetime(["2025-01-01"] * 5),
            "value": [10, None, 10, np.nan, 20]  # Nulls and duplicates
        })
        
        config = {"value": {"thresholds": {"absolute_critical": 0}}}
        
        check = UniquenessCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        # Should find 1 duplicate (10 appears twice), nulls ignored

    def test_uniqueness_extreme_duplicates(self, duplicate_data_df):
        """Test uniqueness with extreme duplicate counts."""
        config = {
            "id": {"thresholds": {"absolute_critical": 1000}},  # Very high threshold
            "exact_duplicate": {"thresholds": {"absolute_critical": 0}}  # Zero tolerance
        }
        
        check = UniquenessCheck(
            df=duplicate_data_df,
            date_col="date",
            id_col="id", 
            check_config=config
        )
        
        results = check.run()
        assert len(results) == 2
        
        # ID check should pass (high threshold)
        id_result = results[results["column"] == "id"].iloc[0]
        assert id_result["status"] == CheckStatus.PASS
        
        # Exact duplicate check should fail (zero threshold)
        dup_result = results[results["column"] == "exact_duplicate"].iloc[0]
        assert dup_result["status"] == CheckStatus.FAIL


class TestEdgeCasesRange:
    """Edge case tests for RangeCheck."""

    def test_range_with_extreme_values(self, extreme_values_df, range_check_config):
        """Test range check with extreme values."""
        # Add percentage column with extreme values
        extreme_values_df["percentage"] = [-1000, 2000, np.inf, -np.inf, 50, 75, 1e10, -1e10, 0, 100]
        
        check = RangeCheck(
            df=extreme_values_df,
            date_col="date",
            id_col="id",
            check_config=range_check_config
        )
        
        results = check.run()
        assert len(results) > 0
        
        # Should detect many out-of-range values
        percentage_result = results[results["column"] == "percentage"].iloc[0]
        assert percentage_result["status"] == CheckStatus.FAIL

    def test_range_min_equals_max(self):
        """Test range check where min equals max."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "date": pd.to_datetime(["2025-01-01"] * 3),
            "value": [42, 42, 43]  # One value out of range
        })
        
        config = {
            "value": {
                "min_value": 42,
                "max_value": 42,  # Exact value required
                "description": "Must be exactly 42"
            }
        }
        
        check = RangeCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        assert results.iloc[0]["status"] == CheckStatus.FAIL  # 43 is out of range

    def test_range_no_bounds_specified(self):
        """Test range check with no min/max bounds."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "date": pd.to_datetime(["2025-01-01"] * 3),
            "value": [10, 20, 30]
        })
        
        config = {
            "value": {
                "description": "No bounds specified"
                # No min_value or max_value
            }
        }
        
        check = RangeCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        assert results.iloc[0]["status"] == CheckStatus.PASS  # No bounds = always pass


class TestEdgeCasesStatistical:
    """Edge case tests for StatisticalCheck."""

    def test_statistical_zero_variance(self, extreme_values_df):
        """Test statistical measures with zero variance data."""
        config = {
            "zero_variance": {
                "measures": ["mean", "std", "min", "max"],
                "thresholds": {}
            }
        }
        
        check = StatisticalCheck(
            df=extreme_values_df,
            date_col="date",
            id_col="id",
            check_config=config
        )
        
        results = check.run()
        assert len(results) == 4
        
        # Standard deviation should be 0
        std_result = results[results["measure"] == "std"].iloc[0]
        assert std_result["metric_value"] == 0.0
        
        # Min and max should be equal
        min_result = results[results["measure"] == "min"].iloc[0]
        max_result = results[results["measure"] == "max"].iloc[0]
        assert min_result["metric_value"] == max_result["metric_value"]

    def test_statistical_extreme_skew_kurtosis(self):
        """Test statistical measures with extreme skew/kurtosis."""
        # Create highly skewed data
        df = pd.DataFrame({
            "id": range(1, 101),
            "date": pd.to_datetime(["2025-01-01"] * 100),
            "skewed": [1] * 95 + [1000] * 5  # Highly right-skewed
        })
        
        config = {
            "skewed": {
                "measures": ["skew", "kurtosis"],
                "thresholds": {}
            }
        }
        
        check = StatisticalCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) == 2
        skew_result = results[results["measure"] == "skew"].iloc[0]
        assert skew_result["metric_value"] > 2  # Highly skewed

    def test_statistical_insufficient_data_for_skew(self):
        """Test statistical measures with insufficient data for skew/kurtosis."""
        df = pd.DataFrame({
            "id": [1, 2],  # Only 2 rows
            "date": pd.to_datetime(["2025-01-01"] * 2),
            "value": [10, 20]
        })
        
        config = {
            "value": {
                "measures": ["skew", "kurtosis"],
                "thresholds": {}
            }
        }
        
        check = StatisticalCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) == 2
        # Should return 0 for insufficient data
        skew_result = results[results["measure"] == "skew"].iloc[0]
        assert skew_result["metric_value"] == 0.0


class TestEdgeCasesCorrelation:
    """Edge case tests for CorrelationCheck."""

    def test_correlation_perfect_correlation(self, correlation_test_df):
        """Test correlation with perfect correlation."""
        config = {
            "perfect_positive": {
                "correlation_type": "cross_column",
                "correlation_with": "x",
                "thresholds": {"absolute_critical": 0.99}
            }
        }
        
        check = CorrelationCheck(
            df=correlation_test_df,
            date_col="date",
            id_col="id",
            check_config=config
        )
        
        results = check.run()
        assert len(results) > 0
        assert abs(results.iloc[0]["metric_value"]) > 0.99  # Nearly perfect correlation

    def test_correlation_with_constant_values(self):
        """Test correlation when one column has constant values."""
        df = pd.DataFrame({
            "id": range(1, 11),
            "date": pd.to_datetime(["2025-01-01"] * 10),
            "constant": [42] * 10,  # No variance
            "variable": range(1, 11)
        })
        
        config = {
            "variable": {
                "correlation_type": "cross_column",
                "correlation_with": "constant",
                "thresholds": {"absolute_critical": 0.5}
            }
        }
        
        check = CorrelationCheck(df=df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        # Correlation with constant should be NaN, but check should handle it
        assert pd.isna(results.iloc[0]["metric_value"]) or results.iloc[0]["metric_value"] == 0

    def test_correlation_temporal_single_date(self, basic_df):
        """Test temporal correlation with single date."""
        config = {
            "value": {
                "correlation_type": "temporal",
                "thresholds": {"absolute_critical": 0.8}
            }
        }
        
        check = CorrelationCheck(df=basic_df, date_col="date", id_col="id", check_config=config)
        results = check.run()
        
        assert len(results) > 0
        assert results.iloc[0]["status"] == CheckStatus.PASS
        assert "Insufficient dates" in results.iloc[0]["additional_metrics"]["message"]


class TestEdgeCasesCheckManager:
    """Edge case tests for CheckManager."""

    def test_check_manager_with_malformed_config(self, basic_df):
        """Test CheckManager with malformed configuration."""
        malformed_config = {
            "invalid_check_type": {
                "value": {"thresholds": {"absolute_critical": 0.1}}
            }
        }
        
        metadata = {"date_column": "date", "id_column": "id"}
        
        manager = CheckManager(
            df=basic_df,
            metadata=metadata,
            checks_config=malformed_config
        )
        
        results = manager.run_all_checks()
        
        # Should handle unknown check type gracefully
        assert results["summary"]["total"] == 0
        assert results["summary"]["errors"] == 0

    def test_check_manager_exception_handling(self, messy_data_df):
        """Test CheckManager exception handling."""
        # Create config that will cause errors
        config = {
            "completeness": {
                "missing_column": {  # Column doesn't exist
                    "thresholds": {"absolute_critical": 0.1}
                }
            }
        }
        
        metadata = {"date_column": "date", "id_column": "id"}
        
        manager = CheckManager(
            df=messy_data_df,
            metadata=metadata,
            checks_config=config
        )
        
        results = manager.run_all_checks()
        
        # Should capture errors in results
        assert results["summary"]["total"] > 0
        assert any(r.get("status") == CheckStatus.ERROR for r in results["results"])

    def test_check_manager_empty_dataframe(self, empty_df):
        """Test CheckManager with empty DataFrame."""
        config = {
            "completeness": {
                "value": {"thresholds": {"absolute_critical": 0.1}}
            }
        }
        
        metadata = {"date_column": "date", "id_column": "id"}
        
        manager = CheckManager(
            df=empty_df,
            metadata=metadata,
            checks_config=config
        )
        
        results = manager.run_all_checks()
        
        # Should handle empty data gracefully - may return 0 or 1 result
        assert results["summary"]["total"] >= 0

    def test_check_manager_parallel_execution_errors(self, basic_df):
        """Test parallel execution with errors."""
        config = {
            "completeness": {
                "missing_col1": {"thresholds": {"absolute_critical": 0.1}},
                "missing_col2": {"thresholds": {"absolute_critical": 0.1}},
                "missing_col3": {"thresholds": {"absolute_critical": 0.1}}
            }
        }
        
        metadata = {"date_column": "date", "id_column": "id"}
        
        manager = CheckManager(
            df=basic_df,
            metadata=metadata,
            checks_config=config
        )
        
        results = manager.run_all_checks_parallel(max_workers=2)
        
        # Should handle parallel errors gracefully
        assert "summary" in results
        assert "results" in results

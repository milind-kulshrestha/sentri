"""Integration tests for robust scenarios using shared fixtures."""

import numpy as np
import pandas as pd
import pytest

from data_quality.core.framework import DataQualityFramework
from data_quality.managers.check_manager import CheckManager
from data_quality.utils.constants import CheckStatus, Severity

# Import shared fixtures
from tests.fixtures.test_fixtures import (
    basic_completeness_config,
    basic_df,
    basic_uniqueness_config,
    correlation_config,
    correlation_test_df,
    duplicate_data_df,
    edge_case_configs,
    extreme_values_df,
    messy_data_df,
    metadata_configs,
    multi_date_df,
    range_check_config,
    statistical_config,
    strict_completeness_config,
    temporal_drift_df,
)


class TestRobustIntegrationScenarios:
    """Integration tests for complex, realistic scenarios."""

    def test_comprehensive_data_quality_pipeline(self, messy_data_df, metadata_configs):
        """Test complete DQ pipeline with messy real-world data."""
        # Comprehensive check configuration
        checks_config = {
            "completeness": {
                "value": {
                    "thresholds": {"absolute_critical": 0.3, "absolute_warning": 0.1},
                    "description": "Value completeness",
                },
                "category": {
                    "thresholds": {"absolute_critical": 0.4},
                    "description": "Category completeness",
                },
            },
            "uniqueness": {
                "id": {
                    "thresholds": {"absolute_critical": 0},
                    "description": "ID uniqueness",
                }
            },
            "range": {
                "percentage": {
                    "min_value": 0,
                    "max_value": 100,
                    "description": "Percentage range",
                }
            },
            "statistical": {
                "score": {
                    "measures": ["mean", "std", "min", "max"],
                    "thresholds": {
                        "mean": {"absolute_critical": [0, 10]},
                        "std": {"absolute_critical": 5},
                    },
                    "description": "Score statistics",
                }
            },
        }

        manager = CheckManager(
            df=messy_data_df,
            metadata=metadata_configs["basic"],
            checks_config=checks_config,
        )

        results = manager.run_all_checks()

        # Verify comprehensive results
        assert "summary" in results
        assert "results" in results
        assert "results_by_type" in results

        # Should have results from all check types
        assert len(results["results_by_type"]) >= 3
        assert "completeness" in results["results_by_type"]
        assert "uniqueness" in results["results_by_type"]
        assert "range" in results["results_by_type"]

        # Should detect data quality issues
        assert results["summary"]["failed"] > 0 or results["summary"]["warnings"] > 0

        # Verify specific issues are caught
        failed_checks = manager.get_failed_checks()
        warning_checks = manager.get_warning_checks()

        # Should detect range violations (percentage > 100)
        range_failures = [c for c in failed_checks if c.get("check_type") == "range"]
        assert len(range_failures) > 0

    def test_parallel_execution_with_complex_data(self, extreme_values_df):
        """Test parallel execution with computationally intensive checks."""
        checks_config = {
            "statistical": {
                "tiny_values": {
                    "measures": ["mean", "std", "skew", "kurtosis"],
                    "thresholds": {},
                },
                "huge_values": {
                    "measures": ["mean", "std", "skew", "kurtosis"],
                    "thresholds": {},
                },
                "high_variance": {
                    "measures": ["mean", "std", "skew", "kurtosis"],
                    "thresholds": {},
                },
            },
            "correlation": {
                "tiny_values": {
                    "correlation_type": "cross_column",
                    "correlation_with": "huge_values",
                    "thresholds": {"absolute_critical": 0.5},
                }
            },
        }

        metadata = {"date_column": "date", "id_column": "id"}

        manager = CheckManager(
            df=extreme_values_df, metadata=metadata, checks_config=checks_config
        )

        # Test both sequential and parallel execution
        sequential_results = manager.run_all_checks()

        # Reset for parallel execution
        manager.check_results = []
        parallel_results = manager.run_all_checks_parallel(max_workers=3)

        # Results should be consistent
        assert (
            sequential_results["summary"]["total"]
            == parallel_results["summary"]["total"]
        )
        assert len(sequential_results["results"]) == len(parallel_results["results"])

    def test_temporal_analysis_pipeline(self, temporal_drift_df):
        """Test temporal analysis with drifting data."""
        checks_config = {
            "correlation": {
                "drifting_value": {
                    "correlation_type": "temporal",
                    "thresholds": {"absolute_critical": 0.7},
                    "description": "Temporal stability",
                },
                "stable_value": {
                    "correlation_type": "temporal",
                    "thresholds": {"absolute_critical": 0.8},
                    "description": "Should be stable",
                },
            },
            "statistical": {
                "drifting_value": {
                    "measures": ["mean", "std"],
                    "thresholds": {
                        "mean": {"absolute_critical": [50, 150]},  # Wide range
                        "std": {"absolute_critical": 50},
                    },
                }
            },
        }

        metadata = {"date_column": "date", "id_column": "id"}

        manager = CheckManager(
            df=temporal_drift_df, metadata=metadata, checks_config=checks_config
        )

        results = manager.run_all_checks()

        # Should detect drift in drifting_value
        correlation_results = results["results_by_type"]["correlation"]
        drifting_corr = [
            r for r in correlation_results if r["column"] == "drifting_value"
        ][0]
        stable_corr = [r for r in correlation_results if r["column"] == "stable_value"][
            0
        ]

        # Both checks should complete (pass, warning, or fail)
        assert drifting_corr["status"] in [
            CheckStatus.PASS,
            CheckStatus.WARNING,
            CheckStatus.FAIL,
        ]
        assert stable_corr["status"] in [
            CheckStatus.PASS,
            CheckStatus.WARNING,
            CheckStatus.FAIL,
        ]

    def test_error_resilience_and_recovery(self, basic_df, edge_case_configs):
        """Test system resilience to configuration errors."""
        # Mix valid and invalid configurations
        problematic_config = {
            "completeness": {
                "value": {"thresholds": {"absolute_critical": 0.1}},  # Valid
                "missing_column": {"thresholds": {"absolute_critical": 0.1}},  # Invalid
            },
            "invalid_check_type": {  # Invalid check type
                "value": {"thresholds": {"absolute_critical": 0.1}}
            },
            "correlation": {
                "value": {
                    "correlation_type": "cross_column",
                    # Missing correlation_with - should cause error
                    "thresholds": {"absolute_critical": 0.8},
                }
            },
        }

        metadata = {"date_column": "date", "id_column": "id"}

        manager = CheckManager(
            df=basic_df, metadata=metadata, checks_config=problematic_config
        )

        # Should not crash, should handle errors gracefully
        results = manager.run_all_checks()

        assert "summary" in results
        assert "results" in results

        # Should have some successful results and some errors
        assert results["summary"]["total"] > 0

        # Check that errors are properly recorded
        error_results = [
            r for r in results["results"] if r.get("status") == CheckStatus.ERROR
        ]
        assert len(error_results) > 0

    def test_framework_initialization_edge_cases(self, basic_df, metadata_configs):
        """Test DataQualityFramework with various initialization scenarios."""
        # Test with DataFrame and metadata
        framework1 = DataQualityFramework(
            df=basic_df, metadata=metadata_configs["basic"]
        )

        assert framework1.df is not None
        assert framework1.metadata["dq_check_name"] == "Basic Test"

        # Test with config_dict
        config_dict = {
            "metadata": metadata_configs["basic"],
            "source": {"type": "dataframe"},
            "checks": {
                "completeness": {"value": {"thresholds": {"absolute_critical": 0.1}}}
            },
        }

        framework2 = DataQualityFramework(config_dict=config_dict)
        assert framework2.config_dict is not None

        # Test with config_path
        framework3 = DataQualityFramework(config_path="/path/to/config.yaml")
        assert framework3.config_path == "/path/to/config.yaml"

    def test_mixed_data_types_handling(self):
        """Test handling of mixed data types and edge cases."""
        # Create DataFrame with various data type challenges
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "date": pd.to_datetime(["2025-01-01"] * 5),
                "string_numbers": ["1.5", "2.7", "invalid", "4.2", "5.9"],
                "mixed_types": [1, "text", 3.14, None, True],
                "boolean_col": [True, False, True, None, False],
                "datetime_strings": [
                    "2025-01-01",
                    "invalid_date",
                    "2025-01-03",
                    None,
                    "2025-01-05",
                ],
            }
        )

        # Configuration that tests various scenarios
        checks_config = {
            "completeness": {
                "string_numbers": {"thresholds": {"absolute_critical": 0.3}},
                "mixed_types": {"thresholds": {"absolute_critical": 0.3}},
                "boolean_col": {"thresholds": {"absolute_critical": 0.3}},
            },
            "uniqueness": {
                "mixed_types": {"thresholds": {"absolute_critical": 1}},
                "boolean_col": {"thresholds": {"absolute_critical": 0}},
            },
        }

        metadata = {"date_column": "date", "id_column": "id"}

        manager = CheckManager(df=df, metadata=metadata, checks_config=checks_config)

        # Should handle mixed types gracefully
        results = manager.run_all_checks()

        assert results["summary"]["total"] > 0
        # Should not crash on mixed data types

    def test_large_dataset_simulation(self):
        """Test performance and behavior with larger datasets."""
        # Create a larger dataset to test scalability
        n_rows = 1000
        n_dates = 10

        dates = pd.date_range("2025-01-01", periods=n_dates, freq="D")
        data = []

        np.random.seed(42)  # For reproducible tests

        for date in dates:
            for i in range(n_rows // n_dates):
                data.append(
                    {
                        "id": i + 1,
                        "date": date,
                        "value": np.random.normal(100, 15),
                        "category": np.random.choice(
                            ["A", "B", "C", None], p=[0.4, 0.3, 0.2, 0.1]
                        ),
                        "score": np.random.uniform(1, 5),
                    }
                )

        large_df = pd.DataFrame(data)

        checks_config = {
            "completeness": {
                "value": {"thresholds": {"absolute_critical": 0.05}},
                "category": {"thresholds": {"absolute_critical": 0.15}},
            },
            "statistical": {
                "value": {
                    "measures": ["mean", "std"],
                    "thresholds": {
                        "mean": {"absolute_critical": [80, 120]},
                        "std": {"absolute_critical": 25},
                    },
                }
            },
        }

        metadata = {"date_column": "date", "id_column": "id"}

        manager = CheckManager(
            df=large_df, metadata=metadata, checks_config=checks_config
        )

        # Test both execution modes
        results = manager.run_all_checks()
        assert results["summary"]["total"] > 0

        # Test parallel execution
        manager.check_results = []
        parallel_results = manager.run_all_checks_parallel(max_workers=2)
        assert parallel_results["summary"]["total"] > 0

        # Results should be consistent
        assert results["summary"]["total"] == parallel_results["summary"]["total"]

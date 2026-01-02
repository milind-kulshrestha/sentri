"""Shared test fixtures for the data quality framework."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def basic_df():
    """Basic DataFrame for simple tests."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "date": pd.to_datetime(["2025-01-01"] * 5),
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
            "category": ["A", "B", "A", "B", "A"],
        }
    )


@pytest.fixture
def multi_date_df():
    """DataFrame with multiple dates for temporal analysis."""
    dates = ["2025-01-01", "2025-01-02", "2025-01-03"]
    data = []

    for i, date in enumerate(dates):
        for entity_id in range(1, 6):
            data.append(
                {
                    "id": entity_id,
                    "date": pd.to_datetime(date),
                    "value": 10.0 * entity_id + i * 5,  # Trending upward
                    "score": entity_id + i * 0.1,
                    "category": ["A", "B", "C"][entity_id % 3],
                }
            )

    return pd.DataFrame(data)


@pytest.fixture
def messy_data_df():
    """DataFrame with various data quality issues."""
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
            "value": [10.0, np.nan, 30.0, np.inf, -np.inf, 15.0, None, 35.0, 0.0, 1e10],
            "category": ["A", None, "B", "", "C", "A", "B", None, "C", "INVALID"],
            "score": [1.0, 2.0, np.nan, 4.0, 5.0, 1.5, 2.5, 3.5, np.nan, 5.5],
            "percentage": [
                50,
                80,
                110,
                -10,
                150,
                55,
                85,
                95,
                105,
                200,
            ],  # Out of 0-100 range
        }
    )


@pytest.fixture
def duplicate_data_df():
    """DataFrame with various types of duplicates."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 3, 3, 4, 5, 5, 6, 7],  # ID duplicates
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
            "value": [10, 20, 30, 30, 30, 15, 25, 25, 35, 45],  # Value duplicates
            "exact_duplicate": ["A", "B", "C", "C", "C", "D", "E", "E", "F", "G"],
        }
    )


@pytest.fixture
def empty_df():
    """Empty DataFrame with correct schema."""
    return pd.DataFrame(
        {
            "id": pd.Series([], dtype="int64"),
            "date": pd.Series([], dtype="datetime64[ns]"),
            "value": pd.Series([], dtype="float64"),
        }
    )


@pytest.fixture
def single_row_df():
    """DataFrame with only one row."""
    return pd.DataFrame(
        {
            "id": [1],
            "date": pd.to_datetime(["2025-01-01"]),
            "value": [42.0],
            "category": ["A"],
        }
    )


@pytest.fixture
def extreme_values_df():
    """DataFrame with extreme statistical values."""
    return pd.DataFrame(
        {
            "id": range(1, 11),
            "date": pd.to_datetime(["2025-01-01"] * 10),
            "tiny_values": [
                1e-10,
                2e-10,
                3e-10,
                4e-10,
                5e-10,
                6e-10,
                7e-10,
                8e-10,
                9e-10,
                1e-9,
            ],
            "huge_values": [1e10, 2e10, 3e10, 4e10, 5e10, 6e10, 7e10, 8e10, 9e10, 1e11],
            "zero_variance": [42.0] * 10,  # No variance
            "high_variance": [1, 1000000, 2, 999999, 3, 1000001, 4, 999998, 5, 1000002],
        }
    )


@pytest.fixture
def correlation_test_df():
    """DataFrame designed for correlation testing."""
    np.random.seed(42)  # For reproducible tests
    n = 100

    # Perfect positive correlation
    x = np.random.normal(0, 1, n)
    perfect_pos = x * 2 + 1

    # Perfect negative correlation
    perfect_neg = -x * 3 + 5

    # No correlation
    no_corr = np.random.normal(0, 1, n)

    # Moderate correlation
    moderate_pos = x * 0.7 + np.random.normal(0, 0.5, n)

    return pd.DataFrame(
        {
            "id": range(1, n + 1),
            "date": pd.to_datetime(["2025-01-01"] * n),
            "x": x,
            "perfect_positive": perfect_pos,
            "perfect_negative": perfect_neg,
            "no_correlation": no_corr,
            "moderate_positive": moderate_pos,
        }
    )


@pytest.fixture
def temporal_drift_df():
    """DataFrame showing drift over time."""
    dates = pd.date_range("2025-01-01", periods=10, freq="D")
    data = []

    for i, date in enumerate(dates):
        # Values that drift upward over time
        base_mean = 100 + i * 10  # Mean increases by 10 each day
        base_std = 5 + i * 2  # Std increases by 2 each day

        for entity_id in range(1, 21):
            value = np.random.normal(base_mean, base_std)
            data.append(
                {
                    "id": entity_id,
                    "date": date,
                    "drifting_value": value,
                    "stable_value": np.random.normal(50, 5),  # No drift
                }
            )

    return pd.DataFrame(data)


# Configuration fixtures
@pytest.fixture
def basic_completeness_config():
    """Basic completeness check configuration."""
    return {
        "value": {
            "thresholds": {"absolute_critical": 0.20},
            "description": "Value completeness check",
        }
    }


@pytest.fixture
def strict_completeness_config():
    """Strict completeness configuration."""
    return {
        "value": {
            "thresholds": {"absolute_critical": 0.01, "absolute_warning": 0.05},
            "description": "Strict completeness check",
        },
        "category": {
            "thresholds": {"absolute_critical": 0.10},
            "filter_condition": "value > 0",
            "description": "Category completeness with filter",
        },
    }


@pytest.fixture
def basic_uniqueness_config():
    """Basic uniqueness check configuration."""
    return {
        "id": {
            "thresholds": {"absolute_critical": 0},
            "description": "ID should be unique",
        }
    }


@pytest.fixture
def range_check_config():
    """Range check configuration."""
    return {
        "percentage": {
            "min_value": 0,
            "max_value": 100,
            "description": "Percentage should be 0-100",
        },
        "score": {"min_value": 1, "max_value": 5, "description": "Score should be 1-5"},
    }


@pytest.fixture
def statistical_config():
    """Statistical check configuration."""
    return {
        "value": {
            "measures": ["mean", "std", "median", "min", "max"],
            "thresholds": {
                "mean": {"absolute_critical": [10, 50]},  # Range threshold
                "std": {"absolute_critical": 20},  # Single threshold
            },
            "description": "Value statistical measures",
        }
    }


@pytest.fixture
def correlation_config():
    """Correlation check configuration."""
    return {
        "perfect_positive": {
            "correlation_type": "cross_column",
            "correlation_with": "x",
            "thresholds": {"absolute_critical": 0.9},
            "description": "Should be highly correlated with x",
        },
        "no_correlation": {
            "correlation_type": "cross_column",
            "correlation_with": "x",
            "thresholds": {"absolute_critical": 0.5},
            "description": "Should have low correlation with x",
        },
    }


@pytest.fixture
def temporal_correlation_config():
    """Temporal correlation configuration."""
    return {
        "drifting_value": {
            "correlation_type": "temporal",
            "thresholds": {"absolute_critical": 0.8},
            "description": "Temporal correlation check",
        }
    }


@pytest.fixture
def edge_case_configs():
    """Various edge case configurations."""
    return {
        "empty_config": {},
        "disabled_config": {"value": {"enabled": False}},
        "invalid_threshold_config": {
            "value": {
                "thresholds": {"absolute_critical": -1}  # Invalid negative threshold
            }
        },
        "missing_required_config": {
            "value": {
                "correlation_type": "cross_column"
                # Missing correlation_with
            }
        },
        "extreme_threshold_config": {
            "value": {"thresholds": {"absolute_critical": 1e10}}
        },
    }


@pytest.fixture
def metadata_configs():
    """Various metadata configurations."""
    return {
        "basic": {
            "date_column": "date",
            "id_column": "id",
            "dq_check_name": "Basic Test",
        },
        "custom_columns": {
            "date_column": "custom_date",
            "id_column": "custom_id",
            "dq_check_name": "Custom Column Test",
        },
        "missing_columns": {
            "date_column": "missing_date",
            "id_column": "missing_id",
            "dq_check_name": "Missing Column Test",
        },
    }

"""Test cases for distribution check."""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from data_quality.checks.distribution import DistributionCheck
from data_quality.utils.constants import CheckStatus


class TestDistributionCheck:
    """Test cases for DistributionCheck class."""

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        np.random.seed(42)
        data = []

        # First period - normal distribution
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-01"),
                    "entity_id": f"id_{i}",
                    "value": np.random.normal(100, 10),
                }
            )

        # Second period - slightly different distribution
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
    def distribution_config(self):
        """Distribution check configuration."""
        return {
            "value": {
                "thresholds": {"absolute_critical": 0.05},
                "description": "Value distribution check",
            }
        }

    def test_distribution_check_basic(self, sample_data, distribution_config):
        """Test basic distribution check functionality."""
        check = DistributionCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["check_type"] == "distribution"
        assert result.iloc[0]["column"] == "value"
        assert "ks_statistic" in result.iloc[0]["additional_metrics"]
        assert "p_value" in result.iloc[0]["additional_metrics"]

    def test_distribution_check_significant_change(self, distribution_config):
        """Test distribution check with significant change."""
        # Create data with very different distributions
        np.random.seed(42)
        data = []

        # First period - normal(0, 1)
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-01"),
                    "entity_id": f"id_{i}",
                    "value": np.random.normal(0, 1),
                }
            )

        # Second period - normal(10, 1) - very different
        for i in range(100):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-02"),
                    "entity_id": f"id_{i}",
                    "value": np.random.normal(10, 1),
                }
            )

        df = pd.DataFrame(data)

        check = DistributionCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.FAIL
        assert result.iloc[0]["severity"] == "CRITICAL"

    def test_distribution_check_no_change(self, distribution_config):
        """Test distribution check with identical distributions."""
        np.random.seed(42)
        data = []

        # Both periods - identical distribution
        for period, date in enumerate(
            [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")]
        ):
            for i in range(100):
                data.append(
                    {
                        "effective_date": date,
                        "entity_id": f"id_{i}",
                        "value": np.random.normal(100, 10),
                    }
                )

        df = pd.DataFrame(data)

        check = DistributionCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS

    def test_distribution_check_insufficient_dates(self, distribution_config):
        """Test distribution check with insufficient dates."""
        data = []
        for i in range(10):
            data.append(
                {
                    "effective_date": pd.Timestamp("2025-01-01"),
                    "entity_id": f"id_{i}",
                    "value": i,
                }
            )

        df = pd.DataFrame(data)

        check = DistributionCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert "Insufficient dates" in result.iloc[0]["additional_metrics"]["message"]

    def test_distribution_check_insufficient_data(self, distribution_config):
        """Test distribution check with insufficient data points."""
        data = [
            {
                "effective_date": pd.Timestamp("2025-01-01"),
                "entity_id": "id_1",
                "value": 1,
            },
            {
                "effective_date": pd.Timestamp("2025-01-02"),
                "entity_id": "id_2",
                "value": 2,
            },
        ]

        df = pd.DataFrame(data)

        check = DistributionCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert (
            "Insufficient data for KS test"
            in result.iloc[0]["additional_metrics"]["message"]
        )

    def test_distribution_check_with_filter(self, sample_data):
        """Test distribution check with filter condition."""
        config = {
            "value": {
                "thresholds": {"absolute_critical": 0.05},
                "filter_condition": 'entity_id.str.contains("1")',
                "description": "Filtered distribution check",
            }
        }

        check = DistributionCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["filter_applied"] == 'entity_id.str.contains("1")'

    def test_distribution_check_missing_column(self, sample_data, distribution_config):
        """Test distribution check with missing column."""
        config = {"missing_column": {"thresholds": {"absolute_critical": 0.05}}}

        check = DistributionCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == "ERROR"
        assert "not found" in result.iloc[0]["error_message"]

    def test_distribution_check_disabled_column(self, sample_data):
        """Test distribution check with disabled column."""
        config = {
            "value": {"enabled": False, "thresholds": {"absolute_critical": 0.05}}
        }

        check = DistributionCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert result.empty

    def test_distribution_check_with_nulls(self, distribution_config):
        """Test distribution check with null values."""
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

        check = DistributionCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        # Should handle nulls gracefully
        assert result.iloc[0]["additional_metrics"]["prev_count"] > 0
        assert result.iloc[0]["additional_metrics"]["curr_count"] > 0

    def test_distribution_check_error_handling(self, sample_data):
        """Test distribution check error handling."""
        config = {
            "value": {
                "thresholds": {"absolute_critical": 0.05},
                "filter_condition": "invalid_syntax ==",  # Invalid filter
            }
        }

        check = DistributionCheck(
            df=sample_data,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == "ERROR"

    def test_distribution_check_empty_dataframe(self, distribution_config):
        """Test distribution check with empty DataFrame."""
        df = pd.DataFrame(columns=["effective_date", "entity_id", "value"])

        check = DistributionCheck(
            df=df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=distribution_config,
        )

        result = check.run()

        assert not result.empty
        assert result.iloc[0]["status"] == CheckStatus.PASS
        assert "Insufficient dates" in result.iloc[0]["additional_metrics"]["message"]

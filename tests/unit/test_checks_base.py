"""Tests for base check classes and check manager."""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from data_quality.checks.base import BaseCheck
from data_quality.core.exceptions import CheckError, FilterError, MissingColumnError
from data_quality.managers.check_manager import CheckManager
from data_quality.utils.constants import CheckStatus, Severity


class TestBaseCheck:
    """Tests for BaseCheck class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "entity_id": [1, 2, 3, 4, 5],
                "effective_date": pd.to_datetime(
                    [
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-02",
                        "2025-01-02",
                        "2025-01-03",
                    ]
                ),
                "value": [100.0, 200.0, np.nan, 150.0, 175.0],
                "category": ["A", "B", "A", "B", "A"],
                "universe": ["US", "US", "EU", "US", "EU"],
            }
        )

    def test_cannot_instantiate_abstract(self):
        """Test that BaseCheck cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseCheck(df=pd.DataFrame(), date_col="date", id_col="id", check_config={})

    def test_normalize_dataframe_lowercase(self, sample_df):
        """Test that DataFrame columns are normalized to lowercase."""

        # Create a concrete implementation for testing
        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        # Create df with uppercase columns
        df = pd.DataFrame(
            {
                "Entity_ID": [1, 2],
                "Effective_Date": ["2025-01-01", "2025-01-02"],
                "VALUE": [100, 200],
            }
        )

        check = ConcreteCheck(
            df=df, date_col="Effective_Date", id_col="Entity_ID", check_config={}
        )

        assert "entity_id" in check.df.columns
        assert "effective_date" in check.df.columns
        assert "value" in check.df.columns

    def test_normalize_config_lowercase(self, sample_df):
        """Test that config keys are normalized to lowercase."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        config = {
            "Column1": {"thresholds": {"absolute_critical": 0.05}},
            "COLUMN2": {"thresholds": {"absolute_critical": 0.10}},
        }

        check = ConcreteCheck(
            df=sample_df,
            date_col="effective_date",
            id_col="entity_id",
            check_config=config,
        )

        assert "column1" in check.check_config
        assert "column2" in check.check_config

    def test_apply_filter_valid(self, sample_df):
        """Test applying valid filter condition."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        filtered = check._apply_filter(check.df, "universe == 'US'")
        assert len(filtered) == 3

    def test_apply_filter_invalid_raises_error(self, sample_df):
        """Test that invalid filter raises FilterError."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        with pytest.raises(FilterError):
            check._apply_filter(check.df, "invalid_column == 'X'")

    def test_apply_filter_none_returns_original(self, sample_df):
        """Test that None filter returns original DataFrame."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        result = check._apply_filter(check.df, None)
        assert len(result) == len(sample_df)

    def test_evaluate_threshold_pass(self, sample_df):
        """Test threshold evaluation when value passes."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        thresholds = {"absolute_critical": 0.10, "absolute_warning": 0.05}

        result = check._evaluate_threshold(0.02, thresholds)
        assert result["status"] == CheckStatus.PASS
        assert result["severity"] == Severity.INFO

    def test_evaluate_threshold_warning(self, sample_df):
        """Test threshold evaluation when value triggers warning."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        thresholds = {"absolute_critical": 0.10, "absolute_warning": 0.05}

        result = check._evaluate_threshold(0.07, thresholds)
        assert result["status"] == CheckStatus.WARNING
        assert result["severity"] == Severity.WARNING

    def test_evaluate_threshold_fail(self, sample_df):
        """Test threshold evaluation when value fails."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        thresholds = {"absolute_critical": 0.10, "absolute_warning": 0.05}

        result = check._evaluate_threshold(0.15, thresholds)
        assert result["status"] == CheckStatus.FAIL
        assert result["severity"] == Severity.CRITICAL

    def test_evaluate_threshold_delta(self, sample_df):
        """Test threshold evaluation for delta values."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        thresholds = {"delta_critical": 0.10, "delta_warning": 0.05}

        # Test with negative delta (should use absolute value)
        result = check._evaluate_threshold(-0.08, thresholds, threshold_type="delta")
        assert result["status"] == CheckStatus.WARNING

    def test_create_result_record(self, sample_df):
        """Test creating standardized result record."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df,
            date_col="effective_date",
            id_col="entity_id",
            check_config={
                "value": {
                    "thresholds": {"absolute_critical": 0.05},
                    "description": "Test check",
                    "column_alias": "value_alias",
                }
            },
        )

        evaluation = {
            "status": CheckStatus.PASS,
            "severity": Severity.INFO,
            "exceeded_threshold": None,
        }

        record = check._create_result_record(
            column="value", date="2025-01-01", metric_value=0.02, evaluation=evaluation
        )

        assert record["check_type"] == "concrete"
        assert record["column"] == "value"
        assert record["date"] == "2025-01-01"
        assert record["metric_value"] == 0.02
        assert record["status"] == CheckStatus.PASS
        assert record["description"] == "Test check"
        assert record["column_alias"] == "value_alias"
        assert "timestamp" in record

    def test_handle_column_error(self, sample_df):
        """Test handling column-level errors."""

        class ConcreteCheck(BaseCheck):
            def run(self):
                return pd.DataFrame()

        check = ConcreteCheck(
            df=sample_df, date_col="effective_date", id_col="entity_id", check_config={}
        )

        error = ValueError("Test error")
        context = {"test": "context"}

        result = check._handle_column_error("value", error, context)

        assert result["status"] == CheckStatus.ERROR
        assert result["severity"] == Severity.ERROR
        assert result["column"] == "value"
        assert "Test error" in result["error_message"]


class TestCheckManager:
    """Tests for CheckManager class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "entity_id": [1, 2, 3, 4, 5],
                "effective_date": pd.to_datetime(
                    [
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-02",
                        "2025-01-02",
                        "2025-01-03",
                    ]
                ),
                "value": [100.0, 200.0, np.nan, 150.0, 175.0],
                "category": ["A", "B", "A", "B", "A"],
            }
        )

    @pytest.fixture
    def metadata(self):
        """Create sample metadata."""
        return {
            "dq_check_name": "Test Check",
            "date_column": "effective_date",
            "id_column": "entity_id",
        }

    def test_initialization(self, sample_df, metadata):
        """Test CheckManager initialization."""
        checks_config = {
            "completeness": {"value": {"thresholds": {"absolute_critical": 0.05}}}
        }

        manager = CheckManager(
            df=sample_df, metadata=metadata, checks_config=checks_config
        )

        assert manager.df is not None
        assert manager.metadata == metadata
        assert manager.checks_config == checks_config

    def test_get_summary_statistics_empty(self, sample_df, metadata):
        """Test getting summary statistics with no results."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})

        summary = manager.get_summary_statistics()
        assert summary["total"] == 0
        assert summary["passed"] == 0
        assert summary["failed"] == 0

    def test_aggregate_results(self, sample_df, metadata):
        """Test aggregating results from check runs."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})

        # Add some mock results
        manager.check_results = [
            {
                "check_type": "completeness",
                "column": "value",
                "status": CheckStatus.PASS,
                "severity": Severity.INFO,
            },
            {
                "check_type": "completeness",
                "column": "category",
                "status": CheckStatus.WARNING,
                "severity": Severity.WARNING,
            },
        ]

        results = manager.aggregate_results()
        assert "results" in results
        assert "summary" in results
        assert len(results["results"]) == 2

    def test_get_summary_with_results(self, sample_df, metadata):
        """Test summary statistics with results."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})

        manager.check_results = [
            {"status": CheckStatus.PASS, "severity": Severity.INFO},
            {"status": CheckStatus.PASS, "severity": Severity.INFO},
            {"status": CheckStatus.WARNING, "severity": Severity.WARNING},
            {"status": CheckStatus.FAIL, "severity": Severity.CRITICAL},
        ]

        summary = manager.get_summary_statistics()
        assert summary["total"] == 4
        assert summary["passed"] == 2
        assert summary["warnings"] == 1
        assert summary["failed"] == 1
        assert summary["pass_rate"] == 50.0

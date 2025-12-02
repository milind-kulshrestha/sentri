"""Tests for CheckManager orchestration logic."""

import pandas as pd
import pytest

from data_quality.managers.check_manager import CheckManager
from data_quality.utils.constants import CheckStatus, Severity


class TestCheckManager:
    """Tests for the CheckManager class."""

    @pytest.fixture
    def sample_df(self):
        """Create a small DataFrame for checks."""
        return pd.DataFrame({
            "id": [1, 2, 3, 4],
            "date": pd.to_datetime([
                "2025-01-01",
                "2025-01-01",
                "2025-01-02",
                "2025-01-02",
            ]),
            "value": [10.0, 20.0, 30.0, 40.0],
        })

    @pytest.fixture
    def metadata(self):
        """Sample metadata configuration."""
        return {
            "date_column": "date",
            "id_column": "id",
            "dq_check_name": "Test Check",
        }

    @pytest.fixture
    def checks_config(self):
        """Configuration with a single completeness check."""
        return {
            "completeness": {
                "value": {
                    "thresholds": {"absolute_critical": 1.0},
                    "description": "Value completeness",
                }
            }
        }

    def test_run_all_checks_success(self, sample_df, metadata, checks_config):
        """run_all_checks should execute configured checks and aggregate results."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config=checks_config)

        results = manager.run_all_checks()

        assert "summary" in results
        assert "results" in results
        assert "results_by_type" in results
        assert results["summary"]["total"] > 0
        assert "completeness" in results["results_by_type"]

    def test_run_all_checks_unknown_type(self, sample_df, metadata):
        """Unknown check types should be ignored and return empty results."""
        manager = CheckManager(
            df=sample_df,
            metadata=metadata,
            checks_config={"unknown_type": {"value": {}}},
        )

        results = manager.run_all_checks()

        assert results["summary"]["total"] == 0
        assert results["summary"]["passed"] == 0
        assert results["results"] == []

    def test_run_all_checks_parallel(self, sample_df, metadata, checks_config):
        """run_all_checks_parallel should execute checks and aggregate results."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config=checks_config)

        results = manager.run_all_checks_parallel(max_workers=2)

        assert results["summary"]["total"] > 0
        assert "completeness" in results["results_by_type"]

    def test_handle_check_error_populates_error_result(self, sample_df, metadata):
        """_handle_check_error should append an error record and reflect in summary."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})

        manager._handle_check_error("completeness", ValueError("boom"))

        assert len(manager.check_results) == 1
        error_record = manager.check_results[0]
        assert error_record["check_type"] == "completeness"
        assert error_record["status"] == CheckStatus.ERROR
        assert error_record["severity"] == Severity.ERROR
        assert error_record["error_type"] == "ValueError"

        summary = manager.get_summary_statistics()
        assert summary["total"] == 1
        assert summary["errors"] == 1

    def test_get_summary_statistics_empty(self, sample_df, metadata):
        """get_summary_statistics should handle case with no results."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})

        summary = manager.get_summary_statistics()

        assert summary["total"] == 0
        assert summary["passed"] == 0
        assert summary["warnings"] == 0
        assert summary["failed"] == 0
        assert summary["errors"] == 0
        assert summary["pass_rate"] == 0.0

    def test_group_results_by_type(self, sample_df, metadata):
        """_group_results_by_type should group records using check_type key."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})
        manager.check_results = [
            {"check_type": "completeness", "status": CheckStatus.PASS},
            {"check_type": "completeness", "status": CheckStatus.FAIL},
            {"check_type": "uniqueness", "status": CheckStatus.PASS},
        ]

        grouped = manager._group_results_by_type()

        assert set(grouped.keys()) == {"completeness", "uniqueness"}
        assert len(grouped["completeness"]) == 2
        assert len(grouped["uniqueness"]) == 1

    def test_get_failed_and_warning_checks(self, sample_df, metadata):
        """get_failed_checks and get_warning_checks should filter by status."""
        manager = CheckManager(df=sample_df, metadata=metadata, checks_config={})
        manager.check_results = [
            {"check_type": "completeness", "status": CheckStatus.PASS},
            {"check_type": "completeness", "status": CheckStatus.FAIL},
            {"check_type": "range", "status": CheckStatus.WARNING},
            {"check_type": "range", "status": CheckStatus.ERROR},
        ]

        failed = manager.get_failed_checks()
        warnings = manager.get_warning_checks()

        assert all(r["status"] == CheckStatus.FAIL for r in failed)
        assert all(r["status"] == CheckStatus.WARNING for r in warnings)
        assert len(failed) == 1
        assert len(warnings) == 1

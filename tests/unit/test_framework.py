"""Tests for the core DataQualityFramework and DQResults container."""

import pandas as pd
import pytest

from data_quality.core.framework import DataQualityFramework, DQResults


class TestDataQualityFramework:
    """Tests for the DataQualityFramework orchestration class."""

    @pytest.fixture
    def sample_df(self):
        """Simple DataFrame used for initialization tests."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "date": pd.to_datetime(
                    [
                        "2025-01-01",
                        "2025-01-01",
                        "2025-01-02",
                    ]
                ),
                "value": [10.0, 20.0, 30.0],
            }
        )

    def test_framework_initialization_with_config_path(self, sample_df):
        """Constructor should store provided arguments and initialize attributes."""
        framework = DataQualityFramework(
            config_path="config.yaml",
            config_dict=None,
            df=sample_df,
            metadata={"dq_check_name": "Test Check"},
        )

        assert framework.config_path == "config.yaml"
        assert framework.config_dict is None
        assert framework.df is sample_df
        assert framework.metadata["dq_check_name"] == "Test Check"
        assert framework.config is None
        assert framework.data_source_manager is None
        assert framework.check_manager is None
        assert framework.output_manager is None
        assert framework.results is None

    def test_run_checks_not_implemented(self, sample_df):
        """run_checks should currently raise NotImplementedError stub."""
        framework = DataQualityFramework(df=sample_df)

        with pytest.raises(NotImplementedError):
            framework.run_checks("2025-01-01", "2025-01-31")

    def test_run_check_not_implemented(self, sample_df):
        """run_check should currently raise NotImplementedError stub."""
        framework = DataQualityFramework(df=sample_df)

        with pytest.raises(NotImplementedError):
            framework.run_check("completeness", {"value": {}})


class TestDQResults:
    """Tests for the DQResults container."""

    @pytest.fixture
    def sample_df(self):
        """Sample DataFrame for results container."""
        return pd.DataFrame(
            {
                "id": [1, 2],
                "status": ["PASS", "FAIL"],
            }
        )

    @pytest.fixture
    def metadata(self):
        """Sample metadata for results container."""
        return {"dq_check_name": "Test Check", "run_id": "abc123"}

    def test_to_dataframe_returns_original_df(self, sample_df, metadata):
        """to_dataframe should return the underlying DataFrame unchanged."""
        results = DQResults(results_df=sample_df, metadata=metadata)

        df_out = results.to_dataframe()

        assert df_out.equals(sample_df)

    def test_not_implemented_methods_raise(self, sample_df, metadata):
        """Export and summary methods should currently raise NotImplementedError."""
        results = DQResults(results_df=sample_df, metadata=metadata)

        with pytest.raises(NotImplementedError):
            results.to_json()

        with pytest.raises(NotImplementedError):
            results.to_html()

        with pytest.raises(NotImplementedError):
            results.to_csv()

        with pytest.raises(NotImplementedError):
            results.get_summary()

        with pytest.raises(NotImplementedError):
            results.get_exit_code()

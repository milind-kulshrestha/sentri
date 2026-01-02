"""Tests for configuration schema validation."""

import pytest
from pydantic import ValidationError

from data_quality.core.config_schema import (
    ColumnCheckConfig,
    CompletenessCheckConfig,
    CSVSourceConfig,
    DQConfig,
    MetadataConfig,
    OracleSourceConfig,
    OutputConfig,
    RangeCheckConfig,
    SnowflakeSourceConfig,
    SourceConfig,
    ThresholdConfig,
    TurnoverCheckConfig,
    UniquenessCheckConfig,
)


class TestThresholdConfig:
    """Tests for ThresholdConfig schema."""

    def test_valid_absolute_thresholds(self):
        """Test valid absolute thresholds."""
        config = ThresholdConfig(absolute_critical=0.05, absolute_warning=0.02)
        assert config.absolute_critical == 0.05
        assert config.absolute_warning == 0.02

    def test_valid_delta_thresholds(self):
        """Test valid delta thresholds."""
        config = ThresholdConfig(
            absolute_critical=0.05, delta_critical=0.03, delta_warning=0.01
        )
        assert config.delta_critical == 0.03
        assert config.delta_warning == 0.01

    def test_requires_absolute_critical(self):
        """Test that absolute_critical is required."""
        with pytest.raises(ValidationError):
            ThresholdConfig(absolute_warning=0.02)

    def test_negative_threshold_invalid(self):
        """Test that negative thresholds are invalid."""
        with pytest.raises(ValidationError):
            ThresholdConfig(absolute_critical=-0.05)


class TestColumnCheckConfig:
    """Tests for ColumnCheckConfig schema."""

    def test_valid_column_config(self):
        """Test valid column check configuration."""
        config = ColumnCheckConfig(
            thresholds=ThresholdConfig(absolute_critical=0.05), description="Test check"
        )
        assert config.thresholds.absolute_critical == 0.05
        assert config.description == "Test check"

    def test_with_filter_condition(self):
        """Test with filter condition."""
        config = ColumnCheckConfig(
            thresholds=ThresholdConfig(absolute_critical=0.05),
            filter_condition="universe == 'US'",
        )
        assert config.filter_condition == "universe == 'US'"

    def test_with_column_alias(self):
        """Test with column alias."""
        config = ColumnCheckConfig(
            thresholds=ThresholdConfig(absolute_critical=0.05),
            column_alias="carbon_em_us",
        )
        assert config.column_alias == "carbon_em_us"

    def test_enabled_default_true(self):
        """Test that enabled defaults to True."""
        config = ColumnCheckConfig(thresholds=ThresholdConfig(absolute_critical=0.05))
        assert config.enabled is True


class TestCompletenessCheckConfig:
    """Tests for CompletenessCheckConfig schema."""

    def test_valid_completeness_config(self):
        """Test valid completeness check config."""
        config = CompletenessCheckConfig(
            thresholds=ThresholdConfig(absolute_critical=0.05), compare_to="previous"
        )
        assert config.compare_to == "previous"


class TestRangeCheckConfig:
    """Tests for RangeCheckConfig schema."""

    def test_valid_range_config(self):
        """Test valid range check config."""
        config = RangeCheckConfig(min_value=0, max_value=10)
        assert config.min_value == 0
        assert config.max_value == 10

    def test_requires_at_least_one_bound(self):
        """Test that at least one bound is required."""
        # Both None should fail
        with pytest.raises(ValidationError):
            RangeCheckConfig()

    def test_min_only(self):
        """Test with only min value."""
        config = RangeCheckConfig(min_value=0)
        assert config.min_value == 0
        assert config.max_value is None

    def test_max_only(self):
        """Test with only max value."""
        config = RangeCheckConfig(max_value=100)
        assert config.min_value is None
        assert config.max_value == 100


class TestSourceConfig:
    """Tests for SourceConfig schemas."""

    def test_csv_source_config(self):
        """Test CSV source configuration."""
        config = CSVSourceConfig(
            file_path="/data/test.csv", date_column="effective_date"
        )
        assert config.file_path == "/data/test.csv"
        assert config.encoding == "utf-8"  # default
        assert config.delimiter == ","  # default

    def test_csv_custom_options(self):
        """Test CSV with custom options."""
        config = CSVSourceConfig(
            file_path="/data/test.csv",
            encoding="latin-1",
            delimiter=";",
            date_column="date",
        )
        assert config.encoding == "latin-1"
        assert config.delimiter == ";"

    def test_oracle_source_config(self):
        """Test Oracle source configuration."""
        config = OracleSourceConfig(
            host="localhost",
            port=1521,
            service_name="orcl",
            username="user",
            password="pass",
            sql="SELECT * FROM table",
        )
        assert config.host == "localhost"
        assert config.port == 1521
        assert config.sql == "SELECT * FROM table"

    def test_snowflake_source_config(self):
        """Test Snowflake source configuration."""
        config = SnowflakeSourceConfig(
            account="myaccount",
            warehouse="compute_wh",
            database="mydb",
            schema_name="public",
            username="user",
            password="pass",
            sql="SELECT * FROM table",
        )
        assert config.account == "myaccount"
        assert config.warehouse == "compute_wh"
        assert config.authenticator == "snowflake"  # default

    def test_source_config_csv_type(self):
        """Test SourceConfig with CSV type."""
        config = SourceConfig(
            type="csv",
            csv=CSVSourceConfig(file_path="/data/test.csv", date_column="date"),
        )
        assert config.type == "csv"
        assert config.csv is not None

    def test_source_config_requires_matching_type(self):
        """Test that source type must match config."""
        with pytest.raises(ValidationError):
            SourceConfig(
                type="oracle",
                csv=CSVSourceConfig(file_path="/data/test.csv", date_column="date"),
            )


class TestMetadataConfig:
    """Tests for MetadataConfig schema."""

    def test_valid_metadata(self):
        """Test valid metadata configuration."""
        config = MetadataConfig(
            dq_check_name="Test Check",
            date_column="effective_date",
            id_column="entity_id",
        )
        assert config.dq_check_name == "Test Check"
        assert config.date_column == "effective_date"
        assert config.id_column == "entity_id"

    def test_optional_description(self):
        """Test that description is optional."""
        config = MetadataConfig(
            dq_check_name="Test", date_column="date", id_column="id"
        )
        assert config.description is None


class TestOutputConfig:
    """Tests for OutputConfig schema."""

    def test_valid_output_config(self):
        """Test valid output configuration."""
        config = OutputConfig(formats=["json", "html"], destination="/output")
        assert "json" in config.formats
        assert config.destination == "/output"

    def test_invalid_format(self):
        """Test that invalid format raises error."""
        with pytest.raises(ValidationError):
            OutputConfig(formats=["json", "invalid_format"], destination="/output")

    def test_default_options(self):
        """Test default output options."""
        config = OutputConfig(formats=["json"], destination="/output")
        assert config.include_passed_checks is True
        assert config.file_prefix == "dq_report"


class TestDQConfig:
    """Tests for main DQConfig schema."""

    def test_minimal_valid_config(self):
        """Test minimal valid configuration."""
        config = DQConfig(
            source=SourceConfig(
                type="csv",
                csv=CSVSourceConfig(file_path="/data/test.csv", date_column="date"),
            ),
            metadata=MetadataConfig(
                dq_check_name="Test", date_column="date", id_column="id"
            ),
            checks={},
            output=OutputConfig(formats=["json"], destination="/output"),
        )
        assert config.source.type == "csv"
        assert config.metadata.dq_check_name == "Test"

    def test_with_completeness_checks(self):
        """Test configuration with completeness checks."""
        config = DQConfig(
            source=SourceConfig(
                type="csv",
                csv=CSVSourceConfig(file_path="/data/test.csv", date_column="date"),
            ),
            metadata=MetadataConfig(
                dq_check_name="Test", date_column="date", id_column="id"
            ),
            checks={
                "completeness": {"column1": {"thresholds": {"absolute_critical": 0.05}}}
            },
            output=OutputConfig(formats=["json"], destination="/output"),
        )
        assert "completeness" in config.checks

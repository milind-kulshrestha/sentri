"""Pydantic schemas for configuration validation."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class ThresholdConfig(BaseModel):
    """Configuration for check thresholds."""

    absolute_critical: float = Field(
        ..., ge=0, description="Critical threshold for absolute value"
    )
    absolute_warning: Optional[float] = Field(
        None, ge=0, description="Warning threshold for absolute value"
    )
    delta_critical: Optional[float] = Field(
        None, ge=0, description="Critical threshold for delta change"
    )
    delta_warning: Optional[float] = Field(
        None, ge=0, description="Warning threshold for delta change"
    )


class ColumnCheckConfig(BaseModel):
    """Base configuration for a column-level check."""

    thresholds: ThresholdConfig
    filter_condition: Optional[str] = Field(
        None, description="Pandas query filter condition"
    )
    column_alias: Optional[str] = Field(
        None, description="Alias name for filtered column"
    )
    description: Optional[str] = Field(None, description="Human-readable description")
    enabled: bool = Field(True, description="Whether the check is enabled")
    compare_to: Optional[str] = Field(
        None, description="Comparison period: 'previous' or specific date"
    )


class CompletenessCheckConfig(ColumnCheckConfig):
    """Configuration for completeness check."""

    pass


class TurnoverCheckConfig(ColumnCheckConfig):
    """Configuration for turnover check."""

    pass


class UniquenessCheckConfig(ColumnCheckConfig):
    """Configuration for uniqueness check."""

    pass


class ValueSpikeCheckConfig(ColumnCheckConfig):
    """Configuration for value spike check."""

    pass


class FrequencyCheckConfig(ColumnCheckConfig):
    """Configuration for frequency check."""

    pass


class CorrelationCheckConfig(ColumnCheckConfig):
    """Configuration for correlation check."""

    correlation_type: str = Field(
        "temporal", description="Type: 'temporal' or 'cross_column'"
    )
    correlation_with: Optional[str] = Field(
        None, description="Column to correlate with (for cross_column)"
    )


class RangeCheckConfig(BaseModel):
    """Configuration for range check."""

    min_value: Optional[float] = Field(None, description="Minimum acceptable value")
    max_value: Optional[float] = Field(None, description="Maximum acceptable value")
    description: Optional[str] = Field(None, description="Human-readable description")
    filter_condition: Optional[str] = Field(
        None, description="Pandas query filter condition"
    )
    enabled: bool = Field(True, description="Whether the check is enabled")

    @model_validator(mode="after")
    def check_at_least_one_bound(self):
        if self.min_value is None and self.max_value is None:
            raise ValueError("At least one of min_value or max_value must be specified")
        return self


class StatisticalMeasureThreshold(BaseModel):
    """Threshold configuration for a statistical measure."""

    absolute_critical: Optional[Union[float, List[float]]] = Field(
        None, description="Critical threshold (or [min, max] range)"
    )
    absolute_warning: Optional[Union[float, List[float]]] = Field(
        None, description="Warning threshold (or [min, max] range)"
    )
    delta_critical: Optional[float] = Field(
        None, ge=0, description="Critical delta threshold (percentage)"
    )
    delta_warning: Optional[float] = Field(
        None, ge=0, description="Warning delta threshold (percentage)"
    )


class StatisticalCheckConfig(BaseModel):
    """Configuration for statistical measures check."""

    measures: List[str] = Field(..., description="Statistical measures to calculate")
    thresholds: Dict[str, StatisticalMeasureThreshold] = Field(
        ..., description="Thresholds per measure"
    )
    description: Optional[str] = Field(None, description="Human-readable description")
    filter_condition: Optional[str] = Field(
        None, description="Pandas query filter condition"
    )
    enabled: bool = Field(True, description="Whether the check is enabled")


class DistributionCheckConfig(ColumnCheckConfig):
    """Configuration for distribution change check."""

    baseline_period: Optional[str] = Field(
        None, description="Baseline period for comparison"
    )


class DriftCheckConfig(ColumnCheckConfig):
    """Configuration for drift check."""

    drift_method: str = Field(
        "psi", description="Drift method: 'psi', 'ks', or 'jensen_shannon'"
    )
    baseline_period: Optional[str] = Field(
        "first", description="Baseline: 'first' or specific date"
    )


# Source configurations
class CSVSourceConfig(BaseModel):
    """Configuration for CSV data source."""

    file_path: str = Field(..., description="Path to CSV file")
    date_column: str = Field(..., description="Name of date column")
    encoding: str = Field("utf-8", description="File encoding")
    delimiter: str = Field(",", description="Column delimiter")
    date_format: Optional[str] = Field(None, description="Date format string")
    parse_dates: Optional[List[str]] = Field(
        None, description="Columns to parse as dates"
    )


class OracleSourceConfig(BaseModel):
    """Configuration for Oracle data source."""

    host: str = Field(..., description="Database host")
    port: int = Field(1521, description="Database port")
    service_name: str = Field(..., description="Oracle service name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    sql: str = Field(..., description="SQL query to execute")
    connection_options: Optional[Dict[str, Any]] = Field(
        None, description="Additional connection options"
    )


class SnowflakeSourceConfig(BaseModel):
    """Configuration for Snowflake data source."""

    account: str = Field(..., description="Snowflake account")
    warehouse: str = Field(..., description="Snowflake warehouse")
    database: str = Field(..., description="Database name")
    schema_name: str = Field(..., description="Schema name", alias="schema")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    sql: str = Field(..., description="SQL query to execute")
    role: Optional[str] = Field(None, description="Snowflake role")
    authenticator: str = Field("snowflake", description="Authentication method")

    class Config:
        populate_by_name = True


class RetryConfig(BaseModel):
    """Configuration for connection retry."""

    enabled: bool = Field(True, description="Whether retry is enabled")
    max_attempts: int = Field(3, ge=1, le=10, description="Maximum retry attempts")
    backoff_factor: float = Field(
        2.0, ge=1.0, le=10.0, description="Backoff multiplier"
    )
    retry_on_errors: Optional[List[str]] = Field(
        None, description="Error types to retry on"
    )


class SourceConfig(BaseModel):
    """Configuration for data source."""

    type: str = Field(
        ..., description="Source type: 'csv', 'oracle', 'snowflake', or custom"
    )
    csv: Optional[CSVSourceConfig] = Field(None, description="CSV source configuration")
    oracle: Optional[OracleSourceConfig] = Field(
        None, description="Oracle source configuration"
    )
    snowflake: Optional[SnowflakeSourceConfig] = Field(
        None, description="Snowflake source configuration"
    )
    connection_retry: Optional[RetryConfig] = Field(
        None, description="Retry configuration"
    )

    @model_validator(mode="after")
    def check_source_config_matches_type(self):
        if self.type == "csv" and self.csv is None:
            raise ValueError("CSV configuration required when type is 'csv'")
        if self.type == "oracle" and self.oracle is None:
            raise ValueError("Oracle configuration required when type is 'oracle'")
        if self.type == "snowflake" and self.snowflake is None:
            raise ValueError(
                "Snowflake configuration required when type is 'snowflake'"
            )

        # Ensure only matching config is provided
        if self.type == "csv" and (
            self.oracle is not None or self.snowflake is not None
        ):
            raise ValueError(
                "Only CSV configuration should be provided when type is 'csv'"
            )
        if self.type == "oracle" and (
            self.csv is not None or self.snowflake is not None
        ):
            raise ValueError(
                "Only Oracle configuration should be provided when type is 'oracle'"
            )
        if self.type == "snowflake" and (
            self.csv is not None or self.oracle is not None
        ):
            raise ValueError(
                "Only Snowflake configuration should be provided when type is 'snowflake'"
            )

        return self


class MetadataConfig(BaseModel):
    """Configuration for DQ check metadata."""

    dq_check_name: str = Field(..., description="Name of the DQ check")
    date_column: str = Field(..., description="Name of the date column")
    id_column: str = Field(..., description="Name of the ID column")
    description: Optional[str] = Field(None, description="Description of the check")


class AlertingPluginConfig(BaseModel):
    """Configuration for an alerting plugin."""

    type: str = Field(..., description="Plugin type (e.g., 'email')")
    config: Dict[str, Any] = Field(..., description="Plugin-specific configuration")


class AlertingConfig(BaseModel):
    """Configuration for alerting system."""

    enabled: bool = Field(True, description="Whether alerting is enabled")
    plugins: List[AlertingPluginConfig] = Field(
        ..., description="List of alerting plugins"
    )


class ExitCodeConfig(BaseModel):
    """Configuration for exit code behavior."""

    exit_on_critical: bool = Field(
        True, description="Exit with error on critical failures"
    )
    exit_on_warning: bool = Field(False, description="Exit with error on warnings")
    exit_on_error: bool = Field(True, description="Exit with error on errors")


class OutputConfig(BaseModel):
    """Configuration for output generation."""

    formats: List[str] = Field(
        ..., description="Output formats: json, html, csv, dataframe"
    )
    destination: str = Field(..., description="Output destination path")
    file_prefix: str = Field("dq_report", description="Prefix for output files")
    include_timestamp: bool = Field(True, description="Include timestamp in filename")
    include_check_name: bool = Field(True, description="Include check name in filename")
    include_passed_checks: bool = Field(
        True, description="Include passed checks in output"
    )
    include_configuration: bool = Field(
        True, description="Include configuration in output"
    )
    compress: bool = Field(False, description="Compress output files")
    pretty_print: bool = Field(True, description="Pretty print JSON output")
    alerting: Optional[AlertingConfig] = Field(
        None, description="Alerting configuration"
    )
    exit_code: Optional[ExitCodeConfig] = Field(
        None, description="Exit code configuration"
    )

    @field_validator("formats")
    @classmethod
    def validate_formats(cls, v):
        valid_formats = {"json", "html", "csv", "dataframe"}
        for fmt in v:
            if fmt not in valid_formats:
                raise ValueError(
                    f"Invalid format '{fmt}'. Must be one of: {valid_formats}"
                )
        return v


class ExecutionConfig(BaseModel):
    """Configuration for check execution."""

    parallel_enabled: bool = Field(False, description="Enable parallel check execution")
    max_workers: int = Field(4, ge=1, le=16, description="Maximum parallel workers")
    timeout_per_check: int = Field(
        300, ge=1, description="Timeout per check in seconds"
    )


class LogOutputConfig(BaseModel):
    """Configuration for a log output."""

    type: str = Field(..., description="Output type: 'console' or 'file'")
    level: str = Field("INFO", description="Log level")
    format: str = Field("text", description="Log format: 'text' or 'json'")
    path: Optional[str] = Field(None, description="File path (for file type)")
    rotation: Optional[str] = Field(None, description="Log rotation: 'daily', 'size'")
    retention_days: Optional[int] = Field(None, description="Days to retain logs")
    max_size_mb: Optional[int] = Field(None, description="Max file size in MB")


class LoggingConfig(BaseModel):
    """Configuration for logging."""

    level: str = Field("INFO", description="Global log level")
    format: str = Field("text", description="Log format: 'text' or 'json'")
    outputs: Optional[List[LogOutputConfig]] = Field(None, description="Log outputs")
    include_context: bool = Field(True, description="Include context in logs")
    correlation_id: str = Field(
        "auto", description="Correlation ID mode: 'auto' or 'manual'"
    )


class DQConfig(BaseModel):
    """Main configuration for Data Quality Framework."""

    source: SourceConfig = Field(..., description="Data source configuration")
    metadata: MetadataConfig = Field(..., description="Metadata configuration")
    checks: Dict[str, Dict[str, Any]] = Field(..., description="Check configurations")
    output: OutputConfig = Field(..., description="Output configuration")
    execution: Optional[ExecutionConfig] = Field(
        None, description="Execution configuration"
    )
    logging: Optional[LoggingConfig] = Field(None, description="Logging configuration")

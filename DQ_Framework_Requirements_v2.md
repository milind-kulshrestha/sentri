# Sentri Data Quality Framework - Requirements v2

**Version:** 2.0  
**Date:** November 19, 2025  
**Status:** Final  
**Document Owner:** DQ Framework Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Project Overview](#2-project-overview)
3. [System Architecture](#3-system-architecture)
4. [Detailed Requirements](#4-detailed-requirements)
   - 4.1 [Configuration Layer](#41-configuration-layer)
   - 4.2 [Data Source Manager](#42-data-source-manager)
   - 4.3 [Check Manager](#43-check-manager)
   - 4.4 [Data Quality Checks](#44-data-quality-checks)
   - 4.5 [Output Manager](#45-output-manager)
   - 4.6 [Alerting System (Optional)](#46-alerting-system-optional)
   - 4.7 [Logging Infrastructure](#47-logging-infrastructure)
5. [Package Structure](#5-package-structure)
6. [Non-Functional Requirements](#6-non-functional-requirements)
7. [Dependencies](#7-dependencies)
8. [Deliverables](#8-deliverables)
9. [Success Criteria](#9-success-criteria)
10. [Timeline and Milestones](#10-timeline-and-milestones)

---

## 1. Executive Summary

The Data Quality (DQ) Framework is a production-ready, pip-installable Python package designed to perform configurable, automated data quality validation across multiple data sources. The framework provides a declarative YAML-based configuration approach with column-wise check definitions, supports multiple data sources through a pluggable connector architecture, and delivers structured results in various formats that users can distribute through their preferred channels.

### Key Features
- **Configuration-Driven**: YAML-based, schema-validated configuration
- **Column-Wise Check Definitions**: Granular control over checks per column
- **Multi-Source Support**: Native support for Oracle, Snowflake, CSV with extensible connector architecture
- **Flexible Output**: JSON, HTML, CSV, DataFrame formats
- **Pluggable Alerting**: Optional email alerting system
- **Production-Ready**: Comprehensive error handling, logging, and testing

---

## 2. Project Overview

### 2.1 Purpose
Deliver a distributable Python package via pip that enables data teams to perform comprehensive data quality validation through YAML configuration files with minimal code integration required.

### 2.2 Scope

**In Scope:**
- Configuration-driven data quality validation framework
- Data Source Manager for multi-source connectivity
- Check Manager for orchestrating data quality checks
- Column-wise check configuration with individual thresholds and filters
- Multiple output formats (JSON, HTML, CSV, pandas DataFrame)
- Optional alerting plugin architecture
- Comprehensive logging and error handling
- Schema-driven validation and standardization

**Out of Scope:**
- Real-time streaming data validation
- Machine learning-based anomaly detection (future enhancement)
- Built-in data remediation capabilities
- GUI/Web interface (CLI and programmatic API only)

### 2.3 Distribution
- **Package Name**: `data-quality-framework`
- **Distribution Method**: PyPI (pip installable)
- **Python Version Support**: Python 3.8+
- **License**: Apache 2.0 / MIT (TBD)

---

## 3. System Architecture

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Configuration Layer                       │
│              (YAML Config + Schema Validation)               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      DQ Framework Core                       │
│                    (Orchestration Engine)                    │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Data Source     │  │   Check Manager  │  │  Output Manager  │
│    Manager       │  │                  │  │                  │
│                  │  │  ┌────────────┐  │  │  ┌────────────┐  │
│  ┌───────────┐   │  │  │Base Check  │  │  │  │  JSON      │  │
│  │Oracle     │   │  │  │   Class    │  │  │  │  HTML      │  │
│  │Snowflake  │   │  │  └────────────┘  │  │  │  CSV       │  │
│  │CSV        │   │  │        │         │  │  │  DataFrame │  │
│  │Custom     │   │  │        ▼         │  │  └────────────┘  │
│  └───────────┘   │  │  ┌────────────┐  │  └──────────────────┘
│                  │  │  │ 10 Check   │  │
│  Connector       │  │  │ Types      │  │           │
│  Registry        │  │  └────────────┘  │           ▼
└──────────────────┘  └──────────────────┘  ┌──────────────────┐
         │                     │             │Optional Alerting │
         │                     │             │   - Email        │
         │                     │             │   - Slack        │
         │                     │             │   - Custom       │
         │                     │             └──────────────────┘
         ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    Logging Infrastructure                    │
│              (Structured JSON/Text Logging)                  │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| **Configuration Layer** | Parse, validate, and normalize YAML configuration |
| **DQ Framework Core** | Orchestrate data retrieval, check execution, and output generation |
| **Data Source Manager** | Handle all data source connections and data retrieval |
| **Check Manager** | Execute configured checks and aggregate results |
| **Output Manager** | Format and export results in requested formats |
| **Alerting System** | Optional plugin for notifications (email, Slack, etc.) |
| **Logging Infrastructure** | Structured logging across all components |

---

## 4. Detailed Requirements

### 4.1 Configuration Layer

#### 4.1.1 Configuration File Structure

The configuration file is divided into four main sections:

```yaml
# Data source definition
source:
  type: oracle | snowflake | csv | custom
  <source_specific_config>

# Metadata about the DQ check
metadata:
  dq_check_name: "Production Data Validation Q4 2025"
  date_column: "effective_date"
  id_column: "bus_ent_id"
  description: "Optional description"

# Check definitions (column-wise)
checks:
  <check_type>:
    <column_name>:
      thresholds: {...}
      filter_condition: "..."
      description: "..."
      # ... other column-specific config

# Output configuration
output:
  formats: [json, html, csv, dataframe]
  destination: "/path/to/output"
  file_prefix: "dq_report"
  include_passed_checks: true
  alerting: {...}  # Optional
```

#### 4.1.2 Schema Validation Requirements

- **Validation Framework**: Pydantic v2.x for schema validation
- **Validation Timing**: Configuration validation before any processing begins
- **Error Handling**: Clear, actionable error messages with line numbers and field paths
- **Required vs Optional**: Explicit marking of required and optional fields
- **Default Values**: Sensible defaults for optional fields
- **Type Checking**: Strict type enforcement (str, int, float, list, dict)

**Example Validation Error:**
```
ConfigurationError: Invalid configuration at 'checks.completeness.column1.thresholds.absolute_critical'
  - Expected type: float
  - Received type: str
  - Value: "0.05"
  - Suggestion: Remove quotes around numeric values
  Line: 45
```

### 4.2 Data Source Manager

The Data Source Manager is responsible for all data retrieval operations, connection management, and error handling related to data sources.

#### 4.2.1 Core Responsibilities

1. **Connection Management**: Establish and maintain connections to data sources
2. **Data Retrieval**: Fetch data based on date ranges and optional filters
3. **Connection Validation**: Pre-flight checks before data retrieval
4. **Error Handling**: Graceful handling of connection and query errors
5. **Retry Logic**: Automatic retry with exponential backoff
6. **Connector Registration**: Dynamic registration of custom connectors

#### 4.2.2 Base Connector Interface

```python
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any

class DataConnector(ABC):
    """
    Base class for all data connectors.
    All custom connectors must inherit from this class.
    """
    
    def __init__(self, **config):
        """
        Initialize connector with configuration parameters.
        
        Args:
            **config: Source-specific configuration parameters
        """
        self.config = config
        self.connection = None
        self.logger = self._setup_logger()
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate connection to data source.
        
        Returns:
            bool: True if connection is valid, False otherwise
        
        Raises:
            ConnectionError: If connection validation fails
        """
        pass
    
    @abstractmethod
    def get_data(
        self, 
        start_date: str, 
        end_date: str, 
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieve data from the data source.
        
        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            **kwargs: Additional source-specific parameters
        
        Returns:
            pd.DataFrame: Retrieved data with normalized column names
        
        Raises:
            DataRetrievalError: If data retrieval fails
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close connection and clean up resources.
        """
        pass
    
    def handle_error(
        self, 
        error: Exception, 
        context: Dict[str, Any]
    ) -> None:
        """
        Handle connection and query errors gracefully.
        
        Args:
            error: The exception that occurred
            context: Additional context about the error
        """
        self.logger.error(
            f"Error in {self.__class__.__name__}: {str(error)}",
            extra={"context": context}
        )
        raise
    
    def retry_with_backoff(
        self, 
        func: callable, 
        max_attempts: int = 3,
        backoff_factor: float = 2.0
    ) -> Any:
        """
        Retry a function with exponential backoff.
        
        Args:
            func: Function to retry
            max_attempts: Maximum number of retry attempts
            backoff_factor: Multiplier for backoff delay
        
        Returns:
            Result of the function call
        """
        pass
```

#### 4.2.3 Native Connector Implementations

**Oracle Connector**

Configuration Parameters:
```yaml
source:
  type: oracle
  oracle:
    host: ${ORACLE_HOST}          # Environment variable
    port: 1521
    service_name: ${ORACLE_SERVICE}
    username: ${ORACLE_USER}
    password: ${ORACLE_PASSWORD}
    sql: |
      SELECT * FROM table_name
      WHERE effective_date >= :start_date 
        AND effective_date <= :end_date
    connection_options:
      timeout: 30
      encoding: utf-8
```

Features:
- Parameterized query support (`:start_date`, `:end_date`)
- Connection pooling
- Thick/thin mode support
- Credential management via environment variables
- SSL/TLS connection support

**Snowflake Connector**

Configuration Parameters:
```yaml
source:
  type: snowflake
  snowflake:
    account: ${SNOWFLAKE_ACCOUNT}
    warehouse: ${SNOWFLAKE_WAREHOUSE}
    database: ${SNOWFLAKE_DATABASE}
    schema: ${SNOWFLAKE_SCHEMA}
    username: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    role: ${SNOWFLAKE_ROLE}
    authenticator: oauth  # oauth | snowflake | externalbrowser
    sql: |
      SELECT * FROM table_name
      WHERE effective_date >= '{start_date}'
        AND effective_date <= '{end_date}'
```

Features:
- OAuth and password authentication
- Session management
- Query optimization
- Result caching
- Multiple authentication methods

**CSV Connector**

Configuration Parameters:
```yaml
source:
  type: csv
  csv:
    file_path: /path/to/data.csv
    encoding: utf-8
    delimiter: ","
    date_column: effective_date
    date_format: "%Y-%m-%d"
    parse_dates: [effective_date]
```

Features:
- Local and network path support
- Automatic date parsing
- Encoding detection
- Large file handling (chunking)

#### 4.2.4 Custom Connector Development

**Registration Mechanism:**

```python
from data_quality.connectors import DataConnector, register_connector

@register_connector('my_custom_source')
class MyCustomConnector(DataConnector):
    def validate_connection(self) -> bool:
        # Implementation
        pass
    
    def get_data(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        # Implementation
        pass
    
    def close(self) -> None:
        # Implementation
        pass
```

**Configuration:**
```yaml
source:
  type: my_custom_source
  my_custom_source:
    custom_param1: value1
    custom_param2: value2
```

**Documentation Requirements:**
- Clear API documentation for base connector class
- Step-by-step guide for implementing custom connectors
- Example implementations for common data sources
- Testing guidelines for custom connectors

#### 4.2.5 Connection Error Handling

**Error Types and Responses:**

| Error Type | Handling Strategy | User Feedback |
|------------|------------------|---------------|
| Authentication Failure | No retry, fail fast | "Authentication failed. Check credentials." |
| Network Timeout | Retry with backoff (3 attempts) | "Connection timeout. Retrying..." |
| Query Syntax Error | No retry, fail fast | "SQL syntax error: <details>" |
| Connection Refused | Retry with backoff | "Connection refused. Retrying..." |
| Data Not Found | No retry, return empty DataFrame | "No data found for specified date range" |

**Retry Configuration:**
```yaml
source:
  connection_retry:
    enabled: true
    max_attempts: 3
    backoff_factor: 2.0  # seconds: 1, 2, 4
    retry_on_errors:
      - ConnectionTimeout
      - NetworkError
      - TemporaryFailure
```

**Logging for Data Source Operations:**
```json
{
  "timestamp": "2025-06-15T10:30:00Z",
  "level": "INFO",
  "component": "DataSourceManager",
  "connector_type": "oracle",
  "event": "data_retrieval_start",
  "context": {
    "start_date": "2025-06-01",
    "end_date": "2025-06-15",
    "query_hash": "a3f5b2c1"
  }
}
```

### 4.3 Check Manager

The Check Manager orchestrates the execution of all configured data quality checks, manages check lifecycle, and aggregates results.

#### 4.3.1 Core Responsibilities

1. **Check Orchestration**: Execute checks in the defined order
2. **Result Aggregation**: Collect and consolidate results from all checks
3. **Error Management**: Handle check-level and column-level errors
4. **Parallel Execution**: Optional parallel check execution
5. **Progress Tracking**: Report check execution progress
6. **Result Normalization**: Ensure consistent result schema

#### 4.3.2 Check Manager Interface

```python
from typing import Dict, List, Any
import pandas as pd

class CheckManager:
    """
    Manages execution of all configured data quality checks.
    """
    
    def __init__(
        self, 
        df: pd.DataFrame,
        metadata: Dict[str, Any],
        checks_config: Dict[str, Any],
        logger: Any = None
    ):
        """
        Initialize Check Manager.
        
        Args:
            df: DataFrame containing data to validate
            metadata: Metadata configuration (date_column, id_column, etc.)
            checks_config: Check definitions from configuration
            logger: Optional logger instance
        """
        self.df = df
        self.metadata = metadata
        self.checks_config = checks_config
        self.logger = logger or self._setup_logger()
        self.check_results = []
        self.check_registry = self._load_check_registry()
    
    def run_all_checks(self) -> Dict[str, Any]:
        """
        Execute all configured checks.
        
        Returns:
            Dict containing aggregated results from all checks
        """
        pass
    
    def run_single_check(
        self, 
        check_type: str, 
        check_config: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Execute a single check type.
        
        Args:
            check_type: Type of check (e.g., 'completeness')
            check_config: Configuration for the check
        
        Returns:
            pd.DataFrame: Check results
        """
        pass
    
    def aggregate_results(self) -> Dict[str, Any]:
        """
        Aggregate results from all executed checks.
        
        Returns:
            Dict containing summary and detailed results
        """
        pass
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Generate summary statistics across all checks.
        
        Returns:
            Dict with counts of passed/failed/warning checks
        """
        pass
```

#### 4.3.3 Check Execution Flow

```
1. Initialize Check Manager
   ├── Load DataFrame
   ├── Parse metadata
   └── Load check configurations

2. For each check type in configuration:
   ├── Validate check configuration
   ├── Instantiate check class
   ├── For each column in check config:
   │   ├── Apply filter condition (if specified)
   │   ├── Execute check logic
   │   ├── Evaluate thresholds
   │   ├── Handle errors (column-level)
   │   └── Store result
   ├── Aggregate column results
   └── Handle errors (check-level)

3. Aggregate all check results
   ├── Normalize result schema
   ├── Calculate summary statistics
   └── Generate check report

4. Return results to Output Manager
```

#### 4.3.4 Parallel Execution (Optional)

For improved performance with large datasets or many checks:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

class CheckManager:
    def run_all_checks_parallel(self, max_workers: int = 4) -> Dict[str, Any]:
        """
        Execute checks in parallel.
        
        Args:
            max_workers: Maximum number of parallel workers
        
        Returns:
            Dict containing aggregated results
        """
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.run_single_check, 
                    check_type, 
                    check_config
                ): check_type
                for check_type, check_config in self.checks_config.items()
            }
            
            for future in as_completed(futures):
                check_type = futures[future]
                try:
                    result = future.result()
                    self.check_results.append({
                        'check_type': check_type,
                        'result': result
                    })
                except Exception as e:
                    self.handle_check_error(check_type, e)
```

**Configuration:**
```yaml
execution:
  parallel_enabled: true
  max_workers: 4
  timeout_per_check: 300  # seconds
```

### 4.4 Data Quality Checks

#### 4.4.1 Column-Wise Check Configuration

All checks are configured at the column level, allowing granular control over validation rules, thresholds, and filters for each column.

**General Configuration Structure:**

```yaml
checks:
  <check_type>:
    <column_name>:
      thresholds:
        absolute_critical: <value>      # Required: Critical failure threshold
        absolute_warning: <value>       # Optional: Warning threshold
        delta_critical: <value>         # Optional: Critical change threshold
        delta_warning: <value>          # Optional: Warning change threshold
      filter_condition: "<pandas_query>"  # Optional: Filter to apply
      column_alias: "<alias_name>"        # Optional: Alias for filtered column
      description: "<check_description>"  # Optional: Human-readable description
      enabled: true                       # Optional: Enable/disable check (default: true)
      compare_to: "previous"             # Optional: previous | specific_date
```

**Key Principles:**
1. Each column has its own configuration dictionary
2. Thresholds are defined per column
3. Filter conditions are applied before check execution
4. Column alias provides meaningful naming for filtered datasets
5. All configuration is optional except column name and required thresholds

#### 4.4.2 Base Check Class

All check implementations inherit from the `BaseCheck` class, which provides common functionality.

```python
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Optional

class BaseCheck(ABC):
    """
    Base class for all data quality checks.
    Provides common functionality for configuration parsing,
    column normalization, threshold evaluation, and error handling.
    """
    
    def __init__(
        self, 
        df: pd.DataFrame,
        date_col: str,
        id_col: str,
        check_config: Dict[str, Dict[str, Any]]
    ):
        """
        Initialize base check.
        
        Args:
            df: DataFrame to validate
            date_col: Name of date column
            id_col: Name of ID column
            check_config: Column-wise check configuration
        """
        self.df = self._normalize_dataframe(df)
        self.date_col = date_col.lower()
        self.id_col = id_col.lower()
        self.check_config = self._normalize_config(check_config)
        self.results = []
        self.logger = self._setup_logger()
    
    @abstractmethod
    def run(self) -> pd.DataFrame:
        """
        Execute the check logic.
        Must be implemented by each check type.
        
        Returns:
            pd.DataFrame: Check results following standard schema
        """
        pass
    
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame column names to lowercase.
        Convert date column to datetime type.
        
        Args:
            df: Input DataFrame
        
        Returns:
            pd.DataFrame: Normalized DataFrame
        """
        df = df.copy()
        df.columns = df.columns.str.lower()
        return df
    
    def _normalize_config(
        self, 
        config: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Normalize configuration keys to lowercase.
        Validate required fields are present.
        
        Args:
            config: Raw check configuration
        
        Returns:
            Dict: Normalized configuration
        """
        normalized = {}
        for col_name, col_config in config.items():
            normalized[col_name.lower()] = col_config
        return normalized
    
    def _apply_filter(
        self, 
        df: pd.DataFrame, 
        filter_condition: Optional[str]
    ) -> pd.DataFrame:
        """
        Apply pandas query filter to DataFrame.
        
        Args:
            df: DataFrame to filter
            filter_condition: Pandas query string
        
        Returns:
            pd.DataFrame: Filtered DataFrame
        
        Raises:
            FilterError: If filter condition is invalid
        """
        if not filter_condition:
            return df
        
        try:
            return df.query(filter_condition)
        except Exception as e:
            self.logger.error(f"Filter error: {filter_condition} - {str(e)}")
            raise FilterError(f"Invalid filter: {filter_condition}") from e
    
    def _evaluate_threshold(
        self,
        value: float,
        thresholds: Dict[str, float],
        threshold_type: str = "absolute"
    ) -> Dict[str, Any]:
        """
        Evaluate a value against configured thresholds.
        
        Args:
            value: Value to evaluate
            thresholds: Threshold configuration
            threshold_type: "absolute" or "delta"
        
        Returns:
            Dict with evaluation results:
                - status: "PASS" | "WARNING" | "FAIL"
                - severity: "INFO" | "WARNING" | "CRITICAL"
                - exceeded_threshold: float | None
        """
        critical_key = f"{threshold_type}_critical"
        warning_key = f"{threshold_type}_warning"
        
        critical = thresholds.get(critical_key)
        warning = thresholds.get(warning_key)
        
        # Check critical threshold
        if critical is not None:
            if threshold_type == "absolute":
                if value > critical:
                    return {
                        "status": "FAIL",
                        "severity": "CRITICAL",
                        "exceeded_threshold": critical
                    }
            else:  # delta
                if abs(value) > critical:
                    return {
                        "status": "FAIL",
                        "severity": "CRITICAL",
                        "exceeded_threshold": critical
                    }
        
        # Check warning threshold
        if warning is not None:
            if threshold_type == "absolute":
                if value > warning:
                    return {
                        "status": "WARNING",
                        "severity": "WARNING",
                        "exceeded_threshold": warning
                    }
            else:  # delta
                if abs(value) > warning:
                    return {
                        "status": "WARNING",
                        "severity": "WARNING",
                        "exceeded_threshold": warning
                    }
        
        # Passed all thresholds
        return {
            "status": "PASS",
            "severity": "INFO",
            "exceeded_threshold": None
        }
    
    def _handle_column_error(
        self, 
        column: str, 
        error: Exception,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle errors that occur during column-level check execution.
        
        Args:
            column: Column name where error occurred
            error: Exception that was raised
            context: Additional context about the error
        
        Returns:
            Dict: Error result following standard schema
        """
        self.logger.error(
            f"Column error in {column}: {str(error)}",
            extra={"context": context}
        )
        
        return {
            "check_type": self.__class__.__name__,
            "column": column,
            "status": "ERROR",
            "severity": "ERROR",
            "error_message": str(error),
            "error_type": type(error).__name__,
            "context": context
        }
    
    def _create_result_record(
        self,
        column: str,
        date: str,
        metric_value: float,
        evaluation: Dict[str, Any],
        additional_metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a standardized result record.
        
        Args:
            column: Column name
            date: Date of measurement
            metric_value: Measured value
            evaluation: Threshold evaluation result
            additional_metrics: Check-specific additional metrics
        
        Returns:
            Dict: Standardized result record
        """
        config = self.check_config.get(column, {})
        
        return {
            "check_type": self.__class__.__name__.replace("Check", "").lower(),
            "column": column,
            "column_alias": config.get("column_alias"),
            "date": date,
            "filter_applied": config.get("filter_condition"),
            "description": config.get("description"),
            "metric_value": round(metric_value, 6),
            "thresholds": config.get("thresholds", {}),
            "status": evaluation["status"],
            "severity": evaluation["severity"],
            "exceeded_threshold": evaluation.get("exceeded_threshold"),
            "additional_metrics": additional_metrics or {},
            "timestamp": pd.Timestamp.now().isoformat()
        }
```

#### 4.4.3 Check Type Specifications

---

**1. COMPLETENESS CHECK**

**Purpose**: Validate missing/null values in specified columns

**Configuration Example:**
```yaml
checks:
  completeness:
    carbon_em:
      thresholds:
        absolute_critical: 0.05   # Max 5% nulls
        absolute_warning: 0.02    # Warning at 2%
        delta_critical: 0.03      # Max 3% increase in nulls
        delta_warning: 0.01       # Warning at 1% increase
      filter_condition: "universe == 'US'"
      column_alias: "carbon_em_us"
      description: "Carbon emissions completeness for US universe"
      compare_to: "previous"
    
    water_em:
      thresholds:
        absolute_critical: 0.10
      description: "Water emissions completeness"
```

**Threshold Interpretation:**
- `absolute_critical`: Maximum acceptable null percentage in current period
- `delta_critical`: Maximum acceptable increase in null percentage vs comparison period

**Metrics Calculated:**
- Null count
- Total count
- Null percentage
- Previous period null percentage (if delta enabled)
- Delta change in null percentage

**Result Schema:**
```python
{
    "check_type": "completeness",
    "column": "carbon_em",
    "column_alias": "carbon_em_us",
    "date": "2025-06-15",
    "filter_applied": "universe == 'US'",
    "metric_value": 0.032,  # 3.2% null
    "thresholds": {
        "absolute_critical": 0.05,
        "absolute_warning": 0.02,
        "delta_critical": 0.03
    },
    "status": "WARNING",  # Exceeded warning threshold
    "severity": "WARNING",
    "additional_metrics": {
        "null_count": 320,
        "total_count": 10000,
        "null_percentage": 3.2,
        "previous_null_percentage": 2.5,
        "delta_change": 0.7
    }
}
```

---

**2. TURNOVER CHECK**

**Purpose**: Track additions and removals of unique identifiers between time periods

**Configuration Example:**
```yaml
checks:
  turnover:
    fmr_ticker:
      thresholds:
        absolute_critical: 0.15   # Max 15% turnover
        absolute_warning: 0.10    # Warning at 10%
        delta_critical: 0.05      # Max 5% change in turnover rate
      description: "Ticker turnover validation"
      compare_to: "previous"
    
    cusip:
      thresholds:
        absolute_critical: 0.20
      filter_condition: "universe == 'Global'"
      description: "CUSIP turnover for global universe"
```

**Threshold Interpretation:**
- `absolute_critical`: Maximum acceptable turnover rate (adds + drops) / total unique IDs
- `delta_critical`: Maximum acceptable change in turnover rate vs comparison period

**Metrics Calculated:**
- Added count (IDs in current but not previous)
- Dropped count (IDs in previous but not current)
- Turnover rate: (added + dropped) / total unique IDs
- Previous turnover rate (if delta enabled)
- List of added IDs (up to 100)
- List of dropped IDs (up to 100)

**Result Schema:**
```python
{
    "check_type": "turnover",
    "column": "fmr_ticker",
    "date": "2025-06-15",
    "metric_value": 0.12,  # 12% turnover
    "thresholds": {
        "absolute_critical": 0.15,
        "absolute_warning": 0.10
    },
    "status": "WARNING",
    "severity": "WARNING",
    "additional_metrics": {
        "added_count": 85,
        "dropped_count": 45,
        "turnover_rate": 0.12,
        "previous_turnover_rate": 0.08,
        "total_unique_ids": 1083,
        "added_ids_sample": ["AAPL", "MSFT", "..."],
        "dropped_ids_sample": ["XOM", "CVX", "..."]
    }
}
```

---

**3. UNIQUENESS CHECK**

**Purpose**: Ensure columns contain unique values (detect duplicates)

**Configuration Example:**
```yaml
checks:
  uniqueness:
    cusip:
      thresholds:
        absolute_critical: 0      # Zero duplicates allowed
      description: "CUSIP must be unique"
    
    bus_ent_eff_dim_key:
      thresholds:
        absolute_critical: 0
        absolute_warning: 5       # Warning if > 5 duplicates
      filter_condition: "effective_date == effective_date.max()"
      description: "Business entity key uniqueness for latest date"
```

**Threshold Interpretation:**
- `absolute_critical`: Maximum acceptable duplicate count
- `absolute_warning`: Warning duplicate count

**Metrics Calculated:**
- Total rows
- Unique value count
- Duplicate count
- Duplicate percentage
- Sample of duplicated values (up to 20)

**Result Schema:**
```python
{
    "check_type": "uniqueness",
    "column": "cusip",
    "date": "2025-06-15",
    "metric_value": 3,  # 3 duplicates found
    "thresholds": {
        "absolute_critical": 0
    },
    "status": "FAIL",
    "severity": "CRITICAL",
    "additional_metrics": {
        "total_rows": 10000,
        "unique_count": 9997,
        "duplicate_count": 3,
        "duplicate_percentage": 0.03,
        "duplicated_values_sample": ["12345678", "87654321"]
    }
}
```

---

**4. VALUE SPIKE CHECK**

**Purpose**: Detect abnormal value changes for individual records across time periods

**Configuration Example:**
```yaml
checks:
  value_spike:
    carbon_em:
      thresholds:
        absolute_critical: 10.0   # Fail if 10x increase
        absolute_warning: 5.0     # Warning if 5x increase
      description: "Carbon emissions spike detection"
    
    water_em:
      thresholds:
        absolute_critical: 15.0
      filter_condition: "env > 5"
      description: "Water emissions spike for high env scores"
```

**Threshold Interpretation:**
- `absolute_critical`: Maximum acceptable multiplier (current value / previous value)
- Applied row-by-row comparing consecutive time periods

**Metrics Calculated:**
- Number of records with spikes
- Maximum spike multiplier
- Average spike multiplier (for records with spikes)
- List of records with spikes (ID, date, previous value, current value, multiplier)

**Result Schema:**
```python
{
    "check_type": "value_spike",
    "column": "carbon_em",
    "date": "2025-06-15",
    "metric_value": 12.5,  # Max spike multiplier found
    "thresholds": {
        "absolute_critical": 10.0,
        "absolute_warning": 5.0
    },
    "status": "FAIL",
    "severity": "CRITICAL",
    "additional_metrics": {
        "spike_count": 5,
        "max_spike_multiplier": 12.5,
        "avg_spike_multiplier": 8.2,
        "affected_records": [
            {
                "id": "ENTITY_123",
                "date": "2025-06-15",
                "previous_value": 100,
                "current_value": 1250,
                "multiplier": 12.5
            }
        ]
    }
}
```

---

**5. FREQUENCY CHECK**

**Purpose**: Monitor distribution changes in categorical data

**Configuration Example:**
```yaml
checks:
  frequency:
    universe:
      thresholds:
        absolute_critical: 0.10   # Max 10% change per category
        absolute_warning: 0.05    # Warning at 5%
      description: "Universe distribution stability"
    
    coverage:
      thresholds:
        absolute_critical: 0.15
      filter_condition: "env > 0"
      description: "Coverage distribution for positive env"
```

**Threshold Interpretation:**
- `absolute_critical`: Maximum acceptable change in frequency percentage for any category
- Calculated as: |current_frequency - previous_frequency|

**Metrics Calculated:**
- Frequency distribution (current period)
- Frequency distribution (previous period)
- Maximum frequency change across all categories
- Number of categories with significant changes
- Detailed changes per category

**Result Schema:**
```python
{
    "check_type": "frequency",
    "column": "universe",
    "date": "2025-06-15",
    "metric_value": 0.08,  # Max frequency change
    "thresholds": {
        "absolute_critical": 0.10,
        "absolute_warning": 0.05
    },
    "status": "WARNING",
    "severity": "WARNING",
    "additional_metrics": {
        "max_frequency_change": 0.08,
        "categories_with_changes": 2,
        "frequency_changes": {
            "US": {
                "previous_frequency": 0.45,
                "current_frequency": 0.53,
                "change": 0.08,
                "change_percentage": 17.8
            },
            "EU": {
                "previous_frequency": 0.30,
                "current_frequency": 0.27,
                "change": -0.03,
                "change_percentage": -10.0
            }
        }
    }
}
```

---

**6. CORRELATION CHECK**

**Purpose**: Validate correlation between columns or across time periods

**Configuration Example:**
```yaml
checks:
  correlation:
    rnk:
      thresholds:
        absolute_critical: 0.80   # Min correlation of 0.80
        absolute_warning: 0.90    # Warning below 0.90
      correlation_type: "temporal"  # temporal | cross_column
      description: "Rank stability across periods"
    
    labor_score:
      thresholds:
        absolute_critical: 0.85
      correlation_type: "cross_column"
      correlation_with: "layoffs_score"
      description: "Labor score correlation with layoffs"
```

**Threshold Interpretation:**
- `absolute_critical`: Minimum acceptable correlation coefficient
- For temporal: correlation between same column across periods
- For cross_column: correlation between two columns in same period

**Metrics Calculated:**
- Correlation coefficient (Pearson)
- Sample size (number of matching records)
- Correlation p-value
- Correlation strength category (weak/moderate/strong)

**Result Schema:**
```python
{
    "check_type": "correlation",
    "column": "rnk",
    "date": "2025-06-15",
    "metric_value": 0.87,  # Correlation coefficient
    "thresholds": {
        "absolute_critical": 0.80,
        "absolute_warning": 0.90
    },
    "status": "WARNING",
    "severity": "WARNING",
    "additional_metrics": {
        "correlation_coefficient": 0.87,
        "correlation_type": "temporal",
        "compared_to": "2025-06-08",
        "sample_size": 9850,
        "p_value": 0.0001,
        "strength": "strong"
    }
}
```

---

**7. RANGE CHECK**

**Purpose**: Validate numeric values fall within expected bounds

**Configuration Example:**
```yaml
checks:
  range:
    env:
      thresholds:
        min_value: 0
        max_value: 10
      description: "Env score must be 0-10"
    
    carbon_em:
      thresholds:
        min_value: 0
        max_value: 1000000
      filter_condition: "universe == 'US'"
      description: "Carbon emissions range for US"
```

**Threshold Interpretation:**
- `min_value`: Minimum acceptable value
- `max_value`: Maximum acceptable value
- Check fails if any value falls outside [min, max]

**Metrics Calculated:**
- Out of range count
- Out of range percentage
- Minimum value in dataset
- Maximum value in dataset
- Values below minimum (sample)
- Values above maximum (sample)

**Result Schema:**
```python
{
    "check_type": "range",
    "column": "env",
    "date": "2025-06-15",
    "metric_value": 0.02,  # 2% out of range
    "thresholds": {
        "min_value": 0,
        "max_value": 10
    },
    "status": "FAIL",
    "severity": "CRITICAL",
    "additional_metrics": {
        "out_of_range_count": 200,
        "total_count": 10000,
        "out_of_range_percentage": 2.0,
        "dataset_min": -1.5,
        "dataset_max": 12.3,
        "below_min_sample": [-1.5, -0.8, -0.3],
        "above_max_sample": [12.3, 11.7, 10.5]
    }
}
```

---

**8. STATISTICAL MEASURES CHECK**

**Purpose**: Monitor statistical properties (mean, median, mode, sum, count, stdev)

**Configuration Example:**
```yaml
checks:
  statistical:
    carbon_em:
      measures: [mean, median, stdev]
      thresholds:
        mean:
          absolute_critical: [0, 500000]  # Min, Max
          absolute_warning: [10000, 400000]
          delta_critical: 0.20  # 20% change
        stdev:
          absolute_critical: [0, 100000]
          delta_critical: 0.30
      description: "Carbon emissions statistical monitoring"
    
    percent_portfolio:
      measures: [sum]
      thresholds:
        sum:
          absolute_critical: [99.9, 100.1]  # Must sum to ~100
      description: "Portfolio weights must sum to 100%"
```

**Threshold Interpretation:**
- For range thresholds: [min, max] acceptable range
- For delta thresholds: Maximum acceptable percentage change

**Metrics Calculated:**
- Requested statistical measures
- Previous period values (for delta comparison)
- Delta changes
- Distribution percentiles (25th, 50th, 75th)

**Result Schema:**
```python
{
    "check_type": "statistical",
    "column": "carbon_em",
    "date": "2025-06-15",
    "metric_value": 285000,  # Primary metric (mean in this case)
    "thresholds": {
        "mean": {
            "absolute_critical": [0, 500000],
            "delta_critical": 0.20
        }
    },
    "status": "PASS",
    "severity": "INFO",
    "additional_metrics": {
        "mean": 285000,
        "median": 220000,
        "stdev": 95000,
        "previous_mean": 275000,
        "mean_delta_percentage": 3.6,
        "percentile_25": 150000,
        "percentile_75": 380000
    }
}
```

---

**9. DISTRIBUTION CHANGE CHECK (KS Statistics)**

**Purpose**: Detect significant distribution shifts using Kolmogorov-Smirnov test

**Configuration Example:**
```yaml
checks:
  distribution:
    carbon_em:
      thresholds:
        absolute_critical: 0.05   # Min p-value (reject if < 0.05)
        absolute_warning: 0.10
      description: "Carbon emissions distribution stability"
    
    water_em:
      thresholds:
        absolute_critical: 0.01
      baseline_period: "2025-01-01"  # Compare to specific date
      description: "Water emissions vs baseline"
```

**Threshold Interpretation:**
- `absolute_critical`: Minimum acceptable p-value from KS test
- p-value < threshold indicates significant distribution change (FAIL)
- Higher p-value = more similar distributions

**Metrics Calculated:**
- KS statistic
- KS test p-value
- Distribution shift interpretation
- Sample sizes compared
- Distribution moments (mean, variance) for both periods

**Result Schema:**
```python
{
    "check_type": "distribution",
    "column": "carbon_em",
    "date": "2025-06-15",
    "metric_value": 0.03,  # p-value
    "thresholds": {
        "absolute_critical": 0.05,
        "absolute_warning": 0.10
    },
    "status": "FAIL",
    "severity": "CRITICAL",
    "additional_metrics": {
        "ks_statistic": 0.12,
        "p_value": 0.03,
        "interpretation": "Significant distribution shift detected",
        "compared_to": "2025-06-08",
        "current_mean": 285000,
        "previous_mean": 275000,
        "current_variance": 9025000000,
        "previous_variance": 8450000000,
        "sample_size_current": 10000,
        "sample_size_previous": 9850
    }
}
```

---

**10. DRIFT CHECK**

**Purpose**: Identify gradual data drift over time using Population Stability Index (PSI) or other drift metrics

**Configuration Example:**
```yaml
checks:
  drift:
    generat_thermal_coal_pct:
      thresholds:
        absolute_critical: 0.25   # PSI > 0.25 indicates significant drift
        absolute_warning: 0.10    # PSI > 0.10 warning
      drift_method: "psi"          # psi | ks | jensen_shannon
      baseline_period: "first"     # first | specific date
      description: "Coal generation percentage drift"
    
    oil_nat_gas_reserves_volume:
      thresholds:
        absolute_critical: 0.20
      drift_method: "psi"
      baseline_period: "2025-01-01"
      description: "Oil/gas reserves drift vs Q1 baseline"
```

**Threshold Interpretation:**
- PSI < 0.1: No significant drift
- PSI 0.1-0.25: Moderate drift (WARNING)
- PSI > 0.25: Significant drift (FAIL)

**Metrics Calculated:**
- PSI score (or alternative drift metric)
- Baseline period used
- Number of bins used in calculation
- Per-bin PSI contributions
- Trend direction (increasing/decreasing/shifting)

**Result Schema:**
```python
{
    "check_type": "drift",
    "column": "generat_thermal_coal_pct",
    "date": "2025-06-15",
    "metric_value": 0.18,  # PSI score
    "thresholds": {
        "absolute_critical": 0.25,
        "absolute_warning": 0.10
    },
    "status": "WARNING",
    "severity": "WARNING",
    "additional_metrics": {
        "drift_score": 0.18,
        "drift_method": "psi",
        "baseline_date": "2025-01-01",
        "interpretation": "Moderate drift detected",
        "bins_used": 10,
        "top_contributing_bins": [
            {"range": "0.0-0.1", "psi": 0.05},
            {"range": "0.4-0.5", "psi": 0.04}
        ],
        "trend": "decreasing"
    }
}
```

---

#### 4.4.4 Error Handling in Checks

**Error Hierarchy:**

```
CheckError (Base)
├── ConfigurationError
│   ├── InvalidThresholdError
│   ├── MissingColumnError
│   └── InvalidFilterError
├── ExecutionError
│   ├── DataTypeError
│   ├── CalculationError
│   └── InsufficientDataError
└── ValidationError
    ├── ThresholdViolationError
    └── ConstraintViolationError
```

**Error Handling Strategy:**

1. **Configuration Errors**: Fail fast during initialization
2. **Column-Level Errors**: Log error, continue with other columns
3. **Check-Level Errors**: Log error, continue with other checks
4. **Critical Errors**: Stop execution, return partial results

**Error Result Format:**

```python
{
    "check_type": "completeness",
    "column": "carbon_em",
    "date": "2025-06-15",
    "status": "ERROR",
    "severity": "ERROR",
    "error_type": "DataTypeError",
    "error_message": "Cannot calculate null percentage: column contains non-numeric types",
    "error_context": {
        "column_dtype": "object",
        "expected_dtype": "numeric",
        "sample_values": ["N/A", "Unknown", 123.45]
    },
    "timestamp": "2025-06-15T10:30:00Z"
}
```

#### 4.4.5 Standard Result Schema

All checks must return results as a pandas DataFrame with the following schema:

**Required Fields:**

| Field | Type | Description |
|-------|------|-------------|
| check_type | str | Type of check (e.g., "completeness") |
| column | str | Column name |
| date | str | Date of measurement (ISO format) |
| metric_value | float | Primary metric value |
| status | str | "PASS" \| "WARNING" \| "FAIL" \| "ERROR" |
| severity | str | "INFO" \| "WARNING" \| "CRITICAL" \| "ERROR" |
| timestamp | str | ISO 8601 timestamp of check execution |

**Optional Fields:**

| Field | Type | Description |
|-------|------|-------------|
| column_alias | str | Alias for filtered column |
| filter_applied | str | Filter condition applied |
| description | str | Check description |
| thresholds | dict | Configured thresholds |
| exceeded_threshold | float | Threshold that was exceeded (if any) |
| additional_metrics | dict | Check-specific additional data |
| error_type | str | Error type (if status="ERROR") |
| error_message | str | Error message (if status="ERROR") |
| compare_to | str | Comparison period reference |

**Example DataFrame Output:**

```python
   check_type    column        date  metric_value  status   severity  \
0  completeness  carbon_em  2025-06-15        0.032  WARNING  WARNING   
1  completeness  water_em   2025-06-15        0.008     PASS     INFO   
2  turnover      fmr_ticker 2025-06-15        0.120  WARNING  WARNING   

                     timestamp  
0  2025-06-15T10:30:00.123456  
1  2025-06-15T10:30:01.234567  
2  2025-06-15T10:30:02.345678  
```

### 4.5 Output Manager

The Output Manager is responsible for formatting check results into various output formats and managing result distribution.

#### 4.5.1 Core Responsibilities

1. **Format Conversion**: Convert check results to requested formats
2. **File Management**: Write output files to specified locations
3. **Report Generation**: Create comprehensive reports
4. **Result Distribution**: Coordinate with optional alerting plugins

#### 4.5.2 Supported Output Formats

**1. JSON Format**

Structure:
```json
{
  "metadata": {
    "dq_check_name": "Production Data Validation Q4 2025",
    "execution_timestamp": "2025-06-15T10:30:00Z",
    "framework_version": "2.0.0",
    "start_date": "2025-06-01",
    "end_date": "2025-06-15",
    "total_checks": 25,
    "total_columns": 12
  },
  "summary": {
    "passed": 18,
    "warnings": 5,
    "failed": 2,
    "errors": 0,
    "pass_rate": 72.0
  },
  "check_results": [
    {
      "check_type": "completeness",
      "column": "carbon_em",
      "date": "2025-06-15",
      "metric_value": 0.032,
      "status": "WARNING",
      "severity": "WARNING",
      "thresholds": {
        "absolute_critical": 0.05,
        "absolute_warning": 0.02
      },
      "additional_metrics": {
        "null_count": 320,
        "total_count": 10000
      }
    }
  ],
  "check_summary_by_type": {
    "completeness": {"passed": 3, "warnings": 2, "failed": 0},
    "turnover": {"passed": 5, "warnings": 1, "failed": 1}
  }
}
```

**2. HTML Format**

Features:
- Responsive design
- Interactive sorting and filtering
- Collapsible sections
- Color-coded status indicators
- Charts and visualizations
- Export buttons (CSV, JSON)

Sections:
1. Executive Summary
2. Check Summary by Type
3. Failed Checks Detail
4. Warning Checks Detail
5. All Checks (optional)
6. Trend Visualizations
7. Configuration Reference

**3. CSV Format**

Flat file with all check results:
```csv
check_type,column,date,metric_value,status,severity,threshold_critical,threshold_warning,description
completeness,carbon_em,2025-06-15,0.032,WARNING,WARNING,0.05,0.02,Carbon emissions completeness
turnover,fmr_ticker,2025-06-15,0.120,WARNING,WARNING,0.15,0.10,Ticker turnover validation
```

**4. DataFrame Format**

Returns pandas DataFrame object for programmatic use:
```python
df_results = dq_framework.get_results(format='dataframe')
# Users can then:
# - Filter: df_results[df_results['status'] == 'FAIL']
# - Aggregate: df_results.groupby('check_type')['status'].value_counts()
# - Export: df_results.to_excel('results.xlsx')
```

#### 4.5.3 Output Configuration

```yaml
output:
  formats:
    - json
    - html
    - csv
  destination:
    type: local  # local | s3 | azure | gcs
    path: "/path/to/output"
  file_naming:
    prefix: "dq_report"
    include_timestamp: true
    include_check_name: true
    # Result: dq_report_Production_Data_Validation_20250615_103000.json
  options:
    include_passed_checks: true
    include_configuration: true
    compress: false
    pretty_print: true  # For JSON
```

#### 4.5.4 Output Manager Interface

```python
from typing import List, Dict, Any, Union
import pandas as pd

class OutputManager:
    """
    Manages formatting and distribution of check results.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Output Manager.
        
        Args:
            config: Output configuration from YAML
        """
        self.config = config
        self.formatters = self._load_formatters()
        self.logger = self._setup_logger()
    
    def generate_outputs(
        self, 
        results: pd.DataFrame,
        metadata: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Generate all configured output formats.
        
        Args:
            results: DataFrame containing check results
            metadata: Metadata about the DQ run
        
        Returns:
            Dict mapping format to file path
        """
        output_files = {}
        
        for format_type in self.config['formats']:
            formatter = self.formatters[format_type]
            file_path = formatter.generate(results, metadata)
            output_files[format_type] = file_path
            
            self.logger.info(
                f"Generated {format_type} output: {file_path}"
            )
        
        return output_files
    
    def generate_json(
        self, 
        results: pd.DataFrame,
        metadata: Dict[str, Any]
    ) -> str:
        """Generate JSON format output"""
        pass
    
    def generate_html(
        self, 
        results: pd.DataFrame,
        metadata: Dict[str, Any]
    ) -> str:
        """Generate HTML format output"""
        pass
    
    def generate_csv(
        self, 
        results: pd.DataFrame,
        metadata: Dict[str, Any]
    ) -> str:
        """Generate CSV format output"""
        pass
    
    def get_dataframe(
        self, 
        results: pd.DataFrame
    ) -> pd.DataFrame:
        """Return results as DataFrame"""
        return results
    
    def get_summary_statistics(
        self, 
        results: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Calculate summary statistics from results.
        
        Returns:
            Dict with summary stats (passed, failed, warnings, etc.)
        """
        pass
```

### 4.6 Alerting System (Optional)

The alerting system is designed as an optional plugin architecture, allowing users to choose whether and how to distribute results.

#### 4.6.1 Plugin Architecture

```python
from abc import ABC, abstractmethod

class AlertingPlugin(ABC):
    """
    Base class for alerting plugins.
    """
    
    @abstractmethod
    def send_alert(
        self, 
        results: pd.DataFrame,
        metadata: Dict[str, Any],
        summary: Dict[str, Any]
    ) -> bool:
        """
        Send alert through this channel.
        
        Returns:
            bool: True if successful, False otherwise
        """
        pass
```

#### 4.6.2 Email Alerting Plugin

**Configuration:**
```yaml
output:
  alerting:
    enabled: true
    plugins:
      - type: email
        config:
          smtp_server: ${SMTP_SERVER}
          smtp_port: 587
          use_tls: true
          username: ${SMTP_USERNAME}
          password: ${SMTP_PASSWORD}
          from_address: "dq-alerts@company.com"
          to_addresses:
            - "data-team@company.com"
          cc_addresses: []
          subject_template: "[DQ Alert] {dq_check_name} - {status}"
          report_type: summary  # summary | detailed | both
          trigger_on:
            - FAIL
            - WARNING  # Optional
          include_attachments: true
          attachment_formats: [json, csv]
```

**Email Template Specifications:**

Summary Report:
- Executive summary with key metrics
- Failed checks table
- Warning checks table (optional)
- Quick links to detailed report
- Status visualization (charts)

Detailed Report:
- All information from summary
- Complete check results
- Threshold details
- Filter conditions applied
- Trend analysis
- Configuration reference

#### 4.6.3 Other Alerting Plugins (Future)

**Slack Plugin:**
```yaml
- type: slack
  config:
    webhook_url: ${SLACK_WEBHOOK}
    channel: "#data-quality"
    mention_on_failure: ["@data-team"]
```

**Custom Plugin:**
```python
from data_quality.alerting import AlertingPlugin, register_plugin

@register_plugin('my_custom_alert')
class MyCustomAlertPlugin(AlertingPlugin):
    def send_alert(self, results, metadata, summary):
        # Custom implementation
        pass
```

#### 4.6.4 Exit Code Management

The framework should return appropriate exit codes for CI/CD integration:

```python
def determine_exit_code(
    results: pd.DataFrame, 
    config: Dict[str, Any]
) -> int:
    """
    Determine system exit code based on check results.
    
    Args:
        results: Check results DataFrame
        config: Exit code configuration
    
    Returns:
        int: 0 for success, 1 for failure
    """
    exit_on_critical = config.get('exit_on_critical', True)
    exit_on_warning = config.get('exit_on_warning', False)
    
    has_critical = (results['severity'] == 'CRITICAL').any()
    has_warning = (results['severity'] == 'WARNING').any()
    
    if exit_on_critical and has_critical:
        return 1
    
    if exit_on_warning and has_warning:
        return 1
    
    return 0
```

**Configuration:**
```yaml
output:
  exit_code:
    exit_on_critical: true
    exit_on_warning: false
    exit_on_error: true
```

### 4.7 Logging Infrastructure

#### 4.7.1 Logging Requirements

**Structured Logging Schema:**
```json
{
  "timestamp": "2025-06-15T10:30:00.123456Z",
  "level": "INFO",
  "component": "CheckManager",
  "event": "check_execution_complete",
  "message": "Completeness check executed successfully",
  "context": {
    "check_type": "completeness",
    "columns_checked": 5,
    "duration_ms": 1234,
    "status": "completed"
  },
  "correlation_id": "uuid-1234-5678"
}
```

#### 4.7.2 Log Levels and Usage

| Level | Usage | Example |
|-------|-------|---------|
| DEBUG | Detailed diagnostic info | "Applying filter: universe == 'US'" |
| INFO | General progress updates | "Executing completeness check" |
| WARNING | Non-critical issues | "No previous period data for delta calculation" |
| ERROR | Error conditions | "Failed to connect to Oracle database" |
| CRITICAL | Critical system failures | "Configuration file not found" |

#### 4.7.3 Logging Configuration

```yaml
logging:
  level: INFO
  format: json  # json | text
  outputs:
    - type: console
      level: INFO
      format: text
    - type: file
      level: DEBUG
      format: json
      path: "/var/log/dq-framework/dq_{timestamp}.log"
      rotation: daily
      retention_days: 30
      max_size_mb: 100
  include_context: true
  correlation_id: auto  # auto | manual
```

#### 4.7.4 Key Logging Events

**Data Source Operations:**
- Connection initialization
- Connection validation
- Data retrieval start/end
- Query execution
- Retry attempts
- Connection errors
- Data statistics (rows retrieved, columns, etc.)

**Check Execution:**
- Check initialization
- Check execution start/end
- Filter application
- Threshold evaluation
- Column-level errors
- Check completion
- Performance metrics

**Output Generation:**
- Output format generation start/end
- File writing
- Alert sending
- Error handling

---

## 5. Package Structure

```
data-quality-framework/
├── src/
│   └── data_quality/
│       ├── __init__.py
│       ├── version.py
│       │
│       ├── core/
│       │   ├── __init__.py
│       │   ├── framework.py          # Main DQ orchestration
│       │   ├── config_loader.py      # YAML loading & validation
│       │   ├── config_schema.py      # Pydantic schemas
│       │   └── exceptions.py         # Custom exceptions
│       │
│       ├── connectors/
│       │   ├── __init__.py
│       │   ├── base.py               # Base DataConnector class
│       │   ├── oracle.py             # Oracle connector
│       │   ├── snowflake.py          # Snowflake connector
│       │   ├── csv.py                # CSV connector
│       │   └── registry.py           # Connector registration
│       │
│       ├── managers/
│       │   ├── __init__.py
│       │   ├── data_source_manager.py    # Data retrieval orchestration
│       │   ├── check_manager.py          # Check execution orchestration
│       │   └── output_manager.py         # Output formatting & distribution
│       │
│       ├── checks/
│       │   ├── __init__.py
│       │   ├── base.py               # Base check class
│       │   ├── completeness.py
│       │   ├── turnover.py
│       │   ├── uniqueness.py
│       │   ├── value_spike.py
│       │   ├── frequency.py
│       │   ├── correlation.py
│       │   ├── range.py
│       │   ├── statistical.py
│       │   ├── distribution.py
│       │   └── drift.py
│       │
│       ├── formatters/
│       │   ├── __init__.py
│       │   ├── json_formatter.py
│       │   ├── html_formatter.py
│       │   ├── csv_formatter.py
│       │   └── templates/
│       │       ├── summary_report.html
│       │       └── detailed_report.html
│       │
│       ├── alerting/
│       │   ├── __init__.py
│       │   ├── base.py               # Base alerting plugin
│       │   ├── email_plugin.py
│       │   └── registry.py           # Plugin registration
│       │
│       └── utils/
│           ├── __init__.py
│           ├── logger.py             # Logging configuration
│           ├── validators.py         # Validation utilities
│           ├── helpers.py            # General utilities
│           └── constants.py          # Framework constants
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_connectors.py
│   │   ├── test_checks.py
│   │   ├── test_managers.py
│   │   └── test_formatters.py
│   ├── integration/
│   │   ├── test_end_to_end.py
│   │   └── test_real_sources.py
│   └── fixtures/
│       ├── sample_data.csv
│       └── sample_configs/
│
├── docs/
│   ├── index.md
│   ├── getting_started.md
│   ├── user_guide/
│   │   ├── installation.md
│   │   ├── configuration.md
│   │   ├── check_reference.md
│   │   └── output_formats.md
│   ├── developer_guide/
│   │   ├── custom_connectors.md
│   │   ├── custom_checks.md
│   │   ├── custom_alerting.md
│   │   └── contributing.md
│   ├── api_reference/
│   │   ├── core.md
│   │   ├── connectors.md
│   │   ├── checks.md
│   │   └── managers.md
│   └── examples/
│       ├── basic_usage.md
│       ├── advanced_filtering.md
│       └── ci_cd_integration.md
│
├── examples/
│   ├── configs/
│   │   ├── basic_config.yaml
│   │   ├── advanced_config.yaml
│   │   └── production_config.yaml
│   ├── notebooks/
│   │   ├── getting_started.ipynb
│   │   └── custom_connectors.ipynb
│   └── scripts/
│       ├── run_checks.py
│       └── schedule_checks.sh
│
├── .github/
│   └── workflows/
│       ├── tests.yml
│       ├── publish.yml
│       └── docs.yml
│
├── setup.py
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── README.md
├── LICENSE
├── CHANGELOG.md
├── CONTRIBUTING.md
└── .gitignore
```

---

## 6. Non-Functional Requirements

### 6.1 Performance

| Metric | Requirement |
|--------|-------------|
| Dataset Size | Support up to 50M rows efficiently |
| Check Execution Time | < 10 minutes for typical workloads (10M rows, 20 checks) |
| Memory Usage | < 4GB for 10M row dataset |
| Startup Time | < 5 seconds |
| Configuration Load | < 1 second |

**Optimization Strategies:**
- Chunked data processing for large datasets
- Lazy evaluation where possible
- Parallel check execution (optional)
- Efficient pandas operations (vectorization)
- Memory-mapped file support for very large CSVs

### 6.2 Reliability

| Metric | Target |
|--------|--------|
| Successful Execution Rate | 99.9% for valid configurations |
| Mean Time Between Failures | > 1000 hours |
| Recovery Time | < 5 seconds (automatic retry) |
| Data Loss Probability | 0% (all results logged) |

**Reliability Features:**
- Comprehensive error handling
- Automatic retry with exponential backoff
- Graceful degradation
- Transaction-like result recording
- Checkpoint/resume capability for long-running checks

### 6.3 Maintainability

| Metric | Target |
|--------|--------|
| Code Coverage | > 85% |
| Documentation Coverage | 100% (all public APIs) |
| Cyclomatic Complexity | < 10 per function |
| Code Duplication | < 5% |

**Maintainability Practices:**
- Comprehensive unit tests
- Integration test suite
- Type hints throughout codebase
- Docstrings for all classes and functions
- Automated code quality checks (flake8, mypy, black)
- Clear contribution guidelines
- Semantic versioning

### 6.4 Usability

**Ease of Installation:**
- Single command installation: `pip install data-quality-framework`
- Minimal dependencies
- Clear dependency resolution

**Ease of Configuration:**
- Intuitive YAML structure
- Schema validation with helpful error messages
- Example configurations provided
- Configuration generator tool (future)

**Ease of Integration:**
- Works with existing pandas workflows
- CLI and programmatic API
- Standard exit codes for CI/CD
- Docker support

**Documentation:**
- Quick start guide (< 5 minutes to first check)
- Comprehensive user guide
- API reference
- Examples for common scenarios
- Video tutorials (future)

### 6.5 Security

**Credential Management:**
- No hardcoded credentials
- Environment variable support
- Secret management integration (AWS Secrets Manager, Azure Key Vault)
- Credential encryption at rest

**SQL Injection Prevention:**
- Parameterized queries only
- No dynamic SQL construction from user input
- Query validation

**Data Privacy:**
- No data transmission outside configured destinations
- Configurable data masking in outputs
- PII detection and redaction (optional)
- Audit logging of data access

**Secure Communication:**
- TLS/SSL support for all database connections
- SMTP TLS for email
- Certificate validation

### 6.6 Scalability

**Horizontal Scaling:**
- Stateless design
- Support for distributed execution (future)
- Cloud-native compatibility

**Vertical Scaling:**
- Efficient memory usage
- Multi-core utilization (parallel checks)
- Streaming processing for very large datasets

### 6.7 Compatibility

**Python Versions:**
- Python 3.8, 3.9, 3.10, 3.11, 3.12

**Operating Systems:**
- Linux (primary)
- macOS
- Windows

**Database Versions:**
- Oracle 11g+
- Snowflake (all versions)

---

## 7. Dependencies

### 7.1 Core Dependencies

```
pandas>=1.5.0,<3.0.0
pyyaml>=6.0
pydantic>=2.0.0,<3.0.0
python-dotenv>=1.0.0
jinja2>=3.1.0
```

### 7.2 Database Connectors

```
# Oracle
oracledb>=1.0.0

# Snowflake
snowflake-connector-python>=3.0.0
snowflake-snowpark-python>=1.0.0
```

### 7.3 Statistical Analysis

```
scipy>=1.10.0
numpy>=1.24.0
```

### 7.4 Optional Dependencies

```
# Email alerting
python-multipart>=0.0.6

# Cloud storage
boto3>=1.26.0  # AWS S3
azure-storage-blob>=12.0.0  # Azure Blob
google-cloud-storage>=2.0.0  # GCS

# Visualization
plotly>=5.0.0
matplotlib>=3.7.0
```

### 7.5 Development Dependencies

```
# Testing
pytest>=7.0.0
pytest-cov>=4.0.0
pytest-mock>=3.10.0
pytest-asyncio>=0.21.0

# Code Quality
black>=23.0.0
flake8>=6.0.0
mypy>=1.0.0
isort>=5.12.0
pylint>=2.17.0

# Documentation
mkdocs>=1.4.0
mkdocs-material>=9.0.0
mkdocstrings[python]>=0.20.0

# Build
build>=0.10.0
twine>=4.0.0
```

### 7.6 Dependency Management

**Strategy:**
- Use version ranges to allow flexibility
- Pin exact versions in development
- Regular security updates
- Automated dependency scanning (Dependabot)

**Compatibility Testing:**
- Test against minimum and maximum supported versions
- Continuous integration testing across version matrix

---

## 8. Deliverables

### 8.1 Package Deliverables

| Deliverable | Description | Format |
|-------------|-------------|--------|
| Python Package | Installable via pip | Wheel + Source Distribution |
| Source Code | Complete source code | Git repository |
| Documentation | Complete user and API docs | HTML (MkDocs) |
| Examples | Configuration examples and scripts | YAML, Python, Shell |
| Tests | Comprehensive test suite | pytest |

### 8.2 Documentation Deliverables

| Document | Content | Target Audience |
|----------|---------|-----------------|
| README | Quick start, installation, basic usage | All users |
| User Guide | Detailed configuration, all check types | End users |
| API Reference | All public APIs with examples | Developers |
| Developer Guide | Custom connectors, checks, plugins | Advanced users |
| Examples | Common use cases with code | All users |
| CHANGELOG | Version history and changes | All users |

### 8.3 Quality Deliverables

| Deliverable | Metric | Target |
|-------------|--------|--------|
| Test Coverage Report | Line and branch coverage | > 85% |
| Performance Benchmarks | Execution time vs data size | Documented |
| Security Audit Report | Vulnerability scan results | Zero critical |
| Code Quality Report | Complexity, duplication | Pass all checks |

---

## 9. Success Criteria

### 9.1 Technical Success Criteria

- [ ] Package successfully installable via pip
- [ ] All 10 check types implemented and functional
- [ ] All 3 native connectors working (Oracle, Snowflake, CSV)
- [ ] Custom connector interface documented and tested
- [ ] All output formats generating correctly (JSON, HTML, CSV, DataFrame)
- [ ] Test coverage > 85%
- [ ] Zero critical security vulnerabilities
- [ ] Documentation complete and published
- [ ] Performance requirements met
- [ ] CI/CD pipeline operational

### 9.2 Functional Success Criteria

- [ ] Can validate 10M+ row dataset in < 10 minutes
- [ ] Column-wise check configuration working as specified
- [ ] Filter conditions applied correctly
- [ ] Threshold evaluation accurate (absolute and delta)
- [ ] Error handling graceful at all levels
- [ ] Results standardized across all check types
- [ ] Logging comprehensive and structured
- [ ] Exit codes appropriate for CI/CD integration

### 9.3 Usability Success Criteria

- [ ] User can run first check in < 5 minutes
- [ ] Configuration errors have clear, actionable messages
- [ ] Examples cover common use cases
- [ ] API intuitive for pandas users
- [ ] No undocumented public APIs
- [ ] Positive user feedback (> 4/5 rating)

### 9.4 Business Success Criteria

- [ ] Reduces manual QA time by 80%
- [ ] Catches data quality issues in development
- [ ] Integrates into existing workflows with minimal friction
- [ ] Adopted by at least 5 teams within organization
- [ ] Active community engagement (GitHub stars, issues, PRs)

---

## 10. Timeline and Milestones

### Phase 1: Foundation (Weeks 1-2)

**Week 1:**
- Core framework structure
- Configuration loader with Pydantic schemas
- Base classes (DataConnector, BaseCheck)
- Logging infrastructure
- Exception hierarchy

**Week 2:**
- Data Source Manager
- Check Manager skeleton
- Output Manager skeleton
- Unit test framework setup

**Deliverables:**
- Working core framework
- Configuration validation
- Basic logging

### Phase 2: Data Connectors (Week 3)

**Week 3:**
- Oracle connector implementation
- Snowflake connector implementation
- CSV connector implementation
- Connector registry
- Connection error handling and retry logic
- Connector unit tests

**Deliverables:**
- 3 working connectors
- Custom connector documentation
- Connection error handling

### Phase 3: Check Implementations (Weeks 4-6)

**Week 4:**
- Completeness check
- Turnover check
- Uniqueness check
- Value spike check
- Check unit tests

**Week 5:**
- Frequency check
- Correlation check
- Range check
- Statistical measures check
- Check unit tests

**Week 6:**
- Distribution change check
- Drift check
- Column-wise configuration parsing
- Filter condition support
- Check integration tests

**Deliverables:**
- All 10 check types implemented
- Column-wise configuration working
- Filter support functional
- Comprehensive check tests

### Phase 4: Output & Alerting (Week 7)

**Week 7:**
- JSON formatter
- HTML formatter with templates
- CSV formatter
- Email alerting plugin
- Output Manager complete
- Exit code logic

**Deliverables:**
- All output formats working
- Optional email alerting
- Professional HTML reports

### Phase 5: Testing & Documentation (Week 8)

**Week 8:**
- Integration test suite
- End-to-end tests
- Performance benchmarking
- Documentation writing (user guide, API reference)
- Example configurations and notebooks
- README and quick start guide

**Deliverables:**
- Test coverage > 85%
- Complete documentation
- Examples and tutorials

### Phase 6: Packaging & Release (Week 9)

**Week 9:**
- Package configuration (setup.py, pyproject.toml)
- CI/CD pipeline setup
- PyPI test deployment
- Security scan
- Final QA
- PyPI production release
- Release announcement

**Deliverables:**
- v1.0.0 released to PyPI
- Documentation published
- Release notes
- GitHub release

### Ongoing: Maintenance & Enhancement

**Post-Release:**
- Bug fixes
- Community support
- Feature requests
- Documentation updates
- Version updates

---

## 11. Configuration Examples

### 11.1 Basic Configuration

```yaml
# basic_config.yaml
source:
  type: csv
  csv:
    file_path: /data/sample_data.csv
    date_column: effective_date

metadata:
  dq_check_name: "Basic Data Validation"
  date_column: effective_date
  id_column: entity_id

checks:
  completeness:
    revenue:
      thresholds:
        absolute_critical: 0.05
      description: "Revenue completeness check"
    
    customer_count:
      thresholds:
        absolute_critical: 0.03
      description: "Customer count completeness"

output:
  formats: [json, html]
  destination:
    type: local
    path: ./output
```

### 11.2 Advanced Configuration

```yaml
# advanced_config.yaml
source:
  type: oracle
  oracle:
    host: ${ORACLE_HOST}
    port: 1521
    service_name: ${ORACLE_SERVICE}
    username: ${ORACLE_USER}
    password: ${ORACLE_PASSWORD}
    sql: |
      SELECT * FROM esg_data
      WHERE effective_date >= :start_date 
        AND effective_date <= :end_date
  connection_retry:
    enabled: true
    max_attempts: 3
    backoff_factor: 2.0

metadata:
  dq_check_name: "ESG Data Production Validation"
  date_column: effective_date
  id_column: bus_ent_id
  description: "Comprehensive ESG data quality validation"

checks:
  completeness:
    carbon_em:
      thresholds:
        absolute_critical: 0.05
        absolute_warning: 0.02
        delta_critical: 0.03
        delta_warning: 0.01
      filter_condition: "universe == 'US' AND env > 0"
      column_alias: "carbon_em_us_positive"
      description: "Carbon emissions for US universe with positive env scores"
      compare_to: previous
    
    water_em:
      thresholds:
        absolute_critical: 0.10
        absolute_warning: 0.05
      description: "Water emissions completeness"

  turnover:
    fmr_ticker:
      thresholds:
        absolute_critical: 0.15
        absolute_warning: 0.10
        delta_critical: 0.05
      description: "Ticker turnover rate monitoring"
    
    cusip:
      thresholds:
        absolute_critical: 0.12
      filter_condition: "universe == 'Global'"
      description: "CUSIP turnover for global universe"

  uniqueness:
    cusip:
      thresholds:
        absolute_critical: 0
      description: "CUSIP must be unique per date"
    
    bus_ent_eff_dim_key:
      thresholds:
        absolute_critical: 0
        absolute_warning: 5
      filter_condition: "effective_date == effective_date.max()"
      description: "Business entity key uniqueness for latest date"

  value_spike:
    carbon_em:
      thresholds:
        absolute_critical: 10.0
        absolute_warning: 5.0
      description: "Detect 10x spikes in carbon emissions"
    
    water_em:
      thresholds:
        absolute_critical: 15.0
      filter_condition: "env > 5"
      description: "Water emissions spike for high env scores"

  frequency:
    universe:
      thresholds:
        absolute_critical: 0.10
        absolute_warning: 0.05
      description: "Universe distribution stability"
    
    coverage:
      thresholds:
        absolute_critical: 0.15
      filter_condition: "env > 0"
      description: "Coverage distribution for positive env"

  correlation:
    rnk:
      thresholds:
        absolute_critical: 0.80
        absolute_warning: 0.90
      correlation_type: temporal
      description: "Rank stability across periods"

  range:
    env:
      thresholds:
        min_value: 0
        max_value: 10
      description: "Env score must be 0-10"
    
    carbon_em:
      thresholds:
        min_value: 0
        max_value: 1000000
      filter_condition: "universe == 'US'"
      description: "Carbon emissions range for US"

  statistical:
    carbon_em:
      measures: [mean, median, stdev]
      thresholds:
        mean:
          absolute_critical: [0, 500000]
          absolute_warning: [10000, 400000]
          delta_critical: 0.20
        stdev:
          absolute_critical: [0, 100000]
          delta_critical: 0.30
      description: "Carbon emissions statistical monitoring"

  distribution:
    carbon_em:
      thresholds:
        absolute_critical: 0.05
        absolute_warning: 0.10
      description: "Carbon emissions distribution stability"

  drift:
    generat_thermal_coal_pct:
      thresholds:
        absolute_critical: 0.25
        absolute_warning: 0.10
      drift_method: psi
      baseline_period: first
      description: "Coal generation percentage drift"

output:
  formats: [json, html, csv, dataframe]
  destination:
    type: local
    path: /data/dq_reports
  file_naming:
    prefix: esg_dq_report
    include_timestamp: true
    include_check_name: true
  options:
    include_passed_checks: true
    include_configuration: true
    compress: false
    pretty_print: true
  exit_code:
    exit_on_critical: true
    exit_on_warning: false
    exit_on_error: true
  alerting:
    enabled: true
    plugins:
      - type: email
        config:
          smtp_server: ${SMTP_SERVER}
          smtp_port: 587
          use_tls: true
          username: ${SMTP_USERNAME}
          password: ${SMTP_PASSWORD}
          from_address: dq-alerts@company.com
          to_addresses:
            - data-team@company.com
            - esg-team@company.com
          subject_template: "[DQ Alert] {dq_check_name} - {status}"
          report_type: summary
          trigger_on:
            - FAIL
            - WARNING
          include_attachments: true
          attachment_formats: [json, csv]

execution:
  parallel_enabled: true
  max_workers: 4
  timeout_per_check: 300

logging:
  level: INFO
  format: json
  outputs:
    - type: console
      level: INFO
      format: text
    - type: file
      level: DEBUG
      format: json
      path: /var/log/dq-framework/dq_{timestamp}.log
      rotation: daily
      retention_days: 30
```

---

## 12. API Usage Examples

### 12.1 Basic Programmatic Usage

```python
from data_quality import DataQualityFramework

# Initialize framework
dq = DataQualityFramework(config_path='config.yaml')

# Run all checks
results = dq.run_checks(
    start_date='2025-06-01',
    end_date='2025-06-15'
)

# Get results in different formats
json_report = results.to_json()
html_report = results.to_html()
df_results = results.to_dataframe()

# Get summary
summary = results.get_summary()
print(f"Passed: {summary['passed']}")
print(f"Failed: {summary['failed']}")
print(f"Warnings: {summary['warnings']}")

# Check exit code
exit_code = results.get_exit_code()
sys.exit(exit_code)
```

### 12.2 Advanced Programmatic Usage

```python
from data_quality import DataQualityFramework
from data_quality.connectors import OracleConnector
from data_quality.checks import CompletenessCheck
import pandas as pd

# Custom data source
df = pd.read_csv('custom_data.csv')

# Initialize with DataFrame
dq = DataQualityFramework(
    df=df,
    metadata={
        'dq_check_name': 'Custom Check',
        'date_column': 'date',
        'id_column': 'entity_id'
    }
)

# Run specific check programmatically
completeness_config = {
    'carbon_em': {
        'thresholds': {
            'absolute_critical': 0.05
        },
        'description': 'Carbon emissions completeness'
    }
}

results = dq.run_check('completeness', completeness_config)

# Filter results
failed_checks = results[results['status'] == 'FAIL']
print(failed_checks)

# Export
results.to_csv('results.csv')
results.to_excel('results.xlsx')
```

### 12.3 CLI Usage

```bash
# Basic usage
dq-check --config config.yaml --start-date 2025-06-01 --end-date 2025-06-15

# With output format specification
dq-check --config config.yaml \
         --start-date 2025-06-01 \
         --end-date 2025-06-15 \
         --output-format json html csv \
         --output-dir ./reports

# Override configuration
dq-check --config config.yaml \
         --start-date 2025-06-01 \
         --end-date 2025-06-15 \
         --log-level DEBUG \
         --parallel \
         --no-email

# CI/CD usage
#!/bin/bash
dq-check --config config.yaml \
         --start-date $(date -d '7 days ago' +%Y-%m-%d) \
         --end-date $(date +%Y-%m-%d)

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "Data quality checks failed!"
    exit 1
fi
echo "Data quality checks passed!"
```

---

## Appendices

### Appendix A: Glossary

| Term | Definition |
|------|------------|
| Absolute Threshold | Maximum acceptable value in a single time period |
| Alerting Plugin | Modular component for sending notifications |
| Check Manager | Component that orchestrates check execution |
| Column-Wise Configuration | Check configuration defined per column |
| Data Connector | Component that retrieves data from a source |
| Data Source Manager | Component that manages data source connections |
| Delta Threshold | Maximum acceptable change between time periods |
| Drift | Gradual change in data distribution over time |
| Filter Condition | Pandas query string to subset data before checks |
| Output Manager | Component that formats and exports results |
| PSI | Population Stability Index - drift metric |
| Turnover | Rate of addition/removal of unique identifiers |

### Appendix B: References

- Pandas Documentation: https://pandas.pydata.org/docs/
- Pydantic Documentation: https://docs.pydantic.dev/
- Oracle Python Driver: https://python-oracledb.readthedocs.io/
- Snowflake Python Connector: https://docs.snowflake.com/en/user-guide/python-connector.html
- Data Quality Patterns: Industry best practices

### Appendix C: Change Log

| Version | Date | Changes |
|---------|------|---------|
| 2.0 | 2025-11-19 | Major revision: Column-wise configuration, flexible output, optional alerting |
| 1.0 | 2025-11-01 | Initial requirements document |

---

**End of Document**

---

**Approval Signatures:**

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Technical Lead | | | |
| Product Owner | | | |
| QA Lead | | | |
| Security Officer | | | |


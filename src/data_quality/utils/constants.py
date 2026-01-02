"""Constants and enumerations for the Data Quality Framework."""

from enum import Enum
from typing import Dict, Any


class CheckStatus(str, Enum):
    """Status of a data quality check."""
    PASS = "PASS"  # nosec B105
    WARNING = "WARNING"
    FAIL = "FAIL"
    ERROR = "ERROR"


class Severity(str, Enum):
    """Severity level of a check result."""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"


class OutputFormat(str, Enum):
    """Supported output formats."""
    JSON = "json"
    HTML = "html"
    CSV = "csv"
    DATAFRAME = "dataframe"


class ConnectorType(str, Enum):
    """Supported data connector types."""
    ORACLE = "oracle"
    SNOWFLAKE = "snowflake"
    CSV = "csv"
    CUSTOM = "custom"


class CheckType(str, Enum):
    """Supported check types."""
    COMPLETENESS = "completeness"
    TURNOVER = "turnover"
    UNIQUENESS = "uniqueness"
    VALUE_SPIKE = "value_spike"
    FREQUENCY = "frequency"
    CORRELATION = "correlation"
    RANGE = "range"
    STATISTICAL = "statistical"
    DISTRIBUTION = "distribution"
    DRIFT = "drift"


class CorrelationType(str, Enum):
    """Types of correlation checks."""
    TEMPORAL = "temporal"
    CROSS_COLUMN = "cross_column"


class DriftMethod(str, Enum):
    """Methods for drift detection."""
    PSI = "psi"
    KS = "ks"
    JENSEN_SHANNON = "jensen_shannon"


# Default threshold values
DEFAULT_THRESHOLDS: Dict[str, Any] = {
    "completeness": {
        "absolute_critical": 0.05,
        "absolute_warning": 0.02,
    },
    "turnover": {
        "absolute_critical": 0.15,
        "absolute_warning": 0.10,
    },
    "uniqueness": {
        "absolute_critical": 0,
    },
    "value_spike": {
        "absolute_critical": 10.0,
        "absolute_warning": 5.0,
    },
    "frequency": {
        "absolute_critical": 0.10,
        "absolute_warning": 0.05,
    },
    "correlation": {
        "absolute_critical": 0.80,
        "absolute_warning": 0.90,
    },
    "distribution": {
        "absolute_critical": 0.05,
        "absolute_warning": 0.10,
    },
    "drift": {
        "absolute_critical": 0.25,
        "absolute_warning": 0.10,
    },
}

# Logging configuration
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
JSON_LOG_FORMAT = {
    "timestamp": "%(asctime)s",
    "level": "%(levelname)s",
    "component": "%(name)s",
    "message": "%(message)s",
}

# Performance limits
MAX_SAMPLE_SIZE = 100  # Max items in sample lists
MAX_QUERY_LENGTH = 200  # Max query length in error messages
DEFAULT_TIMEOUT_MS = 120000  # 2 minutes
MAX_TIMEOUT_MS = 600000  # 10 minutes

# Retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 2.0

# PSI interpretation thresholds
PSI_NO_DRIFT = 0.1
PSI_MODERATE_DRIFT = 0.25

# Correlation strength categories
CORRELATION_WEAK = 0.3
CORRELATION_MODERATE = 0.7
CORRELATION_STRONG = 0.9

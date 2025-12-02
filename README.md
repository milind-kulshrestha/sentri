# Sentri

Sentri is a production-ready, configurable data quality validation framework with 10 check types, multiple data connectors, and flexible output formats.

## Features

- **10 Check Types**: Completeness, Uniqueness, Range, Turnover, Value Spike, Frequency, Correlation, Statistical, Distribution, Drift
- **Multiple Connectors**: CSV, Oracle, Snowflake (extensible)
- **Flexible Configuration**: YAML with environment variable support
- **Multiple Output Formats**: JSON, HTML, CSV, DataFrame
- **Threshold-Based Validation**: Critical, Warning, Pass, Error states
- **Comprehensive Logging**: Text and JSON formats

## Installation

```bash
pip install sentri

# Optional: development extras when working on the project itself
pip install -e ".[dev]"  # Development install
pip install -e ".[all]"  # With all database connectors
```

## Quick Start

### Programmatic Usage

```python
from sentri import DataQualityFramework
from sentri.checks import CompletenessCheck, TurnoverCheck
from sentri.connectors import OracleConnector, SnowflakeConnector

# Example: using DataQualityFramework with a config file
framework = DataQualityFramework(config_path="config.yaml")
results = framework.run_checks(start_date="2025-01-01", end_date="2025-01-31")
```

### Configuration File Usage

```yaml
# config.yaml
source:
  type: csv
  csv:
    file_path: /data/sample.csv
    date_column: effective_date

metadata:
  dq_check_name: "Sample Check"
  date_column: effective_date
  id_column: entity_id

checks:
  completeness:
    value:
      thresholds:
        absolute_critical: 0.05
        absolute_warning: 0.02
      description: "Value completeness"

output:
  formats: [json, html, csv]
  destination: /output
```

## Check Types

| Check Type | Description |
|------------|-------------|
| **Completeness** | Monitor null/missing values |
| **Uniqueness** | Detect duplicate values |
| **Range** | Validate value bounds |
| **Turnover** | Track ID additions/removals |
| **Value Spike** | Detect abnormal value changes |
| **Frequency** | Monitor category distributions |
| **Statistical** | Track mean, std, median, etc. |
| **Correlation** | Validate temporal/cross-column correlation |
| **Distribution** | Detect distribution shifts (KS test) |
| **Drift** | Identify gradual drift (PSI) |

## Project Structure

```
dq_framework/
├── src/data_quality/
│   ├── checks/           # Check implementations
│   ├── connectors/       # Data connectors
│   ├── core/             # Exceptions, config, framework
│   ├── formatters/       # Output formatters
│   ├── managers/         # Check manager
│   └── utils/            # Logger, constants
├── tests/                # Unit tests
├── examples/             # Sample configs and scripts
└── pyproject.toml
```

## Running Tests

```bash
pytest tests/unit/ -v --cov
```

## Thresholds

Each check supports:
- `absolute_critical`: Fails if exceeded
- `absolute_warning`: Warns if exceeded
- `delta_critical`/`delta_warning`: For change-based thresholds

## Output

Results include:
- **Summary**: Total, passed, warnings, failed, pass rate
- **Details**: Check type, column, date, metric value, status

## License

MIT

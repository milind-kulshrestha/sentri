# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sentri is a data quality validation framework with 10 check types, multiple data connectors, and configurable output formats. The package is published as `sentri` on PyPI.

## Common Commands

```bash
# Install for development
pip install -e ".[dev]"

# Run all tests with coverage
pytest tests/ --cov=src/data_quality --cov-report=term-missing

# Run a single test file
pytest tests/unit/test_checks_base.py -v

# Run a specific test
pytest tests/unit/test_checks_base.py::TestBaseCheck::test_method_name -v

# Code quality
black src/ tests/              # Format code
isort src/ tests/              # Sort imports
flake8 src/ tests/             # Lint
mypy src/                      # Type check
bandit -r src/                 # Security scan

# CLI usage
dq-check -c config.yaml --start-date 2025-01-01 --end-date 2025-01-31
```

## Architecture

### Package Structure
- Published package name: `sentri` (import via `from sentri import DataQualityFramework`)
- Internal module: `data_quality` in `src/data_quality/`
- The `src/sentri/` package re-exports from `data_quality` for the public API

### Core Flow
1. **Configuration** (`core/config_loader.py`, `core/config_schema.py`): YAML config parsed and validated via Pydantic schemas (`DQConfig` is the root schema)
2. **Connectors** (`connectors/`): Abstract `DataConnector` base class with implementations (CSV, Oracle, Snowflake). Register new connectors using `@register_connector('name')` decorator
3. **Check Manager** (`managers/check_manager.py`): Orchestrates check execution, supports parallel execution via `ThreadPoolExecutor`
4. **Checks** (`checks/`): 10 check types, all inherit from `BaseCheck`. Each implements `run()` returning a DataFrame
5. **Formatters** (`formatters/`): Output to JSON, HTML, CSV via `OutputManager`

### Check Types
All checks inherit from `BaseCheck` and must implement `run() -> pd.DataFrame`:
- `CompletenessCheck`, `UniquenessCheck`, `RangeCheck`, `TurnoverCheck`, `ValueSpikeCheck`
- `FrequencyCheck`, `CorrelationCheck`, `StatisticalCheck`, `DistributionCheck`, `DriftCheck`

### Key Abstractions
- **BaseCheck** (`checks/base.py`): Provides `_normalize_dataframe()`, `_evaluate_threshold()`, `_apply_filter()`, `_create_result_record()`, `_handle_column_error()`
- **DataConnector** (`connectors/base.py`): Abstract base with `validate_connection()`, `get_data()`, `close()`. Includes retry logic via `retry_with_backoff()`
- **CheckStatus/Severity** (`utils/constants.py`): Enums for PASS/FAIL/WARNING/ERROR states

### Threshold Evaluation
Checks use a consistent threshold model:
- `absolute_critical` / `absolute_warning`: For absolute value thresholds
- `delta_critical` / `delta_warning`: For change-based thresholds
- Status: PASS < WARNING < FAIL < ERROR

### Adding New Components

**New Check:**
```python
# src/data_quality/checks/my_check.py
from data_quality.checks.base import BaseCheck

class MyCheck(BaseCheck):
    def run(self) -> pd.DataFrame:
        # Implementation using self.df, self.date_col, self.id_col, self.check_config
        pass
```
Then add to `checks/__init__.py` and `CheckManager._load_check_registry()`.

**New Connector:**
```python
from data_quality.connectors.base import DataConnector
from data_quality.connectors.registry import register_connector

@register_connector('my_source')
class MyConnector(DataConnector):
    def validate_connection(self) -> bool: ...
    def get_data(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame: ...
    def close(self) -> None: ...
```

## Testing Patterns

- Unit tests in `tests/unit/`, integration tests in `tests/integration/`
- Fixtures in `tests/fixtures/test_fixtures.py`
- Use `pytest-mock` for mocking external dependencies
- Aim for >90% test coverage

## Configuration

YAML configuration with environment variable support. Key sections:
- `source`: Data source configuration (type + connector-specific config)
- `metadata`: `dq_check_name`, `date_column`, `id_column`
- `checks`: Dict of check_type -> column_name -> check config with thresholds
- `output`: Formats (json/html/csv/dataframe), destination, file naming options

"""Tests for configuration loader."""

import os
import tempfile
from pathlib import Path

import pytest

from data_quality.core.config_loader import ConfigLoader, load_config
from data_quality.core.exceptions import ConfigurationError


class TestConfigLoader:
    """Tests for ConfigLoader class."""

    def test_load_valid_yaml(self):
        """Test loading valid YAML configuration."""
        yaml_content = """
source:
  type: csv
  csv:
    file_path: /data/test.csv
    date_column: effective_date

metadata:
  dq_check_name: "Test Check"
  date_column: effective_date
  id_column: entity_id

checks:
  completeness:
    column1:
      thresholds:
        absolute_critical: 0.05

output:
  formats: [json]
  destination: /output
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            config = loader.load()

            assert config.source.type == "csv"
            assert config.metadata.dq_check_name == "Test Check"
            assert "completeness" in config.checks
        finally:
            os.unlink(config_path)

    def test_load_with_environment_variables(self):
        """Test loading config with environment variable substitution."""
        os.environ["TEST_FILE_PATH"] = "/data/env_test.csv"

        yaml_content = """
source:
  type: csv
  csv:
    file_path: ${TEST_FILE_PATH}
    date_column: date

metadata:
  dq_check_name: "Test"
  date_column: date
  id_column: id

checks: {}

output:
  formats: [json]
  destination: /output
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            config = loader.load()

            assert config.source.csv.file_path == "/data/env_test.csv"
        finally:
            os.unlink(config_path)
            del os.environ["TEST_FILE_PATH"]

    def test_missing_env_var_raises_error(self):
        """Test that missing environment variable raises error."""
        yaml_content = """
source:
  type: csv
  csv:
    file_path: ${MISSING_ENV_VAR}
    date_column: date

metadata:
  dq_check_name: "Test"
  date_column: date
  id_column: id

checks: {}

output:
  formats: [json]
  destination: /output
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError) as exc_info:
                loader.load()
            assert "MISSING_ENV_VAR" in str(exc_info.value)
        finally:
            os.unlink(config_path)

    def test_file_not_found_error(self):
        """Test that missing file raises error."""
        loader = ConfigLoader("/nonexistent/path/config.yaml")
        with pytest.raises(ConfigurationError) as exc_info:
            loader.load()
        assert "not found" in str(exc_info.value).lower()

    def test_invalid_yaml_syntax(self):
        """Test that invalid YAML syntax raises error."""
        yaml_content = """
source:
  type: csv
  csv:
    file_path: [invalid yaml
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError) as exc_info:
                loader.load()
            assert (
                "syntax" in str(exc_info.value).lower()
                or "yaml" in str(exc_info.value).lower()
            )
        finally:
            os.unlink(config_path)

    def test_validation_error_with_details(self):
        """Test that validation errors include helpful details."""
        yaml_content = """
source:
  type: csv
  csv:
    file_path: /data/test.csv
    date_column: date

metadata:
  dq_check_name: "Test"
  date_column: date
  id_column: id

checks: {}

output:
  formats: [invalid_format]
  destination: /output
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError):
                loader.load()
        finally:
            os.unlink(config_path)

    def test_load_from_dict(self):
        """Test loading configuration from dictionary."""
        config_dict = {
            "source": {
                "type": "csv",
                "csv": {"file_path": "/data/test.csv", "date_column": "date"},
            },
            "metadata": {
                "dq_check_name": "Test",
                "date_column": "date",
                "id_column": "id",
            },
            "checks": {},
            "output": {"formats": ["json"], "destination": "/output"},
        }

        loader = ConfigLoader.from_dict(config_dict)
        config = loader.load()

        assert config.source.type == "csv"
        assert config.metadata.dq_check_name == "Test"


class TestLoadConfigFunction:
    """Tests for load_config convenience function."""

    def test_load_from_file(self):
        """Test load_config from file path."""
        yaml_content = """
source:
  type: csv
  csv:
    file_path: /data/test.csv
    date_column: date

metadata:
  dq_check_name: "Test"
  date_column: date
  id_column: id

checks: {}

output:
  formats: [json]
  destination: /output
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = load_config(config_path)
            assert config.source.type == "csv"
        finally:
            os.unlink(config_path)

    def test_load_from_dict(self):
        """Test load_config from dictionary."""
        config_dict = {
            "source": {
                "type": "csv",
                "csv": {"file_path": "/data/test.csv", "date_column": "date"},
            },
            "metadata": {
                "dq_check_name": "Test",
                "date_column": "date",
                "id_column": "id",
            },
            "checks": {},
            "output": {"formats": ["json"], "destination": "/output"},
        }

        config = load_config(config_dict=config_dict)
        assert config.source.type == "csv"


class TestComplexConfigurations:
    """Tests for complex configuration scenarios."""

    def test_multiple_check_types(self):
        """Test configuration with multiple check types."""
        yaml_content = """
source:
  type: csv
  csv:
    file_path: /data/test.csv
    date_column: date

metadata:
  dq_check_name: "Complex Test"
  date_column: date
  id_column: id

checks:
  completeness:
    col1:
      thresholds:
        absolute_critical: 0.05
        absolute_warning: 0.02
      filter_condition: "universe == 'US'"
      column_alias: col1_us
  turnover:
    col2:
      thresholds:
        absolute_critical: 0.15
  uniqueness:
    col3:
      thresholds:
        absolute_critical: 0

output:
  formats: [json, html, csv]
  destination: /output
  file_prefix: complex_report
  include_passed_checks: true
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = load_config(config_path)

            assert "completeness" in config.checks
            assert "turnover" in config.checks
            assert "uniqueness" in config.checks
            assert len(config.output.formats) == 3
        finally:
            os.unlink(config_path)

    def test_oracle_source_config(self):
        """Test Oracle source configuration."""
        yaml_content = """
source:
  type: oracle
  oracle:
    host: localhost
    port: 1521
    service_name: orcl
    username: user
    password: pass
    sql: "SELECT * FROM table"

metadata:
  dq_check_name: "Oracle Test"
  date_column: date
  id_column: id

checks: {}

output:
  formats: [json]
  destination: /output
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = load_config(config_path)

            assert config.source.type == "oracle"
            assert config.source.oracle.host == "localhost"
            assert config.source.oracle.port == 1521
        finally:
            os.unlink(config_path)

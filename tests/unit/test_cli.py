"""Tests for CLI interface."""

import pytest
import tempfile
import os
from data_quality.cli import create_parser, main


class TestCLIParser:
    """Tests for CLI argument parser."""

    def test_parser_creation(self):
        """Test parser is created successfully."""
        parser = create_parser()
        assert parser is not None

    def test_required_arguments(self):
        """Test required arguments."""
        parser = create_parser()

        # Should fail without required args
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_valid_arguments(self):
        """Test parsing valid arguments."""
        parser = create_parser()

        args = parser.parse_args([
            '-c', '/path/to/config.yaml',
            '--start-date', '2025-01-01',
            '--end-date', '2025-01-31'
        ])

        assert args.config == '/path/to/config.yaml'
        assert args.start_date == '2025-01-01'
        assert args.end_date == '2025-01-31'

    def test_optional_arguments(self):
        """Test parsing optional arguments."""
        parser = create_parser()

        args = parser.parse_args([
            '-c', '/path/to/config.yaml',
            '--start-date', '2025-01-01',
            '--end-date', '2025-01-31',
            '-o', '/output/dir',
            '--log-level', 'DEBUG',
            '--log-format', 'json',
            '--exit-on-failure'
        ])

        assert args.output == '/output/dir'
        assert args.log_level == 'DEBUG'
        assert args.log_format == 'json'
        assert args.exit_on_failure is True

    def test_default_values(self):
        """Test default argument values."""
        parser = create_parser()

        args = parser.parse_args([
            '-c', '/path/to/config.yaml',
            '--start-date', '2025-01-01',
            '--end-date', '2025-01-31'
        ])

        assert args.log_level == 'INFO'
        assert args.log_format == 'text'
        assert args.exit_on_failure is False
        assert args.exit_on_warning is False


class TestCLIMain:
    """Tests for CLI main function."""

    @pytest.fixture
    def sample_config(self):
        """Create sample configuration file."""
        config_content = """
source:
  type: csv
  csv:
    file_path: /nonexistent/file.csv
    date_column: date

metadata:
  dq_check_name: "CLI Test"
  date_column: date
  id_column: id

checks: {}

output:
  formats: [json]
  destination: /tmp/output
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            f.flush()
            yield f.name
        os.unlink(f.name)

    def test_main_with_missing_config(self):
        """Test main with missing config file."""
        exit_code = main([
            '-c', '/nonexistent/config.yaml',
            '--start-date', '2025-01-01',
            '--end-date', '2025-01-31'
        ])

        assert exit_code == 1

    def test_main_with_invalid_data_source(self, sample_config):
        """Test main with invalid data source."""
        exit_code = main([
            '-c', sample_config,
            '--start-date', '2025-01-01',
            '--end-date', '2025-01-31'
        ])

        # Should fail because CSV file doesn't exist
        assert exit_code == 1

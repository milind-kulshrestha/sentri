"""End-to-end integration tests for the Data Quality Framework."""

import pytest
import tempfile
import os
import json
import pandas as pd
import numpy as np
from pathlib import Path

from data_quality.connectors import CSVConnector
from data_quality.managers import CheckManager
from data_quality.formatters import OutputManager, JSONFormatter, CSVFormatter, HTMLFormatter
from data_quality.core.config_loader import load_config
from data_quality.utils.constants import CheckStatus


class TestEndToEndCSV:
    """End-to-end tests with CSV data source."""

    @pytest.fixture
    def sample_csv_file(self):
        """Create sample CSV file for testing."""
        np.random.seed(42)

        # Create data with multiple dates
        data = []
        for date_num in range(1, 4):
            for i in range(1, 51):
                data.append({
                    'entity_id': i,
                    'effective_date': f'2025-01-0{date_num}',
                    'value': np.random.uniform(50, 150),
                    'score': np.random.uniform(0, 100),
                    'category': np.random.choice(['A', 'B', 'C']),
                })

        df = pd.DataFrame(data)

        # Introduce some nulls
        null_indices = np.random.choice(len(df), 5, replace=False)
        df.loc[null_indices, 'value'] = np.nan

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)

    @pytest.fixture
    def output_dir(self):
        """Create temporary output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_full_pipeline_csv(self, sample_csv_file, output_dir):
        """Test complete pipeline from CSV to output."""
        # 1. Connect to CSV
        connector = CSVConnector(
            file_path=sample_csv_file,
            date_column='effective_date'
        )

        assert connector.validate_connection() is True

        # 2. Get data
        df = connector.get_data(
            start_date='2025-01-01',
            end_date='2025-01-03'
        )

        assert len(df) > 0
        assert 'entity_id' in df.columns
        assert 'value' in df.columns

        # 3. Configure and run checks
        metadata = {
            'dq_check_name': 'Integration Test',
            'date_column': 'effective_date',
            'id_column': 'entity_id'
        }

        checks_config = {
            'completeness': {
                'value': {
                    'thresholds': {'absolute_critical': 0.10},
                    'description': 'Value completeness'
                }
            },
            'uniqueness': {
                'entity_id': {
                    'thresholds': {'absolute_critical': 100},
                    'description': 'Entity uniqueness'
                }
            },
            'range': {
                'score': {
                    'min_value': 0,
                    'max_value': 100,
                    'description': 'Score range'
                }
            }
        }

        manager = CheckManager(
            df=df,
            metadata=metadata,
            checks_config=checks_config
        )

        results = manager.run_all_checks()

        # 4. Verify results structure
        assert 'summary' in results
        assert 'results' in results
        assert results['summary']['total'] > 0

        # 5. Generate outputs
        output_mgr = OutputManager(
            formats=['json', 'csv', 'html'],
            destination=output_dir,
            file_prefix='test_report',
            include_timestamp=False
        )

        output_paths = output_mgr.generate_outputs(results, 'Integration Test')

        # 6. Verify output files
        assert 'json' in output_paths
        assert 'csv' in output_paths
        assert 'html' in output_paths

        # Verify JSON file
        json_path = output_paths['json']
        assert os.path.exists(json_path)
        with open(json_path) as f:
            json_data = json.load(f)
            assert 'summary' in json_data
            assert 'results' in json_data

        # Verify CSV file
        csv_path = output_paths['csv']
        assert os.path.exists(csv_path)
        csv_df = pd.read_csv(csv_path)
        assert len(csv_df) > 0

        # Verify HTML file
        html_path = output_paths['html']
        assert os.path.exists(html_path)
        with open(html_path) as f:
            html_content = f.read()
            assert 'Integration Test' in html_content

        connector.close()

    def test_multiple_check_types(self, sample_csv_file, output_dir):
        """Test running multiple check types together."""
        connector = CSVConnector(
            file_path=sample_csv_file,
            date_column='effective_date'
        )

        df = connector.get_data(
            start_date='2025-01-01',
            end_date='2025-01-03'
        )

        metadata = {
            'dq_check_name': 'Multi-Check Test',
            'date_column': 'effective_date',
            'id_column': 'entity_id'
        }

        # Configure all check types
        checks_config = {
            'completeness': {
                'value': {'thresholds': {'absolute_critical': 0.10}}
            },
            'uniqueness': {
                'entity_id': {'thresholds': {'absolute_critical': 200}}
            },
            'range': {
                'score': {'min_value': 0, 'max_value': 100}
            },
            'turnover': {
                'entity_id': {'thresholds': {'absolute_critical': 0.50}}
            },
            'frequency': {
                'category': {'thresholds': {'absolute_critical': 0.50}}
            }
        }

        manager = CheckManager(
            df=df,
            metadata=metadata,
            checks_config=checks_config
        )

        results = manager.run_all_checks()

        # Should have results for each check type
        assert results['summary']['total'] >= 5

        # Verify results_by_type
        results_by_type = results.get('results_by_type', {})
        assert len(results_by_type) >= 1

        connector.close()

    def test_check_with_filters(self, sample_csv_file):
        """Test checks with filter conditions."""
        connector = CSVConnector(
            file_path=sample_csv_file,
            date_column='effective_date'
        )

        df = connector.get_data(
            start_date='2025-01-01',
            end_date='2025-01-03'
        )

        metadata = {
            'dq_check_name': 'Filter Test',
            'date_column': 'effective_date',
            'id_column': 'entity_id'
        }

        checks_config = {
            'completeness': {
                'value': {
                    'thresholds': {'absolute_critical': 0.20},
                    'filter_condition': "category == 'A'",
                    'column_alias': 'value_cat_a'
                }
            }
        }

        manager = CheckManager(
            df=df,
            metadata=metadata,
            checks_config=checks_config
        )

        results = manager.run_all_checks()

        assert results['summary']['total'] > 0

        connector.close()


class TestConfigurationIntegration:
    """Test configuration loading and validation integration."""

    @pytest.fixture
    def config_file(self):
        """Create sample configuration file."""
        config_content = """
source:
  type: csv
  csv:
    file_path: /tmp/test.csv
    date_column: effective_date

metadata:
  dq_check_name: "Config Integration Test"
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
  formats: [json, html]
  destination: /tmp/output
  file_prefix: config_test
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            f.flush()
            yield f.name
        os.unlink(f.name)

    def test_load_and_validate_config(self, config_file):
        """Test loading and validating configuration."""
        config = load_config(config_file)

        assert config.source.type == 'csv'
        assert config.metadata.dq_check_name == 'Config Integration Test'
        assert 'completeness' in config.checks
        assert 'json' in config.output.formats


class TestFormatterIntegration:
    """Test formatter integration."""

    @pytest.fixture
    def sample_results(self):
        """Create sample results for formatting."""
        return {
            'metadata': {
                'dq_check_name': 'Formatter Test'
            },
            'summary': {
                'total': 3,
                'passed': 2,
                'warnings': 0,
                'failed': 1,
                'pass_rate': 66.7
            },
            'results': [
                {
                    'check_type': 'completeness',
                    'column': 'value',
                    'date': '2025-01-01',
                    'status': CheckStatus.PASS,
                    'metric_value': 0.02,
                    'description': 'Test'
                },
                {
                    'check_type': 'uniqueness',
                    'column': 'id',
                    'date': '2025-01-01',
                    'status': CheckStatus.FAIL,
                    'metric_value': 5,
                    'description': 'Test'
                }
            ]
        }

    def test_json_formatter(self, sample_results):
        """Test JSON formatter."""
        formatter = JSONFormatter()
        json_str = formatter.format(sample_results)

        # Should be valid JSON
        data = json.loads(json_str)
        assert 'summary' in data
        assert 'results' in data

    def test_csv_formatter(self, sample_results):
        """Test CSV formatter."""
        formatter = CSVFormatter()
        csv_str = formatter.format(sample_results)

        # Should contain header and data
        lines = csv_str.strip().split('\n')
        assert len(lines) >= 2  # Header + at least 1 row

    def test_html_formatter(self, sample_results):
        """Test HTML formatter."""
        formatter = HTMLFormatter()
        html_str = formatter.format(sample_results)

        # Should contain key elements
        assert '<html>' in html_str
        assert 'Formatter Test' in html_str
        assert 'Summary' in html_str

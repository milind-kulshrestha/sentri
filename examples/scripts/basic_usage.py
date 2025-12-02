#!/usr/bin/env python3
"""Basic usage example for the Data Quality Framework."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Import framework components
from data_quality.connectors import CSVConnector
from data_quality.managers import CheckManager
from data_quality.formatters import OutputManager
from data_quality.core.config_loader import load_config


def create_sample_data():
    """Create sample data for demonstration."""
    np.random.seed(42)
    dates = [datetime(2025, 1, d) for d in range(1, 4)]

    data = []
    for date in dates:
        for i in range(1, 101):
            data.append({
                'entity_id': i,
                'effective_date': date,
                'value': np.random.uniform(50, 150) + (i * 0.1),
                'score': np.random.uniform(0, 100),
                'category': np.random.choice(['A', 'B', 'C']),
                'universe': np.random.choice(['US', 'EU', 'APAC'])
            })

    df = pd.DataFrame(data)

    # Introduce some nulls
    null_indices = np.random.choice(len(df), 10, replace=False)
    df.loc[null_indices, 'value'] = np.nan

    return df


def run_dq_checks_programmatic():
    """Run DQ checks programmatically without config file."""
    print("Creating sample data...")
    df = create_sample_data()
    print(f"Created DataFrame with {len(df)} rows, {len(df.columns)} columns")

    # Define metadata
    metadata = {
        'dq_check_name': 'Programmatic DQ Check',
        'date_column': 'effective_date',
        'id_column': 'entity_id'
    }

    # Define checks configuration
    checks_config = {
        'completeness': {
            'value': {
                'thresholds': {'absolute_critical': 0.05, 'absolute_warning': 0.02},
                'description': 'Value completeness check'
            }
        },
        'uniqueness': {
            'entity_id': {
                'thresholds': {'absolute_critical': 0},
                'description': 'Entity ID uniqueness'
            }
        },
        'range': {
            'score': {
                'min_value': 0,
                'max_value': 100,
                'description': 'Score range validation'
            }
        }
    }

    # Create and run check manager
    print("\nRunning DQ checks...")
    manager = CheckManager(
        df=df,
        metadata=metadata,
        checks_config=checks_config
    )

    results = manager.run_all_checks()

    # Display results
    summary = results['summary']
    print("\n" + "="*50)
    print("DQ Check Summary")
    print("="*50)
    print(f"Total Checks: {summary['total']}")
    print(f"Passed: {summary['passed']}")
    print(f"Warnings: {summary['warnings']}")
    print(f"Failed: {summary['failed']}")
    print(f"Pass Rate: {summary['pass_rate']}%")

    # Show individual results
    print("\nDetailed Results:")
    print("-"*50)
    for result in results['results']:
        status = result.get('status', 'UNKNOWN')
        if hasattr(status, 'value'):
            status = status.value
        print(f"  [{status}] {result['check_type']}.{result['column']}: {result.get('metric_value', 'N/A')}")

    # Generate outputs
    print("\nGenerating output files...")
    output_mgr = OutputManager(
        formats=['json', 'html', 'csv'],
        destination='/tmp/dq_output',
        file_prefix='demo_dq'
    )

    output_paths = output_mgr.generate_outputs(results, 'Demo Check')

    print("\nOutput files generated:")
    for fmt, path in output_paths.items():
        if isinstance(path, str):
            print(f"  {fmt}: {path}")

    return results


if __name__ == '__main__':
    run_dq_checks_programmatic()

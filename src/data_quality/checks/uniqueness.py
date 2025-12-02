"""Uniqueness check implementation."""

from typing import Any, Dict, List
import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus, MAX_SAMPLE_SIZE


class UniquenessCheck(BaseCheck):
    """
    Check for duplicate values in columns.

    Validates that the number of duplicate values does not exceed
    the configured thresholds.
    """

    def run(self) -> pd.DataFrame:
        """
        Execute uniqueness check for all configured columns.

        Returns:
            pd.DataFrame: Check results
        """
        results = []

        for column, config in self.check_config.items():
            if not config.get('enabled', True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(
                    column, e, {'check': 'uniqueness'}
                )
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check uniqueness for a single column.

        Args:
            column: Column name
            config: Column configuration

        Returns:
            Dict: Check result
        """
        # Apply filter if specified
        df = self._apply_filter(self.df, config.get('filter_condition'))

        if column not in df.columns:
            return self._handle_column_error(
                column,
                ValueError(f"Column '{column}' not found in DataFrame"),
                {'available_columns': list(df.columns)}
            )

        # Calculate duplicate statistics
        total_rows = len(df)
        unique_count = df[column].nunique()

        # Find duplicated values
        duplicated_mask = df[column].duplicated(keep=False)
        duplicate_count = duplicated_mask.sum() - df[column][duplicated_mask].nunique()

        # Get sample of duplicated values
        duplicated_values = df[column][df[column].duplicated(keep=False)].unique()
        duplicated_sample = list(duplicated_values[:MAX_SAMPLE_SIZE])

        # Evaluate against thresholds (threshold is count of duplicates)
        thresholds = config.get('thresholds', {})
        evaluation = self._evaluate_threshold(duplicate_count, thresholds)

        # Get date for result
        dates = self._get_unique_dates()
        date = dates[-1] if dates else pd.Timestamp.now().strftime('%Y-%m-%d')

        # Create result record
        return self._create_result_record(
            column=column,
            date=date,
            metric_value=duplicate_count,
            evaluation=evaluation,
            additional_metrics={
                'total_rows': int(total_rows),
                'unique_count': int(unique_count),
                'duplicate_count': int(duplicate_count),
                'duplicate_percentage': round(duplicate_count / total_rows * 100, 2) if total_rows > 0 else 0,
                'duplicated_values_sample': duplicated_sample
            }
        )

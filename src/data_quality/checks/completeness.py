"""Completeness check implementation."""

from typing import Any, Dict, List

import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus


class CompletenessCheck(BaseCheck):
    """
    Check for missing/null values in columns.

    Validates that the percentage of null values in each column
    does not exceed the configured thresholds.
    """

    def run(self) -> pd.DataFrame:
        """
        Execute completeness check for all configured columns.

        Returns:
            pd.DataFrame: Check results
        """
        results = []

        for column, config in self.check_config.items():
            if not config.get("enabled", True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(
                    column, e, {"check": "completeness"}
                )
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check completeness for a single column.

        Args:
            column: Column name
            config: Column configuration

        Returns:
            Dict: Check result
        """
        # Apply filter if specified
        df = self._apply_filter(self.df, config.get("filter_condition"))

        if column not in df.columns:
            return self._handle_column_error(
                column,
                ValueError(f"Column '{column}' not found in DataFrame"),
                {"available_columns": list(df.columns)},
            )

        # Calculate null statistics
        total_count = len(df)
        null_count = df[column].isna().sum()
        null_percentage = null_count / total_count if total_count > 0 else 0

        # Evaluate against thresholds
        thresholds = config.get("thresholds", {})
        evaluation = self._evaluate_threshold(null_percentage, thresholds)

        # Get date for result
        dates = self._get_unique_dates()
        date = dates[-1] if dates else pd.Timestamp.now().strftime("%Y-%m-%d")

        # Create result record
        return self._create_result_record(
            column=column,
            date=date,
            metric_value=null_percentage,
            evaluation=evaluation,
            additional_metrics={
                "null_count": int(null_count),
                "total_count": int(total_count),
                "null_percentage": round(null_percentage * 100, 2),
            },
        )

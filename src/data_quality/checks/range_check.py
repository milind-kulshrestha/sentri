"""Range check implementation."""

from typing import Any, Dict, List

import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import MAX_SAMPLE_SIZE, CheckStatus, Severity


class RangeCheck(BaseCheck):
    """
    Check that values fall within expected bounds.

    Validates that numeric values in each column fall within
    the configured min/max range.
    """

    def run(self) -> pd.DataFrame:
        """
        Execute range check for all configured columns.

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
                error_result = self._handle_column_error(column, e, {"check": "range"})
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check range for a single column.

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

        # Get bounds
        min_value = config.get("min_value")
        max_value = config.get("max_value")

        # Get non-null values
        values = df[column].dropna()
        total_count = len(values)

        if total_count == 0:
            return self._create_result_record(
                column=column,
                date=pd.Timestamp.now().strftime("%Y-%m-%d"),
                metric_value=0,
                evaluation={
                    "status": CheckStatus.PASS,
                    "severity": Severity.INFO,
                    "exceeded_threshold": None,
                },
                additional_metrics={
                    "out_of_range_count": 0,
                    "total_count": 0,
                    "message": "No non-null values to check",
                },
            )

        # Find out of range values
        below_min = (
            values[values < min_value]
            if min_value is not None
            else pd.Series([], dtype=float)
        )
        above_max = (
            values[values > max_value]
            if max_value is not None
            else pd.Series([], dtype=float)
        )

        out_of_range_count = len(below_min) + len(above_max)
        out_of_range_percentage = (
            out_of_range_count / total_count if total_count > 0 else 0
        )

        # Determine status
        if out_of_range_count > 0:
            status = CheckStatus.FAIL
            severity = Severity.CRITICAL
        else:
            status = CheckStatus.PASS
            severity = Severity.INFO

        evaluation = {
            "status": status,
            "severity": severity,
            "exceeded_threshold": None,
        }

        # Get date for result
        dates = self._get_unique_dates()
        date = dates[-1] if dates else pd.Timestamp.now().strftime("%Y-%m-%d")

        # Create result record
        return self._create_result_record(
            column=column,
            date=date,
            metric_value=out_of_range_percentage,
            evaluation=evaluation,
            additional_metrics={
                "out_of_range_count": int(out_of_range_count),
                "total_count": int(total_count),
                "out_of_range_percentage": round(out_of_range_percentage * 100, 2),
                "dataset_min": float(values.min()) if len(values) > 0 else None,
                "dataset_max": float(values.max()) if len(values) > 0 else None,
                "configured_min": min_value,
                "configured_max": max_value,
                "below_min_sample": list(below_min.head(MAX_SAMPLE_SIZE).values),
                "above_max_sample": list(above_max.head(MAX_SAMPLE_SIZE).values),
            },
        )

"""Turnover check implementation."""

from typing import Any, Dict, List, Set

import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import MAX_SAMPLE_SIZE, CheckStatus


class TurnoverCheck(BaseCheck):
    """
    Track additions and removals of unique identifiers between time periods.

    Validates that the turnover rate (adds + drops) does not exceed
    the configured thresholds.
    """

    def run(self) -> pd.DataFrame:
        """
        Execute turnover check for all configured columns.

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
                    column, e, {"check": "turnover"}
                )
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check turnover for a single column.

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

        # Get unique dates sorted
        dates = sorted(df[self.date_col].unique())

        if len(dates) < 2:
            # Need at least 2 dates for turnover comparison
            return self._create_result_record(
                column=column,
                date=(
                    dates[-1].strftime("%Y-%m-%d")
                    if dates
                    else pd.Timestamp.now().strftime("%Y-%m-%d")
                ),
                metric_value=0,
                evaluation={
                    "status": CheckStatus.PASS,
                    "severity": "INFO",
                    "exceeded_threshold": None,
                },
                additional_metrics={
                    "message": "Insufficient dates for turnover comparison",
                    "date_count": len(dates),
                },
            )

        # Compare latest two dates
        prev_date = dates[-2]
        curr_date = dates[-1]

        prev_ids: Set = set(
            df[df[self.date_col] == prev_date][column].dropna().unique()
        )
        curr_ids: Set = set(
            df[df[self.date_col] == curr_date][column].dropna().unique()
        )

        # Calculate turnover
        added = curr_ids - prev_ids
        dropped = prev_ids - curr_ids
        total_unique = prev_ids | curr_ids

        added_count = len(added)
        dropped_count = len(dropped)
        total_count = len(total_unique)

        turnover_rate = (
            (added_count + dropped_count) / total_count if total_count > 0 else 0
        )

        # Evaluate against thresholds
        thresholds = config.get("thresholds", {})
        evaluation = self._evaluate_threshold(turnover_rate, thresholds)

        # Create result record
        return self._create_result_record(
            column=column,
            date=curr_date.strftime("%Y-%m-%d"),
            metric_value=turnover_rate,
            evaluation=evaluation,
            additional_metrics={
                "added_count": int(added_count),
                "dropped_count": int(dropped_count),
                "turnover_rate": round(turnover_rate * 100, 2),
                "total_unique_ids": int(total_count),
                "previous_date": prev_date.strftime("%Y-%m-%d"),
                "current_date": curr_date.strftime("%Y-%m-%d"),
                "added_ids_sample": list(added)[:MAX_SAMPLE_SIZE],
                "dropped_ids_sample": list(dropped)[:MAX_SAMPLE_SIZE],
            },
        )

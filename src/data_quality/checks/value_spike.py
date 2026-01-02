"""Value spike check implementation."""

from typing import Any, Dict

import numpy as np
import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import MAX_SAMPLE_SIZE, CheckStatus


class ValueSpikeCheck(BaseCheck):
    """
    Detect abnormal value changes for individual records.

    Compares values between consecutive time periods to detect
    unexpected large changes.
    """

    def run(self) -> pd.DataFrame:
        """Execute value spike check for all configured columns."""
        results = []

        for column, config in self.check_config.items():
            if not config.get("enabled", True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(
                    column, e, {"check": "value_spike"}
                )
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check for value spikes in a single column."""
        df = self._apply_filter(self.df, config.get("filter_condition"))

        if column not in df.columns:
            return self._handle_column_error(
                column, ValueError(f"Column '{column}' not found"), {}
            )

        dates = sorted(df[self.date_col].unique())
        if len(dates) < 2:
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
                additional_metrics={"message": "Insufficient dates for comparison"},
            )

        # Compare latest two dates
        prev_df = df[df[self.date_col] == dates[-2]][[self.id_col, column]].set_index(
            self.id_col
        )
        curr_df = df[df[self.date_col] == dates[-1]][[self.id_col, column]].set_index(
            self.id_col
        )

        # Merge and calculate changes
        merged = prev_df.join(curr_df, lsuffix="_prev", rsuffix="_curr", how="inner")
        if len(merged) == 0:
            return self._create_result_record(
                column=column,
                date=dates[-1].strftime("%Y-%m-%d"),
                metric_value=0,
                evaluation={
                    "status": CheckStatus.PASS,
                    "severity": "INFO",
                    "exceeded_threshold": None,
                },
                additional_metrics={"message": "No matching records between periods"},
            )

        # Calculate percentage change
        prev_col = f"{column}_prev"
        curr_col = f"{column}_curr"
        merged["pct_change"] = abs(
            (merged[curr_col] - merged[prev_col]) / merged[prev_col].replace(0, np.nan)
        )

        # Find spikes
        thresholds = config.get("thresholds", {})
        critical_threshold = thresholds.get("absolute_critical", 10.0)
        max_spike = merged["pct_change"].max()

        evaluation = self._evaluate_threshold(max_spike, thresholds)

        return self._create_result_record(
            column=column,
            date=dates[-1].strftime("%Y-%m-%d"),
            metric_value=max_spike,
            evaluation=evaluation,
            additional_metrics={
                "max_spike_percentage": (
                    round(max_spike * 100, 2) if not np.isnan(max_spike) else 0
                ),
                "records_analyzed": len(merged),
                "spikes_above_critical": int(
                    (merged["pct_change"] > critical_threshold).sum()
                ),
            },
        )

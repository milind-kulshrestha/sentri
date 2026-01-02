"""Correlation check implementation."""

from typing import Any, Dict

import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus


class CorrelationCheck(BaseCheck):
    """
    Validate correlation between columns or across time periods.

    Supports temporal correlation and cross-column correlation checks.
    """

    def run(self) -> pd.DataFrame:
        """Execute correlation check for all configured columns."""
        results = []

        for column, config in self.check_config.items():
            if not config.get("enabled", True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(
                    column, e, {"check": "correlation"}
                )
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check correlation for a single column."""
        df = self._apply_filter(self.df, config.get("filter_condition"))

        if column not in df.columns:
            return self._handle_column_error(
                column, ValueError(f"Column '{column}' not found"), {}
            )

        correlation_type = config.get("correlation_type", "temporal")
        dates = self._get_unique_dates()
        date = dates[-1] if dates else pd.Timestamp.now().strftime("%Y-%m-%d")

        if correlation_type == "cross_column":
            other_column = config.get("correlation_with")
            if not other_column or other_column not in df.columns:
                return self._handle_column_error(
                    column,
                    ValueError(f"Correlation column '{other_column}' not found"),
                    {},
                )

            correlation = df[column].corr(df[other_column])
        else:  # temporal
            sorted_dates = sorted(df[self.date_col].unique())
            if len(sorted_dates) < 2:
                return self._create_result_record(
                    column=column,
                    date=date,
                    metric_value=1.0,
                    evaluation={
                        "status": CheckStatus.PASS,
                        "severity": "INFO",
                        "exceeded_threshold": None,
                    },
                    additional_metrics={
                        "message": "Insufficient dates for temporal correlation"
                    },
                )

            prev_values = df[df[self.date_col] == sorted_dates[-2]][
                [self.id_col, column]
            ].set_index(self.id_col)
            curr_values = df[df[self.date_col] == sorted_dates[-1]][
                [self.id_col, column]
            ].set_index(self.id_col)
            merged = prev_values.join(
                curr_values, lsuffix="_prev", rsuffix="_curr", how="inner"
            )

            if len(merged) < 2:
                return self._create_result_record(
                    column=column,
                    date=date,
                    metric_value=1.0,
                    evaluation={
                        "status": CheckStatus.PASS,
                        "severity": "INFO",
                        "exceeded_threshold": None,
                    },
                    additional_metrics={
                        "message": "Insufficient matching records for correlation"
                    },
                )

            correlation = merged[f"{column}_prev"].corr(merged[f"{column}_curr"])

        # For correlation, we usually want high values, so invert threshold logic
        thresholds = config.get("thresholds", {})
        critical = thresholds.get("absolute_critical", 0.8)

        if abs(correlation) < critical:
            status = CheckStatus.FAIL
            severity = "CRITICAL"
        else:
            status = CheckStatus.PASS
            severity = "INFO"

        return self._create_result_record(
            column=column,
            date=date,
            metric_value=correlation,
            evaluation={
                "status": status,
                "severity": severity,
                "exceeded_threshold": critical if status == CheckStatus.FAIL else None,
            },
            additional_metrics={
                "correlation_type": correlation_type,
                "correlation_value": round(correlation, 4),
            },
        )

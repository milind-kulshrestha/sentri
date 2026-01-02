"""Statistical measures check implementation."""

from typing import Any, Dict

import numpy as np
import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus


class StatisticalCheck(BaseCheck):
    """
    Monitor statistical properties (mean, median, stdev, etc.).

    Calculates specified statistical measures and validates against thresholds.
    """

    def run(self) -> pd.DataFrame:
        """Execute statistical check for all configured columns."""
        results = []

        for column, config in self.check_config.items():
            if not config.get("enabled", True):
                continue

            try:
                column_results = self._check_column(column, config)
                results.extend(column_results)
            except Exception as e:
                error_result = self._handle_column_error(
                    column, e, {"check": "statistical"}
                )
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]):
        """Check statistical measures for a single column."""
        df = self._apply_filter(self.df, config.get("filter_condition"))

        if column not in df.columns:
            return [
                self._handle_column_error(
                    column, ValueError(f"Column '{column}' not found"), {}
                )
            ]

        values = df[column].dropna()
        if len(values) == 0:
            return []

        measures = config.get("measures", ["mean", "std", "median"])
        measure_thresholds = config.get("thresholds", {})
        results = []

        # Calculate all measures
        stats = {
            "mean": values.mean(),
            "median": values.median(),
            "std": values.std(),
            "min": values.min(),
            "max": values.max(),
            "count": len(values),
            "skew": values.skew() if len(values) > 2 else 0,
            "kurtosis": values.kurtosis() if len(values) > 3 else 0,
        }

        dates = self._get_unique_dates()
        date = dates[-1] if dates else pd.Timestamp.now().strftime("%Y-%m-%d")

        for measure in measures:
            if measure not in stats:
                continue

            value = stats[measure]
            thresholds = measure_thresholds.get(measure, {})

            # Check against range thresholds if specified
            status = CheckStatus.PASS
            severity = "INFO"
            exceeded = None

            if isinstance(thresholds.get("absolute_critical"), list):
                min_val, max_val = thresholds["absolute_critical"]
                if value < min_val or value > max_val:
                    status = CheckStatus.FAIL
                    severity = "CRITICAL"
                    exceeded = thresholds["absolute_critical"]
            elif thresholds.get("absolute_critical") is not None:
                if value > thresholds["absolute_critical"]:
                    status = CheckStatus.FAIL
                    severity = "CRITICAL"
                    exceeded = thresholds["absolute_critical"]

            results.append(
                {
                    "check_type": "statistical",
                    "column": column,
                    "measure": measure,
                    "date": date,
                    "metric_value": round(float(value), 6),
                    "status": status,
                    "severity": severity,
                    "exceeded_threshold": exceeded,
                    "thresholds": thresholds,
                    "timestamp": pd.Timestamp.now().isoformat(),
                }
            )

        return results

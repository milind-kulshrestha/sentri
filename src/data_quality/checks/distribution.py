"""Distribution change check implementation."""

from typing import Any, Dict
import pandas as pd
import numpy as np
from scipy import stats

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus


class DistributionCheck(BaseCheck):
    """
    Detect significant distribution shifts using KS test.

    Compares value distributions between time periods using
    Kolmogorov-Smirnov test.
    """

    def run(self) -> pd.DataFrame:
        """Execute distribution check for all configured columns."""
        results = []

        for column, config in self.check_config.items():
            if not config.get('enabled', True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(column, e, {'check': 'distribution'})
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check distribution for a single column."""
        df = self._apply_filter(self.df, config.get('filter_condition'))

        if column not in df.columns:
            return self._handle_column_error(
                column, ValueError(f"Column '{column}' not found"), {}
            )

        dates = sorted(df[self.date_col].unique())
        if len(dates) < 2:
            return self._create_result_record(
                column=column,
                date=dates[-1].strftime('%Y-%m-%d') if dates else pd.Timestamp.now().strftime('%Y-%m-%d'),
                metric_value=0,
                evaluation={'status': CheckStatus.PASS, 'severity': 'INFO', 'exceeded_threshold': None},
                additional_metrics={'message': 'Insufficient dates for distribution comparison'}
            )

        # Get distributions for both periods
        prev_values = df[df[self.date_col] == dates[-2]][column].dropna().values
        curr_values = df[df[self.date_col] == dates[-1]][column].dropna().values

        if len(prev_values) < 2 or len(curr_values) < 2:
            return self._create_result_record(
                column=column,
                date=dates[-1].strftime('%Y-%m-%d'),
                metric_value=0,
                evaluation={'status': CheckStatus.PASS, 'severity': 'INFO', 'exceeded_threshold': None},
                additional_metrics={'message': 'Insufficient data for KS test'}
            )

        # Perform KS test
        ks_stat, p_value = stats.ks_2samp(prev_values, curr_values)

        thresholds = config.get('thresholds', {})
        critical = thresholds.get('absolute_critical', 0.05)

        # Use p-value for evaluation (lower p-value = more significant change)
        if p_value < critical:
            status = CheckStatus.FAIL
            severity = 'CRITICAL'
        else:
            status = CheckStatus.PASS
            severity = 'INFO'

        return self._create_result_record(
            column=column,
            date=dates[-1].strftime('%Y-%m-%d'),
            metric_value=p_value,
            evaluation={'status': status, 'severity': severity, 'exceeded_threshold': critical if status == CheckStatus.FAIL else None},
            additional_metrics={
                'ks_statistic': round(ks_stat, 4),
                'p_value': round(p_value, 4),
                'prev_count': len(prev_values),
                'curr_count': len(curr_values)
            }
        )

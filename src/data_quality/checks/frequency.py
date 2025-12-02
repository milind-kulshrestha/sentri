"""Frequency check implementation."""

from typing import Any, Dict
import pandas as pd

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus


class FrequencyCheck(BaseCheck):
    """
    Monitor distribution changes in categorical data.

    Compares category frequencies between time periods.
    """

    def run(self) -> pd.DataFrame:
        """Execute frequency check for all configured columns."""
        results = []

        for column, config in self.check_config.items():
            if not config.get('enabled', True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(column, e, {'check': 'frequency'})
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check frequency distribution for a single column."""
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
                additional_metrics={'message': 'Insufficient dates for comparison'}
            )

        # Calculate frequencies for both periods
        prev_freq = df[df[self.date_col] == dates[-2]][column].value_counts(normalize=True)
        curr_freq = df[df[self.date_col] == dates[-1]][column].value_counts(normalize=True)

        # Calculate maximum frequency change
        all_categories = set(prev_freq.index) | set(curr_freq.index)
        max_change = 0
        for cat in all_categories:
            prev_val = prev_freq.get(cat, 0)
            curr_val = curr_freq.get(cat, 0)
            max_change = max(max_change, abs(curr_val - prev_val))

        thresholds = config.get('thresholds', {})
        evaluation = self._evaluate_threshold(max_change, thresholds)

        return self._create_result_record(
            column=column,
            date=dates[-1].strftime('%Y-%m-%d'),
            metric_value=max_change,
            evaluation=evaluation,
            additional_metrics={
                'max_frequency_change': round(max_change * 100, 2),
                'category_count': len(all_categories),
                'current_distribution': curr_freq.head(10).to_dict()
            }
        )

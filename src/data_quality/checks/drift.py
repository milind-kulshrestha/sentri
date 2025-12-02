"""Drift check implementation using PSI."""

from typing import Any, Dict
import pandas as pd
import numpy as np

from data_quality.checks.base import BaseCheck
from data_quality.utils.constants import CheckStatus, PSI_NO_DRIFT, PSI_MODERATE_DRIFT


class DriftCheck(BaseCheck):
    """
    Identify gradual data drift over time using PSI.

    Population Stability Index (PSI) measures distribution drift
    between baseline and current periods.
    """

    def run(self) -> pd.DataFrame:
        """Execute drift check for all configured columns."""
        results = []

        for column, config in self.check_config.items():
            if not config.get('enabled', True):
                continue

            try:
                result = self._check_column(column, config)
                results.append(result)
            except Exception as e:
                error_result = self._handle_column_error(column, e, {'check': 'drift'})
                results.append(error_result)

        return pd.DataFrame(results) if results else pd.DataFrame()

    def _check_column(self, column: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check drift for a single column."""
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
                additional_metrics={'message': 'Insufficient dates for drift comparison'}
            )

        # Get baseline and current distributions
        baseline_values = df[df[self.date_col] == dates[0]][column].dropna()
        current_values = df[df[self.date_col] == dates[-1]][column].dropna()

        if len(baseline_values) < 10 or len(current_values) < 10:
            return self._create_result_record(
                column=column,
                date=dates[-1].strftime('%Y-%m-%d'),
                metric_value=0,
                evaluation={'status': CheckStatus.PASS, 'severity': 'INFO', 'exceeded_threshold': None},
                additional_metrics={'message': 'Insufficient data for PSI calculation'}
            )

        # Calculate PSI
        psi = self._calculate_psi(baseline_values, current_values)

        thresholds = config.get('thresholds', {})
        critical = thresholds.get('absolute_critical', PSI_MODERATE_DRIFT)
        warning = thresholds.get('absolute_warning', PSI_NO_DRIFT)

        if psi >= critical:
            status = CheckStatus.FAIL
            severity = 'CRITICAL'
            interpretation = 'Significant drift detected'
        elif psi >= warning:
            status = CheckStatus.WARNING
            severity = 'WARNING'
            interpretation = 'Moderate drift detected'
        else:
            status = CheckStatus.PASS
            severity = 'INFO'
            interpretation = 'No significant drift'

        return self._create_result_record(
            column=column,
            date=dates[-1].strftime('%Y-%m-%d'),
            metric_value=psi,
            evaluation={'status': status, 'severity': severity, 'exceeded_threshold': critical if status == CheckStatus.FAIL else None},
            additional_metrics={
                'psi': round(psi, 4),
                'interpretation': interpretation,
                'baseline_date': dates[0].strftime('%Y-%m-%d'),
                'current_date': dates[-1].strftime('%Y-%m-%d')
            }
        )

    def _calculate_psi(self, baseline: pd.Series, current: pd.Series, n_bins: int = 10) -> float:
        """
        Calculate Population Stability Index.

        Args:
            baseline: Baseline period values
            current: Current period values
            n_bins: Number of bins for discretization

        Returns:
            PSI value
        """
        # Create bins from baseline
        bins = np.percentile(baseline, np.linspace(0, 100, n_bins + 1))
        bins = np.unique(bins)  # Remove duplicates

        if len(bins) < 2:
            return 0.0

        # Calculate proportions in each bin
        baseline_props = np.histogram(baseline, bins=bins)[0] / len(baseline)
        current_props = np.histogram(current, bins=bins)[0] / len(current)

        # Avoid division by zero
        baseline_props = np.where(baseline_props == 0, 0.0001, baseline_props)
        current_props = np.where(current_props == 0, 0.0001, current_props)

        # Calculate PSI
        psi = np.sum((current_props - baseline_props) * np.log(current_props / baseline_props))

        return float(psi)

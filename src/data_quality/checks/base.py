"""Base class for all data quality checks."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd

from data_quality.core.exceptions import CheckError, FilterError
from data_quality.utils.constants import CheckStatus, Severity
from data_quality.utils.logger import get_logger


class BaseCheck(ABC):
    """
    Base class for all data quality checks.

    Provides common functionality for configuration parsing,
    column normalization, threshold evaluation, and error handling.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        date_col: str,
        id_col: str,
        check_config: Dict[str, Dict[str, Any]],
    ):
        """
        Initialize base check.

        Args:
            df: DataFrame to validate
            date_col: Name of date column
            id_col: Name of ID column
            check_config: Column-wise check configuration
        """
        self.df = self._normalize_dataframe(df)
        self.date_col = date_col.lower()
        self.id_col = id_col.lower()
        self.check_config = self._normalize_config(check_config)
        self.results: List[Dict[str, Any]] = []
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def run(self) -> pd.DataFrame:
        """
        Execute the check logic.

        Must be implemented by each check type.

        Returns:
            pd.DataFrame: Check results following standard schema
        """
        pass

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame column names to lowercase.

        Args:
            df: Input DataFrame

        Returns:
            pd.DataFrame: Normalized DataFrame
        """
        df = df.copy()
        df.columns = df.columns.str.lower()
        return df

    def _normalize_config(
        self, config: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Normalize configuration keys to lowercase.

        Args:
            config: Raw check configuration

        Returns:
            Dict: Normalized configuration
        """
        normalized = {}
        for col_name, col_config in config.items():
            normalized[col_name.lower()] = col_config
        return normalized

    def _apply_filter(
        self, df: pd.DataFrame, filter_condition: Optional[str]
    ) -> pd.DataFrame:
        """
        Apply pandas query filter to DataFrame.

        Args:
            df: DataFrame to filter
            filter_condition: Pandas query string

        Returns:
            pd.DataFrame: Filtered DataFrame

        Raises:
            FilterError: If filter condition is invalid
        """
        if not filter_condition:
            return df

        try:
            return df.query(filter_condition)
        except Exception as e:
            self.logger.error(f"Filter error: {filter_condition} - {str(e)}")
            raise FilterError(
                f"Invalid filter: {filter_condition}", filter_condition=filter_condition
            ) from e

    def _evaluate_threshold(
        self,
        value: float,
        thresholds: Dict[str, float],
        threshold_type: str = "absolute",
    ) -> Dict[str, Any]:
        """
        Evaluate a value against configured thresholds.

        Args:
            value: Value to evaluate
            thresholds: Threshold configuration
            threshold_type: "absolute" or "delta"

        Returns:
            Dict with evaluation results:
                - status: CheckStatus enum
                - severity: Severity enum
                - exceeded_threshold: float | None
        """
        critical_key = f"{threshold_type}_critical"
        warning_key = f"{threshold_type}_warning"

        critical = thresholds.get(critical_key)
        warning = thresholds.get(warning_key)

        # For delta thresholds, use absolute value
        compare_value = abs(value) if threshold_type == "delta" else value

        # Check critical threshold
        if critical is not None:
            if compare_value > critical:
                return {
                    "status": CheckStatus.FAIL,
                    "severity": Severity.CRITICAL,
                    "exceeded_threshold": critical,
                }

        # Check warning threshold
        if warning is not None:
            if compare_value > warning:
                return {
                    "status": CheckStatus.WARNING,
                    "severity": Severity.WARNING,
                    "exceeded_threshold": warning,
                }

        # Passed all thresholds
        return {
            "status": CheckStatus.PASS,
            "severity": Severity.INFO,
            "exceeded_threshold": None,
        }

    def _handle_column_error(
        self, column: str, error: Exception, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle errors that occur during column-level check execution.

        Args:
            column: Column name where error occurred
            error: Exception that was raised
            context: Additional context about the error

        Returns:
            Dict: Error result following standard schema
        """
        self.logger.error(
            f"Column error in {column}: {str(error)}", extra={"context": context}
        )

        return {
            "check_type": self.__class__.__name__.replace("Check", "").lower(),
            "column": column,
            "status": CheckStatus.ERROR,
            "severity": Severity.ERROR,
            "error_message": str(error),
            "error_type": type(error).__name__,
            "context": context,
        }

    def _create_result_record(
        self,
        column: str,
        date: str,
        metric_value: float,
        evaluation: Dict[str, Any],
        additional_metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a standardized result record.

        Args:
            column: Column name
            date: Date of measurement
            metric_value: Measured value
            evaluation: Threshold evaluation result
            additional_metrics: Check-specific additional metrics

        Returns:
            Dict: Standardized result record
        """
        config = self.check_config.get(column, {})

        return {
            "check_type": self.__class__.__name__.replace("Check", "").lower(),
            "column": column,
            "column_alias": config.get("column_alias"),
            "date": date,
            "filter_applied": config.get("filter_condition"),
            "description": config.get("description"),
            "metric_value": round(metric_value, 6),
            "thresholds": config.get("thresholds", {}),
            "status": evaluation["status"],
            "severity": evaluation["severity"],
            "exceeded_threshold": evaluation.get("exceeded_threshold"),
            "additional_metrics": additional_metrics or {},
            "timestamp": pd.Timestamp.now().isoformat(),
        }

    def _get_unique_dates(self) -> List[str]:
        """
        Get unique dates from the DataFrame.

        Returns:
            List of date strings in ISO format
        """
        if self.date_col not in self.df.columns:
            return []

        dates = self.df[self.date_col].dropna().unique()
        return sorted([pd.Timestamp(d).strftime("%Y-%m-%d") for d in dates])

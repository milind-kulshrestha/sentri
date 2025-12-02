"""Check Manager for orchestrating data quality checks."""

from typing import Any, Dict, List, Optional
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_quality.utils.logger import get_logger
from data_quality.utils.constants import CheckStatus, Severity


class CheckManager:
    """
    Manages execution of all configured data quality checks.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        metadata: Dict[str, Any],
        checks_config: Dict[str, Any],
        logger: Any = None
    ):
        """
        Initialize Check Manager.

        Args:
            df: DataFrame containing data to validate
            metadata: Metadata configuration (date_column, id_column, etc.)
            checks_config: Check definitions from configuration
            logger: Optional logger instance
        """
        self.df = df
        self.metadata = metadata
        self.checks_config = checks_config
        self.logger = logger or get_logger("check_manager")
        self.check_results: List[Dict[str, Any]] = []
        self.check_registry = self._load_check_registry()

    def _load_check_registry(self) -> Dict[str, type]:
        """
        Load the registry of available check types.

        Returns:
            Dict mapping check type names to check classes
        """
        # Import here to avoid circular imports
        from data_quality.checks import (
            CompletenessCheck,
            UniquenessCheck,
            RangeCheck,
            TurnoverCheck,
            ValueSpikeCheck,
            FrequencyCheck,
            CorrelationCheck,
            StatisticalCheck,
            DistributionCheck,
            DriftCheck,
        )

        return {
            'completeness': CompletenessCheck,
            'uniqueness': UniquenessCheck,
            'range': RangeCheck,
            'turnover': TurnoverCheck,
            'value_spike': ValueSpikeCheck,
            'frequency': FrequencyCheck,
            'correlation': CorrelationCheck,
            'statistical': StatisticalCheck,
            'distribution': DistributionCheck,
            'drift': DriftCheck,
        }

    def run_all_checks(self) -> Dict[str, Any]:
        """
        Execute all configured checks.

        Returns:
            Dict containing aggregated results from all checks
        """
        self.logger.info(f"Starting check execution for {len(self.checks_config)} check types")

        for check_type, check_config in self.checks_config.items():
            try:
                results = self.run_single_check(check_type, check_config)
                if isinstance(results, pd.DataFrame):
                    self.check_results.extend(results.to_dict('records'))
                elif isinstance(results, list):
                    self.check_results.extend(results)
            except Exception as e:
                self.logger.error(f"Error running {check_type} check: {str(e)}")
                self._handle_check_error(check_type, e)

        return self.aggregate_results()

    def run_single_check(
        self,
        check_type: str,
        check_config: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Execute a single check type.

        Args:
            check_type: Type of check (e.g., 'completeness')
            check_config: Configuration for the check

        Returns:
            pd.DataFrame: Check results
        """
        self.logger.info(f"Running {check_type} check")

        check_class = self.check_registry.get(check_type)
        if check_class is None:
            self.logger.warning(f"Unknown check type: {check_type}")
            return pd.DataFrame()

        check_instance = check_class(
            df=self.df,
            date_col=self.metadata.get('date_column', 'date'),
            id_col=self.metadata.get('id_column', 'id'),
            check_config=check_config
        )

        results = check_instance.run()
        self.logger.info(f"Completed {check_type} check with {len(results)} results")

        return results

    def run_all_checks_parallel(self, max_workers: int = 4) -> Dict[str, Any]:
        """
        Execute checks in parallel.

        Args:
            max_workers: Maximum number of parallel workers

        Returns:
            Dict containing aggregated results
        """
        self.logger.info(f"Starting parallel check execution with {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.run_single_check,
                    check_type,
                    check_config
                ): check_type
                for check_type, check_config in self.checks_config.items()
            }

            for future in as_completed(futures):
                check_type = futures[future]
                try:
                    results = future.result()
                    if isinstance(results, pd.DataFrame):
                        self.check_results.extend(results.to_dict('records'))
                    elif isinstance(results, list):
                        self.check_results.extend(results)
                except Exception as e:
                    self._handle_check_error(check_type, e)

        return self.aggregate_results()

    def _handle_check_error(self, check_type: str, error: Exception) -> None:
        """
        Handle errors that occur during check execution.

        Args:
            check_type: Type of check that failed
            error: Exception that was raised
        """
        self.logger.error(f"Check {check_type} failed: {str(error)}")
        self.check_results.append({
            'check_type': check_type,
            'column': None,
            'status': CheckStatus.ERROR,
            'severity': Severity.ERROR,
            'error_message': str(error),
            'error_type': type(error).__name__
        })

    def aggregate_results(self) -> Dict[str, Any]:
        """
        Aggregate results from all executed checks.

        Returns:
            Dict containing summary and detailed results
        """
        summary = self.get_summary_statistics()

        return {
            'metadata': self.metadata,
            'summary': summary,
            'results': self.check_results,
            'results_by_type': self._group_results_by_type()
        }

    def _group_results_by_type(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group results by check type.

        Returns:
            Dict mapping check types to their results
        """
        grouped = {}
        for result in self.check_results:
            check_type = result.get('check_type', 'unknown')
            if check_type not in grouped:
                grouped[check_type] = []
            grouped[check_type].append(result)
        return grouped

    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Generate summary statistics across all checks.

        Returns:
            Dict with counts of passed/failed/warning checks
        """
        total = len(self.check_results)

        if total == 0:
            return {
                'total': 0,
                'passed': 0,
                'warnings': 0,
                'failed': 0,
                'errors': 0,
                'pass_rate': 0.0
            }

        passed = sum(1 for r in self.check_results if r.get('status') == CheckStatus.PASS)
        warnings = sum(1 for r in self.check_results if r.get('status') == CheckStatus.WARNING)
        failed = sum(1 for r in self.check_results if r.get('status') == CheckStatus.FAIL)
        errors = sum(1 for r in self.check_results if r.get('status') == CheckStatus.ERROR)

        pass_rate = (passed / total * 100) if total > 0 else 0.0

        return {
            'total': total,
            'passed': passed,
            'warnings': warnings,
            'failed': failed,
            'errors': errors,
            'pass_rate': round(pass_rate, 1)
        }

    def get_failed_checks(self) -> List[Dict[str, Any]]:
        """
        Get all failed checks.

        Returns:
            List of failed check results
        """
        return [
            r for r in self.check_results
            if r.get('status') == CheckStatus.FAIL
        ]

    def get_warning_checks(self) -> List[Dict[str, Any]]:
        """
        Get all warning checks.

        Returns:
            List of warning check results
        """
        return [
            r for r in self.check_results
            if r.get('status') == CheckStatus.WARNING
        ]

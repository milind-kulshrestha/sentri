"""Base class for alerting plugins."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from data_quality.utils.logger import get_logger


class AlertPlugin(ABC):
    """Base class for all alerting plugins."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize alert plugin.

        Args:
            config: Plugin-specific configuration
        """
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def send_alert(
        self,
        results: Dict[str, Any],
        failed_checks: List[Dict[str, Any]],
        warning_checks: List[Dict[str, Any]]
    ) -> bool:
        """
        Send alert based on check results.

        Args:
            results: Full results dictionary
            failed_checks: List of failed check results
            warning_checks: List of warning check results

        Returns:
            bool: True if alert sent successfully
        """
        pass

    def should_alert(
        self,
        failed_checks: List[Dict[str, Any]],
        warning_checks: List[Dict[str, Any]]
    ) -> bool:
        """
        Determine if an alert should be sent.

        Args:
            failed_checks: List of failed checks
            warning_checks: List of warning checks

        Returns:
            bool: True if alert should be sent
        """
        alert_on_warning = self.config.get('alert_on_warning', False)
        alert_on_failure = self.config.get('alert_on_failure', True)

        if alert_on_failure and failed_checks:
            return True
        if alert_on_warning and warning_checks:
            return True

        return False

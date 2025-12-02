"""Alerting plugins for the Data Quality Framework."""

from data_quality.alerting.base import AlertPlugin
from data_quality.alerting.email_plugin import EmailAlertPlugin

__all__ = ["AlertPlugin", "EmailAlertPlugin"]

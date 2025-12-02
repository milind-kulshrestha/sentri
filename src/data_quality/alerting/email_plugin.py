"""Email alerting plugin."""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, List

from data_quality.alerting.base import AlertPlugin
from data_quality.core.exceptions import AlertingError


class EmailAlertPlugin(AlertPlugin):
    """Send alerts via email."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize email alert plugin.

        Args:
            config: Email configuration with keys:
                - smtp_host: SMTP server host
                - smtp_port: SMTP server port
                - username: SMTP username (optional)
                - password: SMTP password (optional)
                - from_address: Sender email
                - to_addresses: List of recipient emails
                - subject_prefix: Email subject prefix
                - use_tls: Use TLS (default True)
        """
        super().__init__(config)
        self.smtp_host = config.get('smtp_host', 'localhost')
        self.smtp_port = config.get('smtp_port', 587)
        self.username = config.get('username')
        self.password = config.get('password')
        self.from_address = config.get('from_address', 'dq-framework@company.com')
        self.to_addresses = config.get('to_addresses', [])
        self.subject_prefix = config.get('subject_prefix', '[DQ Alert]')
        self.use_tls = config.get('use_tls', True)

    def send_alert(
        self,
        results: Dict[str, Any],
        failed_checks: List[Dict[str, Any]],
        warning_checks: List[Dict[str, Any]]
    ) -> bool:
        """
        Send email alert.

        Args:
            results: Full results dictionary
            failed_checks: List of failed check results
            warning_checks: List of warning check results

        Returns:
            bool: True if email sent successfully
        """
        if not self.to_addresses:
            self.logger.warning("No email recipients configured")
            return False

        if not self.should_alert(failed_checks, warning_checks):
            self.logger.info("No alert conditions met")
            return False

        try:
            # Build email
            msg = self._build_email(results, failed_checks, warning_checks)

            # Send email
            self._send_email(msg)

            self.logger.info(f"Alert email sent to {len(self.to_addresses)} recipients")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send alert email: {str(e)}")
            raise AlertingError(f"Email alert failed: {str(e)}")

    def _build_email(
        self,
        results: Dict[str, Any],
        failed_checks: List[Dict[str, Any]],
        warning_checks: List[Dict[str, Any]]
    ) -> MIMEMultipart:
        """Build email message."""
        metadata = results.get('metadata', {})
        summary = results.get('summary', {})
        check_name = metadata.get('dq_check_name', 'DQ Check')

        # Determine severity
        if failed_checks:
            severity = "CRITICAL"
        else:
            severity = "WARNING"

        subject = f"{self.subject_prefix} {severity}: {check_name}"

        # Build body
        body = self._build_body(check_name, summary, failed_checks, warning_checks)

        msg = MIMEMultipart()
        msg['From'] = self.from_address
        msg['To'] = ', '.join(self.to_addresses)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        return msg

    def _build_body(
        self,
        check_name: str,
        summary: Dict[str, Any],
        failed_checks: List[Dict[str, Any]],
        warning_checks: List[Dict[str, Any]]
    ) -> str:
        """Build email body HTML."""
        html = f"""
        <html>
        <body>
        <h2>Data Quality Alert: {check_name}</h2>

        <h3>Summary</h3>
        <ul>
            <li>Total Checks: {summary.get('total', 0)}</li>
            <li>Passed: {summary.get('passed', 0)}</li>
            <li>Warnings: {summary.get('warnings', 0)}</li>
            <li>Failed: {summary.get('failed', 0)}</li>
            <li>Pass Rate: {summary.get('pass_rate', 0)}%</li>
        </ul>
        """

        if failed_checks:
            html += "<h3 style='color: red;'>Failed Checks</h3><ul>"
            for check in failed_checks:
                html += f"<li>{check.get('check_type')}.{check.get('column')}: {check.get('metric_value', 'N/A')}</li>"
            html += "</ul>"

        if warning_checks:
            html += "<h3 style='color: orange;'>Warning Checks</h3><ul>"
            for check in warning_checks:
                html += f"<li>{check.get('check_type')}.{check.get('column')}: {check.get('metric_value', 'N/A')}</li>"
            html += "</ul>"

        html += """
        <p>Please review the full report for details.</p>
        </body>
        </html>
        """

        return html

    def _send_email(self, msg: MIMEMultipart) -> None:
        """Send email via SMTP."""
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()

            if self.username and self.password:
                server.login(self.username, self.password)

            server.sendmail(
                self.from_address,
                self.to_addresses,
                msg.as_string()
            )

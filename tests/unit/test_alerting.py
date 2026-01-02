"""Tests for alerting plugins."""

from unittest.mock import MagicMock, patch

import pytest

from data_quality.alerting.base import AlertPlugin
from data_quality.alerting.email_plugin import EmailAlertPlugin
from data_quality.utils.constants import CheckStatus


class TestAlertPlugin:
    """Tests for base AlertPlugin."""

    def test_cannot_instantiate_abstract(self):
        """Test that base class cannot be instantiated."""
        with pytest.raises(TypeError):
            AlertPlugin({})

    def test_should_alert_on_failure(self):
        """Test should_alert returns True on failures."""

        class ConcretePlugin(AlertPlugin):
            def send_alert(self, results, failed, warnings):
                return True

        plugin = ConcretePlugin({"alert_on_failure": True})
        failed = [{"check": "test"}]
        assert plugin.should_alert(failed, []) is True

    def test_should_not_alert_when_empty(self):
        """Test should_alert returns False when no issues."""

        class ConcretePlugin(AlertPlugin):
            def send_alert(self, results, failed, warnings):
                return True

        plugin = ConcretePlugin({})
        assert plugin.should_alert([], []) is False

    def test_should_alert_on_warning(self):
        """Test should_alert with warning configuration."""

        class ConcretePlugin(AlertPlugin):
            def send_alert(self, results, failed, warnings):
                return True

        plugin = ConcretePlugin({"alert_on_warning": True})
        warnings = [{"check": "test"}]
        assert plugin.should_alert([], warnings) is True


class TestEmailAlertPlugin:
    """Tests for EmailAlertPlugin."""

    @pytest.fixture
    def email_config(self):
        """Create sample email configuration."""
        return {
            "smtp_host": "localhost",
            "smtp_port": 587,
            "from_address": "test@example.com",
            "to_addresses": ["recipient@example.com"],
            "subject_prefix": "[Test]",
            "alert_on_failure": True,
        }

    @pytest.fixture
    def sample_results(self):
        """Create sample results."""
        return {
            "metadata": {"dq_check_name": "Test Check"},
            "summary": {
                "total": 10,
                "passed": 8,
                "warnings": 1,
                "failed": 1,
                "pass_rate": 80.0,
            },
        }

    def test_initialization(self, email_config):
        """Test plugin initialization."""
        plugin = EmailAlertPlugin(email_config)
        assert plugin.smtp_host == "localhost"
        assert plugin.smtp_port == 587
        assert len(plugin.to_addresses) == 1

    def test_no_recipients_returns_false(self, sample_results):
        """Test that no recipients returns False."""
        plugin = EmailAlertPlugin({"smtp_host": "localhost", "to_addresses": []})

        result = plugin.send_alert(
            sample_results, failed_checks=[{"check": "test"}], warning_checks=[]
        )

        assert result is False

    def test_no_alert_conditions_returns_false(self, email_config, sample_results):
        """Test that no alert conditions returns False."""
        plugin = EmailAlertPlugin(email_config)

        result = plugin.send_alert(sample_results, failed_checks=[], warning_checks=[])

        assert result is False

    @patch("smtplib.SMTP")
    def test_send_alert_success(self, mock_smtp, email_config, sample_results):
        """Test successful alert sending."""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        plugin = EmailAlertPlugin(email_config)

        result = plugin.send_alert(
            sample_results,
            failed_checks=[
                {"check_type": "completeness", "column": "value", "metric_value": 0.15}
            ],
            warning_checks=[],
        )

        assert result is True
        mock_server.sendmail.assert_called_once()

    def test_build_body_with_failures(self, email_config):
        """Test email body generation with failures."""
        plugin = EmailAlertPlugin(email_config)

        body = plugin._build_body(
            "Test Check",
            {"total": 10, "passed": 8, "warnings": 1, "failed": 1, "pass_rate": 80.0},
            [{"check_type": "completeness", "column": "value", "metric_value": 0.15}],
            [],
        )

        assert "Test Check" in body
        assert "Failed Checks" in body
        assert "completeness" in body

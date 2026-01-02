"""HTML output formatter."""

from pathlib import Path
from typing import Any, Dict

from jinja2 import Template

from data_quality.utils.constants import CheckStatus
from data_quality.utils.logger import get_logger

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - {{ metadata.dq_check_name }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .summary-item { display: inline-block; margin-right: 30px; }
        .pass { color: #28a745; }
        .warning { color: #ffc107; }
        .fail { color: #dc3545; }
        .error { color: #6c757d; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .status-pass { background-color: #d4edda; }
        .status-warning { background-color: #fff3cd; }
        .status-fail { background-color: #f8d7da; }
        .status-error { background-color: #e2e3e5; }
    </style>
</head>
<body>
    <h1>Data Quality Report</h1>
    <h2>{{ metadata.dq_check_name }}</h2>

    <div class="summary">
        <h3>Summary</h3>
        <div class="summary-item"><strong>Total Checks:</strong> {{ summary.total }}</div>
        <div class="summary-item"><strong class="pass">Passed:</strong> {{ summary.passed }}</div>
        <div class="summary-item"><strong class="warning">Warnings:</strong> {{ summary.warnings }}</div>
        <div class="summary-item"><strong class="fail">Failed:</strong> {{ summary.failed }}</div>
        <div class="summary-item"><strong>Pass Rate:</strong> {{ summary.pass_rate }}%</div>
    </div>

    <h3>Check Results</h3>
    <table>
        <tr>
            <th>Check Type</th>
            <th>Column</th>
            <th>Date</th>
            <th>Status</th>
            <th>Metric Value</th>
            <th>Description</th>
        </tr>
        {% for result in results %}
        <tr class="status-{{ result.status.value|lower if result.status else 'pass' }}">
            <td>{{ result.check_type }}</td>
            <td>{{ result.column }}</td>
            <td>{{ result.date }}</td>
            <td>{{ result.status.value if result.status else result.status }}</td>
            <td>{{ "%.4f"|format(result.metric_value) if result.metric_value else "N/A" }}</td>
            <td>{{ result.description or "" }}</td>
        </tr>
        {% endfor %}
    </table>

    <p style="margin-top: 20px; color: #666;">
        Generated at: {{ timestamp }}
    </p>
</body>
</html>
"""


class HTMLFormatter:
    """Format DQ results as HTML report."""

    def __init__(self):
        self.logger = get_logger("html_formatter")
        self.template = Template(HTML_TEMPLATE)

    def format(self, results: Dict[str, Any]) -> str:
        """
        Format results as HTML string.

        Args:
            results: Results dictionary

        Returns:
            HTML string
        """
        import pandas as pd

        return self.template.render(
            metadata=results.get("metadata", {}),
            summary=results.get("summary", {}),
            results=results.get("results", []),
            timestamp=pd.Timestamp.now().isoformat(),
        )

    def save(self, results: Dict[str, Any], path: str) -> str:
        """
        Save results to HTML file.

        Args:
            results: Results dictionary
            path: Output file path

        Returns:
            Path to saved file
        """
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        html_content = self.format(results)
        output_path.write_text(html_content)

        self.logger.info(f"HTML report saved to {output_path}")
        return str(output_path)

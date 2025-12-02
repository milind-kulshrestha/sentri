"""Output manager for coordinating result formatting."""

from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd

from data_quality.formatters.json_formatter import JSONFormatter
from data_quality.formatters.csv_formatter import CSVFormatter
from data_quality.formatters.html_formatter import HTMLFormatter
from data_quality.utils.logger import get_logger


class OutputManager:
    """
    Manages output generation in multiple formats.
    """

    def __init__(
        self,
        formats: List[str],
        destination: str,
        file_prefix: str = "dq_report",
        include_timestamp: bool = True,
        include_check_name: bool = True,
        pretty_print: bool = True,
    ):
        """
        Initialize Output Manager.

        Args:
            formats: List of output formats (json, csv, html, dataframe)
            destination: Output directory path
            file_prefix: Prefix for output files
            include_timestamp: Include timestamp in filename
            include_check_name: Include check name in filename
            pretty_print: Pretty print JSON output
        """
        self.formats = formats
        self.destination = Path(destination)
        self.file_prefix = file_prefix
        self.include_timestamp = include_timestamp
        self.include_check_name = include_check_name
        self.logger = get_logger("output_manager")

        # Initialize formatters
        self.json_formatter = JSONFormatter(pretty_print=pretty_print)
        self.csv_formatter = CSVFormatter()
        self.html_formatter = HTMLFormatter()

    def generate_outputs(
        self,
        results: Dict[str, Any],
        check_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Generate outputs in all configured formats.

        Args:
            results: Results dictionary
            check_name: Name of the check for filename

        Returns:
            Dict mapping format to output path
        """
        self.destination.mkdir(parents=True, exist_ok=True)

        output_paths = {}
        base_name = self._generate_filename(check_name)

        for fmt in self.formats:
            try:
                if fmt == "json":
                    path = self.destination / f"{base_name}.json"
                    output_paths["json"] = self.json_formatter.save(results, str(path))

                elif fmt == "csv":
                    path = self.destination / f"{base_name}.csv"
                    output_paths["csv"] = self.csv_formatter.save(results, str(path))

                elif fmt == "html":
                    path = self.destination / f"{base_name}.html"
                    output_paths["html"] = self.html_formatter.save(results, str(path))

                elif fmt == "dataframe":
                    # Return DataFrame directly (no file)
                    result_list = results.get("results", [])
                    output_paths["dataframe"] = pd.DataFrame(result_list)

            except Exception as e:
                self.logger.error(f"Failed to generate {fmt} output: {str(e)}")

        self.logger.info(f"Generated {len(output_paths)} outputs to {self.destination}")
        return output_paths

    def _generate_filename(self, check_name: Optional[str] = None) -> str:
        """Generate filename based on configuration."""
        parts = [self.file_prefix]

        if self.include_check_name and check_name:
            safe_name = check_name.replace(" ", "_").lower()
            parts.append(safe_name)

        if self.include_timestamp:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            parts.append(timestamp)

        return "_".join(parts)

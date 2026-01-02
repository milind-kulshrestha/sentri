"""CSV output formatter."""

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from data_quality.utils.logger import get_logger


class CSVFormatter:
    """Format DQ results as CSV."""

    def __init__(self):
        self.logger = get_logger("csv_formatter")

    def format(self, results: Dict[str, Any]) -> str:
        """
        Format results as CSV string.

        Args:
            results: Results dictionary with 'results' list

        Returns:
            CSV string
        """
        df = self._results_to_dataframe(results)
        return df.to_csv(index=False)

    def save(self, results: Dict[str, Any], path: str) -> str:
        """
        Save results to CSV file.

        Args:
            results: Results dictionary
            path: Output file path

        Returns:
            Path to saved file
        """
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df = self._results_to_dataframe(results)
        df.to_csv(output_path, index=False)

        self.logger.info(f"CSV results saved to {output_path}")
        return str(output_path)

    def _results_to_dataframe(self, results: Dict[str, Any]) -> pd.DataFrame:
        """Convert results to DataFrame."""
        result_list = results.get("results", [])

        if not result_list:
            return pd.DataFrame()

        # Flatten nested dictionaries
        flattened = []
        for result in result_list:
            flat = {}
            for key, value in result.items():
                if isinstance(value, dict):
                    for k, v in value.items():
                        flat[f"{key}_{k}"] = v
                elif isinstance(value, list):
                    flat[key] = str(value)
                elif hasattr(value, "value"):  # Enum
                    flat[key] = value.value
                else:
                    flat[key] = value
            flattened.append(flat)

        return pd.DataFrame(flattened)

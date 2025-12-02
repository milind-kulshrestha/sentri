"""JSON output formatter."""

import json
from typing import Any, Dict, Optional
from pathlib import Path
import pandas as pd

from data_quality.utils.logger import get_logger


class JSONFormatter:
    """Format DQ results as JSON."""

    def __init__(self, pretty_print: bool = True):
        self.pretty_print = pretty_print
        self.logger = get_logger("json_formatter")

    def format(self, results: Dict[str, Any]) -> str:
        """
        Format results as JSON string.

        Args:
            results: Results dictionary

        Returns:
            JSON string
        """
        # Convert any DataFrames to dicts
        serializable = self._make_serializable(results)

        if self.pretty_print:
            return json.dumps(serializable, indent=2, default=str)
        return json.dumps(serializable, default=str)

    def save(self, results: Dict[str, Any], path: str) -> str:
        """
        Save results to JSON file.

        Args:
            results: Results dictionary
            path: Output file path

        Returns:
            Path to saved file
        """
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        json_content = self.format(results)
        output_path.write_text(json_content)

        self.logger.info(f"JSON results saved to {output_path}")
        return str(output_path)

    def _make_serializable(self, obj: Any) -> Any:
        """Convert object to JSON-serializable format."""
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, (pd.Timestamp, pd.Timedelta)):
            return str(obj)
        elif hasattr(obj, 'value'):  # Enum
            return obj.value
        return obj

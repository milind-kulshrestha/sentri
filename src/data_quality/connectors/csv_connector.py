"""CSV file data connector."""

from pathlib import Path
from typing import List, Optional

import pandas as pd

from data_quality.connectors.base import DataConnector
from data_quality.connectors.registry import register_connector
from data_quality.core.exceptions import ConnectionError, DataRetrievalError


@register_connector("csv")
class CSVConnector(DataConnector):
    """
    Connector for CSV file data sources.

    Supports local file paths with date filtering and various CSV options.
    """

    def __init__(
        self,
        file_path: str,
        date_column: str,
        encoding: str = "utf-8",
        delimiter: str = ",",
        date_format: Optional[str] = None,
        parse_dates: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Initialize CSV connector.

        Args:
            file_path: Path to CSV file
            date_column: Name of the date column
            encoding: File encoding (default: utf-8)
            delimiter: Column delimiter (default: comma)
            date_format: Optional date format string
            parse_dates: Optional list of columns to parse as dates
            **kwargs: Additional configuration
        """
        super().__init__(
            file_path=file_path,
            date_column=date_column,
            encoding=encoding,
            delimiter=delimiter,
            date_format=date_format,
            parse_dates=parse_dates,
            **kwargs,
        )
        self.file_path = Path(file_path)
        self.date_column = date_column.lower()
        self.encoding = encoding
        self.delimiter = delimiter
        self.date_format = date_format
        self.parse_dates = parse_dates or [date_column]

    def validate_connection(self) -> bool:
        """
        Validate that the CSV file exists and is readable.

        Returns:
            bool: True if file exists and is readable

        Raises:
            ConnectionError: If file does not exist or is not readable
        """
        if not self.file_path.exists():
            raise ConnectionError(
                f"CSV file not found: {self.file_path}",
                connector_type="csv",
                context={"file_path": str(self.file_path)},
            )

        if not self.file_path.is_file():
            raise ConnectionError(
                f"Path is not a file: {self.file_path}",
                connector_type="csv",
                context={"file_path": str(self.file_path)},
            )

        # Try to read first few rows to validate
        try:
            pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                nrows=5,
            )
        except Exception as e:
            raise ConnectionError(
                f"Cannot read CSV file: {str(e)}",
                connector_type="csv",
                context={"file_path": str(self.file_path)},
            )

        self.logger.info(f"CSV file validated: {self.file_path}")
        return True

    def get_data(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from CSV file with date filtering.

        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            **kwargs: Additional parameters (ignored for CSV)

        Returns:
            pd.DataFrame: Filtered data with normalized column names

        Raises:
            DataRetrievalError: If data retrieval fails
        """
        try:
            self.logger.info(
                f"Reading CSV file: {self.file_path}",
                extra={"context": {"start_date": start_date, "end_date": end_date}},
            )

            # Read CSV file
            df = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
            )

            # Normalize column names
            df = self._normalize_dataframe(df)

            # Parse dates
            date_col_lower = self.date_column.lower()
            if date_col_lower in df.columns:
                df[date_col_lower] = pd.to_datetime(
                    df[date_col_lower], format=self.date_format
                )

                # Filter by date range
                start = pd.to_datetime(start_date)
                end = pd.to_datetime(end_date)

                df = df[(df[date_col_lower] >= start) & (df[date_col_lower] <= end)]

            self.logger.info(
                f"Retrieved {len(df)} rows from CSV",
                extra={"context": {"rows": len(df), "columns": len(df.columns)}},
            )

            return df

        except Exception as e:
            raise DataRetrievalError(
                f"Failed to read CSV file: {str(e)}",
                context={"file_path": str(self.file_path)},
            )

    def close(self) -> None:
        """Close connector (no-op for CSV)."""
        self.logger.debug("CSV connector closed")

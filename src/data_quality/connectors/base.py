"""Base class for all data connectors."""

import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional

import pandas as pd

from data_quality.core.exceptions import ConnectionError, DataRetrievalError
from data_quality.utils.logger import get_logger


class DataConnector(ABC):
    """
    Base class for all data connectors.

    All custom connectors must inherit from this class and implement
    the abstract methods.
    """

    def __init__(self, **config):
        """
        Initialize connector with configuration parameters.

        Args:
            **config: Source-specific configuration parameters
        """
        self.config = config
        self.connection = None
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate connection to data source.

        Returns:
            bool: True if connection is valid

        Raises:
            ConnectionError: If connection validation fails
        """
        pass

    @abstractmethod
    def get_data(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the data source.

        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            **kwargs: Additional source-specific parameters

        Returns:
            pd.DataFrame: Retrieved data with normalized column names

        Raises:
            DataRetrievalError: If data retrieval fails
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection and clean up resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    def handle_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """
        Handle connection and query errors gracefully.

        Args:
            error: The exception that occurred
            context: Additional context about the error
        """
        self.logger.error(
            f"Error in {self.__class__.__name__}: {str(error)}",
            extra={"context": context},
        )
        raise

    def retry_with_backoff(
        self,
        func: Callable[[], Any],
        max_attempts: int = 3,
        backoff_factor: float = 2.0,
    ) -> Any:
        """
        Retry a function with exponential backoff.

        Args:
            func: Function to retry
            max_attempts: Maximum number of retry attempts
            backoff_factor: Multiplier for backoff delay

        Returns:
            Result of the function call

        Raises:
            Exception: The last exception if all retries fail
        """
        last_exception: Optional[Exception] = None
        delay = 1.0

        for attempt in range(max_attempts):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    self.logger.warning(
                        f"Attempt {attempt + 1} failed: {str(e)}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                    delay *= backoff_factor

        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Retry failed with no exception captured")

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame column names to lowercase.

        Args:
            df: Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with lowercase column names
        """
        df = df.copy()
        df.columns = df.columns.str.lower()
        return df

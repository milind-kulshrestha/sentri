"""Public Sentri API.

This module provides a friendly import surface over the internal
`data_quality` package used by the Sentri data quality framework.
"""

from data_quality import __version__
from data_quality.core.framework import DataQualityFramework

__all__ = [
    "__version__",
    "DataQualityFramework",
]

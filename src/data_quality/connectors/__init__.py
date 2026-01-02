"""Data connectors for the Data Quality Framework."""

from data_quality.connectors.base import DataConnector
from data_quality.connectors.csv_connector import CSVConnector
from data_quality.connectors.registry import (
    ConnectorRegistry,
    get_connector,
    list_connectors,
    register_connector,
)

__all__ = [
    "DataConnector",
    "ConnectorRegistry",
    "register_connector",
    "get_connector",
    "list_connectors",
    "CSVConnector",
]

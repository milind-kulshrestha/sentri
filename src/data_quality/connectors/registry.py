"""Connector registry for managing data connectors."""

from typing import Dict, List, Optional, Type

from data_quality.connectors.base import DataConnector


class ConnectorRegistry:
    """
    Registry for data connectors.

    Allows dynamic registration and retrieval of connector classes.
    """

    def __init__(self):
        """Initialize empty registry."""
        self._connectors: Dict[str, Type[DataConnector]] = {}

    def register(self, name: str):
        """
        Decorator to register a connector class.

        Args:
            name: Name to register the connector under

        Returns:
            Decorator function
        """
        def decorator(cls: Type[DataConnector]) -> Type[DataConnector]:
            self._connectors[name] = cls
            return cls

        return decorator

    def get(self, name: str) -> Optional[Type[DataConnector]]:
        """
        Get a connector class by name.

        Args:
            name: Name of the connector

        Returns:
            Connector class or None if not found
        """
        return self._connectors.get(name)

    def list_connectors(self) -> List[str]:
        """
        List all registered connector names.

        Returns:
            List of connector names
        """
        return list(self._connectors.keys())


# Global registry instance
_global_registry = ConnectorRegistry()


def register_connector(name: str):
    """
    Decorator to register a connector in the global registry.

    Args:
        name: Name to register the connector under

    Returns:
        Decorator function
    """
    return _global_registry.register(name)


def get_connector(name: str) -> Optional[Type[DataConnector]]:
    """
    Get a connector class from the global registry.

    Args:
        name: Name of the connector

    Returns:
        Connector class or None if not found
    """
    return _global_registry.get(name)


def list_connectors() -> List[str]:
    """
    List all connectors in the global registry.

    Returns:
        List of connector names
    """
    return _global_registry.list_connectors()

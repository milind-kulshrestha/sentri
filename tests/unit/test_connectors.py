"""Tests for data connectors."""

import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from data_quality.connectors.base import DataConnector
from data_quality.connectors.csv_connector import CSVConnector
from data_quality.connectors.registry import (
    ConnectorRegistry,
    get_connector,
    register_connector,
)
from data_quality.core.exceptions import ConnectionError, DataRetrievalError


class TestDataConnectorBase:
    """Tests for base DataConnector class."""

    def test_cannot_instantiate_abstract(self):
        """Test that base class cannot be instantiated."""
        with pytest.raises(TypeError):
            DataConnector()

    def test_subclass_must_implement_methods(self):
        """Test that subclass must implement abstract methods."""

        class IncompleteConnector(DataConnector):
            pass

        with pytest.raises(TypeError):
            IncompleteConnector()


class TestConnectorRegistry:
    """Tests for ConnectorRegistry."""

    def test_register_connector(self):
        """Test registering a connector."""
        registry = ConnectorRegistry()

        @registry.register("test_connector")
        class TestConnector(DataConnector):
            def validate_connection(self):
                return True

            def get_data(self, start_date, end_date, **kwargs):
                return pd.DataFrame()

            def close(self):
                pass

        assert "test_connector" in registry._connectors
        assert registry.get("test_connector") == TestConnector

    def test_get_nonexistent_connector(self):
        """Test getting a non-existent connector."""
        registry = ConnectorRegistry()
        result = registry.get("nonexistent")
        assert result is None

    def test_list_connectors(self):
        """Test listing registered connectors."""
        registry = ConnectorRegistry()

        @registry.register("conn1")
        class Conn1(DataConnector):
            def validate_connection(self):
                return True

            def get_data(self, start_date, end_date, **kwargs):
                return pd.DataFrame()

            def close(self):
                pass

        @registry.register("conn2")
        class Conn2(DataConnector):
            def validate_connection(self):
                return True

            def get_data(self, start_date, end_date, **kwargs):
                return pd.DataFrame()

            def close(self):
                pass

        connectors = registry.list_connectors()
        assert "conn1" in connectors
        assert "conn2" in connectors


class TestCSVConnector:
    """Tests for CSVConnector."""

    @pytest.fixture
    def sample_csv(self):
        """Create a sample CSV file."""
        content = """entity_id,effective_date,value,category
1,2025-01-01,100,A
2,2025-01-01,200,B
3,2025-01-02,150,A
4,2025-01-02,250,B
5,2025-01-03,175,A
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = f.name
        yield temp_path
        os.unlink(temp_path)

    def test_validate_connection_success(self, sample_csv):
        """Test successful connection validation."""
        connector = CSVConnector(file_path=sample_csv, date_column="effective_date")
        assert connector.validate_connection() is True

    def test_validate_connection_file_not_found(self):
        """Test validation with non-existent file."""
        connector = CSVConnector(file_path="/nonexistent/file.csv", date_column="date")
        with pytest.raises(ConnectionError):
            connector.validate_connection()

    def test_get_data_all_dates(self, sample_csv):
        """Test getting all data."""
        connector = CSVConnector(file_path=sample_csv, date_column="effective_date")
        df = connector.get_data(start_date="2025-01-01", end_date="2025-01-03")

        assert len(df) == 5
        assert "entity_id" in df.columns
        assert "effective_date" in df.columns

    def test_get_data_filtered_dates(self, sample_csv):
        """Test getting data with date filter."""
        connector = CSVConnector(file_path=sample_csv, date_column="effective_date")
        df = connector.get_data(start_date="2025-01-01", end_date="2025-01-02")

        assert len(df) == 4  # Only Jan 1 and Jan 2

    def test_get_data_normalizes_columns(self, sample_csv):
        """Test that column names are normalized to lowercase."""
        # Create CSV with uppercase columns
        content = """Entity_ID,Effective_Date,Value
1,2025-01-01,100
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            temp_path = f.name

        try:
            connector = CSVConnector(file_path=temp_path, date_column="Effective_Date")
            df = connector.get_data(start_date="2025-01-01", end_date="2025-01-01")

            # Check columns are lowercase
            assert "entity_id" in df.columns
            assert "effective_date" in df.columns
            assert "value" in df.columns
        finally:
            os.unlink(temp_path)

    def test_get_data_with_custom_delimiter(self):
        """Test reading CSV with custom delimiter."""
        content = """entity_id;date;value
1;2025-01-01;100
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            temp_path = f.name

        try:
            connector = CSVConnector(
                file_path=temp_path, date_column="date", delimiter=";"
            )
            df = connector.get_data(start_date="2025-01-01", end_date="2025-01-01")

            assert len(df) == 1
            assert df.iloc[0]["value"] == 100
        finally:
            os.unlink(temp_path)

    def test_get_data_with_encoding(self):
        """Test reading CSV with specific encoding."""
        content = "entity_id,date,name\n1,2025-01-01,José\n"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            connector = CSVConnector(
                file_path=temp_path, date_column="date", encoding="utf-8"
            )
            df = connector.get_data(start_date="2025-01-01", end_date="2025-01-01")

            assert df.iloc[0]["name"] == "José"
        finally:
            os.unlink(temp_path)

    def test_close(self, sample_csv):
        """Test closing connector."""
        connector = CSVConnector(file_path=sample_csv, date_column="effective_date")
        # Should not raise
        connector.close()

    def test_context_manager(self, sample_csv):
        """Test using connector as context manager."""
        with CSVConnector(
            file_path=sample_csv, date_column="effective_date"
        ) as connector:
            df = connector.get_data(start_date="2025-01-01", end_date="2025-01-03")
            assert len(df) == 5

    def test_empty_result(self, sample_csv):
        """Test getting data with no matching dates."""
        connector = CSVConnector(file_path=sample_csv, date_column="effective_date")
        df = connector.get_data(start_date="2024-01-01", end_date="2024-01-01")

        assert len(df) == 0

    def test_date_parsing(self, sample_csv):
        """Test that dates are properly parsed."""
        connector = CSVConnector(file_path=sample_csv, date_column="effective_date")
        df = connector.get_data(start_date="2025-01-01", end_date="2025-01-03")

        assert pd.api.types.is_datetime64_any_dtype(df["effective_date"])


class TestGlobalRegistry:
    """Tests for global registry functions."""

    def test_register_connector_decorator(self):
        """Test global register_connector decorator."""

        @register_connector("global_test")
        class GlobalTestConnector(DataConnector):
            def validate_connection(self):
                return True

            def get_data(self, start_date, end_date, **kwargs):
                return pd.DataFrame()

            def close(self):
                pass

        connector_class = get_connector("global_test")
        assert connector_class == GlobalTestConnector

    def test_get_connector_csv(self):
        """Test getting CSV connector from global registry."""
        connector_class = get_connector("csv")
        assert connector_class == CSVConnector

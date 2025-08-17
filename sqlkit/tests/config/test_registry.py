"""
Tests for sqlkit.config.registry module.

This module tests table registry functionality.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from pydantic import ValidationError

from sqlkit.config.registry import TableRegistry
from sqlkit.core.table import SQLTable

FILE = Path(__file__).parent / "file" / "config.yaml"


class TestTableRegistry:
    """Test TableRegistry functionality."""

    @pytest.fixture
    def registry(self):
        """Create TableRegistry instance."""
        return TableRegistry.from_file(FILE)

    def test_registry_initialization(self):
        """Test registry initialization."""
        registry = TableRegistry.from_file(FILE)

        assert registry.loader is not None
        assert registry._table_cache == {}

    def test_get_table_postgresql(self, registry):
        """Test getting PostgreSQL table."""
        table = registry.get_table("users")

        assert isinstance(table, SQLTable)
        assert table.name == "users"
        assert table.schema == "analytics"

        # Verify columns
        columns = table.columns
        assert len(columns) == 3
        column_names = [col.name for col in columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names

    def test_get_table_redshift(self, registry):
        """Test getting Redshift table."""
        table = registry.get_table("redshift_sales")

        assert isinstance(table, SQLTable)
        assert table.name == "redshift_sales"

        # Check Redshift-specific attributes
        assert hasattr(table, "sort_keys")
        assert hasattr(table, "dist_key")
        assert hasattr(table, "dist_style")

    def test_get_table_caching(self, registry):
        """Test that tables are cached."""
        table1 = registry.get_table("users")
        table2 = registry.get_table("users")

        # Should be the same instance (cached)
        assert table1 is table2

    def test_get_table_not_found(self, registry):
        """Test getting non-existent table."""
        with pytest.raises(KeyError, match="Table 'nonexistent' not found"):
            registry.get_table("nonexistent")

    def test_list_tables(self, registry):
        """Test listing available tables."""
        tables = registry.list_tables()

        assert isinstance(tables, list)
        assert "users" in tables
        assert "redshift_sales" in tables
        assert len(tables) == 2

    def test_clear_cache(self, registry):
        """Test clearing table cache."""
        # Get a table to populate cache
        table1 = registry.get_table("users")
        assert "users" in registry._table_cache

        # Clear cache
        registry.clear_cache()
        assert registry._table_cache == {}

        # Get table again - should be new instance
        table2 = registry.get_table("users")
        assert table1 is not table2

    def test_reload_config(self, registry):
        """Test reloading configuration."""
        # Get initial table
        table1 = registry.get_table("users")

        # Reload config
        registry.reload_config()

        # Cache should be cleared
        assert registry._table_cache == {}

        # Get table again - should be new instance
        table2 = registry.get_table("users")
        assert table1 is not table2

    @patch("sqlkit.config.registry.Table")
    def test_create_table_from_config(self, mock_table_factory, registry):
        """Test table creation from configuration."""
        mock_table_instance = Mock()
        mock_table_factory.return_value = mock_table_instance

        # Get table config
        table_config = registry.loader.get_table_config("users")

        # Create table
        registry._create_table_from_config("users", table_config)

        # Verify factory was called with correct arguments
        mock_table_factory.assert_called_once()
        call_args = mock_table_factory.call_args

        # Check table name and columns
        assert call_args[0][0] == "users"  # table name
        assert len(call_args[0]) == 4  # name + 3 columns

        # Check keyword arguments
        kwargs = call_args[1]
        assert kwargs["dialect"] == "postgresql"
        assert kwargs["schema"] == "analytics"
        assert "_yaml_config" in kwargs

    def test_table_has_yaml_config(self, registry):
        """Test that created table has YAML configuration."""
        table = registry.get_table("users")

        assert hasattr(table, "_yaml_config")
        assert table._yaml_config is not None
        assert "dialect_methods" in table._yaml_config

    def test_invalid_column_type_error(self):
        """Test error handling for invalid column type."""
        # Create config with invalid column type
        invalid_config = {
            "tables": {
                "invalid_table": {
                    "dialect": "postgresql",
                    "columns": [
                        {
                            "name": "id",
                            "type": "InvalidType",  # Invalid type
                        }
                    ],
                }
            }
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(invalid_config, f)
            invalid_file = Path(f.name)

        with pytest.raises(ValidationError, match="Unsupported column type"):
            TableRegistry.from_file(invalid_file)


class TestTableRegistryIntegration:
    """Integration tests for TableRegistry with actual table creation."""

    @pytest.fixture
    def integration_config_dict(self):
        """Configuration for integration testing."""
        return {
            "tables": {
                "simple_table": {
                    "dialect": "sqlite",
                    "columns": [
                        {
                            "name": "id",
                            "type": "Integer",
                            "primary_key": True,
                        },
                        {
                            "name": "name",
                            "type": "String",
                            "length": 100,
                        },
                    ],
                }
            }
        }

    @pytest.fixture
    def integration_config_file(self, integration_config_dict):
        """Create temporary config file for integration testing."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(integration_config_dict, f)
            return Path(f.name)

    def test_end_to_end_table_creation(self, integration_config_file):
        """Test complete end-to-end table creation."""
        registry = TableRegistry.from_file(integration_config_file)
        table = registry.get_table("simple_table")

        # Verify table properties
        assert table.name == "simple_table"
        assert len(table.columns) == 2

        # Verify we can generate SQL
        create_sql = table.create().compile()
        assert "CREATE TABLE" in create_sql.upper()
        assert "SIMPLE_TABLE" in create_sql.upper()
        assert "ID" in create_sql.upper()
        assert "NAME" in create_sql.upper()

    def test_table_operations_work(self, integration_config_file):
        """Test that table operations work correctly."""
        registry = TableRegistry.from_file(integration_config_file)
        table = registry.get_table("simple_table")

        # Test basic operations
        select_sql = table.select().compile()
        assert "SELECT" in select_sql.upper()

        insert_sql = table.insert(id=1, name="Test").compile()
        assert "INSERT" in insert_sql.upper()

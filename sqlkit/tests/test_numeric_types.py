"""
Tests for Numeric data type support.

This module tests Numeric type functionality across different dialects
and configuration methods.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sqlkit import Column, Integer, Numeric, Table, from_config


class TestNumericTypes:
    """Test Numeric data type functionality."""

    def test_numeric_with_precision_and_scale(self):
        """Test Numeric type with both precision and scale."""
        table = Table(
            "financial_data",
            Column("id", Integer, primary_key=True),
            Column("amount", Numeric(18, 5)),
            dialect="postgresql",
        )

        create_sql = str(table.create().compile())
        assert "NUMERIC(18, 5)" in create_sql.upper()

    def test_numeric_with_precision_only(self):
        """Test Numeric type with precision only."""
        table = Table(
            "financial_data",
            Column("id", Integer, primary_key=True),
            Column("count", Numeric(10)),
            dialect="postgresql",
        )

        create_sql = str(table.create().compile())
        assert "NUMERIC(10)" in create_sql.upper()

    def test_numeric_without_parameters(self):
        """Test Numeric type without parameters."""
        table = Table(
            "financial_data",
            Column("id", Integer, primary_key=True),
            Column("value", Numeric()),
            dialect="postgresql",
        )

        create_sql = str(table.create().compile())
        assert "NUMERIC" in create_sql.upper()

    @pytest.mark.parametrize(
        "dialect",
        ["mysql", "postgresql", "sqlite", "redshift", "oracle"],
    )
    def test_numeric_across_dialects(self, dialect):
        """Test Numeric type works across different dialects."""
        table = Table(
            "test_table",
            Column("id", Integer, primary_key=True),
            Column("price", Numeric(10, 2)),
            dialect=dialect,
        )

        create_sql = str(table.create().compile())
        # All dialects should support some form of numeric type
        assert any(
            keyword in create_sql.upper()
            for keyword in ["NUMERIC", "DECIMAL", "NUMBER"]
        )


class TestNumericYamlConfig:
    """Test Numeric type with YAML configuration."""

    @pytest.fixture
    def numeric_config_dict(self):
        """Sample configuration with Numeric types."""
        return {
            "tables": {
                "financial_data": {
                    "dialect": "postgresql",
                    "columns": [
                        {"name": "id", "type": "Integer", "primary_key": True},
                        {
                            "name": "amount",
                            "type": "Numeric",
                            "precision": 18,
                            "scale": 5,
                        },
                        {
                            "name": "rate",
                            "type": "Numeric",
                            "precision": 10,
                            "scale": 4,
                        },
                        {
                            "name": "count",
                            "type": "Numeric",
                            "precision": 10,
                        },
                        {
                            "name": "simple_numeric",
                            "type": "Numeric",
                        },
                    ],
                }
            }
        }

    @pytest.fixture
    def numeric_config_file(self, numeric_config_dict):
        """Create temporary YAML config file for Numeric testing."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(numeric_config_dict, f)
            return Path(f.name)

    def test_yaml_numeric_with_precision_and_scale(self, numeric_config_file):
        """Test YAML-configured Numeric with precision and scale."""
        table = from_config("financial_data", numeric_config_file)
        create_sql = str(table.create().compile())

        assert "NUMERIC(18, 5)" in create_sql.upper()
        assert "NUMERIC(10, 4)" in create_sql.upper()

    def test_yaml_numeric_with_precision_only(self, numeric_config_file):
        """Test YAML-configured Numeric with precision only."""
        table = from_config("financial_data", numeric_config_file)
        create_sql = str(table.create().compile())

        assert "NUMERIC(10)" in create_sql.upper()

    def test_yaml_numeric_without_parameters(self, numeric_config_file):
        """Test YAML-configured Numeric without parameters."""
        table = from_config("financial_data", numeric_config_file)
        create_sql = str(table.create().compile())

        # Should have at least one basic NUMERIC without parameters
        assert "NUMERIC" in create_sql.upper()

    def test_yaml_validation_invalid_precision(self):
        """Test that invalid precision values are handled."""
        config_dict = {
            "tables": {
                "test_table": {
                    "dialect": "postgresql",
                    "columns": [
                        {
                            "name": "invalid_col",
                            "type": "Numeric",
                            "precision": "invalid",  # Should be int
                        }
                    ],
                }
            }
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(config_dict, f)
            invalid_file = Path(f.name)

        # Should raise ValidationError due to invalid precision type
        with pytest.raises(
            Exception
        ):  # Could be ValidationError or ValueError
            from_config("test_table", invalid_file)


class TestNumericEdgeCases:
    """Test edge cases for Numeric type support."""

    def test_numeric_column_in_select(self):
        """Test that Numeric columns work in SELECT statements."""
        table = Table(
            "financial_data",
            Column("id", Integer, primary_key=True),
            Column("amount", Numeric(18, 5)),
            dialect="postgresql",
        )

        select_sql = str(table.select().compile())
        assert "amount" in select_sql

    def test_numeric_column_in_where_clause(self):
        """Test that Numeric columns work in WHERE clauses."""
        table = Table(
            "financial_data",
            Column("id", Integer, primary_key=True),
            Column("amount", Numeric(18, 5)),
            dialect="postgresql",
        )

        where_sql = str(
            table.select().where(table.c.amount > 1000.50).compile()
        )
        assert "amount" in where_sql
        assert "1000.5" in where_sql or "1000.50" in where_sql

    def test_multiple_numeric_columns(self):
        """Test table with multiple Numeric columns."""
        table = Table(
            "financial_data",
            Column("id", Integer, primary_key=True),
            Column("price", Numeric(10, 2)),
            Column("tax_rate", Numeric(5, 4)),
            Column("discount", Numeric(3, 2)),
            dialect="postgresql",
        )

        create_sql = str(table.create().compile())
        assert "NUMERIC(10, 2)" in create_sql.upper()
        assert "NUMERIC(5, 4)" in create_sql.upper()
        assert "NUMERIC(3, 2)" in create_sql.upper()

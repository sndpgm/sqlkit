"""
Tests for flexible column type specification.

This module tests the enhanced Column class and TypeParser functionality
for string-based type specifications with various aliases.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sqlkit import Column, Integer, Numeric, String, Table, from_config
from sqlkit.core.type_parser import TypeParser, parse_column_type


class TestTypeParser:
    """Test TypeParser functionality."""

    def test_simple_type_aliases(self):
        """Test simple type name aliases."""
        # Integer aliases
        assert TypeParser.parse_type_spec("int") == Integer
        assert TypeParser.parse_type_spec("integer") == Integer
        assert TypeParser.parse_type_spec("INT") == Integer
        assert TypeParser.parse_type_spec("Integer") == Integer

        # String aliases
        assert TypeParser.parse_type_spec("str") == String
        assert TypeParser.parse_type_spec("string") == String
        assert TypeParser.parse_type_spec("varchar") == String
        assert TypeParser.parse_type_spec("VARCHAR") == String

    def test_string_with_length(self):
        """Test string types with length parameters."""
        # Various string type aliases with length
        str_100 = TypeParser.parse_type_spec("string(100)")
        assert isinstance(str_100, String)
        assert str_100.length == 100

        varchar_255 = TypeParser.parse_type_spec("varchar(255)")
        assert isinstance(varchar_255, String)
        assert varchar_255.length == 255

        str_50 = TypeParser.parse_type_spec("str(50)")
        assert isinstance(str_50, String)
        assert str_50.length == 50

    def test_numeric_with_precision_and_scale(self):
        """Test numeric types with precision and scale."""
        # Numeric with precision and scale
        num_18_5 = TypeParser.parse_type_spec("numeric(18,5)")
        assert isinstance(num_18_5, Numeric)
        assert num_18_5.precision == 18
        assert num_18_5.scale == 5

        # Decimal alias
        dec_10_2 = TypeParser.parse_type_spec("decimal(10,2)")
        assert isinstance(dec_10_2, Numeric)
        assert dec_10_2.precision == 10
        assert dec_10_2.scale == 2

    def test_numeric_with_precision_only(self):
        """Test numeric types with precision only."""
        num_10 = TypeParser.parse_type_spec("numeric(10)")
        assert isinstance(num_10, Numeric)
        assert num_10.precision == 10
        assert num_10.scale is None

    def test_invalid_type_spec(self):
        """Test invalid type specifications."""
        with pytest.raises(ValueError, match="Unknown column type"):
            TypeParser.parse_type_spec("invalid_type")

        with pytest.raises(ValueError, match="Invalid length parameter"):
            TypeParser.parse_type_spec("string(invalid)")

    def test_already_parsed_type(self):
        """Test that already-parsed types are returned as-is."""
        assert TypeParser.parse_type_spec(Integer) == Integer
        assert TypeParser.parse_type_spec(String) == String


class TestFlexibleColumn:
    """Test enhanced Column class with string type specifications."""

    def test_column_with_string_types(self):
        """Test Column creation with string type specifications."""
        # Integer column
        col_int = Column("id", "int", primary_key=True)
        assert col_int.name == "id"
        assert isinstance(col_int.type, Integer)
        assert col_int.primary_key is True

        # String column with length
        col_str = Column("name", "string(255)", nullable=False)
        assert col_str.name == "name"
        assert isinstance(col_str.type, String)
        assert col_str.type.length == 255
        assert col_str.nullable is False

    def test_column_with_numeric_types(self):
        """Test Column creation with numeric type specifications."""
        # Numeric with precision and scale
        col_num = Column("amount", "numeric(18,5)")
        assert col_num.name == "amount"
        assert isinstance(col_num.type, Numeric)
        assert col_num.type.precision == 18
        assert col_num.type.scale == 5

    def test_column_with_various_aliases(self):
        """Test Column creation with various type aliases."""
        aliases_to_test = [
            ("id", "int", Integer),
            ("count", "INTEGER", Integer),
            ("name", "str", String),
            ("description", "varchar", String),
            ("amount", "decimal", Numeric),
            ("rate", "NUMERIC", Numeric),
        ]

        for name, type_spec, expected_type in aliases_to_test:
            col = Column(name, type_spec)
            assert col.name == name
            assert isinstance(col.type, expected_type)


class TestTableWithFlexibleTypes:
    """Test Table creation with flexible column types."""

    def test_table_with_string_types(self):
        """Test table creation with string type specifications."""
        table = Table(
            "users",
            Column("id", "int", primary_key=True),
            Column("name", "string(255)", nullable=False),
            Column("email", "varchar(255)", unique=True),
            Column("amount", "numeric(18,5)"),
            Column("is_active", "bool", default=True),
            dialect="postgresql",
        )

        # Verify table creation
        create_sql = str(table.create().compile())
        assert "INTEGER" in create_sql.upper()
        assert (
            "VARCHAR(255)" in create_sql.upper()
            or "CHARACTER VARYING(255)" in create_sql.upper()
        )
        assert "NUMERIC(18, 5)" in create_sql.upper()
        assert "BOOLEAN" in create_sql.upper()

    def test_table_cross_dialect_compatibility(self):
        """Test that flexible types work across dialects."""
        for dialect in ["mysql", "postgresql", "sqlite", "redshift"]:
            table = Table(
                "test_table",
                Column("id", "int", primary_key=True),
                Column("name", "string(100)"),
                Column("amount", "numeric(10,2)"),
                dialect=dialect,
            )

            # Should generate SQL without errors
            create_sql = str(table.create().compile())
            assert len(create_sql) > 0


class TestYamlWithFlexibleTypes:
    """Test YAML configuration with flexible type specifications."""

    @pytest.fixture
    def flexible_types_config(self):
        """Configuration using flexible type specifications."""
        return {
            "tables": {
                "users": {
                    "dialect": "postgresql",
                    "columns": [
                        {"name": "id", "type": "int", "primary_key": True},
                        {"name": "name", "type": "varchar", "length": 255},
                        {"name": "email", "type": "string", "length": 100},
                        {
                            "name": "amount",
                            "type": "numeric",
                            "precision": 18,
                            "scale": 5,
                        },
                        {"name": "count", "type": "decimal", "precision": 10},
                        {
                            "name": "is_active",
                            "type": "bool",
                            "nullable": False,
                        },
                    ],
                }
            }
        }

    @pytest.fixture
    def flexible_config_file(self, flexible_types_config):
        """Create temporary YAML file with flexible types."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(flexible_types_config, f)
            return Path(f.name)

    def test_yaml_with_flexible_types(self, flexible_config_file):
        """Test YAML configuration with flexible type aliases."""
        table = from_config("users", flexible_config_file)

        # Verify table creation
        create_sql = str(table.create().compile())
        assert "INTEGER" in create_sql.upper()
        assert (
            "VARCHAR(255)" in create_sql.upper()
            or "CHARACTER VARYING(255)" in create_sql.upper()
        )
        assert (
            "VARCHAR(100)" in create_sql.upper()
            or "CHARACTER VARYING(100)" in create_sql.upper()
        )
        assert "NUMERIC(18, 5)" in create_sql.upper()
        assert "NUMERIC(10)" in create_sql.upper()
        assert "BOOLEAN" in create_sql.upper()

    def test_yaml_mixed_type_formats(self):
        """Test YAML with mixed old and new type formats."""
        config = {
            "tables": {
                "mixed_table": {
                    "dialect": "postgresql",
                    "columns": [
                        {
                            "name": "id",
                            "type": "Integer",
                            "primary_key": True,
                        },  # Old format
                        {
                            "name": "name",
                            "type": "str",
                            "length": 255,
                        },  # New format
                        {
                            "name": "amount",
                            "type": "Numeric",
                            "precision": 10,
                            "scale": 2,
                        },  # Old format
                        {"name": "count", "type": "int"},  # New format
                    ],
                }
            }
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(config, f)
            config_file = Path(f.name)

        try:
            table = from_config("mixed_table", config_file)
            create_sql = str(table.create().compile())

            # Should contain expected SQL elements
            assert "INTEGER" in create_sql.upper()
            assert (
                "VARCHAR(255)" in create_sql.upper()
                or "CHARACTER VARYING(255)" in create_sql.upper()
            )
            assert "NUMERIC(10, 2)" in create_sql.upper()

        finally:
            config_file.unlink()


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_invalid_yaml_type_spec(self):
        """Test error handling for invalid type specifications in YAML."""
        config = {
            "tables": {
                "invalid_table": {
                    "dialect": "postgresql",
                    "columns": [
                        {"name": "bad_col", "type": "invalid_type_name"}
                    ],
                }
            }
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(config, f)
            config_file = Path(f.name)

        try:
            with pytest.raises(
                Exception
            ):  # Could be ValidationError or ValueError
                from_config("invalid_table", config_file)
        finally:
            config_file.unlink()

    def test_case_insensitive_types(self):
        """Test that type specifications are case-insensitive."""
        variations = [
            ("int", Integer),
            ("INT", Integer),
            ("Integer", Integer),
            ("string", String),
            ("STRING", String),
            ("varchar", String),
            ("VARCHAR", String),
        ]

        for type_spec, expected_type in variations:
            parsed = parse_column_type(type_spec)
            assert parsed == expected_type

    def test_whitespace_handling(self):
        """Test that whitespace in type specifications is handled correctly."""
        # Test various whitespace scenarios
        specs_to_test = [
            "string( 100 )",
            "numeric( 18 , 5 )",
            "varchar(255)",
            " int ",
        ]

        for spec in specs_to_test:
            # Should not raise an exception
            parsed = parse_column_type(spec)
            assert parsed is not None

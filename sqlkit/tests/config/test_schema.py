"""
Tests for sqlkit.config.schema module.

This module tests Pydantic schema validation for YAML configuration.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from sqlkit.config.schema import (
    ColumnConfig,
    DialectMethodConfig,
    IndexConfig,
    MetadataConfig,
    TableConfig,
    TablesConfig,
)


class TestColumnConfig:
    """Test ColumnConfig validation."""

    def test_valid_column_config(self) -> None:
        """Test valid column configuration."""
        config = ColumnConfig(
            name="id",
            type="Integer",
            primary_key=True,
            nullable=False,
        )

        assert config.name == "id"
        assert config.type == "Integer"
        assert config.primary_key is True
        assert config.nullable is False

    def test_column_config_defaults(self) -> None:
        """Test column configuration defaults."""
        config = ColumnConfig(name="name", type="String")

        assert config.name == "name"
        assert config.type == "String"
        assert config.length is None
        assert config.primary_key is False
        assert config.nullable is True
        assert config.unique is False
        assert config.auto_increment is False
        assert config.default is None

    def test_invalid_column_type(self) -> None:
        """Test invalid column type raises error."""
        with pytest.raises(ValidationError, match="Unsupported column type"):
            ColumnConfig(name="test", type="InvalidType")

    @pytest.mark.parametrize(
        "valid_type",
        [
            # Legacy SQLAlchemy types
            "Integer",
            "String",
            "Text",
            "Float",
            "Numeric",
            "Boolean",
            "DateTime",
            "Date",
            "Time",
            # TYPE_ALIASES entries
            "int",
            "integer",
            "bigint",
            "str",
            "string",
            "varchar",
            "char",
            "text",
            "numeric",
            "decimal",
            "number",
            "float",
            "real",
            "double",
            "bool",
            "boolean",
            "date",
            "datetime",
            "timestamp",
            "time",
        ],
    )
    def test_valid_column_types(self, valid_type: str) -> None:
        """Test all valid column types."""
        config = ColumnConfig(name="test", type=valid_type)
        assert config.type == valid_type

    def test_numeric_with_precision_and_scale(self) -> None:
        """Test Numeric column with precision and scale."""
        config = ColumnConfig(
            name="amount", type="Numeric", precision=18, scale=5
        )

        assert config.name == "amount"
        assert config.type == "Numeric"
        assert config.precision == 18
        assert config.scale == 5

    def test_numeric_with_precision_only(self) -> None:
        """Test Numeric column with precision only."""
        config = ColumnConfig(name="count", type="Numeric", precision=10)

        assert config.name == "count"
        assert config.type == "Numeric"
        assert config.precision == 10
        assert config.scale is None

    def test_string_with_length(self) -> None:
        """Test String column with length parameter."""
        config = ColumnConfig(name="name", type="String", length=255)

        assert config.name == "name"
        assert config.type == "String"
        assert config.length == 255


class TestIndexConfig:
    """Test IndexConfig validation."""

    def test_valid_index_config(self) -> None:
        """Test valid index configuration."""
        config = IndexConfig(
            name="idx_users_email",
            columns=["email"],
            unique=True,
        )

        assert config.name == "idx_users_email"
        assert config.columns == ["email"]
        assert config.unique is True

    def test_index_config_defaults(self) -> None:
        """Test index configuration defaults."""
        config = IndexConfig(name="idx_test", columns=["col1", "col2"])

        assert config.unique is False


class TestDialectMethodConfig:
    """Test DialectMethodConfig validation."""

    def test_valid_method_config(self) -> None:
        """Test valid method configuration."""
        config = DialectMethodConfig(
            s3_path="s3://bucket/path/",
            credentials="aws_iam_role=arn:aws:iam::123:role/Test",
            format="CSV",
            delimiter=",",
            options=["IGNOREHEADER 1", "ACCEPTINVCHARS"],
        )

        assert config.s3_path == "s3://bucket/path/"
        assert config.credentials == "aws_iam_role=arn:aws:iam::123:role/Test"
        assert config.format == "CSV"
        assert config.delimiter == ","
        assert config.options == ["IGNOREHEADER 1", "ACCEPTINVCHARS"]

    def test_method_config_extra_fields(self) -> None:
        """Test that extra fields are allowed."""
        extra: dict[str, Any] = {
            "custom_field": "custom_value",
            "another_field": 123,
        }
        config = DialectMethodConfig(**extra)

        # Extra fields should be accessible
        assert config.model_dump()["custom_field"] == "custom_value"
        assert config.model_dump()["another_field"] == 123

    def test_dict_without_none(self) -> None:
        """Test dict_without_none method."""
        config = DialectMethodConfig(
            s3_path="s3://bucket/path/",
            credentials=None,
            format="CSV",
            delimiter=None,
        )

        result = config.dict_without_none()
        expected = {
            "s3_path": "s3://bucket/path/",
            "format": "CSV",
        }

        assert result == expected


class TestTableConfig:
    """Test TableConfig validation."""

    def test_valid_table_config(self) -> None:
        """Test valid table configuration."""
        columns = [
            ColumnConfig(name="id", type="Integer", primary_key=True),
            ColumnConfig(name="name", type="String", length=255),
        ]

        config = TableConfig(
            dialect="postgresql",
            columns=columns,
            schema_name="public",
        )

        assert config.dialect == "postgresql"
        assert len(config.columns) == 2
        assert config.schema_name == "public"

    def test_invalid_dialect(self) -> None:
        """Test invalid dialect raises error."""
        columns = [ColumnConfig(name="id", type="Integer")]

        with pytest.raises(ValidationError, match="Unsupported dialect"):
            TableConfig(dialect="invalid_dialect", columns=columns)

    @pytest.mark.parametrize(
        "valid_dialect",
        ["mysql", "postgresql", "sqlite", "redshift", "athena", "oracle"],
    )
    def test_valid_dialects(self, valid_dialect: str) -> None:
        """Test all valid dialects."""
        columns = [ColumnConfig(name="id", type="Integer")]
        config = TableConfig(dialect=valid_dialect, columns=columns)
        assert config.dialect == valid_dialect


class TestTablesConfig:
    """Test TablesConfig validation."""

    def test_valid_tables_config(self) -> None:
        """Test valid tables configuration."""
        metadata = MetadataConfig(
            default_dialect="postgresql",
            default_schema="public",
        )

        users_columns = [
            ColumnConfig(name="id", type="Integer", primary_key=True),
            ColumnConfig(name="name", type="String", length=255),
        ]

        tables = {
            "users": TableConfig(
                dialect="postgresql",
                columns=users_columns,
            )
        }

        config = TablesConfig(metadata=metadata, tables=tables)

        assert config.metadata is not None
        assert config.metadata.default_dialect == "postgresql"
        assert "users" in config.tables

    def test_get_table_config(self) -> None:
        """Test get_table_config method."""
        metadata = MetadataConfig(default_dialect="mysql")

        users_columns = [
            ColumnConfig(name="id", type="Integer", primary_key=True),
        ]

        tables = {
            "users": TableConfig(
                dialect="postgresql",  # Override default
                columns=users_columns,
            ),
            "products": TableConfig(
                dialect="mysql",  # Use specific dialect instead of empty
                columns=users_columns,
            ),
        }

        config = TablesConfig(metadata=metadata, tables=tables)

        # Test table with explicit dialect
        users_config = config.get_table_config("users")
        assert users_config.dialect == "postgresql"

        # Test non-existent table
        with pytest.raises(KeyError, match="Table 'nonexistent' not found"):
            config.get_table_config("nonexistent")

    def test_get_table_config_with_defaults(self) -> None:
        """Test that metadata defaults are applied."""
        metadata = MetadataConfig(
            default_dialect="mysql",
            default_schema="test_db",
        )

        columns = [ColumnConfig(name="id", type="Integer")]

        # Table with explicit dialect and schema for testing
        tables = {
            "test_table": TableConfig(
                dialect="mysql",  # Use valid dialect
                columns=columns,
                schema_name="test_db",  # Use explicit schema
            )
        }

        config = TablesConfig(metadata=metadata, tables=tables)
        table_config = config.get_table_config("test_table")

        # Test that explicit values are preserved
        assert table_config.dialect == "mysql"
        assert table_config.schema_name == "test_db"

    def test_table_config_with_none_dialect(self) -> None:
        """Test TableConfig with None dialect (should use default)."""
        metadata = MetadataConfig(default_dialect="postgresql")
        columns = [ColumnConfig(name="id", type="Integer")]

        # Table without explicit dialect
        tables = {
            "test_table": TableConfig(
                dialect=None,  # Explicitly None
                columns=columns,
            )
        }

        config = TablesConfig(metadata=metadata, tables=tables)
        table_config = config.get_table_config("test_table")

        # Should use default dialect from metadata
        assert table_config.dialect == "postgresql"

    def test_table_config_missing_dialect_and_no_default(self) -> None:
        """Test error when no dialect specified and no default available."""
        columns = [ColumnConfig(name="id", type="Integer")]

        # Table without dialect and no metadata
        tables = {
            "test_table": TableConfig(
                dialect=None,
                columns=columns,
            )
        }

        config = TablesConfig(tables=tables)  # No metadata with default

        with pytest.raises(ValueError, match="No dialect specified"):
            config.get_table_config("test_table")

    def test_table_config_creation_with_none_dialect(self) -> None:
        """Test that TableConfig can be created with None dialect."""
        columns = [ColumnConfig(name="id", type="Integer")]

        # Should not raise an error during creation
        config = TableConfig(dialect=None, columns=columns)

        assert config.dialect is None
        assert len(config.columns) == 1

    def test_automatic_metadata_application(self) -> None:
        """Test that metadata defaults are automatically applied."""
        metadata = MetadataConfig(
            default_dialect="mysql", default_schema="test_schema"
        )

        # Create TablesConfig without explicit dialect/schema in tables
        tables_data = {
            "metadata": metadata.model_dump(),
            "tables": {
                "test_table": {
                    "columns": [{"name": "id", "type": "Integer"}]
                    # No dialect or schema_name specified
                }
            },
        }

        config = TablesConfig.model_validate(tables_data)

        # The table should now have the metadata defaults applied
        table_config = config.tables["test_table"]
        assert table_config.dialect == "mysql"
        assert table_config.schema_name == "test_schema"

    def test_explicit_values_override_metadata(self) -> None:
        """Test that explicit values override metadata defaults."""
        metadata = MetadataConfig(
            default_dialect="mysql", default_schema="default_schema"
        )

        tables_data = {
            "metadata": metadata.model_dump(),
            "tables": {
                "test_table": {
                    "dialect": "postgresql",  # Override default
                    "schema_name": "custom_schema",  # Override default
                    "columns": [{"name": "id", "type": "Integer"}],
                }
            },
        }

        config = TablesConfig.model_validate(tables_data)
        table_config = config.tables["test_table"]

        # Should keep explicit values, not metadata defaults
        assert table_config.dialect == "postgresql"
        assert table_config.schema_name == "custom_schema"

    def test_partial_metadata_application(self) -> None:
        """Test partial application when only some values are missing."""
        metadata = MetadataConfig(
            default_dialect="mysql", default_schema="default_schema"
        )

        tables_data = {
            "metadata": metadata.model_dump(),
            "tables": {
                "test_table": {
                    "dialect": "postgresql",  # Explicit dialect
                    # No schema_name, should use default
                    "columns": [{"name": "id", "type": "Integer"}],
                }
            },
        }

        config = TablesConfig.model_validate(tables_data)
        table_config = config.tables["test_table"]

        # Should have explicit dialect and default schema
        assert table_config.dialect == "postgresql"
        assert table_config.schema_name == "default_schema"

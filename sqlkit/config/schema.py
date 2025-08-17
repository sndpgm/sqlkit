"""
Pydantic schemas for YAML configuration validation.

This module defines the structure and validation rules for YAML-based
table definitions and dialect-specific method configurations.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class ColumnConfig(BaseModel):
    """
    Configuration for a table column.

    Attributes
    ----------
    name : str
        Column name.
    type : str
        SQLAlchemy column type (e.g., 'Integer', 'String', 'DateTime').
    length : Optional[int]
        Length for string types.
    precision : Optional[int]
        Precision for numeric types (total number of digits).
    scale : Optional[int]
        Scale for numeric types (number of decimal places).
    primary_key : bool
        Whether this is a primary key column.
    nullable : bool
        Whether the column can be NULL.
    unique : bool
        Whether the column has a unique constraint.
    auto_increment : bool
        Whether the column auto-increments.
    default : Optional[Union[str, int, float, bool]]
        Default value for the column.
    """

    name: str
    type: str
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    auto_increment: bool = False
    default: str | int | float | bool | None = None

    @field_validator("type")
    @classmethod
    def validate_column_type(cls, v: str) -> str:
        """Validate that column type is supported."""
        from sqlkit.core.type_parser import TypeParser

        # Try to parse the type to validate it
        try:
            TypeParser.parse_type_spec(v)
            return v
        except ValueError:
            # If parsing fails, check against legacy valid types
            legacy_sqlalchemy_types = {
                "Integer",
                "String",
                "Text",
                "Float",
                "Numeric",
                "Boolean",
                "DateTime",
                "Date",
                "Time",
            }
            # Get all aliases from TypeParser dynamically
            type_aliases: set[str] = set(TypeParser.TYPE_ALIASES.keys())

            valid_types: set[str] = legacy_sqlalchemy_types | type_aliases

            if v not in valid_types:
                raise ValueError(f"Unsupported column type: {v}")
            return v


class IndexConfig(BaseModel):
    """
    Configuration for a table index.

    Attributes
    ----------
    name : str
        Index name.
    columns : List[str]
        List of column names in the index.
    unique : bool
        Whether this is a unique index.
    """

    name: str
    columns: list[str]
    unique: bool = False


class DialectMethodConfig(BaseModel):
    """
    Configuration for dialect-specific method parameters.

    This class stores pre-configured parameters for methods like copy_from_s3,
    unload_to_s3, etc. Template variables in string values will be expanded
    at runtime.

    Attributes
    ----------
    All attributes are optional and depend on the specific method being
    configured.
    Common attributes include:
    - s3_path: S3 path for data operations
    - credentials: Authentication credentials
    - format: Data format (CSV, JSON, PARQUET, etc.)
    - delimiter: Field delimiter for CSV
    - options: List of additional options
    """

    model_config = ConfigDict(extra="allow")

    # Common S3 operations parameters
    s3_path: str | None = None
    credentials: str | None = None
    format: str | None = None
    delimiter: str | None = None
    quote_char: str | None = None
    escape: bool | None = None
    date_format: str | None = None
    options: list[str] | None = None

    # File operations
    file_path: str | None = None
    fields_terminated_by: str | None = None
    lines_terminated_by: str | None = None
    ignore_lines: int | None = None

    # Table operations
    add_partitions: bool | None = None

    def dict_without_none(self) -> dict[str, Any]:
        """Return dictionary representation excluding None values."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class TableConfig(BaseModel):
    """
    Configuration for a single table.

    Attributes
    ----------
    dialect : str
        Database dialect (mysql, postgresql, sqlite, redshift, athena, oracle).
    columns : List[ColumnConfig]
        List of column configurations.
    schema_name : Optional[str]
        Database schema name.
    indexes : Optional[List[IndexConfig]]
        List of index configurations.
    options : Optional[Dict[str, Any]]
        Dialect-specific table options.
    dialect_methods : Optional[Dict[str, DialectMethodConfig]]
        Pre-configured parameters for dialect-specific methods.
    """

    dialect: str | None = None
    columns: list[ColumnConfig]
    schema_name: str | None = None
    indexes: list[IndexConfig] | None = None
    options: dict[str, Any] | None = None
    dialect_methods: dict[str, DialectMethodConfig] | None = None

    def model_post_init(self, __context: Any) -> None:
        """Apply metadata defaults after model initialization."""
        if __context and "metadata" in __context:
            metadata: MetadataConfig = __context["metadata"]

            # Apply default dialect if not specified
            if not self.dialect and metadata.default_dialect:
                self.dialect = metadata.default_dialect

            # Apply default schema if not specified
            if not self.schema_name and metadata.default_schema:
                self.schema_name = metadata.default_schema

    @field_validator("dialect")
    @classmethod
    def validate_dialect(cls, v: str | None) -> str | None:
        """Validate that dialect is supported."""
        if v is None:
            return None

        # TODO: escape from hard-coding
        valid_dialects = {
            "mysql",
            "postgresql",
            "sqlite",
            "redshift",
            "athena",
            "oracle",
        }
        if v not in valid_dialects:
            raise ValueError(f"Unsupported dialect: {v}")
        return v


class MetadataConfig(BaseModel):
    """
    Global metadata configuration.

    Attributes
    ----------
    default_dialect : Optional[str]
        Default dialect to use when not specified in table config.
    default_schema : Optional[str]
        Default schema to use when not specified in table config.
    """

    default_dialect: str | None = None
    default_schema: str | None = None


class TablesConfig(BaseModel):
    """
    Root configuration containing all tables and metadata.

    Attributes
    ----------
    metadata : Optional[MetadataConfig]
        Global configuration metadata.
    tables : Dict[str, TableConfig]
        Dictionary mapping table names to their configurations.
    """

    metadata: MetadataConfig | None = None
    tables: dict[str, TableConfig]

    def model_post_init(self, __context: Any) -> None:
        """Apply metadata defaults to all tables after initialization."""
        if self.metadata:
            context = {"metadata": self.metadata}

            # Re-create each table config with metadata context
            for table_name, table_config in self.tables.items():
                # Create new instance with metadata context
                table_data = table_config.model_dump()
                new_table_config = TableConfig.model_validate(
                    table_data, context=context
                )
                self.tables[table_name] = new_table_config

    def get_table_config(self, table_name: str) -> TableConfig:
        """
        Get configuration for a specific table.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        TableConfig
            Table configuration with defaults already applied.

        Raises
        ------
        KeyError
            If table is not found in configuration.
        ValueError
            If no dialect is specified for the table and no default_dialect
            is available in metadata.
        """
        if table_name not in self.tables:
            raise KeyError(f"Table '{table_name}' not found in configuration")

        config = self.tables[table_name]

        # Ensure dialect is specified (should have been applied during init)
        if not config.dialect:
            raise ValueError(
                f"No dialect specified for table '{table_name}' and no "
                "default_dialect in metadata"
            )

        return config

"""
Table factory for creating dialect-specific table instances.

This module provides a unified interface for creating table instances
with different database dialects while maintaining backward compatibility
with direct dialect-specific class usage. It also supports YAML-based
configuration for table definitions.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlkit.core.table import SQLTable

if TYPE_CHECKING:
    from sqlkit.core.column import Column


def Table(  # noqa: N802
    name: str,
    *columns: Column,
    dialect: str | None = None,
    schema: str | None = None,
    **kwargs: Any,
) -> SQLTable:
    """
    Create a table instance with the specified dialect.

    This function provides a unified interface for creating dialect-specific
    table instances. It automatically returns the appropriate table class
    based on the dialect parameter.

    Parameters
    ----------
    name : str
        The name of the table.
    *columns : Column
        Column definitions for the table.
    dialect : str or None, optional
        The database dialect to use. Supported values:
        - "mysql": MySQL database
        - "postgresql": PostgreSQL database
        - "sqlite": SQLite database
        - "redshift": Amazon Redshift
        - "athena": AWS Athena
        - "oracle": Oracle Database
        If None, returns a generic SQLTable instance.
    schema : str or None, optional
        The database schema name.
    **kwargs : Any
        Additional dialect-specific keyword arguments.

    Returns
    -------
    SQLTable
        A dialect-specific table instance.

    Raises
    ------
    ValueError
        If an unsupported dialect is specified.

    Examples
    --------
    >>> from sqlkit import Table, Column, Integer, String
    >>>
    >>> # Create a Redshift table
    >>> table = Table(
    ...     "users",
    ...     Column("id", Integer, primary_key=True),
    ...     Column("name", String(255)),
    ...     dialect="redshift",
    ...     sort_keys=["id"],
    ...     dist_key="id"
    ... )
    >>>
    >>> # Create a MySQL table
    >>> table = Table(
    ...     "products",
    ...     Column("id", Integer, primary_key=True),
    ...     Column("name", String(255)),
    ...     dialect="mysql",
    ...     engine="InnoDB"
    ... )
    """
    # Import here to avoid circular imports
    if dialect == "mysql":
        from sqlkit.dialects.mysql import MySQLTable

        return MySQLTable(name, *columns, schema=schema, **kwargs)
    elif dialect == "postgresql":
        from sqlkit.dialects.postgresql import PostgreSQLTable

        return PostgreSQLTable(name, *columns, schema=schema, **kwargs)
    elif dialect == "sqlite":
        from sqlkit.dialects.sqlite import SQLiteTable

        return SQLiteTable(name, *columns, schema=schema, **kwargs)
    elif dialect == "redshift":
        from sqlkit.dialects.redshift import RedshiftTable

        return RedshiftTable(name, *columns, schema=schema, **kwargs)
    elif dialect == "athena":
        from sqlkit.dialects.athena import AthenaTable

        return AthenaTable(name, *columns, schema=schema, **kwargs)
    elif dialect == "oracle":
        from sqlkit.dialects.oracle import OracleTable

        return OracleTable(name, *columns, schema=schema, **kwargs)
    elif dialect is None:
        # Create a concrete SQLTable class for generic usage
        class GenericSQLTable(SQLTable):
            """Generic SQL table implementation."""

            pass

        return GenericSQLTable(name, *columns, schema=schema, **kwargs)
    else:
        raise ValueError(
            f"Unsupported dialect: {dialect}. "
            f"Supported dialects: mysql, postgresql, sqlite, "
            f"redshift, athena, oracle"
        )


# Add a class-like interface for factory methods
class TableFactory:
    """
    Factory class for creating tables from various sources.

    This class provides static methods for creating table instances
    from YAML configuration files or other sources.
    """

    @staticmethod
    def from_config(
        table_name: str, config_file: str | Path | None = None
    ) -> SQLTable:
        """
        Create table instance from YAML configuration.

        Parameters
        ----------
        table_name : str
            Name of the table as defined in YAML configuration.
        config_file : Optional[str | Path]
            Path to YAML configuration file. If None, looks for
            'tables.yaml' in current directory.

        Returns
        -------
        SQLTable
            Configured table instance with enhanced dialect methods.

        Raises
        ------
        FileNotFoundError
            If configuration file is not found.
        KeyError
            If table is not defined in configuration.
        ValueError
            If configuration is invalid.

        Examples
        --------
        >>> # Create table from default config file
        >>> table = TableFactory.from_config("users")
        >>>
        >>> # Create table from specific config file
        >>> table = TableFactory.from_config("products", "my_tables.yaml")
        >>>
        >>> # Use pre-configured method
        >>> copy_query = table.copy_from_s3(
        ...     template_vars={"date": "2024-01-15"}
        ... )
        """
        from sqlkit.config.registry import TableRegistry

        if config_file is None:
            config_file = Path("tables.yaml")

        registry = TableRegistry.from_file(config_file)
        return registry.get_table(table_name)


# Add convenience function at module level
def from_config(
    table_name: str, config_file: str | Path | None = None
) -> SQLTable:
    """
    Create table instance from YAML configuration.

    This is a convenience function that delegates to
    TableFactory.from_config().

    Parameters
    ----------
    table_name : str
        Name of the table as defined in YAML configuration.
    config_file : Optional[str | Path]
        Path to YAML configuration file. If None, looks for
        'tables.yaml' in current directory.

    Returns
    -------
    SQLTable
        Configured table instance with enhanced dialect methods.

    Examples
    --------
    >>> from sqlkit.core.factory import from_config
    >>> table = from_config("users", "config/tables.yaml")
    """
    return TableFactory.from_config(table_name, config_file)

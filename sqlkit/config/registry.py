"""
Table registry for YAML-based configuration.

This module provides a centralized registry for managing table instances
created from YAML configuration files.
"""

from __future__ import annotations

from pathlib import Path

from sqlkit.config.loader import YamlLoader
from sqlkit.core.factory import Table
from sqlkit.core.table import SQLTable


class TableRegistry:
    """
    Registry for managing tables created from YAML configuration.

    This class provides a centralized way to access table instances that
    are configured via YAML files. Tables are created lazily when first
    accessed and cached for subsequent use.

    Attributes
    ----------
    loader : YamlLoader
        YAML configuration loader.
    _table_cache : Dict[str, SQLTable]
        Cache of created table instances.
    """

    def __init__(self, loader: YamlLoader) -> None:
        """
        Initialize registry with YAML loader.

        Parameters
        ----------
        loader : YamlLoader
            Configured YAML loader instance.
        """
        self.loader = loader
        self._table_cache: dict[str, SQLTable] = {}

    def get_table(self, table_name: str) -> SQLTable:
        """
        Get table instance by name.

        Creates table from YAML configuration if not already cached.
        The returned table instance will have enhanced methods that
        automatically use YAML-configured parameters.

        Parameters
        ----------
        table_name : str
            Name of the table as defined in YAML configuration.

        Returns
        -------
        SQLTable
            Table instance with YAML configuration applied.

        Raises
        ------
        KeyError
            If table is not found in configuration.
        ValueError
            If table configuration is invalid.
        """
        if table_name in self._table_cache:
            return self._table_cache[table_name]

        # Get table configuration
        table_config = self.loader.get_table_config(table_name)

        # Create table instance using factory
        table = self._create_table_from_config(table_name, table_config)

        # Cache the table
        self._table_cache[table_name] = table

        return table

    def _create_table_from_config(
        self, table_name: str, config: dict
    ) -> SQLTable:
        """
        Create table instance from configuration dictionary.

        Parameters
        ----------
        table_name : str
            Name of the table.
        config : Dict
            Table configuration dictionary.

        Returns
        -------
        SQLTable
            Configured table instance.
        """
        from sqlkit.core import Column
        from sqlkit.core.type_parser import parse_column_type

        # Create columns
        columns = []
        for col_config in config["columns"]:
            # Build type specification with parameters
            type_spec = col_config["type"]

            # Handle parameterized types in YAML
            if col_config.get("length") is not None:
                # String with length: String -> string(100)
                type_spec = f"{type_spec}({col_config['length']})"
            elif col_config.get("precision") is not None:
                # Numeric with precision/scale: Numeric -> numeric(18,5)
                if col_config.get("scale") is not None:
                    type_spec = (
                        f"{type_spec}({col_config['precision']},"
                        f"{col_config['scale']})"
                    )
                else:
                    type_spec = f"{type_spec}({col_config['precision']})"

            try:
                # Parse the type specification
                parsed_type = parse_column_type(type_spec)
            except ValueError as e:
                raise ValueError(
                    f"Invalid column type specification '{type_spec}' "
                    f"for column '{col_config['name']}': {e}"
                )

            # Create column with options
            column = Column(
                col_config["name"],
                parsed_type,
                primary_key=col_config.get("primary_key", False),
                nullable=col_config.get("nullable", True),
                unique=col_config.get("unique", False),
                autoincrement=col_config.get("auto_increment", False),
                default=col_config.get("default"),
            )
            columns.append(column)

        # Prepare table creation arguments
        table_kwargs = {
            "dialect": config["dialect"],
            "_yaml_config": config,  # Pass config for method enhancement
        }

        # Add schema if specified
        if config.get("schema_name"):
            table_kwargs["schema"] = config["schema_name"]

        # Add dialect-specific options
        if config.get("options"):
            table_kwargs.update(config["options"])

        # Create table using factory
        table = Table(table_name, *columns, **table_kwargs)

        return table

    def list_tables(self) -> list[str]:
        """
        Get list of all available table names.

        Returns
        -------
        list[str]
            List of table names defined in configuration.
        """
        return list(self.loader.config.tables.keys())

    def clear_cache(self) -> None:
        """Clear the table cache, forcing recreation on next access."""
        self._table_cache.clear()

    def reload_config(self) -> None:
        """
        Reload configuration from file and clear cache.

        This is useful for development when configuration files are
        being modified and you want to pick up changes.
        """
        self.loader = YamlLoader(self.loader.config_file)
        self.clear_cache()

    @classmethod
    def from_file(cls, config_file: str | Path) -> TableRegistry:
        """
        Create registry from YAML configuration file.

        Parameters
        ----------
        config_file : str | Path
            Path to YAML configuration file.

        Returns
        -------
        TableRegistry
            Configured registry instance.
        """
        loader = YamlLoader.from_file(config_file)
        return cls(loader)

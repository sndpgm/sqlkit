from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import mysql

from sqlkit.core.table import SQLTable
from sqlkit.operations.special import MySQLSpecialQuery

if TYPE_CHECKING:
    from sqlkit.operations.ddl import CreateTableQuery
    from sqlkit.operations.dml import InsertQuery


class MySQLTable(SQLTable):
    """
    MySQL specific table implementation.

    This class provides MySQL-specific functionality including REPLACE,
    INSERT IGNORE, SHOW CREATE TABLE, and LOAD DATA INFILE operations.
    It also supports YAML-based configuration for pre-defined method
    parameters.

    Attributes
    ----------
    engine_type : str
        MySQL storage engine (InnoDB, MyISAM, etc.).
    charset : str
        Character set for the table.
    collation : str
        Collation for the table.
    _yaml_config : Optional[Dict[str, Any]]
        YAML configuration for this table, if available.
    """

    def __init__(
        self,
        name: str,
        *columns: Any,
        schema: str | None = None,
        _yaml_config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize MySQL table.

        Parameters
        ----------
        name : str
            Table name.
        *columns : Any
            Column definitions.
        schema : Optional[str]
            Database schema name.
        _yaml_config : Optional[Dict[str, Any]]
            YAML configuration dictionary (used internally by factory).
        **kwargs : Any
            Additional MySQL-specific options:
            - engine: Storage engine (default: InnoDB)
            - charset: Character set (default: utf8mb4)
            - collation: Collation (default: utf8mb4_unicode_ci)
        """
        super().__init__(
            name, *columns, dialect=mysql.dialect(), schema=schema
        )
        self.engine_type = kwargs.get("engine", "InnoDB")
        self.charset = kwargs.get("charset", "utf8mb4")
        self.collation = kwargs.get("collation", "utf8mb4_unicode_ci")
        self._yaml_config = _yaml_config or {}

    def create(self, if_not_exists: bool = False) -> CreateTableQuery:
        """MySQL specific CREATE TABLE with engine and charset options"""
        from sqlkit.operations.ddl import CreateTableQuery

        return CreateTableQuery(
            self,
            if_not_exists=if_not_exists,
            dialect=self.dialect,
        )

    def insert(self, **values: Any) -> InsertQuery:
        """MySQL specific INSERT with ON DUPLICATE KEY UPDATE support"""
        from sqlkit.operations.dml import InsertQuery

        return InsertQuery(self, values, dialect=self.dialect)

    def replace(self, **values: Any) -> MySQLSpecialQuery:
        """MySQL REPLACE statement"""
        return MySQLSpecialQuery(self, "REPLACE", values, dialect=self.dialect)

    def insert_ignore(self, **values: Any) -> MySQLSpecialQuery:
        """MySQL INSERT IGNORE statement"""
        return MySQLSpecialQuery(
            self, "INSERT_IGNORE", values, dialect=self.dialect
        )

    def show_create(self) -> MySQLSpecialQuery:
        """MySQL SHOW CREATE TABLE statement"""
        return MySQLSpecialQuery(self, "SHOW_CREATE", dialect=self.dialect)

    def load_data_infile(
        self,
        file_path: str | None = None,
        template_vars: dict[str, str] | None = None,
        use_config: bool = True,
        **options: Any,
    ) -> MySQLSpecialQuery:
        """
        MySQL LOAD DATA INFILE with YAML configuration support.

        Parameters
        ----------
        file_path : Optional[str]
            Path to the file to load. If None, uses YAML configuration.
        template_vars : Optional[Dict[str, str]]
            Variables for template expansion in YAML config.
        use_config : bool
            Whether to use YAML configuration. Default True.
        **options : Any
            Additional LOAD DATA INFILE options that override YAML settings:
            - fields_terminated_by: Field delimiter
            - lines_terminated_by: Line delimiter
            - ignore_lines: Number of lines to ignore at start
            - local: Whether to use LOCAL keyword
            - replace: Whether to use REPLACE keyword
            - ignore: Whether to use IGNORE keyword

        Returns
        -------
        MySQLSpecialQuery
            Query object for LOAD DATA INFILE operation.

        Raises
        ------
        ValueError
            If file_path is not provided and not configured in YAML.
        """
        final_options = {}

        # Handle YAML configuration mode
        if use_config and self._yaml_config.get("dialect_methods", {}).get(
            "load_data_infile"
        ):
            config = self._get_method_config("load_data_infile", template_vars)

            # Use configured file_path if not provided as argument
            if file_path is None:
                file_path = config.pop("file_path", None)

            # Start with YAML config, then apply argument overrides
            final_options.update(config)
            final_options.update(options)
        else:
            # Manual mode - use only provided arguments
            final_options.update(options)

        if file_path is None:
            raise ValueError(
                "file_path is required either as argument or in YAML config"
            )

        return MySQLSpecialQuery(
            self,
            "LOAD_DATA_INFILE",
            {"file_path": file_path, **final_options},
            dialect=self.dialect,
        )

    def _get_method_config(
        self, method_name: str, template_vars: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """
        Get method configuration from YAML with template expansion.

        Parameters
        ----------
        method_name : str
            Name of the method to get configuration for.
        template_vars : Optional[Dict[str, str]]
            Template variables for expansion.

        Returns
        -------
        Dict[str, Any]
            Method configuration dictionary.
        """
        method_config: dict[str, Any] = self._yaml_config.get(
            "dialect_methods", {}
        ).get(method_name, {})

        if template_vars and method_config:
            # Perform template expansion
            from sqlkit.config.loader import YamlLoader

            loader = YamlLoader.__new__(
                YamlLoader
            )  # Create instance without file loading
            expanded_config = loader.expand_templates(
                method_config, template_vars
            )
            method_config = expanded_config

        return method_config.copy()

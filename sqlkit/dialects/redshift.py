from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import postgresql  # Redshift is based on PostgreSQL

from sqlkit.core.table import SQLTable
from sqlkit.operations.special import RedshiftSpecialQuery

if TYPE_CHECKING:
    from sqlkit.operations.ddl import CreateTableQuery


class RedshiftTable(SQLTable):
    """
    Amazon Redshift specific table implementation.

    This class provides Redshift-specific functionality including COPY/UNLOAD
    operations, distribution keys, sort keys, and other Redshift features.
    It also supports YAML-based configuration for pre-defined method
    parameters.

    Attributes
    ----------
    sort_keys : list[str]
        List of column names to use as sort keys.
    dist_key : Optional[str]
        Column name to use as distribution key.
    dist_style : str
        Distribution style (AUTO, EVEN, KEY, ALL).
    table_type : str
        Table type (PERMANENT or TEMP).
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
        Initialize Redshift table.

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
            Additional Redshift-specific options:
            - sort_keys: List of sort key columns
            - dist_key: Distribution key column
            - dist_style: Distribution style
            - table_type: Table type (PERMANENT or TEMP)
        """
        # Redshift uses PostgreSQL dialect as base
        super().__init__(
            name, *columns, dialect=postgresql.dialect(), schema=schema
        )
        self.sort_keys = kwargs.get("sort_keys", [])
        self.dist_key = kwargs.get("dist_key")
        self.dist_style = kwargs.get("dist_style", "AUTO")
        self.table_type = kwargs.get("table_type", "PERMANENT")
        self._yaml_config = _yaml_config or {}

    def create(self, if_not_exists: bool = False) -> CreateTableQuery:
        """Redshift specific CREATE TABLE with distribution and sort keys"""
        from sqlkit.operations.ddl import CreateTableQuery

        return CreateTableQuery(
            self,
            if_not_exists=if_not_exists,
            dialect=self.dialect,
        )

    def copy_from_s3(
        self,
        s3_path: str | None = None,
        template_vars: dict[str, str] | None = None,
        use_config: bool = True,
        **options: Any,
    ) -> RedshiftSpecialQuery:
        """
        Redshift COPY command from S3 with YAML configuration support.

        This method supports multiple usage patterns:
        1. YAML config only: copy_from_s3()
        2. YAML config with template vars:
           copy_from_s3(template_vars={"date": "2024-01-15"})
        3. YAML config with overrides:
           copy_from_s3(format="JSON", delimiter="|")
        4. Manual mode:
           copy_from_s3("s3://bucket/path", credentials="...", format="CSV")
        5. Force manual: copy_from_s3("s3://bucket/path", use_config=False)

        Parameters
        ----------
        s3_path : Optional[str]
            S3 path to copy from. If None, uses YAML configuration.
        template_vars : Optional[Dict[str, str]]
            Variables for template expansion in YAML config.
        use_config : bool
            Whether to use YAML configuration. Default True.
        **options : Any
            Additional COPY options that override YAML settings:
            - credentials: AWS credentials or IAM role
            - format: Data format (CSV, JSON, PARQUET, etc.)
            - delimiter: Field delimiter for CSV
            - quote_char: Quote character
            - escape: Whether to enable escape characters
            - date_format: Date format string
            - options: List of additional COPY options

        Returns
        -------
        RedshiftSpecialQuery
            Query object for COPY FROM S3 operation.

        Raises
        ------
        ValueError
            If s3_path is not provided and not configured in YAML.
        """
        final_options = {}

        # Handle YAML configuration mode
        if use_config and self._yaml_config.get("dialect_methods", {}).get(
            "copy_from_s3"
        ):
            config = self._get_method_config("copy_from_s3", template_vars)

            # Use configured s3_path if not provided as argument
            if s3_path is None:
                s3_path = config.pop("s3_path", None)

            # Start with YAML config, then apply argument overrides
            final_options.update(config)
            final_options.update(options)
        else:
            # Manual mode - use only provided arguments
            final_options.update(options)

        if s3_path is None:
            raise ValueError(
                "s3_path is required either as argument or in YAML config"
            )

        return RedshiftSpecialQuery(
            self,
            "COPY_FROM_S3",
            {"s3_path": s3_path, **final_options},
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

    def unload_to_s3(
        self,
        query: str | None = None,
        s3_path: str | None = None,
        template_vars: dict[str, str] | None = None,
        use_config: bool = True,
        **options: Any,
    ) -> RedshiftSpecialQuery:
        """
        Redshift UNLOAD command to S3 with YAML configuration support.

        Parameters
        ----------
        query : Optional[str]
            SQL query to unload. If None, uses a basic SELECT * from this
            table.
        s3_path : Optional[str]
            S3 path to unload to. If None, uses YAML configuration.
        template_vars : Optional[Dict[str, str]]
            Variables for template expansion in YAML config.
        use_config : bool
            Whether to use YAML configuration. Default True.
        **options : Any
            Additional UNLOAD options that override YAML settings.

        Returns
        -------
        RedshiftSpecialQuery
            Query object for UNLOAD TO S3 operation.

        Raises
        ------
        ValueError
            If required parameters are not provided.
        """
        final_options = {}

        # Handle YAML configuration mode
        if use_config and self._yaml_config.get("dialect_methods", {}).get(
            "unload_to_s3"
        ):
            config = self._get_method_config("unload_to_s3", template_vars)

            # Use configured s3_path if not provided as argument
            if s3_path is None:
                s3_path = config.pop("s3_path", None)

            # Start with YAML config, then apply argument overrides
            final_options.update(config)
            final_options.update(options)
        else:
            # Manual mode - use only provided arguments
            final_options.update(options)

        if s3_path is None:
            raise ValueError(
                "s3_path is required either as argument or in YAML config"
            )

        # Default query if not provided
        if query is None:
            query = f"SELECT * FROM {self.name}"

        return RedshiftSpecialQuery(
            self,
            "UNLOAD_TO_S3",
            {"query": query, "s3_path": s3_path, **final_options},
            dialect=self.dialect,
        )

    def analyze_compression(self) -> RedshiftSpecialQuery:
        """Redshift ANALYZE COMPRESSION statement"""
        return RedshiftSpecialQuery(
            self, "ANALYZE_COMPRESSION", dialect=self.dialect
        )

    def vacuum_reindex(self) -> RedshiftSpecialQuery:
        """Redshift VACUUM REINDEX statement"""
        return RedshiftSpecialQuery(
            self, "VACUUM_REINDEX", dialect=self.dialect
        )

    def deep_copy(self, new_table_name: str) -> RedshiftSpecialQuery:
        """Redshift deep copy using CREATE TABLE AS"""
        return RedshiftSpecialQuery(
            self,
            "DEEP_COPY",
            {"new_table_name": new_table_name},
            dialect=self.dialect,
        )

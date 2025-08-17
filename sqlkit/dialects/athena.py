from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import (
    postgresql,
)  # Athena uses Presto SQL which is PostgreSQL-like

from sqlkit.core.table import SQLTable
from sqlkit.operations.special import AthenaSpecialQuery

if TYPE_CHECKING:
    from sqlkit.operations.ddl import CreateTableQuery


class AthenaTable(SQLTable):
    """
    AWS Athena specific table implementation.

    This class provides Athena-specific functionality including CTAS
    operations, partition management, MSCK REPAIR, and other Athena features.
    It also supports YAML-based configuration for pre-defined method
    parameters.

    Attributes
    ----------
    location : Optional[str]
        S3 location for the table data.
    stored_as : str
        Storage format (PARQUET, ORC, TEXTFILE, etc.).
    partition_by : list[str]
        List of columns to partition by.
    table_type : str
        Table type (EXTERNAL or managed).
    serde : Optional[str]
        SerDe class for custom serialization.
    input_format : Optional[str]
        Input format class.
    output_format : Optional[str]
        Output format class.
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
        Initialize Athena table.

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
            Additional Athena-specific options:
            - location: S3 location for table data
            - stored_as: Storage format
            - partition_by: List of partition columns
            - table_type: Table type
            - serde: SerDe class
            - input_format: Input format class
            - output_format: Output format class
        """
        super().__init__(
            name, *columns, dialect=postgresql.dialect(), schema=schema
        )
        self.location = kwargs.get("location")
        self.stored_as = kwargs.get("stored_as", "PARQUET")
        self.partition_by = kwargs.get("partition_by", [])
        self.table_type = kwargs.get("table_type", "EXTERNAL")
        self.serde = kwargs.get("serde")
        self.input_format = kwargs.get("input_format")
        self.output_format = kwargs.get("output_format")
        self._yaml_config = _yaml_config or {}

    def create(self, if_not_exists: bool = False) -> CreateTableQuery:
        """Athena specific CREATE TABLE with S3 location and format"""
        from sqlkit.operations.ddl import CreateTableQuery

        return CreateTableQuery(
            self,
            if_not_exists=if_not_exists,
            dialect=self.dialect,
        )

    def create_as_select(  # type: ignore[override]
        self,
        query: str,
        table_name: str | None = None,
        **options: Any,
    ) -> AthenaSpecialQuery:
        """Athena CTAS with S3 location and format options"""
        return AthenaSpecialQuery(
            self,
            "CTAS",
            {
                "query": query,
                "table_name": table_name or f"{self.name}_copy",
                "location": options.get("location"),
                "format": options.get("format", self.stored_as),
                "partition_by": options.get("partition_by", []),
                **options,
            },
            dialect=self.dialect,
        )

    def add_partition(
        self, partition_spec: str, location: str | None = None
    ) -> AthenaSpecialQuery:
        """Athena ALTER TABLE ADD PARTITION"""
        return AthenaSpecialQuery(
            self,
            "ADD_PARTITION",
            {"partition_spec": partition_spec, "location": location},
            dialect=self.dialect,
        )

    def drop_partition(self, partition_spec: str) -> AthenaSpecialQuery:
        """Athena ALTER TABLE DROP PARTITION"""
        return AthenaSpecialQuery(
            self,
            "DROP_PARTITION",
            {"partition_spec": partition_spec},
            dialect=self.dialect,
        )

    def msck_repair(
        self,
        template_vars: dict[str, str] | None = None,
        use_config: bool = True,
        **options: Any,
    ) -> AthenaSpecialQuery:
        """
        Athena MSCK REPAIR TABLE with YAML configuration support.

        Parameters
        ----------
        template_vars : Optional[Dict[str, str]]
            Variables for template expansion in YAML config.
        use_config : bool
            Whether to use YAML configuration. Default True.
        **options : Any
            Additional MSCK REPAIR options that override YAML settings.

        Returns
        -------
        AthenaSpecialQuery
            Query object for MSCK REPAIR TABLE operation.
        """
        final_options = {}

        # Handle YAML configuration mode
        if use_config and self._yaml_config.get("dialect_methods", {}).get(
            "msck_repair"
        ):
            config = self._get_method_config("msck_repair", template_vars)
            final_options.update(config)

        # Apply argument overrides
        final_options.update(options)

        return AthenaSpecialQuery(
            self, "MSCK_REPAIR", final_options, dialect=self.dialect
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

    def show_partitions(self) -> AthenaSpecialQuery:
        """Athena SHOW PARTITIONS"""
        return AthenaSpecialQuery(
            self, "SHOW_PARTITIONS", dialect=self.dialect
        )

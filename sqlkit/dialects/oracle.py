from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import oracle

from sqlkit.core.table import SQLTable
from sqlkit.operations.special import OracleSpecialQuery

if TYPE_CHECKING:
    from sqlkit.operations.ddl import CreateTableQuery
    from sqlkit.operations.dml import InsertQuery


class OracleTable(SQLTable):
    """
    Oracle Database specific table implementation.

    This class provides Oracle-specific functionality including MERGE
    operations, tablespace management, and other Oracle features.
    It also supports YAML-based configuration for pre-defined method
    parameters.

    Attributes
    ----------
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
        Initialize Oracle table.

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
            Additional Oracle-specific options.
        """
        super().__init__(
            name, *columns, dialect=oracle.dialect(), schema=schema
        )
        self._yaml_config = _yaml_config or {}
        self.tablespace = kwargs.get("tablespace")
        self.organization = kwargs.get(
            "organization", "HEAP"
        )  # HEAP, INDEX, EXTERNAL
        self.compress = kwargs.get("compress", False)
        self.parallel = kwargs.get("parallel")

    def create(self, if_not_exists: bool = False) -> CreateTableQuery:
        """Oracle specific CREATE TABLE with tablespace and organization"""
        from sqlkit.operations.ddl import CreateTableQuery

        return CreateTableQuery(
            self,
            # Oracle doesn't support IF NOT EXISTS
            if_not_exists=if_not_exists,
            dialect=self.dialect,
        )

    def insert(self, **values: Any) -> InsertQuery:
        """Oracle specific INSERT with MERGE support"""
        from sqlkit.operations.dml import InsertQuery

        return InsertQuery(self, values, dialect=self.dialect)

    def merge(
        self, source_query: str, on_condition: str, **options: Any
    ) -> OracleSpecialQuery:
        """Oracle MERGE statement"""
        return OracleSpecialQuery(
            self,
            "MERGE",
            {
                "source_query": source_query,
                "on_condition": on_condition,
                **options,
            },
            dialect=self.dialect,
        )

    def truncate(self, reuse_storage: bool = True) -> OracleSpecialQuery:  # type: ignore[override]
        """Oracle TRUNCATE with storage options"""
        return OracleSpecialQuery(
            self,
            "TRUNCATE",
            {"reuse_storage": reuse_storage},
            dialect=self.dialect,
        )

    def analyze_table(
        self,
        estimate_percent: int | None = None,
        method: str = "FOR ALL COLUMNS",
    ) -> OracleSpecialQuery:
        """Oracle ANALYZE TABLE statement"""
        return OracleSpecialQuery(
            self,
            "ANALYZE_TABLE",
            {"estimate_percent": estimate_percent, "method": method},
            dialect=self.dialect,
        )

    def create_index(
        self, index_name: str, columns: list[str], **options: Any
    ) -> OracleSpecialQuery:
        """Oracle CREATE INDEX with options"""
        return OracleSpecialQuery(
            self,
            "CREATE_INDEX",
            {
                "index_name": index_name,
                "columns": columns,
                "tablespace": options.get("tablespace"),
                "parallel": options.get("parallel"),
                "compress": options.get("compress"),
                **options,
            },
            dialect=self.dialect,
        )

    def create_sequence(
        self, sequence_name: str, **options: Any
    ) -> OracleSpecialQuery:
        """Oracle CREATE SEQUENCE"""
        return OracleSpecialQuery(
            self,
            "CREATE_SEQUENCE",
            {"sequence_name": sequence_name, **options},
            dialect=self.dialect,
        )

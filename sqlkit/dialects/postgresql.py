from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import postgresql

from sqlkit.core.table import SQLTable
from sqlkit.operations.special import PostgreSQLSpecialQuery

if TYPE_CHECKING:
    from sqlkit.operations.dml import InsertQuery


class PostgreSQLTable(SQLTable):
    """
    PostgreSQL specific table implementation.

    This class provides PostgreSQL-specific functionality including COPY
    operations, UPSERT capabilities, and other PostgreSQL features.
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
        Initialize PostgreSQL table.

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
            Additional PostgreSQL-specific options.
        """
        super().__init__(
            name, *columns, dialect=postgresql.dialect(), schema=schema
        )
        self._yaml_config = _yaml_config or {}

    def insert(self, **values: Any) -> InsertQuery:
        """PostgreSQL specific INSERT with ON CONFLICT support"""
        from sqlkit.operations.dml import InsertQuery

        return InsertQuery(self, values, dialect=self.dialect)

    def upsert(
        self, conflict_columns: list[str], **values: Any
    ) -> InsertQuery:
        """PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)"""
        insert_query = self.insert(**values)
        return insert_query.on_conflict_do_update(**values)  # type: ignore[call-arg]

    def copy_from_csv(
        self, file_path: str, **options: Any
    ) -> PostgreSQLSpecialQuery:
        """PostgreSQL COPY FROM CSV"""
        return PostgreSQLSpecialQuery(
            self,
            "COPY_FROM",
            {"file_path": file_path, **options},
            dialect=self.dialect,
        )

    def copy_to_csv(
        self,
        file_path: str,
        query: str | None = None,
        **options: Any,
    ) -> PostgreSQLSpecialQuery:
        """PostgreSQL COPY TO CSV"""
        return PostgreSQLSpecialQuery(
            self,
            "COPY_TO",
            {"file_path": file_path, "query": query, **options},
            dialect=self.dialect,
        )

    def analyze(self) -> PostgreSQLSpecialQuery:
        """PostgreSQL ANALYZE statement"""
        return PostgreSQLSpecialQuery(self, "ANALYZE", dialect=self.dialect)

    def vacuum(self, full: bool = False) -> PostgreSQLSpecialQuery:
        """PostgreSQL VACUUM statement"""
        return PostgreSQLSpecialQuery(
            self, "VACUUM", {"full": full}, dialect=self.dialect
        )

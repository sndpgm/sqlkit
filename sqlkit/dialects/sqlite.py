from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import sqlite

from sqlkit.core.table import SQLTable
from sqlkit.operations.special import SQLiteSpecialQuery

if TYPE_CHECKING:
    from sqlkit.operations.dml import InsertQuery


class SQLiteTable(SQLTable):
    """
    SQLite specific table implementation.

    This class provides SQLite-specific functionality including INSERT OR
    REPLACE, INSERT OR IGNORE, ATTACH/DETACH DATABASE, and PRAGMA operations.
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
        Initialize SQLite table.

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
            Additional SQLite-specific options.
        """
        super().__init__(
            name, *columns, dialect=sqlite.dialect(), schema=schema
        )
        self._yaml_config = _yaml_config or {}

    def insert(self, **values: Any) -> InsertQuery:
        """SQLite specific INSERT with OR REPLACE/OR IGNORE support"""
        from sqlkit.operations.dml import InsertQuery

        return InsertQuery(self, values, dialect=self.dialect)

    def insert_or_replace(self, **values: Any) -> SQLiteSpecialQuery:
        """SQLite INSERT OR REPLACE statement"""
        return SQLiteSpecialQuery(
            self, "INSERT_OR_REPLACE", values, dialect=self.dialect
        )

    def insert_or_ignore(self, **values: Any) -> SQLiteSpecialQuery:
        """SQLite INSERT OR IGNORE statement"""
        return SQLiteSpecialQuery(
            self, "INSERT_OR_IGNORE", values, dialect=self.dialect
        )

    def attach_database(self, db_path: str, alias: str) -> SQLiteSpecialQuery:
        """SQLite ATTACH DATABASE statement"""
        return SQLiteSpecialQuery(
            self,
            "ATTACH_DATABASE",
            {"db_path": db_path, "alias": alias},
            dialect=self.dialect,
        )

    def detach_database(self, alias: str) -> SQLiteSpecialQuery:
        """SQLite DETACH DATABASE statement"""
        return SQLiteSpecialQuery(
            self, "DETACH_DATABASE", {"alias": alias}, dialect=self.dialect
        )

    def pragma(
        self, pragma_name: str, value: str | None = None
    ) -> SQLiteSpecialQuery:
        """SQLite PRAGMA statement"""
        return SQLiteSpecialQuery(
            self,
            "PRAGMA",
            {"pragma_name": pragma_name, "value": value},
            dialect=self.dialect,
        )

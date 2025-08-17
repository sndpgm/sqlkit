"""
Base table class for all database dialects.

This module provides the core SQLTable class that serves as the foundation
for all database-specific table implementations.
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.schema import Column

if TYPE_CHECKING:
    from sqlkit.operations.ddl import (
        CreateTableQuery,
        CTASQuery,
        DropTableQuery,
        TruncateQuery,
    )
    from sqlkit.operations.dml import (
        DeleteQuery,
        InsertQuery,
        SelectQuery,
        UpdateQuery,
    )


class SQLTable(ABC):
    """
    Base table class for all database dialects.

    Parameters
    ----------
    name : str
        The name of the table.
    *columns : Column
        SQLAlchemy Column objects defining the table structure.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.
    schema : str, optional
        Database schema name.

    Attributes
    ----------
    name : str
        Table name.
    schema : str or None
        Schema name.
    dialect : Dialect or None
        SQLAlchemy dialect.
    """

    def __init__(
        self,
        name: str,
        *columns: Column,
        dialect: Dialect | None = None,
        schema: str | None = None,
    ) -> None:
        """Initialize SQLTable instance."""
        self.name = name
        self.schema = schema
        self.dialect = dialect
        self._metadata = MetaData(schema=schema)
        self._table = Table(name, self._metadata, *columns)

    @property
    def table(self) -> Table:
        """
        Get the underlying SQLAlchemy Table object.

        Returns
        -------
        Table
            SQLAlchemy Table instance.
        """
        return self._table

    @property
    def c(self) -> Any:
        """
        Get column access shorthand.

        Returns
        -------
        Any
            Table columns for use in queries.
        """
        return self._table.c

    @property
    def columns(self) -> Any:
        """
        Get all table columns.

        Returns
        -------
        Any
            All columns in the table.
        """
        return self._table.columns

    def create(self, if_not_exists: bool = False) -> CreateTableQuery:
        """
        Create a CREATE TABLE query.

        Parameters
        ----------
        if_not_exists : bool, default False
            Whether to use IF NOT EXISTS clause.

        Returns
        -------
        CreateTableQuery
            Query object for creating the table.
        """
        from sqlkit.operations.ddl import CreateTableQuery

        return CreateTableQuery(
            self, if_not_exists=if_not_exists, dialect=self.dialect
        )

    def drop(self, if_exists: bool = False) -> DropTableQuery:
        """
        Create a DROP TABLE query.

        Parameters
        ----------
        if_exists : bool, default False
            Whether to use IF EXISTS clause.

        Returns
        -------
        DropTableQuery
            Query object for dropping the table.
        """
        from sqlkit.operations.ddl import DropTableQuery

        return DropTableQuery(self, if_exists=if_exists, dialect=self.dialect)

    def truncate(self) -> TruncateQuery:
        """
        Create a TRUNCATE TABLE query.

        Returns
        -------
        TruncateQuery
            Query object for truncating the table.
        """
        from sqlkit.operations.ddl import TruncateQuery

        return TruncateQuery(self, dialect=self.dialect)

    def select(self, *columns: str | Column) -> SelectQuery:
        """
        Create a SELECT query.

        Parameters
        ----------
        *columns : str or Column
            Columns to select. If empty, selects all columns.

        Returns
        -------
        SelectQuery
            Query object for selecting data.
        """
        from sqlkit.operations.dml import SelectQuery

        return SelectQuery(self, columns, dialect=self.dialect)

    def insert(self, **values: Any) -> InsertQuery:
        """
        Create an INSERT query.

        Parameters
        ----------
        **values : Any
            Column-value pairs to insert.

        Returns
        -------
        InsertQuery
            Query object for inserting data.
        """
        from sqlkit.operations.dml import InsertQuery

        return InsertQuery(self, values, dialect=self.dialect)

    def update(self, **values: Any) -> UpdateQuery:
        """
        Create an UPDATE query.

        Parameters
        ----------
        **values : Any
            Column-value pairs to update.

        Returns
        -------
        UpdateQuery
            Query object for updating data.
        """
        from sqlkit.operations.dml import UpdateQuery

        return UpdateQuery(self, values, dialect=self.dialect)

    def delete(self) -> DeleteQuery:
        """
        Create a DELETE query.

        Returns
        -------
        DeleteQuery
            Query object for deleting data.
        """
        from sqlkit.operations.dml import DeleteQuery

        return DeleteQuery(self, dialect=self.dialect)

    def create_as_select(
        self, query: Any, table_name: str | None = None
    ) -> CTASQuery:
        """
        Create a CREATE TABLE AS SELECT query.

        Parameters
        ----------
        query : Any
            SELECT query to use for table creation.
        table_name : str, optional
            Name for the new table. Defaults to {original_name}_copy.

        Returns
        -------
        CTASQuery
            Query object for CTAS operation.
        """
        from sqlkit.operations.ddl import CTASQuery

        return CTASQuery(
            self,
            query,
            table_name or f"{self.name}_copy",
            dialect=self.dialect,
        )

    def __repr__(self) -> str:
        """Return string representation of the table."""
        return f"<{self.__class__.__name__}('{self.name}')>"

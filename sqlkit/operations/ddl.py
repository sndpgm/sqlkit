"""
DDL (Data Definition Language) query operations.

This module provides query classes for DDL operations like CREATE TABLE,
DROP TABLE, TRUNCATE, and CREATE TABLE AS SELECT.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Dialect
from sqlalchemy.sql import ClauseElement

from sqlkit.operations.base import BaseQuery

if TYPE_CHECKING:
    from sqlkit.core.table import SQLTable


class CreateTableQuery(BaseQuery):
    """
    Query class for CREATE TABLE operations.

    Parameters
    ----------
    table : SQLTable
        The table to create.
    if_not_exists : bool, default False
        Whether to use IF NOT EXISTS clause.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.

    Attributes
    ----------
    if_not_exists : bool
        Whether to use IF NOT EXISTS clause.
    """

    def __init__(
        self,
        table: SQLTable,
        if_not_exists: bool = False,
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize CreateTableQuery."""
        super().__init__(table, dialect)
        self.if_not_exists = if_not_exists

    def build(self) -> ClauseElement:
        """
        Build CREATE TABLE SQL statement.

        Returns
        -------
        ClauseElement
            SQL text element for CREATE TABLE.
        """
        if_not_exists_clause = "IF NOT EXISTS " if self.if_not_exists else ""
        create_sql = f"CREATE TABLE {if_not_exists_clause}{self.table.name} ("

        columns = []
        for col in self.table.table.columns:
            col_def = f"{col.name} {col.type}"
            if col.primary_key:
                col_def += " PRIMARY KEY"
            if not col.nullable:
                col_def += " NOT NULL"
            columns.append(col_def)

        create_sql += ", ".join(columns) + ")"
        return text(create_sql)


class DropTableQuery(BaseQuery):
    """
    Query class for DROP TABLE operations.

    Parameters
    ----------
    table : SQLTable
        The table to drop.
    if_exists : bool, default False
        Whether to use IF EXISTS clause.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.

    Attributes
    ----------
    if_exists : bool
        Whether to use IF EXISTS clause.
    """

    def __init__(
        self,
        table: SQLTable,
        if_exists: bool = False,
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize DropTableQuery."""
        super().__init__(table, dialect)
        self.if_exists = if_exists

    def build(self) -> ClauseElement:
        """
        Build DROP TABLE SQL statement.

        Returns
        -------
        ClauseElement
            SQL text element for DROP TABLE.
        """
        if_exists_clause = "IF EXISTS " if self.if_exists else ""
        drop_sql = f"DROP TABLE {if_exists_clause}{self.table.name}"
        return text(drop_sql)


class TruncateQuery(BaseQuery):
    """
    Query class for TRUNCATE TABLE operations.

    Parameters
    ----------
    table : SQLTable
        The table to truncate.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.
    """

    def build(self) -> ClauseElement:
        """
        Build TRUNCATE TABLE SQL statement.

        Returns
        -------
        ClauseElement
            SQL text element for TRUNCATE TABLE.
        """
        return text(f"TRUNCATE TABLE {self.table.name}")


class CTASQuery(BaseQuery):
    """
    Query class for CREATE TABLE AS SELECT operations.

    Parameters
    ----------
    table : SQLTable
        The source table.
    select_query : Any
        The SELECT query to use for table creation.
    new_table_name : str
        Name for the new table.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.

    Attributes
    ----------
    select_query : Any
        The SELECT query.
    new_table_name : str
        Name for the new table.
    """

    def __init__(
        self,
        table: SQLTable,
        select_query: Any,
        new_table_name: str,
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize CTASQuery."""
        super().__init__(table, dialect)
        self.select_query = select_query
        self.new_table_name = new_table_name

    def build(self) -> ClauseElement:
        """
        Build CREATE TABLE AS SELECT SQL statement.

        Returns
        -------
        ClauseElement
            SQL text element for CTAS.
        """
        if hasattr(self.select_query, "compile"):
            select_sql = self.select_query.compile()
        else:
            select_sql = str(self.select_query)

        ctas_sql = f"CREATE TABLE {self.new_table_name} AS {select_sql}"
        return text(ctas_sql)

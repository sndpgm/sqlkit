"""
DML (Data Manipulation Language) query operations.

This module provides query classes for DML operations like SELECT, INSERT,
UPDATE, and DELETE.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update
from sqlalchemy.engine import Dialect
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import Column

from sqlkit.operations.base import BaseQuery

if TYPE_CHECKING:
    from sqlkit.core.table import SQLTable


class SelectQuery(BaseQuery):
    """
    Query class for SELECT operations.

    Parameters
    ----------
    table : SQLTable
        The table to select from.
    columns : tuple
        Columns to select. Empty tuple means all columns.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.

    Attributes
    ----------
    columns : list
        List of columns to select.
    """

    def __init__(
        self,
        table: SQLTable,
        columns: tuple[str | Column, ...],
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize SelectQuery."""
        super().__init__(table, dialect)
        self.columns: list[Any] = list(columns) if columns else [table.table]
        self._where_conditions: list[Any] = []
        self._joins: list[tuple[Any, Any, str]] = []
        self._order_by: list[Any] = []
        self._group_by: list[Any] = []
        self._having: list[Any] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None

    def where(self, condition: Any) -> SelectQuery:
        """
        Add WHERE clause condition.

        Parameters
        ----------
        condition : Any
            SQLAlchemy condition expression.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._where_conditions.append(condition)
        return self

    def join(
        self,
        other_table: Any,
        on_condition: Any,
        join_type: str = "INNER",
    ) -> SelectQuery:
        """
        Add JOIN clause.

        Parameters
        ----------
        other_table : Any
            Table to join with.
        on_condition : Any
            Join condition.
        join_type : str, default "INNER"
            Type of join (INNER, LEFT, RIGHT).

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._joins.append((other_table, on_condition, join_type))
        return self

    def left_join(self, other_table: Any, on_condition: Any) -> SelectQuery:
        """
        Add LEFT JOIN clause.

        Parameters
        ----------
        other_table : Any
            Table to join with.
        on_condition : Any
            Join condition.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        return self.join(other_table, on_condition, "LEFT")

    def right_join(self, other_table: Any, on_condition: Any) -> SelectQuery:
        """
        Add RIGHT JOIN clause.

        Parameters
        ----------
        other_table : Any
            Table to join with.
        on_condition : Any
            Join condition.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        return self.join(other_table, on_condition, "RIGHT")

    def order_by(self, *columns: Any) -> SelectQuery:
        """
        Add ORDER BY clause.

        Parameters
        ----------
        *columns : Any
            Columns to order by.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._order_by.extend(columns)
        return self

    def group_by(self, *columns: Any) -> SelectQuery:
        """
        Add GROUP BY clause.

        Parameters
        ----------
        *columns : Any
            Columns to group by.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._group_by.extend(columns)
        return self

    def having(self, condition: Any) -> SelectQuery:
        """
        Add HAVING clause condition.

        Parameters
        ----------
        condition : Any
            HAVING condition.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._having.append(condition)
        return self

    def limit(self, count: int) -> SelectQuery:
        """
        Add LIMIT clause.

        Parameters
        ----------
        count : int
            Number of rows to limit.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._limit_val = count
        return self

    def offset(self, count: int) -> SelectQuery:
        """
        Add OFFSET clause.

        Parameters
        ----------
        count : int
            Number of rows to offset.

        Returns
        -------
        SelectQuery
            Self for method chaining.
        """
        self._offset_val = count
        return self

    def build(self) -> ClauseElement:
        """
        Build SELECT SQL statement.

        Returns
        -------
        ClauseElement
            SQLAlchemy select statement.
        """
        # Convert string column names to proper column references
        columns = []
        for col in self.columns:
            if isinstance(col, str):
                # Convert string to table column reference
                columns.append(getattr(self.table.c, col))
            else:
                columns.append(col)

        stmt = select(*columns)

        for condition in self._where_conditions:
            stmt = stmt.where(condition)

        for table, on_condition, join_type in self._joins:
            table_obj = table.table if hasattr(table, "table") else table
            if join_type.upper() == "LEFT":
                stmt = stmt.outerjoin(table_obj, on_condition)
            elif join_type.upper() == "RIGHT":
                # SQLAlchemy doesn't have direct right join
                stmt = stmt.join(table_obj, on_condition)
            else:
                stmt = stmt.join(table_obj, on_condition)

        if self._group_by:
            stmt = stmt.group_by(*self._group_by)

        for condition in self._having:
            stmt = stmt.having(condition)

        if self._order_by:
            stmt = stmt.order_by(*self._order_by)

        if self._limit_val:
            stmt = stmt.limit(self._limit_val)

        if self._offset_val:
            stmt = stmt.offset(self._offset_val)

        return stmt


class InsertQuery(BaseQuery):
    """Query class for INSERT operations."""

    def __init__(
        self,
        table: SQLTable,
        values: dict[str, Any],
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize InsertQuery."""
        super().__init__(table, dialect)
        self.values = values
        self._on_conflict: Any = None

    def on_conflict_do_nothing(self) -> InsertQuery:
        """Set ON CONFLICT DO NOTHING."""
        self._on_conflict = "DO_NOTHING"
        return self

    def on_conflict_do_update(self, **update_values: Any) -> InsertQuery:
        """Set ON CONFLICT DO UPDATE."""
        self._on_conflict = ("DO_UPDATE", update_values)
        return self

    def build(self) -> ClauseElement:
        """Build INSERT SQL statement."""
        stmt = insert(self.table.table)
        if self.values:
            stmt = stmt.values(**self.values)
        return stmt


class UpdateQuery(BaseQuery):
    """Query class for UPDATE operations."""

    def __init__(
        self,
        table: SQLTable,
        values: dict[str, Any],
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize UpdateQuery."""
        super().__init__(table, dialect)
        self.values = values
        self._where_conditions: list[Any] = []

    def where(self, condition: Any) -> UpdateQuery:
        """Add WHERE clause condition."""
        self._where_conditions.append(condition)
        return self

    def build(self) -> ClauseElement:
        """Build UPDATE SQL statement."""
        stmt = update(self.table.table).values(**self.values)
        for condition in self._where_conditions:
            stmt = stmt.where(condition)
        return stmt


class DeleteQuery(BaseQuery):
    """Query class for DELETE operations."""

    def __init__(
        self, table: SQLTable, dialect: Dialect | None = None
    ) -> None:
        """Initialize DeleteQuery."""
        super().__init__(table, dialect)
        self._where_conditions: list[Any] = []

    def where(self, condition: Any) -> DeleteQuery:
        """Add WHERE clause condition."""
        self._where_conditions.append(condition)
        return self

    def build(self) -> ClauseElement:
        """Build DELETE SQL statement."""
        stmt = delete(self.table.table)
        for condition in self._where_conditions:
            stmt = stmt.where(condition)
        return stmt

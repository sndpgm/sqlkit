"""
Base classes for SQL query operations.

This module provides the foundation classes for all SQL query operations
in SQLKit.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import Dialect
from sqlalchemy.sql import ClauseElement

if TYPE_CHECKING:
    from sqlkit.core.table import SQLTable


class BaseQuery(ABC):
    """
    Base class for all SQL queries.

    Parameters
    ----------
    table : SQLTable
        The table this query operates on.
    dialect : Dialect, optional
        SQLAlchemy dialect for SQL generation.

    Attributes
    ----------
    table : SQLTable
        The target table.
    dialect : Dialect or None
        SQLAlchemy dialect.
    """

    def __init__(
        self, table: SQLTable, dialect: Dialect | None = None
    ) -> None:
        """Initialize BaseQuery instance."""
        self.table = table
        self.dialect = dialect

    @abstractmethod
    def build(self) -> ClauseElement:
        """
        Build the SQLAlchemy query object.

        Returns
        -------
        ClauseElement
            SQLAlchemy query object ready for compilation.
        """
        pass

    def compile(self, **kwargs: Any) -> str:
        """
        Compile the query to SQL string.

        Parameters
        ----------
        **kwargs : Any
            Additional compilation arguments passed to SQLAlchemy.

        Returns
        -------
        str
            Compiled SQL string.
        """
        query = self.build()
        compile_kwargs: dict[str, Any] = {"literal_binds": True}
        compile_kwargs.update(kwargs)

        if self.dialect:
            return str(
                query.compile(
                    dialect=self.dialect, compile_kwargs=compile_kwargs
                )
            )
        return str(query.compile(compile_kwargs=compile_kwargs))

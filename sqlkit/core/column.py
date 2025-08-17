"""
Column definitions and data types for SQLKit.

This module re-exports SQLAlchemy column and data type classes for use
in SQLKit table definitions.
"""

from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    Time,
)
from sqlalchemy import (
    Column as SQLAColumn,
)

from sqlkit.core.type_parser import parse_column_type


class Column(SQLAColumn):
    """
    Enhanced Column class with flexible type specification support.

    This class extends SQLAlchemy's Column to support string-based type
    specifications with various aliases and formats.

    Parameters
    ----------
    name : str
        Column name.
    type_ : str, type, or SQLAlchemy type
        Column type specification. Can be:
        - SQLAlchemy type class (Integer, String, etc.)
        - String specification ('int', 'string(100)', 'numeric(18,5)', etc.)
    *args : Any
        Additional positional arguments for SQLAlchemy Column.
    **kwargs : Any
        Additional keyword arguments for SQLAlchemy Column.

    Examples
    --------
    >>> Column('id', 'int', primary_key=True)
    >>> Column('name', 'string(255)', nullable=False)
    >>> Column('amount', 'numeric(18,5)')
    >>> Column('created_at', 'datetime')
    >>> Column('is_active', 'bool', default=True)
    """

    def __init__(
        self, name: str, type_: str | type | Any, *args: Any, **kwargs: Any
    ) -> None:
        """Initialize enhanced Column with type parsing."""
        # Parse the type specification
        parsed_type = parse_column_type(type_)

        # Call parent constructor with parsed type
        super().__init__(name, parsed_type, *args, **kwargs)


__all__ = [
    "Column",
    "Integer",
    "String",
    "DateTime",
    "Boolean",
    "Float",
    "Numeric",
    "Text",
    "Date",
    "Time",
]

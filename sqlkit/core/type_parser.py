"""
Type parsing utilities for flexible column type specification.

This module provides functionality to parse string-based column type
specifications and convert them to SQLAlchemy column types.
"""

import re
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


class TypeParser:
    """
    Parser for flexible column type specifications.

    Supports various aliases and formats for specifying column types,
    including length and precision parameters.

    Examples
    --------
    - 'int', 'integer', 'INT' -> Integer
    - 'str', 'string', 'varchar' -> String
    - 'string(100)', 'varchar(255)' -> String(length)
    - 'numeric(18,5)', 'decimal(10,2)' -> Numeric(precision, scale)
    """

    # Type aliases mapping (all keys in lowercase for case-insensitive lookup)
    TYPE_ALIASES = {
        # Integer types
        "int": Integer,
        "integer": Integer,
        "bigint": Integer,
        # String types
        "str": String,
        "string": String,
        "varchar": String,
        "char": String,
        "text": Text,
        # Numeric types
        "numeric": Numeric,
        "decimal": Numeric,
        "number": Numeric,
        # Float types
        "float": Float,
        "real": Float,
        "double": Float,
        # Boolean types
        "bool": Boolean,
        "boolean": Boolean,
        # Date/Time types
        "date": Date,
        "datetime": DateTime,
        "timestamp": DateTime,
        "time": Time,
    }

    @classmethod
    def parse_type_spec(cls, type_spec: str | type) -> Any:
        """
        Parse a type specification string or return the type if already a type.

        Parameters
        ----------
        type_spec : str or type
            Type specification string (e.g., 'int', 'string(100)',
            'numeric(18,5)') or SQLAlchemy type class.

        Returns
        -------
        Any
            SQLAlchemy column type class.

        Raises
        ------
        ValueError
            If the type specification is not recognized.

        Examples
        --------
        >>> TypeParser.parse_type_spec('int')
        <class 'sqlalchemy.sql.sqltypes.Integer'>
        >>> TypeParser.parse_type_spec('string(100)')
        String(length=100)
        >>> TypeParser.parse_type_spec('numeric(18,5)')
        Numeric(precision=18, scale=5)
        """
        # If already a type, return as-is
        if not isinstance(type_spec, str):
            return type_spec

        # Parse string specification
        return cls._parse_string_spec(type_spec)

    @classmethod
    def _parse_string_spec(cls, spec: str) -> Any:
        """Parse a string type specification."""
        # Check for parameterized types: type(param1, param2, ...)
        param_match = re.match(r"^(\w+)\s*\(\s*(.*?)\s*\)$", spec.strip())

        if param_match:
            base_type = param_match.group(1)
            params_str = param_match.group(2)
            return cls._parse_parameterized_type(base_type, params_str)
        else:
            # Simple type without parameters
            return cls._parse_simple_type(spec.strip())

    @classmethod
    def _parse_simple_type(cls, type_name: str) -> Any:
        """Parse a simple type name without parameters."""
        # Convert to lowercase for case-insensitive lookup
        normalized_name = type_name.lower()
        if normalized_name in cls.TYPE_ALIASES:
            return cls.TYPE_ALIASES[normalized_name]
        else:
            raise ValueError(f"Unknown column type: {type_name}")

    @classmethod
    def _parse_parameterized_type(cls, base_type: str, params_str: str) -> Any:
        """Parse a parameterized type specification."""
        # Get the base type class with case-insensitive lookup
        normalized_base = base_type.lower()
        if normalized_base not in cls.TYPE_ALIASES:
            raise ValueError(f"Unknown column type: {base_type}")

        base_class = cls.TYPE_ALIASES[normalized_base]

        # Parse parameters
        if not params_str.strip():
            return base_class()

        # Split parameters by comma
        params = [p.strip() for p in params_str.split(",")]

        # Handle different type-specific parameter patterns
        if base_class in (String, Text):
            # String types: length parameter
            if len(params) == 1:
                try:
                    length = int(params[0])
                    return base_class(length)
                except ValueError:
                    raise ValueError(
                        f"Invalid length parameter for {base_type}: "
                        f"{params[0]}"
                    )
            else:
                raise ValueError(
                    f"String types expect 1 parameter, got {len(params)}"
                )

        elif base_class == Numeric:
            # Numeric types: precision and optional scale
            if len(params) == 1:
                try:
                    precision = int(params[0])
                    return base_class(precision)
                except ValueError:
                    raise ValueError(
                        f"Invalid precision parameter for {base_type}: "
                        f"{params[0]}"
                    )
            elif len(params) == 2:
                try:
                    precision = int(params[0])
                    scale = int(params[1])
                    return base_class(precision, scale)
                except ValueError:
                    raise ValueError(
                        f"Invalid precision/scale parameters for {base_type}: "
                        f"{params}"
                    )
            else:
                raise ValueError(
                    f"Numeric types expect 1-2 parameters, got {len(params)}"
                )

        else:
            # For other types, just pass parameters as-is
            try:
                int_params = [int(p) for p in params]
                return base_class(*int_params)
            except ValueError:
                raise ValueError(
                    f"Invalid parameters for {base_type}: {params}"
                )


def parse_column_type(type_spec: str | type) -> Any:
    """
    Convenience function to parse column type specifications.

    This is a shortcut for TypeParser.parse_type_spec().

    Parameters
    ----------
    type_spec : str or type
        Type specification.

    Returns
    -------
    Any
        SQLAlchemy column type class.
    """
    return TypeParser.parse_type_spec(type_spec)

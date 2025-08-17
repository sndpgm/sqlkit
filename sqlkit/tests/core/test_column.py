"""
Tests for sqlkit.core.column module.

This module tests the column definitions and data type re-exports
from SQLAlchemy using pytest.
"""

from sqlalchemy import Column as SQLAColumn
from sqlalchemy.sql.sqltypes import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    Time,
)

from sqlkit.core.column import (
    Boolean as SQLKitBoolean,
)
from sqlkit.core.column import (
    Column,
)
from sqlkit.core.column import (
    Date as SQLKitDate,
)
from sqlkit.core.column import (
    DateTime as SQLKitDateTime,
)
from sqlkit.core.column import (
    Float as SQLKitFloat,
)
from sqlkit.core.column import (
    Integer as SQLKitInteger,
)
from sqlkit.core.column import (
    String as SQLKitString,
)
from sqlkit.core.column import (
    Text as SQLKitText,
)
from sqlkit.core.column import (
    Time as SQLKitTime,
)


class TestColumn:
    """Test column functionality."""

    def test_column_is_subclass_of_sqlalchemy_column(self):
        """Test that Column is a subclass of SQLAlchemy Column."""
        assert issubclass(Column, SQLAColumn)

    def test_column_creation(self):
        """Test creating a column instance."""
        col = Column("test", Integer, primary_key=True)
        assert col.name == "test"
        assert col.type.__class__ == Integer
        assert col.primary_key is True

    def test_column_with_string_type(self):
        """Test creating a column with String type."""
        col = Column("name", String(255), nullable=False)
        assert col.name == "name"
        assert isinstance(col.type, String)
        assert col.nullable is False

    def test_column_with_boolean_type(self):
        """Test creating a column with Boolean type."""
        col = Column("active", Boolean, default=True)
        assert col.name == "active"
        assert isinstance(col.type, Boolean)

    def test_column_with_datetime_type(self):
        """Test creating a column with DateTime type."""
        col = Column("created_at", DateTime)
        assert col.name == "created_at"
        assert isinstance(col.type, DateTime)


class TestDataTypeReExports:
    """Test that all data types are properly re-exported."""

    def test_integer_type(self):
        """Test Integer type re-export."""
        assert SQLKitInteger is Integer

    def test_string_type(self):
        """Test String type re-export."""
        assert SQLKitString is String

    def test_datetime_type(self):
        """Test DateTime type re-export."""
        assert SQLKitDateTime is DateTime

    def test_boolean_type(self):
        """Test Boolean type re-export."""
        assert SQLKitBoolean is Boolean

    def test_float_type(self):
        """Test Float type re-export."""
        assert SQLKitFloat is Float

    def test_text_type(self):
        """Test Text type re-export."""
        assert SQLKitText is Text

    def test_date_type(self):
        """Test Date type re-export."""
        assert SQLKitDate is Date

    def test_time_type(self):
        """Test Time type re-export."""
        assert SQLKitTime is Time

    def test_all_types_available(self):
        """Test that all expected types are available in __all__."""
        from sqlkit.core.column import __all__

        expected_types = [
            "Column",
            "Integer",
            "String",
            "DateTime",
            "Boolean",
            "Float",
            "Text",
            "Date",
            "Time",
        ]

        for expected_type in expected_types:
            assert expected_type in __all__

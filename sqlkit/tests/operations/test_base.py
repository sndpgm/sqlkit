"""
Tests for sqlkit.operations.base module.

This module tests the base query functionality and
abstract classes using pytest.
"""

from unittest.mock import Mock

import pytest
from sqlalchemy import text

from sqlkit.operations.base import BaseQuery


class ConcreteQuery(BaseQuery):
    """Concrete implementation of BaseQuery for testing."""

    def build(self):
        """Build a simple test query."""
        return text("SELECT 1")


class TestBaseQuery:
    """Test BaseQuery abstract base class."""

    @pytest.fixture
    def mock_table(self):
        """Create a mock table for testing."""
        mock_table = Mock()
        mock_table.name = "test_table"
        return mock_table

    def test_base_query_initialization(self, mock_table):
        """Test BaseQuery initialization."""
        query = ConcreteQuery(mock_table)
        assert query.table == mock_table
        assert query.dialect is None

    def test_base_query_with_dialect(self, mock_table):
        """Test BaseQuery initialization with dialect."""
        mock_dialect = Mock()
        query = ConcreteQuery(mock_table, dialect=mock_dialect)
        assert query.table == mock_table
        assert query.dialect == mock_dialect

    def test_build_method_abstract(self, mock_table):
        """Test that build method is abstract."""
        with pytest.raises(TypeError):
            # Use type: ignore to suppress static analysis warning
            BaseQuery(mock_table)  # type: ignore[abstract]

    def test_compile_method(self, mock_table):
        """Test compile method returns SQL string."""
        query = ConcreteQuery(mock_table)
        sql = query.compile()
        assert isinstance(sql, str)
        assert "SELECT 1" in sql

    def test_compile_with_kwargs(self, mock_table):
        """Test compile method with additional kwargs."""
        query = ConcreteQuery(mock_table)
        # This should not raise an error
        sql = query.compile(literal_binds=False)
        assert isinstance(sql, str)

    def test_compile_with_dialect(self, mock_table):
        """Test compile method with dialect."""
        from sqlalchemy.dialects import sqlite

        query = ConcreteQuery(mock_table, dialect=sqlite.dialect())
        sql = query.compile()
        assert isinstance(sql, str)
        assert "SELECT 1" in sql

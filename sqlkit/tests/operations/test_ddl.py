"""
Tests for sqlkit.operations.ddl module.

This module tests DDL query functionality using pytest.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from sqlkit.core import Column
from sqlkit.core.column import Integer
from sqlkit.operations.ddl import (
    CreateTableQuery,
    CTASQuery,
    DropTableQuery,
    TruncateQuery,
)
from sqlkit.tests.conftest import ConcreteSQLTable, assert_sql_contains


class TestDDLQueries:
    """Test DDL query classes."""

    @pytest.fixture
    def table(self, sample_columns):
        """Create a table for DDL testing."""
        return ConcreteSQLTable("test_table", *sample_columns)

    def test_create_table_query(self, table):
        """Test CreateTableQuery basic functionality."""
        query = CreateTableQuery(table)
        assert query.table == table
        assert query.if_not_exists is False

        sql = query.compile()
        assert_sql_contains(sql, "CREATE TABLE test_table")
        assert_sql_contains(sql, "id")
        assert_sql_contains(sql, "name")

    def test_create_table_query_if_not_exists(self, table):
        """Test CreateTableQuery with IF NOT EXISTS."""
        query = CreateTableQuery(table, if_not_exists=True)
        assert query.if_not_exists is True

        sql = query.compile()
        assert_sql_contains(sql, "CREATE TABLE IF NOT EXISTS test_table")

    def test_drop_table_query(self, table):
        """Test DropTableQuery basic functionality."""
        query = DropTableQuery(table)
        assert query.table == table
        assert query.if_exists is False

        sql = query.compile()
        assert_sql_contains(sql, "DROP TABLE test_table")

    def test_drop_table_query_if_exists(self, table):
        """Test DropTableQuery with IF EXISTS."""
        query = DropTableQuery(table, if_exists=True)
        assert query.if_exists is True

        sql = query.compile()
        assert_sql_contains(sql, "DROP TABLE IF EXISTS test_table")

    def test_truncate_query(self, table):
        """Test TruncateQuery functionality."""
        query = TruncateQuery(table)
        assert query.table == table

        sql = query.compile()
        assert_sql_contains(sql, "TRUNCATE TABLE test_table")

    def test_ctas_query(self, table):
        """Test CTASQuery functionality."""
        mock_select_query = Mock()
        mock_select_query.compile.return_value = (
            "SELECT * FROM source_table WHERE id > 100"
        )

        query = CTASQuery(table, mock_select_query, "new_test_table")
        assert query.table == table
        assert query.select_query == mock_select_query
        assert query.new_table_name == "new_test_table"

        sql = query.compile()
        assert_sql_contains(sql, "CREATE TABLE new_test_table AS")
        assert_sql_contains(sql, "SELECT * FROM source_table")

    def test_ctas_query_with_string_query(self, table):
        """Test CTASQuery with string query."""
        string_query = "SELECT * FROM another_table"
        query = CTASQuery(table, string_query, "string_test_table")

        sql = query.compile()
        assert_sql_contains(sql, "CREATE TABLE string_test_table AS")
        assert_sql_contains(sql, "SELECT * FROM another_table")


class TestDDLQueryEdgeCases:
    """Test DDL query edge cases and error conditions."""

    @pytest.fixture
    def table(self):
        """Create a minimal table for edge case testing."""
        return ConcreteSQLTable(
            "edge_case_table", Column("id", Integer, primary_key=True)
        )

    def test_create_table_with_schema(self, table):
        """Test CREATE TABLE with schema."""
        table.schema = "test_schema"
        query = CreateTableQuery(table)
        sql = query.compile()
        # Schema handling varies by dialect, just check basic structure
        assert_sql_contains(sql, "CREATE TABLE")
        assert_sql_contains(sql, "edge_case_table")

    def test_drop_table_basic_functionality(self, table):
        """Test basic DROP TABLE functionality."""
        query = DropTableQuery(table, if_exists=True)
        sql = query.compile()
        assert_sql_contains(sql, "DROP TABLE")
        assert_sql_contains(sql, "edge_case_table")

    def test_ctas_query_compilation_error_handling(self, table):
        """Test CTAS query with mock that raises error."""
        mock_query = Mock()
        mock_query.compile.side_effect = Exception("Compilation error")

        query = CTASQuery(table, mock_query, "error_table")
        with pytest.raises(Exception, match="Compilation error"):
            query.compile()

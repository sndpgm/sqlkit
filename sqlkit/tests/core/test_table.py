"""
Tests for sqlkit.core.table module.

This module tests the base SQLTable class functionality using pytest.
"""

from __future__ import annotations

from unittest.mock import Mock

from sqlalchemy import MetaData, Table

from sqlkit.core import Column
from sqlkit.core.column import Integer
from sqlkit.tests.conftest import ConcreteSQLTable, assert_sql_contains


class TestSQLTable:
    """Test SQLTable base functionality."""

    def test_table_initialization(self, sample_columns):
        """Test table initialization."""
        table = ConcreteSQLTable("test_table", *sample_columns)

        assert table.name == "test_table"
        assert table.schema is None
        assert table.dialect is None
        assert isinstance(table._metadata, MetaData)
        assert isinstance(table._table, Table)

    def test_table_with_schema(self):
        """Test table initialization with schema."""
        table = ConcreteSQLTable(
            "test_table",
            Column("id", Integer, primary_key=True),
            schema="test_schema",
        )
        assert table.name == "test_table"
        assert table.schema == "test_schema"
        assert table._metadata.schema == "test_schema"

    def test_table_with_dialect(self):
        """Test table initialization with dialect."""
        mock_dialect = Mock()
        table = ConcreteSQLTable(
            "test_table",
            Column("id", Integer, primary_key=True),
            dialect=mock_dialect,
        )
        assert table.dialect == mock_dialect

    def test_table_property(self, basic_table):
        """Test table property returns SQLAlchemy Table."""
        sqlalchemy_table = basic_table.table
        assert isinstance(sqlalchemy_table, Table)
        assert sqlalchemy_table.name == "test_table"

    def test_columns_property(self, basic_table):
        """Test columns property returns table columns."""
        columns = basic_table.columns
        assert len(columns) == 3
        column_names = [col.name for col in columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names

    def test_c_property(self, basic_table):
        """Test c property for column access."""
        c = basic_table.c
        assert hasattr(c, "id")
        assert hasattr(c, "name")
        assert hasattr(c, "email")
        assert c.id.name == "id"
        assert c.name.name == "name"

    def test_create_method(self, basic_table):
        """Test create method returns CreateTableQuery."""
        create_query = basic_table.create()
        assert create_query.table == basic_table
        assert create_query.if_not_exists is False

    def test_create_method_with_if_not_exists(self, basic_table):
        """Test create method with if_not_exists flag."""
        create_query = basic_table.create(if_not_exists=True)
        assert create_query.table == basic_table
        assert create_query.if_not_exists is True

    def test_drop_method(self, basic_table):
        """Test drop method returns DropTableQuery."""
        drop_query = basic_table.drop()
        assert drop_query.table == basic_table
        assert drop_query.if_exists is False

    def test_drop_method_with_if_exists(self, basic_table):
        """Test drop method with if_exists flag."""
        drop_query = basic_table.drop(if_exists=True)
        assert drop_query.table == basic_table
        assert drop_query.if_exists is True

    def test_truncate_method(self, basic_table):
        """Test truncate method returns TruncateQuery."""
        truncate_query = basic_table.truncate()
        assert truncate_query.table == basic_table

    def test_select_method(self, basic_table):
        """Test select method returns SelectQuery."""
        select_query = basic_table.select()
        assert select_query.table == basic_table
        assert len(select_query.columns) == 1  # [table.table] for all

    def test_select_method_with_columns(self, basic_table):
        """Test select method with specific columns."""
        select_query = basic_table.select("name", "id")
        assert select_query.table == basic_table
        assert len(select_query.columns) == 2

    def test_insert_method(self, basic_table):
        """Test insert method returns InsertQuery."""
        insert_query = basic_table.insert(name="Test", id=1)
        assert insert_query.table == basic_table
        assert insert_query.values == {"name": "Test", "id": 1}

    def test_update_method(self, basic_table):
        """Test update method returns UpdateQuery."""
        update_query = basic_table.update(name="Updated")
        assert update_query.table == basic_table
        assert update_query.values == {"name": "Updated"}

    def test_delete_method(self, basic_table):
        """Test delete method returns DeleteQuery."""
        delete_query = basic_table.delete()
        assert delete_query.table == basic_table

    def test_create_as_select_method(self, basic_table):
        """Test create_as_select method returns CTASQuery."""
        mock_query = Mock()
        ctas_query = basic_table.create_as_select(mock_query)
        assert ctas_query.table == basic_table
        assert ctas_query.select_query == mock_query
        assert ctas_query.new_table_name == "test_table_copy"

    def test_create_as_select_with_custom_name(self, basic_table):
        """Test create_as_select with custom table name."""
        mock_query = Mock()
        ctas_query = basic_table.create_as_select(
            mock_query, table_name="custom_table"
        )
        assert ctas_query.new_table_name == "custom_table"

    def test_repr(self, basic_table):
        """Test string representation of table."""
        repr_str = repr(basic_table)
        assert "ConcreteSQLTable" in repr_str
        assert "test_table" in repr_str


class TestTableSQLGeneration:
    """Test SQL generation functionality."""

    def test_create_table_sql(self, basic_table):
        """Test CREATE TABLE SQL generation."""
        create_sql = basic_table.create().compile()
        assert_sql_contains(create_sql, "CREATE TABLE test_table")
        assert_sql_contains(create_sql, "id")
        assert_sql_contains(create_sql, "name")
        assert_sql_contains(create_sql, "email")

    def test_select_sql(self, basic_table):
        """Test SELECT SQL generation."""
        select_sql = basic_table.select("name", "email").compile()
        assert_sql_contains(select_sql, "SELECT")
        assert_sql_contains(select_sql, "name")
        assert_sql_contains(select_sql, "email")

    def test_insert_sql(self, basic_table):
        """Test INSERT SQL generation."""
        insert_sql = basic_table.insert(
            name="Test User", email="test@example.com"
        ).compile()
        assert_sql_contains(insert_sql, "INSERT INTO test_table")

    def test_update_sql(self, basic_table):
        """Test UPDATE SQL generation."""
        update_sql = (
            basic_table.update(name="Updated Name")
            .where(basic_table.c.id == 1)
            .compile()
        )
        assert_sql_contains(update_sql, "UPDATE test_table")
        assert_sql_contains(update_sql, "WHERE")

    def test_delete_sql(self, basic_table):
        """Test DELETE SQL generation."""
        delete_sql = (
            basic_table.delete().where(basic_table.c.id == 1).compile()
        )
        assert_sql_contains(delete_sql, "DELETE FROM test_table")
        assert_sql_contains(delete_sql, "WHERE")

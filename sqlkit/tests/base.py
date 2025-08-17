"""
Base test classes and utilities for SQLKit testing.

This module provides common test fixtures, utilities, and base classes
that can be used across all test modules.
"""

from __future__ import annotations

import pytest

from sqlkit.core import Column, SQLTable
from sqlkit.core.column import Integer, String


class BaseTestCase:
    """
    Base test case for all SQLKit tests.

    Provides common setup, teardown, and utility methods for testing
    SQLKit functionality.
    """

    @pytest.fixture(autouse=True)
    def setup_test_case(self) -> None:
        """Set up common test fixtures."""
        self.test_table = None
        self.sample_columns = [
            Column("id", Integer, primary_key=True),
            Column("name", String(255), nullable=False),
            Column("email", String(255), unique=True),
        ]

    def assert_sql_contains(self, sql: str, expected: str) -> None:
        """
        Assert that SQL string contains expected substring.

        Parameters
        ----------
        sql : str
            Generated SQL string.
        expected : str
            Expected substring.
        """
        assert expected.upper() in sql.upper()

    def assert_sql_not_contains(self, sql: str, not_expected: str) -> None:
        """
        Assert that SQL string does not contain substring.

        Parameters
        ----------
        sql : str
            Generated SQL string.
        not_expected : str
            Substring that should not be present.
        """
        assert not_expected.upper() not in sql.upper()

    def create_test_table(
        self, table_class: type[SQLTable], table_name: str = "test_table"
    ) -> SQLTable:
        """
        Create a test table instance.

        Parameters
        ----------
        table_class : type[SQLTable]
            The table class to instantiate.
        table_name : str, default "test_table"
            Name for the test table.

        Returns
        -------
        SQLTable
            Test table instance.
        """
        return table_class(table_name, *self.sample_columns)


class DatabaseTestMixin:
    """
    Mixin class for database-specific test cases.

    Provides common test methods that can be mixed into specific
    database dialect test classes.
    """

    table: SQLTable | None = None

    def assert_sql_contains(self, sql: str, expected: str) -> None:
        """
        Assert that SQL string contains expected substring.

        Parameters
        ----------
        sql : str
            Generated SQL string.
        expected : str
            Expected substring.
        """
        assert expected.upper() in sql.upper()

    def assert_sql_not_contains(self, sql: str, not_expected: str) -> None:
        """
        Assert that SQL string does not contain substring.

        Parameters
        ----------
        sql : str
            Generated SQL string.
        not_expected : str
            Substring that should not be present.
        """
        assert not_expected.upper() not in sql.upper()

    def test_create_table(self) -> None:
        """Test CREATE TABLE statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        create_sql = self.table.create().compile()
        self.assert_sql_contains(create_sql, "CREATE TABLE")
        self.assert_sql_contains(create_sql, self.table.name)

    def test_create_table_if_not_exists(self) -> None:
        """Test CREATE TABLE IF NOT EXISTS statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        create_sql = self.table.create(if_not_exists=True).compile()
        self.assert_sql_contains(create_sql, "CREATE TABLE")
        # Note: Not all dialects support IF NOT EXISTS

    def test_drop_table(self) -> None:
        """Test DROP TABLE statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        drop_sql = self.table.drop().compile()
        self.assert_sql_contains(drop_sql, "DROP TABLE")
        self.assert_sql_contains(drop_sql, self.table.name)

    def test_truncate_table(self) -> None:
        """Test TRUNCATE TABLE statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        truncate_sql = self.table.truncate().compile()
        self.assert_sql_contains(truncate_sql, "TRUNCATE TABLE")
        self.assert_sql_contains(truncate_sql, self.table.name)

    def test_select_all(self) -> None:
        """Test SELECT * statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        select_sql = self.table.select().compile()
        self.assert_sql_contains(select_sql, "SELECT")
        self.assert_sql_contains(select_sql, "FROM")
        self.assert_sql_contains(select_sql, self.table.name)

    def test_select_columns(self) -> None:
        """Test SELECT with specific columns."""
        if self.table is None:
            pytest.skip("Table not initialized")

        select_sql = self.table.select("name", "email").compile()
        self.assert_sql_contains(select_sql, "SELECT")
        self.assert_sql_contains(select_sql, "name")
        self.assert_sql_contains(select_sql, "email")

    def test_select_with_where(self) -> None:
        """Test SELECT with WHERE clause."""
        if self.table is None:
            pytest.skip("Table not initialized")

        select_sql = self.table.select().where(self.table.c.id > 10).compile()
        self.assert_sql_contains(select_sql, "SELECT")
        self.assert_sql_contains(select_sql, "WHERE")

    def test_insert(self) -> None:
        """Test INSERT statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        insert_sql = self.table.insert(
            name="Test User", email="test@example.com"
        ).compile()
        self.assert_sql_contains(insert_sql, "INSERT INTO")
        self.assert_sql_contains(insert_sql, self.table.name)

    def test_update(self) -> None:
        """Test UPDATE statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        update_sql = (
            self.table.update(name="Updated Name")
            .where(self.table.c.id == 1)
            .compile()
        )
        self.assert_sql_contains(update_sql, "UPDATE")
        self.assert_sql_contains(update_sql, self.table.name)
        self.assert_sql_contains(update_sql, "SET")

    def test_delete(self) -> None:
        """Test DELETE statement generation."""
        if self.table is None:
            pytest.skip("Table not initialized")

        delete_sql = self.table.delete().where(self.table.c.id == 1).compile()
        self.assert_sql_contains(delete_sql, "DELETE FROM")
        self.assert_sql_contains(delete_sql, self.table.name)
        self.assert_sql_contains(delete_sql, "WHERE")


class DialectTestCase(BaseTestCase, DatabaseTestMixin):
    """
    Base test case for database dialect testing.

    Combines BaseTestCase functionality with DatabaseTestMixin
    to provide a complete testing foundation for dialect-specific tests.

    Note: This class should not be run directly as it doesn't have a table.
    """

    # Prevent pytest from running tests on this base class
    __test__ = False

    @pytest.fixture(autouse=True)
    def setup_dialect_test_case(self) -> None:
        """Set up dialect test case."""
        super().setup_test_case()
        self.table = None  # To be set by subclasses

    def get_table_class(self) -> type[SQLTable]:
        """
        Get the table class for this dialect.

        Returns
        -------
        type[SQLTable]
            The table class to test.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Subclasses must implement get_table_class")

    def create_table(self, name: str = "test_table") -> SQLTable:
        """
        Create a table instance for testing.

        Parameters
        ----------
        name : str, default "test_table"
            Table name.

        Returns
        -------
        SQLTable
            Table instance.
        """
        table_class = self.get_table_class()
        return self.create_test_table(table_class, name)

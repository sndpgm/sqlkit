"""
Tests for sqlkit.core.factory module.

This module tests the Table factory function that creates dialect-specific
table instances based on the dialect parameter using pytest.
"""

from __future__ import annotations

import pytest

from sqlkit.core import Column
from sqlkit.core.column import Integer, String
from sqlkit.core.factory import Table
from sqlkit.core.table import SQLTable
from sqlkit.dialects.athena import AthenaTable
from sqlkit.dialects.mysql import MySQLTable
from sqlkit.dialects.oracle import OracleTable
from sqlkit.dialects.postgresql import PostgreSQLTable
from sqlkit.dialects.redshift import RedshiftTable
from sqlkit.dialects.sqlite import SQLiteTable
from sqlkit.tests.conftest import assert_sql_contains


class TestTableFactory:
    """Test Table factory function."""

    @pytest.fixture
    def sample_columns(self):
        """Provide sample columns for factory testing."""
        return [
            Column("id", Integer, primary_key=True),
            Column("name", String(255), nullable=False),
        ]

    def test_mysql_table_creation(self, sample_columns):
        """Test MySQL table creation via factory function."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="mysql",
            engine="InnoDB",
            charset="utf8mb4",
        )

        assert isinstance(table, MySQLTable)
        assert table.name == "test_table"
        assert table.engine_type == "InnoDB"
        assert table.charset == "utf8mb4"

    def test_postgresql_table_creation(self, sample_columns):
        """Test PostgreSQL table creation via factory function."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="postgresql",
            schema="public",
        )

        assert isinstance(table, PostgreSQLTable)
        assert table.name == "test_table"
        assert table.schema == "public"

    def test_sqlite_table_creation(self, sample_columns):
        """Test SQLite table creation via factory function."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="sqlite",
        )

        assert isinstance(table, SQLiteTable)
        assert table.name == "test_table"

    def test_redshift_table_creation(self, sample_columns):
        """Test Redshift table creation via factory function."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="redshift",
            sort_keys=["id"],
            dist_key="id",
            dist_style="KEY",
        )

        assert isinstance(table, RedshiftTable)
        assert table.name == "test_table"
        assert table.sort_keys == ["id"]
        assert table.dist_key == "id"
        assert table.dist_style == "KEY"

    def test_athena_table_creation(self, sample_columns):
        """Test Athena table creation via factory function."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="athena",
            location="s3://bucket/path/",
            stored_as="PARQUET",
        )

        assert isinstance(table, AthenaTable)
        assert table.name == "test_table"
        assert table.location == "s3://bucket/path/"
        assert table.stored_as == "PARQUET"

    def test_oracle_table_creation(self, sample_columns):
        """Test Oracle table creation via factory function."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="oracle",
            tablespace="USERS",
            organization="HEAP",
        )

        assert isinstance(table, OracleTable)
        assert table.name == "test_table"
        assert table.tablespace == "USERS"
        assert table.organization == "HEAP"

    def test_generic_table_creation(self, sample_columns):
        """Test generic table creation when dialect is None."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect=None,
        )

        assert isinstance(table, SQLTable)
        assert table.name == "test_table"
        # Should be a generic table, not a specific dialect
        assert not isinstance(table, MySQLTable)
        assert not isinstance(table, PostgreSQLTable)

    def test_invalid_dialect_raises_error(self, sample_columns):
        """Test that invalid dialect raises ValueError."""
        with pytest.raises(
            ValueError, match="Unsupported dialect: invalid_dialect"
        ):
            Table(
                "test_table",
                *sample_columns,
                dialect="invalid_dialect",
            )

    def test_table_functionality_mysql(self):
        """Test that factory-created MySQL table has proper functionality."""
        table = Table(
            "users",
            Column("id", Integer, primary_key=True),
            Column("email", String(255), unique=True),
            dialect="mysql",
        )

        # Test basic SQL generation
        create_sql = table.create().compile()
        assert_sql_contains(create_sql, "CREATE TABLE users")

        insert_sql = table.insert(id=1, email="test@example.com").compile()
        assert_sql_contains(insert_sql, "INSERT INTO users")

    def test_table_functionality_redshift(self):
        """Test factory-created Redshift table has proper functionality."""
        table = Table(
            "products",
            Column("id", Integer, primary_key=True),
            Column("name", String(255)),
            dialect="redshift",
            sort_keys=["id"],
        )

        # Test basic SQL generation
        create_sql = table.create().compile()
        assert_sql_contains(create_sql, "CREATE TABLE products")

        # Test Redshift-specific features
        assert isinstance(table, RedshiftTable)
        copy_query = table.copy_from_s3("s3://bucket/data.csv")
        assert copy_query.query_type == "COPY_FROM_S3"

    def test_table_with_schema(self, sample_columns):
        """Test table creation with schema parameter."""
        table = Table(
            "test_table",
            *sample_columns,
            dialect="postgresql",
            schema="analytics",
        )

        assert table.schema == "analytics"

    @pytest.mark.parametrize(
        "dialect,expected_class",
        [
            ("mysql", MySQLTable),
            ("postgresql", PostgreSQLTable),
            ("sqlite", SQLiteTable),
            ("redshift", RedshiftTable),
            ("athena", AthenaTable),
            ("oracle", OracleTable),
        ],
    )
    def test_all_supported_dialects(self, dialect, expected_class):
        """Test that all supported dialects can be created."""
        # Create fresh columns for each test to avoid SQLAlchemy conflicts
        columns = [
            Column("id", Integer, primary_key=True),
            Column("name", String(255), nullable=False),
        ]
        table = Table(
            f"test_table_{dialect}",
            *columns,
            dialect=dialect,
        )
        assert isinstance(table, expected_class)
        assert table.name == f"test_table_{dialect}"


class TestTableFactoryIntegration:
    """Integration tests for Table factory function."""

    def test_backward_compatibility(self):
        """Test that direct dialect class usage still works."""
        # Test that we can still use dialect classes directly
        mysql_table = MySQLTable(
            "direct_mysql",
            Column("id", Integer, primary_key=True),
        )

        factory_table = Table(
            "factory_mysql",
            Column("id", Integer, primary_key=True),
            dialect="mysql",
        )

        # Both should be MySQL tables
        assert isinstance(mysql_table, MySQLTable)
        assert isinstance(factory_table, MySQLTable)

        # Both should have the same functionality
        mysql_sql = mysql_table.create().compile()
        factory_sql = factory_table.create().compile()

        assert_sql_contains(mysql_sql, "CREATE TABLE direct_mysql")
        assert_sql_contains(factory_sql, "CREATE TABLE factory_mysql")

    @pytest.mark.parametrize(
        "dialect,expected_class",
        [
            ("mysql", MySQLTable),
            ("postgresql", PostgreSQLTable),
            ("sqlite", SQLiteTable),
            ("redshift", RedshiftTable),
            ("athena", AthenaTable),
            ("oracle", OracleTable),
        ],
    )
    def test_cross_dialect_functionality(self, dialect, expected_class):
        """Test functionality across different dialects."""
        # Create fresh columns for each test
        columns = [
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        ]
        table = Table(
            f"test_{dialect}",
            *columns,
            dialect=dialect,
        )

        assert isinstance(table, expected_class)

        # Test basic operations work for all dialects
        create_sql = table.create().compile()
        assert_sql_contains(create_sql, f"CREATE TABLE test_{dialect}")

        select_sql = table.select().compile()
        assert_sql_contains(select_sql, "SELECT")
        assert_sql_contains(select_sql, f"test_{dialect}")

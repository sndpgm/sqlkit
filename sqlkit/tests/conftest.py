"""
Pytest configuration and shared fixtures for SQLKit testing.

This module provides common fixtures and utilities that can be used
across all test modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sqlkit.core import Column
from sqlkit.core.column import Integer, String
from sqlkit.core.table import SQLTable

if TYPE_CHECKING:
    pass


class ConcreteSQLTable(SQLTable):
    """Concrete implementation of SQLTable for testing."""

    pass


@pytest.fixture
def sample_columns():
    """Provide sample columns for testing."""
    return [
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("email", String(255), unique=True),
    ]


@pytest.fixture
def basic_table(sample_columns):
    """Provide a basic table for testing."""
    return ConcreteSQLTable("test_table", *sample_columns)


@pytest.fixture
def mysql_table(sample_columns):
    """Provide a MySQL table for testing."""
    from sqlkit.dialects.mysql import MySQLTable

    return MySQLTable(
        "test_table",
        *sample_columns,
        engine="InnoDB",
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )


@pytest.fixture
def postgresql_table(sample_columns):
    """Provide a PostgreSQL table for testing."""
    from sqlkit.dialects.postgresql import PostgreSQLTable

    return PostgreSQLTable("test_table", *sample_columns, schema="public")


@pytest.fixture
def sqlite_table(sample_columns):
    """Provide a SQLite table for testing."""
    from sqlkit.dialects.sqlite import SQLiteTable

    return SQLiteTable("test_table", *sample_columns)


@pytest.fixture
def redshift_table(sample_columns):
    """Provide a Redshift table for testing."""
    from sqlkit.dialects.redshift import RedshiftTable

    return RedshiftTable(
        "test_table",
        *sample_columns,
        sort_keys=["id"],
        dist_key="id",
        dist_style="KEY",
    )


@pytest.fixture
def athena_table(sample_columns):
    """Provide an Athena table for testing."""
    from sqlkit.dialects.athena import AthenaTable

    return AthenaTable(
        "test_table",
        *sample_columns,
        location="s3://bucket/path/",
        stored_as="PARQUET",
        partition_by=["name"],
    )


@pytest.fixture
def oracle_table(sample_columns):
    """Provide an Oracle table for testing."""
    from sqlkit.dialects.oracle import OracleTable

    return OracleTable(
        "test_table",
        *sample_columns,
        tablespace="USERS",
        organization="HEAP",
        compress=True,
    )


@pytest.fixture(
    params=[
        "mysql",
        "postgresql",
        "sqlite",
        "redshift",
        "athena",
        "oracle",
    ]
)
def dialect_name(request):
    """Parameterized fixture for all supported dialects."""
    return request.param


@pytest.fixture
def dialect_table_classes():
    """Provide mapping of dialect names to table classes."""
    from sqlkit.dialects.athena import AthenaTable
    from sqlkit.dialects.mysql import MySQLTable
    from sqlkit.dialects.oracle import OracleTable
    from sqlkit.dialects.postgresql import PostgreSQLTable
    from sqlkit.dialects.redshift import RedshiftTable
    from sqlkit.dialects.sqlite import SQLiteTable

    return {
        "mysql": MySQLTable,
        "postgresql": PostgreSQLTable,
        "sqlite": SQLiteTable,
        "redshift": RedshiftTable,
        "athena": AthenaTable,
        "oracle": OracleTable,
    }


def assert_sql_contains(sql: str, expected: str) -> None:
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


def assert_sql_not_contains(sql: str, not_expected: str) -> None:
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


# Make assertion helpers available as pytest fixtures
@pytest.fixture
def assert_sql_contains_func():
    """Provide SQL assertion function."""
    return assert_sql_contains


@pytest.fixture
def assert_sql_not_contains_func():
    """Provide SQL not-contains assertion function."""
    return assert_sql_not_contains

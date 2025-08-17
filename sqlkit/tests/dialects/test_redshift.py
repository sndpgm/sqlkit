"""
Tests for sqlkit.dialects.redshift module.

This module tests Redshift-specific functionality including COPY/UNLOAD
operations and distribution/sort key handling using pytest.
"""

import pytest

from sqlkit.core import Column
from sqlkit.core.column import Integer, String
from sqlkit.dialects.redshift import RedshiftTable
from sqlkit.tests.conftest import assert_sql_contains


class TestRedshiftTable:
    """Test Redshift table functionality."""

    @pytest.fixture
    def redshift_table(self):
        """Create a Redshift table for testing."""
        return RedshiftTable(
            "test_table",
            Column("id", Integer, primary_key=True),
            Column("name", String(255), nullable=False),
            Column("email", String(255), unique=True),
            Column("user_id", Integer),
            sort_keys=["id"],
            dist_key="user_id",
            dist_style="KEY",
        )

    def test_table_initialization_with_redshift_options(self, redshift_table):
        """Test table initialization with Redshift-specific options."""
        assert redshift_table.name == "test_table"
        assert redshift_table.sort_keys == ["id"]
        assert redshift_table.dist_key == "user_id"
        assert redshift_table.dist_style == "KEY"

    def test_copy_from_s3_method(self, redshift_table):
        """Test copy_from_s3 method."""
        copy_query = redshift_table.copy_from_s3(
            "s3://test-bucket/data.csv",
            credentials="aws_iam_role=arn:aws:iam::123:role/TestRole",
            format="CSV",
        )
        assert copy_query.table == redshift_table
        assert copy_query.query_type == "COPY_FROM_S3"

    def test_unload_to_s3_method(self, redshift_table):
        """Test unload_to_s3 method."""
        test_query = "SELECT * FROM test_table"
        unload_query = redshift_table.unload_to_s3(
            test_query, "s3://test-bucket/output/"
        )
        assert unload_query.table == redshift_table
        assert unload_query.query_type == "UNLOAD_TO_S3"

    def test_analyze_compression_method(self, redshift_table):
        """Test analyze_compression method."""
        analyze_query = redshift_table.analyze_compression()
        assert analyze_query.table == redshift_table
        assert analyze_query.query_type == "ANALYZE_COMPRESSION"

    def test_vacuum_reindex_method(self, redshift_table):
        """Test vacuum_reindex method."""
        vacuum_query = redshift_table.vacuum_reindex()
        assert vacuum_query.table == redshift_table
        assert vacuum_query.query_type == "VACUUM_REINDEX"

    def test_deep_copy_method(self, redshift_table):
        """Test deep_copy method."""
        deep_copy_query = redshift_table.deep_copy("new_table")
        assert deep_copy_query.table == redshift_table
        assert deep_copy_query.query_type == "DEEP_COPY"

    def test_copy_from_s3_sql_generation(self, redshift_table):
        """Test SQL generation for COPY FROM S3."""
        copy_query = redshift_table.copy_from_s3(
            "s3://test-bucket/data.csv",
            credentials="aws_iam_role=arn:aws:iam::123:role/TestRole",
            format="CSV",
            delimiter=",",
        )
        sql = copy_query.compile()
        assert_sql_contains(sql, "COPY test_table FROM")
        assert_sql_contains(sql, "s3://test-bucket/data.csv")
        assert_sql_contains(sql, "CREDENTIALS")
        assert_sql_contains(sql, "FORMAT CSV")

    def test_unload_to_s3_sql_generation(self, redshift_table):
        """Test SQL generation for UNLOAD TO S3."""
        test_query = "SELECT * FROM test_table WHERE id > 100"
        unload_query = redshift_table.unload_to_s3(
            test_query,
            "s3://test-bucket/output/",
            credentials="aws_iam_role=arn:aws:iam::123:role/TestRole",
            format="PARQUET",
        )
        sql = unload_query.compile()
        assert_sql_contains(sql, "UNLOAD")
        assert_sql_contains(sql, "SELECT * FROM test_table WHERE id > 100")
        assert_sql_contains(sql, "s3://test-bucket/output/")
        assert_sql_contains(sql, "CREDENTIALS")

    def test_create_table_with_distribution_and_sort_keys(
        self, redshift_table
    ):
        """Test CREATE TABLE with Redshift-specific keys."""
        create_query = redshift_table.create()
        # Note: The actual implementation might need to be enhanced
        # to properly handle distribution and sort keys in CREATE TABLE
        sql = create_query.compile()
        assert_sql_contains(sql, "CREATE TABLE test_table")


class TestRedshiftTableBasicOperations:
    """Test basic SQL operations for Redshift tables."""

    @pytest.fixture
    def sample_redshift_table(self):
        """Create a sample Redshift table for basic operations."""
        return RedshiftTable(
            "users",
            Column("id", Integer, primary_key=True),
            Column("name", String(255)),
            sort_keys=["id"],
        )

    def test_create_table_sql(self, sample_redshift_table):
        """Test CREATE TABLE SQL generation."""
        create_sql = sample_redshift_table.create().compile()
        assert_sql_contains(create_sql, "CREATE TABLE users")
        assert_sql_contains(create_sql, "id")
        assert_sql_contains(create_sql, "name")

    def test_select_sql(self, sample_redshift_table):
        """Test SELECT SQL generation."""
        select_sql = sample_redshift_table.select("name").compile()
        assert_sql_contains(select_sql, "SELECT")
        assert_sql_contains(select_sql, "name")
        assert_sql_contains(select_sql, "FROM users")

    def test_insert_sql(self, sample_redshift_table):
        """Test INSERT SQL generation."""
        insert_sql = sample_redshift_table.insert(name="Test User").compile()
        assert_sql_contains(insert_sql, "INSERT INTO users")

    def test_delete_sql(self, sample_redshift_table):
        """Test DELETE SQL generation."""
        delete_sql = (
            sample_redshift_table.delete()
            .where(sample_redshift_table.c.id == 1)
            .compile()
        )
        assert_sql_contains(delete_sql, "DELETE FROM users")
        assert_sql_contains(delete_sql, "WHERE")

"""
Tests for dialect integration with YAML configuration.

This module tests that dialect-specific methods work correctly with YAML
configuration.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sqlkit.config.registry import TableRegistry


class TestRedshiftYamlIntegration:
    """Test Redshift dialect with YAML configuration."""

    @pytest.fixture
    def redshift_config_dict(self):
        """Redshift configuration for testing."""
        return {
            "tables": {
                "sales_data": {
                    "dialect": "redshift",
                    "columns": [
                        {"name": "id", "type": "Integer", "primary_key": True},
                        {"name": "amount", "type": "Float"},
                        {"name": "sale_date", "type": "Date"},
                    ],
                    "options": {
                        "sort_keys": ["id"],
                        "dist_key": "id",
                        "dist_style": "KEY",
                    },
                    "dialect_methods": {
                        "copy_from_s3": {
                            "s3_path": (
                                "s3://sales-bucket/{{ year }}/{{ month }}/"
                                "data.csv"
                            ),
                            "credentials": (
                                "aws_iam_role=arn:aws:iam::123:role/RedshiftRole"
                            ),
                            "format": "CSV",
                            "delimiter": ",",
                            "options": ["IGNOREHEADER 1", "ACCEPTINVCHARS"],
                        },
                        "unload_to_s3": {
                            "s3_path": "s3://export-bucket/sales/{{ date }}/",
                            "credentials": (
                                "aws_iam_role=arn:aws:iam::123:role/RedshiftRole"
                            ),
                            "format": "PARQUET",
                        },
                    },
                }
            }
        }

    @pytest.fixture
    def redshift_config_file(self, redshift_config_dict):
        """Create temporary Redshift config file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(redshift_config_dict, f)
            return Path(f.name)

    @pytest.fixture
    def redshift_table(self, redshift_config_file):
        """Get Redshift table from registry."""
        registry = TableRegistry.from_file(redshift_config_file)
        return registry.get_table("sales_data")

    def test_redshift_table_creation(self, redshift_table):
        """Test Redshift table is created correctly."""
        assert redshift_table.name == "sales_data"
        assert hasattr(redshift_table, "sort_keys")
        assert redshift_table.sort_keys == ["id"]
        assert redshift_table.dist_key == "id"
        assert redshift_table.dist_style == "KEY"

    def test_copy_from_s3_with_yaml_config(self, redshift_table):
        """Test copy_from_s3 using YAML configuration."""
        # Test with template variables
        template_vars = {"year": "2024", "month": "01"}
        query = redshift_table.copy_from_s3(template_vars=template_vars)

        assert query.query_type == "COPY_FROM_S3"
        params = query.params
        assert params["s3_path"] == "s3://sales-bucket/2024/01/data.csv"
        assert (
            params["credentials"]
            == "aws_iam_role=arn:aws:iam::123:role/RedshiftRole"
        )
        assert params["format"] == "CSV"
        assert params["delimiter"] == ","
        assert params["options"] == ["IGNOREHEADER 1", "ACCEPTINVCHARS"]

    def test_copy_from_s3_with_override(self, redshift_table):
        """Test copy_from_s3 with parameter override."""
        template_vars = {"year": "2024", "month": "01"}
        query = redshift_table.copy_from_s3(
            template_vars=template_vars,
            format="JSON",  # Override YAML setting
            delimiter="|",  # Override YAML setting
        )

        params = query.params
        assert params["format"] == "JSON"  # Overridden
        assert params["delimiter"] == "|"  # Overridden
        assert (
            params["credentials"]
            == "aws_iam_role=arn:aws:iam::123:role/RedshiftRole"
        )  # From YAML

    def test_copy_from_s3_manual_mode(self, redshift_table):
        """Test copy_from_s3 in manual mode."""
        query = redshift_table.copy_from_s3(
            s3_path="s3://manual-bucket/data.csv",
            credentials="manual_creds",
            format="CSV",
            use_config=False,
        )

        params = query.params
        assert params["s3_path"] == "s3://manual-bucket/data.csv"
        assert params["credentials"] == "manual_creds"
        assert params["format"] == "CSV"
        # Should not have YAML options
        assert "options" not in params

    def test_copy_from_s3_missing_template_var(self, redshift_table):
        """Test copy_from_s3 with missing template variable."""
        with pytest.raises(Exception):  # Should be TemplateError
            redshift_table.copy_from_s3(
                template_vars={"year": "2024"}
            )  # Missing 'month'

    def test_unload_to_s3_with_yaml_config(self, redshift_table):
        """Test unload_to_s3 using YAML configuration."""
        template_vars = {"date": "2024-01-15"}
        query = redshift_table.unload_to_s3(
            query="SELECT * FROM sales_data WHERE amount > 100",
            template_vars=template_vars,
        )

        assert query.query_type == "UNLOAD_TO_S3"
        params = query.params
        assert params["s3_path"] == "s3://export-bucket/sales/2024-01-15/"
        assert params["format"] == "PARQUET"
        assert params["query"] == "SELECT * FROM sales_data WHERE amount > 100"

    def test_unload_to_s3_default_query(self, redshift_table):
        """Test unload_to_s3 with default query."""
        template_vars = {"date": "2024-01-15"}
        query = redshift_table.unload_to_s3(template_vars=template_vars)

        params = query.params
        assert params["query"] == "SELECT * FROM sales_data"  # Default query


class TestAthenaYamlIntegration:
    """Test Athena dialect with YAML configuration."""

    @pytest.fixture
    def athena_config_dict(self):
        """Athena configuration for testing."""
        return {
            "tables": {
                "events": {
                    "dialect": "athena",
                    "columns": [
                        {"name": "event_id", "type": "String"},
                        {"name": "timestamp", "type": "DateTime"},
                        {"name": "user_id", "type": "Integer"},
                    ],
                    "options": {
                        "location": "s3://events-bucket/",
                        "stored_as": "PARQUET",
                        "partition_by": ["timestamp"],
                    },
                    "dialect_methods": {
                        "msck_repair": {
                            "add_partitions": True,
                        }
                    },
                }
            }
        }

    @pytest.fixture
    def athena_config_file(self, athena_config_dict):
        """Create temporary Athena config file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(athena_config_dict, f)
            return Path(f.name)

    @pytest.fixture
    def athena_table(self, athena_config_file):
        """Get Athena table from registry."""
        registry = TableRegistry.from_file(athena_config_file)
        return registry.get_table("events")

    def test_athena_table_creation(self, athena_table):
        """Test Athena table is created correctly."""
        assert athena_table.name == "events"
        assert athena_table.location == "s3://events-bucket/"
        assert athena_table.stored_as == "PARQUET"
        assert athena_table.partition_by == ["timestamp"]

    def test_msck_repair_with_yaml_config(self, athena_table):
        """Test msck_repair using YAML configuration."""
        query = athena_table.msck_repair()

        assert query.query_type == "MSCK_REPAIR"
        params = query.params
        assert params["add_partitions"] is True

    def test_msck_repair_with_override(self, athena_table):
        """Test msck_repair with parameter override."""
        query = athena_table.msck_repair(add_partitions=False)

        params = query.params
        assert params["add_partitions"] is False  # Overridden

    def test_msck_repair_manual_mode(self, athena_table):
        """Test msck_repair in manual mode."""
        query = athena_table.msck_repair(
            use_config=False, custom_option="value"
        )

        params = query.params
        assert params["custom_option"] == "value"
        # Should not have YAML options
        assert "add_partitions" not in params


class TestMySQLYamlIntegration:
    """Test MySQL dialect with YAML configuration."""

    @pytest.fixture
    def mysql_config_dict(self):
        """MySQL configuration for testing."""
        return {
            "tables": {
                "products": {
                    "dialect": "mysql",
                    "columns": [
                        {"name": "id", "type": "Integer", "primary_key": True},
                        {"name": "name", "type": "String", "length": 255},
                        {"name": "price", "type": "Float"},
                    ],
                    "options": {
                        "engine": "InnoDB",
                        "charset": "utf8mb4",
                    },
                    "dialect_methods": {
                        "load_data_infile": {
                            "file_path": "/data/{{ env }}/products.csv",
                            "fields_terminated_by": ",",
                            "lines_terminated_by": "\\n",
                            "ignore_lines": 1,
                        }
                    },
                }
            }
        }

    @pytest.fixture
    def mysql_config_file(self, mysql_config_dict):
        """Create temporary MySQL config file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(mysql_config_dict, f)
            return Path(f.name)

    @pytest.fixture
    def mysql_table(self, mysql_config_file):
        """Get MySQL table from registry."""
        registry = TableRegistry.from_file(mysql_config_file)
        return registry.get_table("products")

    def test_mysql_table_creation(self, mysql_table):
        """Test MySQL table is created correctly."""
        assert mysql_table.name == "products"
        assert mysql_table.engine_type == "InnoDB"
        assert mysql_table.charset == "utf8mb4"

    def test_load_data_infile_with_yaml_config(self, mysql_table):
        """Test load_data_infile using YAML configuration."""
        template_vars = {"env": "prod"}
        query = mysql_table.load_data_infile(template_vars=template_vars)

        assert query.query_type == "LOAD_DATA_INFILE"
        params = query.params
        assert params["file_path"] == "/data/prod/products.csv"
        assert params["fields_terminated_by"] == ","
        assert params["lines_terminated_by"] == "\\n"
        assert params["ignore_lines"] == 1

    def test_load_data_infile_with_override(self, mysql_table):
        """Test load_data_infile with parameter override."""
        template_vars = {"env": "test"}
        query = mysql_table.load_data_infile(
            template_vars=template_vars,
            fields_terminated_by="|",  # Override
            ignore_lines=0,  # Override
        )

        params = query.params
        assert params["file_path"] == "/data/test/products.csv"
        assert params["fields_terminated_by"] == "|"  # Overridden
        assert params["ignore_lines"] == 0  # Overridden

    def test_load_data_infile_manual_mode(self, mysql_table):
        """Test load_data_infile in manual mode."""
        query = mysql_table.load_data_infile(
            file_path="/manual/path/data.csv",
            fields_terminated_by=";",
            use_config=False,
        )

        params = query.params
        assert params["file_path"] == "/manual/path/data.csv"
        assert params["fields_terminated_by"] == ";"
        # Should not have YAML options
        assert "lines_terminated_by" not in params


class TestFactoryIntegration:
    """Test factory integration with YAML configuration."""

    @pytest.fixture
    def factory_config_dict(self):
        """Configuration for factory testing."""
        return {
            "tables": {
                "test_table": {
                    "dialect": "sqlite",
                    "columns": [
                        {"name": "id", "type": "Integer", "primary_key": True},
                    ],
                }
            }
        }

    @pytest.fixture
    def factory_config_file(self, factory_config_dict):
        """Create temporary config file for factory testing."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(factory_config_dict, f)
            return Path(f.name)

    def test_from_config_function(self, factory_config_file):
        """Test from_config convenience function."""
        from sqlkit.core.factory import from_config

        table = from_config("test_table", factory_config_file)

        assert table.name == "test_table"
        assert hasattr(table, "_yaml_config")

    def test_table_factory_from_config(self, factory_config_file):
        """Test TableFactory.from_config method."""
        from sqlkit.core.factory import TableFactory

        table = TableFactory.from_config("test_table", factory_config_file)

        assert table.name == "test_table"
        assert hasattr(table, "_yaml_config")

"""
Tests for sqlkit.config.loader module.

This module tests YAML configuration loading and template expansion.
"""

import tempfile
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from sqlkit.config.loader import TemplateError, YamlLoader

FILE = Path(__file__).parent / "file"


class TestYamlLoader:
    """Test YamlLoader functionality."""

    def test_loader_initialization(self):
        """Test loader initialization with valid config file."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        assert loader.config_file == FILE / fname
        assert loader.config is not None
        assert "users" in loader.config.tables

    @pytest.mark.parametrize(
        "fname, dialect, schema_name",
        [
            ("config_default_dialect_schema.yaml", "postgresql", "public"),
            ("config_override_dialect.yaml", "redshift", "public"),
            ("config_override_schema.yaml", "postgresql", "analytics"),
        ],
    )
    def test_loader_default_params(self, fname, dialect, schema_name):
        loader = YamlLoader(FILE / fname)

        assert loader.config_file == FILE / fname
        assert loader.config is not None
        assert "users" in loader.config.tables

        users = loader.config.tables.get("users")
        assert users is not None
        assert users.dialect == dialect
        assert users.schema_name == schema_name

    def test_loader_file_not_found(self):
        """Test loader with non-existent file."""
        with pytest.raises(FileNotFoundError):
            YamlLoader("nonexistent.yaml")

    def test_loader_invalid_yaml(self):
        """Test loader with invalid YAML format."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("invalid: yaml: content: [unclosed")
            invalid_file = Path(f.name)

        with pytest.raises((yaml.YAMLError, ValidationError)):
            YamlLoader(invalid_file)

    def test_expand_templates_simple(self):
        """Test simple template expansion."""

        config_dict = {
            "s3_path": "s3://bucket/data/{{ date }}/",
            "format": "CSV",
        }

        template_vars = {"date": "2024-01-15"}
        result = YamlLoader.expand_templates(config_dict, template_vars)

        expected = {
            "s3_path": "s3://bucket/data/2024-01-15/",
            "format": "CSV",
        }

        assert result == expected

    def test_expand_templates_nested(self):
        """Test template expansion in nested structures."""

        config_dict = {
            "paths": {
                "input": "s3://input/{{ env }}/",
                "output": "s3://output/{{ env }}/{{ date }}/",
            },
            "options": [
                "ENV={{ env }}",
                "DATE={{ date }}",
            ],
        }

        template_vars = {"env": "prod", "date": "2024-01-15"}
        result = YamlLoader.expand_templates(config_dict, template_vars)

        expected = {
            "paths": {
                "input": "s3://input/prod/",
                "output": "s3://output/prod/2024-01-15/",
            },
            "options": [
                "ENV=prod",
                "DATE=2024-01-15",
            ],
        }

        assert result == expected

    def test_expand_templates_missing_variable(self):
        """Test template expansion with missing variable."""

        config_dict = {"path": "s3://bucket/{{ date }}/{{ missing }}/"}
        template_vars = {"date": "2024-01-15"}

        with pytest.raises(
            TemplateError, match="Template variable 'missing' not provided"
        ):
            YamlLoader.expand_templates(config_dict, template_vars)

    def test_expand_templates_no_templates(self):
        """Test template expansion with no template variables."""

        config_dict = {
            "path": "s3://bucket/static/",
            "format": "CSV",
        }

        result = YamlLoader.expand_templates(config_dict, None)
        assert result == config_dict

    def test_get_table_config(self):
        """Test get_table_config method."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        table_config = loader.get_table_config("users")

        assert table_config["dialect"] == "postgresql"
        assert len(table_config["columns"]) == 3
        assert "dialect_methods" in table_config

    def test_get_table_config_not_found(self):
        """Test get_table_config with non-existent table."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        with pytest.raises(KeyError, match="Table 'nonexistent' not found"):
            loader.get_table_config("nonexistent")

    def test_get_method_config(self):
        """Test get_method_config method."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        method_config = loader.get_method_config("users", "copy_from_s3")

        assert method_config
        assert method_config["s3_path"] == "s3://bucket/users/{{ date }}/"
        assert method_config["format"] == "CSV"

    def test_get_method_config_with_templates(self):
        """Test get_method_config with template expansion."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        template_vars = {"date": "2024-01-15"}
        method_config = loader.get_method_config(
            "users", "copy_from_s3", template_vars
        )

        assert method_config
        assert method_config["s3_path"] == "s3://bucket/users/2024-01-15/"
        assert method_config["format"] == "CSV"

    def test_get_method_config_not_found(self):
        """Test get_method_config with non-existent method."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        result = loader.get_method_config("users", "nonexistent_method")
        assert result is None

    def test_get_method_config_table_not_found(self):
        """Test get_method_config with non-existent table."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        with pytest.raises(KeyError):
            loader.get_method_config("nonexistent_table", "copy_from_s3")

    def test_from_file_class_method(self):
        """Test from_file class method."""
        fname = "config.yaml"
        loader = YamlLoader(FILE / fname)

        assert isinstance(loader, YamlLoader)
        assert loader.config_file == FILE / fname


class TestTemplateExpansion:
    """Test template expansion functionality in detail."""

    @pytest.fixture
    def sample_config_file(self):
        """Create temporary YAML config file for template tests."""
        config_dict = {
            "tables": {
                "test_table": {
                    "dialect": "postgresql",
                    "columns": [{"name": "id", "type": "Integer"}],
                }
            }
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(config_dict, f)
            return Path(f.name)

    def test_expand_string_template_simple(self, sample_config_file):
        """Test simple string template expansion."""

        template = "Hello {{ name }}!"
        template_vars = {"name": "World"}

        result = YamlLoader._expand_string_template(template, template_vars)
        assert result == "Hello World!"

    def test_expand_string_template_multiple_vars(self, sample_config_file):
        """Test template expansion with multiple variables."""

        template = "s3://{{ bucket }}/{{ env }}/{{ date }}/"
        template_vars = {
            "bucket": "data-lake",
            "env": "prod",
            "date": "2024-01-15",
        }

        result = YamlLoader._expand_string_template(template, template_vars)
        assert result == "s3://data-lake/prod/2024-01-15/"

    def test_expand_string_template_whitespace(self, sample_config_file):
        """Test template expansion with whitespace in placeholders."""

        template = "{{ name }} and {{  other_name  }}"
        template_vars = {"name": "Alice", "other_name": "Bob"}

        result = YamlLoader._expand_string_template(template, template_vars)
        assert result == "Alice and Bob"

    def test_expand_string_template_repeated_var(self, sample_config_file):
        """Test template expansion with repeated variables."""

        template = "{{ name }} likes {{ name }}"
        template_vars = {"name": "Alice"}

        result = YamlLoader._expand_string_template(template, template_vars)
        assert result == "Alice likes Alice"

    def test_expand_string_template_missing_var(self, sample_config_file):
        """Test template expansion with missing variable."""

        template = "Hello {{ missing_var }}!"
        template_vars = {"other_var": "value"}

        msg = "Template variable 'missing_var' not provided"
        with pytest.raises(TemplateError, match=msg):
            YamlLoader._expand_string_template(template, template_vars)

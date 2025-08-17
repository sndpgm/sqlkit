"""
YAML configuration loader.

This module provides functionality to load and parse YAML configuration files
for table definitions, with template variable expansion support.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from sqlkit.config.schema import TablesConfig


class TemplateError(Exception):
    """Raised when template variable expansion fails."""

    pass


class YamlLoader:
    """
    YAML configuration loader with template variable support.

    This class loads YAML configuration files and provides template variable
    expansion using a simple {{ variable }} syntax.

    Attributes
    ----------
    config : TablesConfig
        Loaded and validated configuration.
    """

    def __init__(self, config_file: str | Path) -> None:
        """
        Initialize loader with configuration file.

        Parameters
        ----------
        config_file : str | Path
            Path to YAML configuration file.

        Raises
        ------
        FileNotFoundError
            If configuration file does not exist.
        ValidationError
            If configuration format is invalid.
        """
        self.config_file = Path(config_file)
        if not self.config_file.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_file}"
            )

        self.config = self._load_config()

    def _load_config(self) -> TablesConfig:
        """
        Load and validate YAML configuration.

        Returns
        -------
        TablesConfig
            Validated configuration object.

        Raises
        ------
        ValidationError
            If configuration format is invalid.
        """
        with open(self.config_file, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        try:
            return TablesConfig(**raw_config)
        except ValidationError:
            # Re-raise the original ValidationError with context
            raise

    @classmethod
    def expand_templates(
        cls,
        config_dict: dict[str, Any],
        template_vars: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Expand template variables in configuration dictionary.

        Template variables use {{ variable_name }} syntax and are replaced
        with values from the template_vars dictionary.

        Parameters
        ----------
        config_dict : Dict[str, Any]
            Configuration dictionary that may contain template variables.
        template_vars : Optional[Dict[str, str]]
            Dictionary of template variable names to values.

        Returns
        -------
        Dict[str, Any]
            Configuration dictionary with expanded template variables.

        Raises
        ------
        TemplateError
            If required template variable is not provided.
        """
        if template_vars is None:
            template_vars = {}

        def expand_value(value: Any) -> Any:
            """Recursively expand template variables in a value."""
            if isinstance(value, str):
                return cls._expand_string_template(value, template_vars)
            elif isinstance(value, dict):
                return {k: expand_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [expand_value(item) for item in value]
            else:
                return value

        result = expand_value(config_dict)
        return result  # type: ignore[no-any-return]

    @staticmethod
    def _expand_string_template(
        template: str, template_vars: dict[str, str]
    ) -> str:
        """
        Expand template variables in a string.

        Parameters
        ----------
        template : str
            String that may contain {{ variable }} placeholders.
        template_vars : Dict[str, str]
            Dictionary of variable names to values.

        Returns
        -------
        str
            String with template variables expanded.

        Raises
        ------
        TemplateError
            If required template variable is not provided.
        """
        # Find all template variables in the string
        pattern = r"\{\{\s*(\w+)\s*\}\}"
        matches = re.findall(pattern, template)

        result = template
        for var_name in matches:
            if var_name not in template_vars:
                raise TemplateError(
                    f"Template variable '{var_name}' not provided. "
                    f"Available variables: {list(template_vars.keys())}"
                )

            # Replace all occurrences of this variable
            var_pattern = r"\{\{\s*" + re.escape(var_name) + r"\s*\}\}"
            result = re.sub(var_pattern, template_vars[var_name], result)

        return result

    def get_table_config(self, table_name: str) -> dict[str, Any]:
        """
        Get configuration dictionary for a specific table.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        Dict[str, Any]
            Table configuration as dictionary.

        Raises
        ------
        KeyError
            If table is not found in configuration.
        """
        table_config = self.config.get_table_config(table_name)
        return table_config.model_dump()

    def get_method_config(
        self,
        table_name: str,
        method_name: str,
        template_vars: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        """
        Get method configuration with template expansion.

        Parameters
        ----------
        table_name : str
            Name of the table.
        method_name : str
            Name of the dialect method.
        template_vars : Optional[Dict[str, str]]
            Template variables for expansion.

        Returns
        -------
        Optional[Dict[str, Any]]
            Method configuration dictionary, or None if not configured.

        Raises
        ------
        KeyError
            If table is not found in configuration.
        TemplateError
            If template expansion fails.
        """
        table_config = self.config.get_table_config(table_name)

        if not table_config.dialect_methods:
            return None

        if method_name not in table_config.dialect_methods:
            return None

        method_config = table_config.dialect_methods[
            method_name
        ].dict_without_none()

        if template_vars:
            method_config = self.__class__.expand_templates(
                method_config, template_vars
            )

        return method_config

    @classmethod
    def from_file(cls, config_file: str | Path) -> YamlLoader:
        """
        Create loader instance from configuration file.

        Parameters
        ----------
        config_file : str | Path
            Path to YAML configuration file.

        Returns
        -------
        YamlLoader
            Configured loader instance.
        """
        return cls(config_file)

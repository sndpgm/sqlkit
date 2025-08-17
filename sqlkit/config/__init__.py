"""
Configuration management for SQLKit.

This module provides YAML-based table definition and configuration management.
"""

from sqlkit.config.loader import YamlLoader
from sqlkit.config.registry import TableRegistry
from sqlkit.config.schema import DialectMethodConfig, TableConfig, TablesConfig

__all__ = [
    "YamlLoader",
    "TableRegistry",
    "DialectMethodConfig",
    "TableConfig",
    "TablesConfig",
]

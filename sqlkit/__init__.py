from sqlkit.config.registry import TableRegistry
from sqlkit.core import Column, SQLTable
from sqlkit.core.column import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    Time,
)
from sqlkit.core.factory import Table, from_config
from sqlkit.dialects import (
    AthenaTable,
    MySQLTable,
    OracleTable,
    PostgreSQLTable,
    RedshiftTable,
    SQLiteTable,
)

__version__ = "0.1.0"

__all__ = [
    # Core classes
    "SQLTable",
    "Column",
    "Table",  # Factory function
    # YAML configuration
    "from_config",
    "TableRegistry",
    # Data types
    "Integer",
    "String",
    "DateTime",
    "Boolean",
    "Float",
    "Numeric",
    "Text",
    "Date",
    "Time",
    # Dialect-specific tables
    "MySQLTable",
    "PostgreSQLTable",
    "SQLiteTable",
    "RedshiftTable",
    "AthenaTable",
    "OracleTable",
]

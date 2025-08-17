# SQLKit - Modern SQL Query Builder

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://python.org)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Type checking: mypy](https://img.shields.io/badge/type%20checking-mypy-blue.svg)](http://mypy-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A powerful Python library for object-oriented SQL query construction with support for multiple database dialects. SQLKit provides both a unified interface and dialect-specific optimizations for modern data engineering workflows, with advanced YAML-based configuration for environment-aware deployments.

## 🚀 Key Features

- **Unified Interface**: Create tables for any database with `Table(dialect="redshift", ...)`
- **Flexible Type System**: String-based type specifications with aliases (`'int'`, `'string(100)'`, `'numeric(18,5)'`)
- **YAML Configuration**: Define tables and pre-configure method parameters in YAML files
- **Automatic Metadata Application**: Default dialect and schema from YAML metadata
- **Template Variables**: Dynamic configuration with `{{ variable }}` syntax and classmethod expansion
- **Multi-Database Support**: MySQL, PostgreSQL, SQLite, Redshift, Athena, Oracle
- **Type Safety**: Full type annotations and mypy compatibility
- **SQLAlchemy Core**: Built on proven SQL generation foundation
- **Database-Specific Optimizations**: Leverage unique features of each database
- **Clean API**: Intuitive method chaining and query building

## 📦 Installation

```bash
# Install with uv (recommended)
uv add sqlkit

# Or with pip
pip install sqlkit
```

## 🎯 Quick Start

### Unified Interface (Recommended)

```python
from sqlkit import Table, Column, Integer, String, DateTime

# Create a Redshift table with distribution and sort keys
users = Table(
    "users",
    Column("id", "int", primary_key=True),           # Flexible type syntax
    Column("name", "string(255)", nullable=False),   # String with length
    Column("email", "varchar(255)", unique=True),    # Type aliases
    Column("amount", "numeric(18,5)"),               # Precision and scale
    Column("created_at", "datetime"),                # Date/time types
    dialect="redshift",
    sort_keys=["created_at"],
    dist_key="id",
    dist_style="KEY"
)

# Create a MySQL table with engine and charset
products = Table(
    "products",
    Column("id", Integer, primary_key=True),  # SQLAlchemy types also work
    Column("name", "str(255)"),               # Short alias
    Column("price", "decimal(10,2)"),         # Numeric with precision
    dialect="mysql",
    engine="InnoDB",
    charset="utf8mb4"
)

# Generate SQL
create_sql = users.create().compile()
select_sql = users.select().where(users.c.id > 100).compile()
```

### Flexible Type System

SQLKit supports multiple ways to specify column types:

```python
from sqlkit import Table, Column

# All these are equivalent ways to define columns
table = Table(
    "flexible_types",
    # SQLAlchemy types (traditional)
    Column("id", Integer, primary_key=True),

    # String-based with case-insensitive aliases
    Column("name1", "string(255)"),      # String with length
    Column("name2", "varchar(100)"),     # Alias for String
    Column("name3", "str(50)"),          # Short alias
    Column("name4", "String"),           # SQLAlchemy name

    # Numeric types with precision/scale
    Column("amount1", "numeric(18,5)"),  # Precision 18, scale 5
    Column("amount2", "decimal(10,2)"),  # Alias for Numeric
    Column("amount3", "number(15)"),     # Precision only

    # Integer types and aliases
    Column("count1", "int"),             # Short form
    Column("count2", "integer"),         # Full name
    Column("count3", "bigint"),          # Big integer

    # Float types
    Column("rate1", "float"),            # Standard float
    Column("rate2", "real"),             # Alias
    Column("rate3", "double"),           # Double precision

    # Boolean types
    Column("active1", "bool"),           # Short form
    Column("active2", "boolean"),        # Full name

    # Date/time types
    Column("created", "datetime"),       # Date and time
    Column("updated", "timestamp"),      # Alias for DateTime
    Column("birth_date", "date"),        # Date only
    Column("login_time", "time"),        # Time only

    dialect="postgresql"
)
```

### YAML Configuration with Flexible Types

```yaml
# tables.yaml
metadata:
  default_dialect: "postgresql"  # Applied to tables without explicit dialect
  default_schema: "public"       # Applied to tables without explicit schema

tables:
  users:
    # dialect and schema_name are optional - will use metadata defaults
    columns:
      - name: "id"
        type: "int"              # Flexible type syntax
        primary_key: true
      - name: "name"
        type: "varchar"          # Type alias
        length: 255
        nullable: false
      - name: "email"
        type: "string"           # Another alias
        length: 255
        unique: true
      - name: "amount"
        type: "numeric"          # Numeric with precision/scale
        precision: 18
        scale: 5
      - name: "count"
        type: "decimal"          # Alias for numeric
        precision: 10
      - name: "is_active"
        type: "bool"             # Boolean type
        nullable: false
    options:
      sort_keys: ["id"]
    dialect_methods:
      copy_from_s3:
        s3_path: "s3://data-bucket/users/{{ date }}/"
        credentials: "aws_iam_role=arn:aws:iam::123:role/DataRole"
        format: "CSV"
        delimiter: ","

  products:
    dialect: "mysql"           # Override default dialect
    # schema_name will use metadata default "public"
    columns:
      - name: "id"
        type: "integer"          # Full type name
        primary_key: true
      - name: "name"
        type: "str"             # Short string alias
        length: 255
```

```python
from sqlkit import from_config, YamlLoader

# Load table from YAML configuration (metadata defaults applied automatically)
users = from_config("users", "tables.yaml")
print(f"Users dialect: {users.dialect}")      # "postgresql" (from metadata)
print(f"Users schema: {users.schema_name}")   # "public" (from metadata)

products = from_config("products", "tables.yaml")
print(f"Products dialect: {products.dialect}")    # "mysql" (explicit override)
print(f"Products schema: {products.schema_name}") # "public" (from metadata)

# Use template expansion as classmethod (no instance needed)
config = {
    "s3_path": "s3://bucket/{{ env }}/{{ date }}/",
    "format": "{{ format }}"
}
expanded = YamlLoader.expand_templates(config, {
    "env": "production",
    "date": "2024-01-15",
    "format": "PARQUET"
})
# Result: {"s3_path": "s3://bucket/production/2024-01-15/", "format": "PARQUET"}

# Use pre-configured methods with template variables
copy_query = users.copy_from_s3(template_vars={"date": "2024-01-15"})
```

## 🏗️ Architecture

```
sqlkit/
├── core/                    # Core classes and functionality
│   ├── table.py            # Base SQLTable class
│   ├── column.py           # Enhanced Column with flexible types
│   ├── type_parser.py      # String-based type parsing system
│   ├── factory.py          # Unified Table factory function
│   └── __init__.py
├── config/                 # YAML configuration system
│   ├── schema.py          # Pydantic schemas with auto-metadata application
│   ├── loader.py          # YAML loading with classmethod template expansion
│   ├── registry.py        # Table registry and caching
│   └── __init__.py
├── dialects/               # Database-specific implementations
│   ├── mysql.py           # MySQL dialect
│   ├── postgresql.py      # PostgreSQL dialect
│   ├── sqlite.py          # SQLite dialect
│   ├── redshift.py        # Amazon Redshift dialect
│   ├── athena.py          # AWS Athena dialect
│   ├── oracle.py          # Oracle Database dialect
│   └── __init__.py
├── operations/            # Query operation builders
│   ├── base.py           # Base query classes
│   ├── ddl.py            # DDL operations (CREATE, DROP, etc.)
│   ├── dml.py            # DML operations (SELECT, INSERT, etc.)
│   ├── special.py        # Database-specific special operations
│   └── __init__.py
└── __init__.py           # Main package exports
```

## 💾 Database Support

### MySQL
```python
mysql_table = Table(
    "users",
    Column("id", "int", primary_key=True, autoincrement=True),
    Column("name", "varchar(255)", nullable=False),
    Column("email", "string(255)", unique=True),
    dialect="mysql",
    engine='InnoDB',
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)

# MySQL-specific operations
replace_sql = mysql_table.replace(name="John", email="john@example.com").compile()
ignore_sql = mysql_table.insert_ignore(name="Jane", email="jane@example.com").compile()
show_sql = mysql_table.show_create().compile()
load_sql = mysql_table.load_data_infile(
    "/path/to/data.csv",
    delimiter=",",
    enclosed_by='"'
).compile()
```

### PostgreSQL
```python
pg_table = Table(
    "products",
    Column("id", "integer", primary_key=True),
    Column("name", "str(255)"),
    Column("price", "decimal(10,2)"),
    dialect="postgresql",
    schema="public"
)

# PostgreSQL-specific operations
upsert_sql = pg_table.upsert(['id'], name="Product A", price=2000).compile()
copy_from_sql = pg_table.copy_from_csv('/tmp/data.csv', delimiter=',').compile()
analyze_sql = pg_table.analyze().compile()
vacuum_sql = pg_table.vacuum(full=True).compile()
```

### Amazon Redshift
```python
redshift_table = Table(
    "events",
    Column("event_id", "int", primary_key=True),
    Column("user_id", "integer"),
    Column("event_type", "varchar(100)"),
    Column("timestamp", "datetime"),
    dialect="redshift",
    sort_keys=['timestamp'],
    dist_key='user_id',
    dist_style='KEY'
)

# Redshift S3 operations
copy_sql = redshift_table.copy_from_s3(
    "s3://my-bucket/data/",
    credentials="aws_iam_role=arn:aws:iam::123:role/Role",
    format="PARQUET"
).compile()

unload_sql = redshift_table.unload_to_s3(
    "SELECT * FROM events WHERE timestamp > '2024-01-01'",
    "s3://my-bucket/exports/",
    credentials="aws_iam_role=arn:aws:iam::123:role/Role"
).compile()
```

### AWS Athena
```python
athena_table = Table(
    "sales",
    Column("sale_id", "int"),
    Column("product_id", "integer"),
    Column("amount", "decimal(10,2)"),
    Column("sale_date", "string(10)"),
    dialect="athena",
    location="s3://data-lake/sales/",
    stored_as="PARQUET",
    partition_by=["sale_date"]
)

# Athena CTAS and partition management
ctas_sql = athena_table.create_as_select(
    "SELECT * FROM source_table WHERE amount > 100",
    table_name="high_value_sales",
    location="s3://data-lake/high_value_sales/",
    format="PARQUET"
).compile()

add_partition_sql = athena_table.add_partition(
    "sale_date='2024-01-01'",
    location="s3://data-lake/sales/year=2024/month=01/day=01/"
).compile()

# MSCK REPAIR TABLE
repair_sql = athena_table.msck_repair().compile()
```

### Oracle Database
```python
oracle_table = Table(
    "customers",
    Column("customer_id", "int", primary_key=True),
    Column("name", "varchar(255)"),
    Column("email", "string(255)"),
    dialect="oracle",
    tablespace="USERS",
    organization="HEAP",
    compress=True
)

# Oracle-specific operations
merge_sql = oracle_table.merge(
    "SELECT * FROM temp_customers",
    "target.customer_id = source.customer_id"
).compile()

index_sql = oracle_table.create_index(
    "idx_customer_email",
    ["email"],
    tablespace="INDEXES"
).compile()
```

### SQLite
```python
sqlite_table = Table(
    "logs",
    Column("id", "int", primary_key=True),
    Column("message", "str(1000)"),
    dialect="sqlite"
)

# SQLite-specific operations
replace_sql = sqlite_table.insert_or_replace(message="Log entry").compile()
attach_sql = sqlite_table.attach_database('/path/to/db.sqlite', 'backup').compile()
pragma_sql = sqlite_table.pragma('cache_size', 10000).compile()
```

## ⚙️ YAML Configuration System

SQLKit's YAML configuration system allows you to define table schemas and pre-configure dialect-specific method parameters, making your data workflows more maintainable and environment-aware.

### Automatic Metadata Application

```yaml
# tables.yaml
metadata:
  default_dialect: "postgresql"
  default_schema: "analytics"

tables:
  # Table without explicit dialect/schema - will use metadata defaults
  users:
    columns:
      - name: "id"
        type: "int"
        primary_key: true
      - name: "name"
        type: "varchar(255)"

  # Table with explicit overrides
  products:
    dialect: "mysql"           # Override default dialect
    schema_name: "inventory"   # Override default schema
    columns:
      - name: "id"
        type: "integer"
        primary_key: true
```

```python
from sqlkit import from_config

# Load tables - metadata defaults are automatically applied
users = from_config("users", "tables.yaml")
print(f"Users: {users.dialect}, {users.schema_name}")      # "postgresql", "analytics"

products = from_config("products", "tables.yaml")
print(f"Products: {products.dialect}, {products.schema_name}")  # "mysql", "inventory"
```

### Template Variables with Classmethod Expansion

```yaml
tables:
  logs:
    dialect: "redshift"
    dialect_methods:
      copy_from_s3:
        s3_path: "s3://data-bucket/logs/{{ environment }}/{{ date }}/"
        credentials: "aws_iam_role=arn:aws:iam::{{ account_id }}:role/{{ role_name }}"
        format: "{{ file_format }}"
        delimiter: "{{ delimiter }}"
```

```python
from sqlkit import from_config, YamlLoader

# Method 1: Use with table configuration
logs_table = from_config("logs", "tables.yaml")
copy_query = logs_table.copy_from_s3(template_vars={
    "environment": "production",
    "date": "2024-01-15",
    "account_id": "123456789",
    "role_name": "DataRole",
    "file_format": "CSV",
    "delimiter": ","
})

# Method 2: Use template expansion directly (no instance needed)
config = {
    "s3_path": "s3://bucket/{{ env }}/data/",
    "settings": {
        "format": "{{ format }}",
        "options": ["HEADER", "ENV={{ env }}"]
    }
}

expanded = YamlLoader.expand_templates(config, {
    "env": "prod",
    "format": "PARQUET"
})
# Result: Fully expanded configuration dictionary
```

### Multi-Environment Configuration

```yaml
# config/production.yaml
metadata:
  default_dialect: "redshift"
  default_schema: "prod_analytics"

tables:
  users:
    dialect_methods:
      copy_from_s3:
        s3_path: "s3://prod-data-bucket/users/{{ date }}/"
        credentials: "aws_iam_role=arn:aws:iam::123:role/ProdDataRole"

# config/staging.yaml
metadata:
  default_dialect: "postgresql"
  default_schema: "staging_analytics"

tables:
  users:
    dialect_methods:
      copy_from_s3:
        s3_path: "s3://staging-data-bucket/users/{{ date }}/"
        credentials: "aws_iam_role=arn:aws:iam::123:role/StagingDataRole"
```

```python
import os
from sqlkit import from_config

# Environment-aware configuration
env = os.getenv("ENVIRONMENT", "staging")
config_file = f"config/{env}.yaml"
users_table = from_config("users", config_file)

# Metadata defaults are automatically applied based on environment
print(f"Dialect: {users_table.dialect}")      # "redshift" (prod) or "postgresql" (staging)
print(f"Schema: {users_table.schema_name}")   # "prod_analytics" or "staging_analytics"

# Same code works across environments
copy_query = users_table.copy_from_s3(template_vars={"date": "2024-01-15"})
```

## 🛡️ Type Safety & Quality

SQLKit is built with modern Python best practices:

- **Python 3.13+**: Latest Python with advanced type hints
- **Full Type Annotations**: Complete mypy compatibility with strict settings
- **Code Quality Tools**:
  - **Black**: Code formatting with 79-character line limit
  - **Ruff**: Fast linting with comprehensive rules (E, W, F, I, N, UP)
  - **Mypy**: Static type checking with strict configuration
- **Comprehensive Testing**: 170+ test cases covering all features including flexible types and YAML system
- **Documentation**: NumPy-style docstrings throughout
- **Configuration Validation**: Pydantic schemas ensure type-safe YAML configurations
- **Dynamic Type System**: String-based type specifications with full SQLAlchemy compatibility

## 🤝 Contributing

SQLKit follows strict quality standards and uses pandas-style prefixes for clear issue categorization.

### Issue and Pull Request Prefixes

When creating issues or pull requests, please use the following prefixes:

#### Issue Prefixes
- **`BUG:`** Bug reports and fixes
  - Example: `BUG: Template expansion fails with nested dictionaries`
- **`ENH:`** New features and enhancements
  - Example: `ENH: Add support for MongoDB dialect`
- **`DOC:`** Documentation improvements
  - Example: `DOC: Add examples for Oracle-specific operations`
- **`TST:`** Test-related changes
  - Example: `TST: Add tests for flexible type system`
- **`CLN:`** Code cleanup and refactoring
  - Example: `CLN: Refactor dialect registry initialization`
- **`PERF:`** Performance improvements
  - Example: `PERF: Optimize YAML template expansion`
- **`REG:`** Regressions
  - Example: `REG: Type parsing broken in version 0.2.0`

#### Pull Request Prefixes
- **`BUG:`** Bug fixes
- **`ENH:`** New features
- **`DOC:`** Documentation updates
- **`TST:`** Test additions/improvements
- **`CLN:`** Code cleanup
- **`PERF:`** Performance improvements
- **`REL:`** Release-related changes

### Quality Requirements

1. **Code Quality**: All code must pass quality checks
   ```bash
   uv run black sqlkit/
   uv run ruff check sqlkit/ --fix
   uv run mypy sqlkit/
   ```

2. **Testing**: Tests required for new features
   ```bash
   uv run pytest -v --cov=sqlkit
   ```

3. **Documentation**: NumPy-style docstrings for all public APIs

4. **Line Limit**: 79-character line limit (enforced by black and ruff)

5. **Type Safety**: Full type annotations required

### Development Setup

```bash
# Clone and setup
git clone <repository-url>
cd sqlkit

# Install with uv (recommended)
uv sync --all-groups

# Run quality checks
uv run python scripts/dev.py check

# Run specific checks
uv run python scripts/dev.py format
uv run python scripts/dev.py lint
uv run python scripts/dev.py typecheck
uv run python scripts/dev.py test
```

## 📋 Requirements

- **Python 3.13+**: Modern Python with advanced type hints
- **SQLAlchemy 2.0+**: Core SQL generation and dialect support
- **Pydantic 2.0+**: Configuration validation and parsing
- **PyYAML 6.0+**: YAML configuration file support

## 🔧 Development Tools

- **uv**: Fast Python package installer and resolver
- **Black 25.1.0+**: Code formatting
- **Ruff 0.12.4+**: Fast Python linter
- **Mypy 1.17.0+**: Static type checking
- **Pytest 8.4.1+**: Testing framework

## 📄 License

MIT License - see LICENSE file for details.

## 🔗 Related Projects

- [SQLAlchemy](https://sqlalchemy.org/) - The foundation for SQL generation
- [dbt](https://getdbt.com/) - Data transformation workflows
- [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration
- [Pydantic](https://pydantic.dev/) - Data validation and settings management
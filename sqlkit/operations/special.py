from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.sql import ClauseElement

from sqlkit.operations.base import BaseQuery


class SpecialQuery(BaseQuery):
    """Base class for database-specific special queries"""

    def __init__(
        self,
        table: Any,
        query_type: str,
        params: dict[str, Any] | None = None,
        dialect: Any = None,
    ) -> None:
        super().__init__(table, dialect)
        self.query_type = query_type
        self.params = params or {}


# MySQL Special Queries
class MySQLSpecialQuery(SpecialQuery):
    def build(self) -> ClauseElement:
        if self.query_type == "REPLACE":
            values_str = ", ".join(
                [f"{k} = '{v}'" for k, v in self.params.items()]
            )
            return text(f"REPLACE INTO {self.table.name} SET {values_str}")

        elif self.query_type == "INSERT_IGNORE":
            values_str = ", ".join(
                [f"{k} = '{v}'" for k, v in self.params.items()]
            )
            return text(
                f"INSERT IGNORE INTO {self.table.name} SET {values_str}"
            )

        elif self.query_type == "SHOW_CREATE":
            return text(f"SHOW CREATE TABLE {self.table.name}")

        return text("")


# PostgreSQL Special Queries
class PostgreSQLSpecialQuery(SpecialQuery):
    def build(self) -> ClauseElement:
        if self.query_type == "COPY_FROM":
            file_path = self.params["file_path"]
            format_str = self.params.get("format", "CSV")
            delimiter = self.params.get("delimiter", ",")
            header = "HEADER" if self.params.get("header", True) else ""
            return text(
                f"COPY {self.table.name} FROM '{file_path}' "
                f"WITH {format_str} {header} DELIMITER '{delimiter}'"
            )

        elif self.query_type == "COPY_TO":
            file_path = self.params["file_path"]
            query = self.params.get(
                "query", f"SELECT * FROM {self.table.name}"
            )
            format_str = self.params.get("format", "CSV")
            return text(f"COPY ({query}) TO '{file_path}' WITH {format_str}")

        elif self.query_type == "ANALYZE":
            return text(f"ANALYZE {self.table.name}")

        elif self.query_type == "VACUUM":
            full_str = "FULL" if self.params.get("full", False) else ""
            return text(f"VACUUM {full_str} {self.table.name}")

        return text("")


# SQLite Special Queries
class SQLiteSpecialQuery(SpecialQuery):
    def build(self) -> ClauseElement:
        if self.query_type == "INSERT_OR_REPLACE":
            columns = ", ".join(self.params.keys())
            values = ", ".join([f"'{v}'" for v in self.params.values()])
            return text(
                f"INSERT OR REPLACE INTO {self.table.name} "
                f"({columns}) VALUES ({values})"
            )

        elif self.query_type == "INSERT_OR_IGNORE":
            columns = ", ".join(self.params.keys())
            values = ", ".join([f"'{v}'" for v in self.params.values()])
            return text(
                f"INSERT OR IGNORE INTO {self.table.name} "
                f"({columns}) VALUES ({values})"
            )

        elif self.query_type == "ATTACH_DATABASE":
            return text(
                f"ATTACH DATABASE '{self.params['db_path']}' "
                f"AS {self.params['alias']}"
            )

        elif self.query_type == "DETACH_DATABASE":
            return text(f"DETACH DATABASE {self.params['alias']}")

        elif self.query_type == "PRAGMA":
            pragma_name = self.params["pragma_name"]
            value = self.params.get("value")
            if value:
                return text(f"PRAGMA {pragma_name} = {value}")
            return text(f"PRAGMA {pragma_name}")

        return text("")


# Redshift Special Queries
class RedshiftSpecialQuery(SpecialQuery):
    def build(self) -> ClauseElement:
        if self.query_type == "COPY_FROM_S3":
            s3_path = self.params["s3_path"]
            credentials = self.params.get("credentials", "")
            format_opt = self.params.get("format", "CSV")
            delimiter = self.params.get("delimiter", ",")
            options = []

            if credentials:
                options.append(f"CREDENTIALS '{credentials}'")
            options.append(f"FORMAT {format_opt}")
            if delimiter and format_opt.upper() == "CSV":
                options.append(f"DELIMITER '{delimiter}'")

            options_str = " ".join(options)
            return text(
                f"COPY {self.table.name} FROM '{s3_path}' {options_str}"
            )

        elif self.query_type == "UNLOAD_TO_S3":
            query = self.params["query"]
            s3_path = self.params["s3_path"]
            credentials = self.params.get("credentials", "")
            format_opt = self.params.get("format", "CSV")

            options = []
            if credentials:
                options.append(f"CREDENTIALS '{credentials}'")
            options.append(f"FORMAT {format_opt}")

            options_str = " ".join(options)
            return text(f"UNLOAD ('{query}') TO '{s3_path}' {options_str}")

        elif self.query_type == "ANALYZE_COMPRESSION":
            return text(f"ANALYZE COMPRESSION {self.table.name}")

        elif self.query_type == "VACUUM_REINDEX":
            return text(f"VACUUM REINDEX {self.table.name}")

        elif self.query_type == "DEEP_COPY":
            new_table = self.params["new_table_name"]
            return text(
                f"CREATE TABLE {new_table} AS SELECT * FROM {self.table.name}"
            )

        return text("")


# Athena Special Queries
class AthenaSpecialQuery(SpecialQuery):
    def build(self) -> ClauseElement:
        if self.query_type == "CTAS":
            table_name = self.params["table_name"]
            query = self.params["query"]
            location = self.params.get("location")
            format_opt = self.params.get("format", "PARQUET")
            partition_by = self.params.get("partition_by", [])

            options = []
            if location:
                options.append(f"external_location = '{location}'")
            options.append(f"format = '{format_opt}'")
            if partition_by:
                partition_cols = ", ".join(partition_by)
                options.append(f"partitioned_by = ARRAY[{partition_cols}]")

            options_str = ", ".join(options)
            return text(
                f"CREATE TABLE {table_name} WITH ({options_str}) AS {query}"
            )

        elif self.query_type == "ADD_PARTITION":
            partition_spec = self.params["partition_spec"]
            location = self.params.get("location")
            location_str = f" LOCATION '{location}'" if location else ""
            return text(
                f"ALTER TABLE {self.table.name} ADD PARTITION "
                f"({partition_spec}){location_str}"
            )

        elif self.query_type == "DROP_PARTITION":
            partition_spec = self.params["partition_spec"]
            return text(
                f"ALTER TABLE {self.table.name} DROP PARTITION "
                f"({partition_spec})"
            )

        elif self.query_type == "MSCK_REPAIR":
            return text(f"MSCK REPAIR TABLE {self.table.name}")

        elif self.query_type == "SHOW_PARTITIONS":
            return text(f"SHOW PARTITIONS {self.table.name}")

        return text("")


# Oracle Special Queries
class OracleSpecialQuery(SpecialQuery):
    def build(self) -> ClauseElement:
        if self.query_type == "MERGE":
            source_query = self.params["source_query"]
            on_condition = self.params["on_condition"]
            return text(
                f"""
                MERGE INTO {self.table.name} target
                USING ({source_query}) source
                ON ({on_condition})
                WHEN MATCHED THEN UPDATE SET ...
                WHEN NOT MATCHED THEN INSERT ...
            """
            )

        elif self.query_type == "TRUNCATE":
            storage_clause = (
                "REUSE STORAGE"
                if self.params.get("reuse_storage", True)
                else "DROP STORAGE"
            )
            return text(f"TRUNCATE TABLE {self.table.name} {storage_clause}")

        elif self.query_type == "ANALYZE_TABLE":
            estimate = self.params.get("estimate_percent")
            method = self.params.get("method", "FOR ALL COLUMNS")
            estimate_str = (
                f"ESTIMATE STATISTICS SAMPLE {estimate} PERCENT"
                if estimate
                else "COMPUTE STATISTICS"
            )
            return text(
                f"ANALYZE TABLE {self.table.name} {estimate_str} {method}"
            )

        elif self.query_type == "CREATE_INDEX":
            index_name = self.params["index_name"]
            columns = ", ".join(self.params["columns"])
            tablespace = self.params.get("tablespace")
            tablespace_str = f" TABLESPACE {tablespace}" if tablespace else ""
            return text(
                f"CREATE INDEX {index_name} ON {self.table.name} "
                f"({columns}){tablespace_str}"
            )

        elif self.query_type == "CREATE_SEQUENCE":
            seq_name = self.params["sequence_name"]
            start_with = self.params.get("start_with", 1)
            increment_by = self.params.get("increment_by", 1)
            return text(
                f"CREATE SEQUENCE {seq_name} START WITH {start_with} "
                f"INCREMENT BY {increment_by}"
            )

        return text("")

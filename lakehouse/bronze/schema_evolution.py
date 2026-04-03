from __future__ import annotations

from pyspark.sql.types import StructType


def detect_new_columns(existing_columns: list[str], incoming_columns: list[str]) -> list[str]:
    return sorted(column for column in incoming_columns if column not in set(existing_columns))


def schema_column_names(schema: StructType) -> list[str]:
    return [field.name for field in schema.fields]


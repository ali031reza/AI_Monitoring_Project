"""Shared helpers for safe, idempotent Parquet table writes.

Event tables are append-only. This module inserts only records whose
business keys have not already been persisted. It deliberately refuses
schema changes and conflicting duplicate keys rather than silently
discarding or rewriting historical records.

Parquet/Hive does not enforce unique constraints or provide an atomic
MERGE operation. Run one writer per target table at a time. For
multi-writer production workloads, use a transactional table format.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def append_unique_records(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    key_columns: Sequence[str],
    *,
    logger: logging.Logger,
    layer_name: str,
) -> int:
    """Append unseen records using a null-safe composite business key."""

    keys = tuple(key_columns)

    if not keys or len(keys) != len(set(keys)):
        raise ValueError("key_columns must contain unique column names.")

    missing = [column for column in keys if column not in dataframe.columns]
    if missing:
        raise ValueError(
            f"{layer_name} input is missing key columns: {missing}"
        )

    if "event_id" in keys:
        invalid_event_id = dataframe.filter(
            F.col("event_id").isNull()
            | (F.trim(F.col("event_id").cast("string")) == "")
        ).limit(1).count()

        if invalid_event_id:
            raise ValueError(
                f"{layer_name} event records contain a missing event_id."
            )

    duplicate_keys_exist = (
        dataframe.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )

    if duplicate_keys_exist:
        payload = F.to_json(
            F.struct(*[F.col(column) for column in dataframe.columns])
        )
        conflicting_keys_exist = (
            dataframe
            .select(*keys, payload.alias("_record_payload"))
            .groupBy(*keys)
            .agg(
                F.countDistinct("_record_payload").alias(
                    "_payload_versions"
                )
            )
            .filter(F.col("_payload_versions") > 1)
            .limit(1)
            .count()
            > 0
        )

        if conflicting_keys_exist:
            raise ValueError(
                f"{layer_name} input contains conflicting records "
                f"for key columns {keys}."
            )

    incoming = dataframe.dropDuplicates(list(keys))
    table_exists = spark.catalog.tableExists(table_name)

    if table_exists:
        existing = spark.table(table_name)

        incoming_schema = {
            field.name: field.dataType.simpleString()
            for field in incoming.schema.fields
        }
        existing_schema = {
            field.name: field.dataType.simpleString()
            for field in existing.schema.fields
        }

        if incoming_schema != existing_schema:
            raise ValueError(
                f"Schema mismatch for {table_name}. Refusing to append "
                "until the table schema has been migrated explicitly."
            )

        existing_keys = (
            existing.select(*keys).dropDuplicates().alias("existing")
        )
        incoming_alias = incoming.alias("incoming")
        join_condition = None

        for column in keys:
            key_match = F.col(
                f"incoming.`{column}`"
            ).eqNullSafe(
                F.col(f"existing.`{column}`")
            )
            join_condition = (
                key_match
                if join_condition is None
                else join_condition & key_match
            )

        new_records = (
            incoming_alias
            .join(existing_keys, on=join_condition, how="left_anti")
            .select("incoming.*")
            .select(*existing.columns)
        )

        new_count = new_records.count()
        if new_count == 0:
            logger.info(
                "%s table %s is up to date; no new records to append.",
                layer_name,
                table_name,
            )
            return 0
    else:
        new_records = incoming
        new_count = new_records.count()

    (
        new_records.write
        .format("parquet")
        .mode("append")
        .saveAsTable(table_name)
    )

    logger.info(
        "%s table %s appended %d new records.",
        layer_name,
        table_name,
        new_count,
    )
    return new_count

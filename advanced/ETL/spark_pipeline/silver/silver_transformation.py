"""
Silver layer transformations for the AI Monitoring project.

Reads raw Bronze tables, validates and normalizes their schemas,
and writes structured Silver tables using Parquet.
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from advanced.ETL.spark_pipeline.table_writer import append_unique_records


logger = logging.getLogger(__name__)


class SilverTransformation:
    """Transform Bronze datasets into clean Silver datasets."""

    PERSON_COLUMNS = (
        "Time_stamp",
        "Zone",
        "detected_persons_id",
        "authorized",
        "camera_id",
        "event_id",
        "detector_type",
    )

    PLATE_COLUMNS = (
        "Time_stamp",
        "Zone",
        "detected_plates",
        "camera_id",
        "event_id",
        "detector_type",
    )

    EMPLOYEE_COLUMNS = (
        "employee_id",
        "plate_number",
    )

    CAMERA_COLUMNS = (
        "camera_id",
        "zone_id",
        "detector_type",
        "enabled",
        "kafka_partition",
    )

    RESTRICTED_ACCESS_COLUMNS = (
        "employee_id",
        "zone_id",
    )

    def __init__(
        self,
        spark: SparkSession,
        source_database: str = "bronze",
        target_database: str = "silver",
    ) -> None:

        if spark is None:
            raise TypeError("A SparkSession is required.")

        for name, database in (
            ("source_database", source_database),
            ("target_database", target_database),
        ):
            if (
                not isinstance(database, str)
                or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database)
            ):
                raise ValueError(f"Invalid {name}: {database!r}")

        self.spark = spark
        self.source_database = source_database
        self.target_database = target_database

        self.person_ids_schema = T.ArrayType(T.StringType())
        self.authorization_schema = T.ArrayType(T.BooleanType())
        self.plates_schema = T.ArrayType(T.StringType())

    # ========================================================
    # 1. GENERAL VALIDATION HELPERS
    # ========================================================

    @staticmethod
    def _validate_table_name(table_name: str) -> None:

        if (
            not isinstance(table_name, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name)
        ):
            raise ValueError(
                f"Invalid table name: {table_name!r}"
            )

    @staticmethod
    def _require_columns(
        df: DataFrame,
        required_columns: tuple[str, ...],
        table_name: str,
    ) -> None:

        missing = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Table {table_name} is missing required columns: {missing}"
            )

    @staticmethod
    def _assert_no_rows(
        df: DataFrame,
        condition,
        message: str,
    ) -> None:
        """Raise an error when at least one invalid record exists."""

        if df.filter(condition).limit(1).count() > 0:
            raise ValueError(message)

    def _validate_text_columns(
        self,
        df: DataFrame,
        columns: tuple[str, ...],
        dataset_name: str,
    ) -> None:

        for column in columns:

            self._assert_no_rows(
                df,
                F.col(column).isNull()
                | (F.trim(F.col(column).cast("string")) == ""),
                f"{dataset_name} contains missing or empty values "
                f"in column {column!r}.",
            )

    # ========================================================
    # 2. DATABASE MANAGEMENT
    # ========================================================

    def _ensure_silver_database(self) -> None:

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.target_database}"
        )

        logger.info(
            "Silver database is ready: %s",
            self.target_database,
        )

    # ========================================================
    # 3. BRONZE TABLE READER
    # ========================================================

    def _read_bronze_table(
        self,
        table_name: str,
    ) -> DataFrame:

        self._validate_table_name(table_name)

        full_table_name = (
            f"{self.source_database}.{table_name}"
        )

        if not self.spark.catalog.tableExists(full_table_name):
            raise ValueError(
                f"Bronze table does not exist: {full_table_name}"
            )

        logger.info("Reading Bronze table: %s", full_table_name)

        return self.spark.table(full_table_name)

    # ========================================================
    # 4. SILVER TABLE WRITER
    # ========================================================

    def _write_silver_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        key_columns: tuple[str, ...] | None = None,
    ) -> None:
        """Persist Silver event history or refresh a reference snapshot."""

        self._validate_table_name(table_name)

        if mode not in ("overwrite", "append"):
            raise ValueError(
                f"Invalid write mode: {mode!r}"
            )

        self._ensure_silver_database()
        full_table_name = f"{self.target_database}.{table_name}"

        if mode == "append":
            if not key_columns:
                raise ValueError(
                    "key_columns are required for append mode."
                )

            append_unique_records(
                spark=self.spark,
                dataframe=df,
                table_name=full_table_name,
                key_columns=key_columns,
                logger=logger,
                layer_name="Silver",
            )
            return

        if key_columns:
            raise ValueError(
                "key_columns can only be used with append mode."
            )

        logger.info(
            "Refreshing Silver reference snapshot: %s",
            full_table_name,
        )
        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .saveAsTable(full_table_name)
        )
        logger.info(
            "Successfully refreshed Silver reference snapshot: %s",
            full_table_name,
        )

    # ========================================================
    # 5. COMMON EVENT NORMALIZATION
    # ========================================================

    def _normalize_event_context(
        self,
        df: DataFrame,
        dataset_name: str,
    ) -> DataFrame:

        df = df.withColumn(
            "Time_stamp",
            F.try_to_timestamp(
                F.trim(F.col("Time_stamp").cast("string"))
            ),
        )

        self._assert_no_rows(
            df,
            F.col("Time_stamp").isNull(),
            f"{dataset_name} contains invalid timestamps.",
        )

        for column in (
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
        ):
            df = df.withColumn(
                column,
                F.trim(F.col(column).cast("string")),
            )

        self._validate_text_columns(
            df,
            (
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",
            ),
            dataset_name,
        )

        return df

    # ========================================================
    # 6. JSON ARRAY PARSING
    # ========================================================

    def _parse_array_column(
        self,
        df: DataFrame,
        column: str,
        array_schema: T.ArrayType,
        dataset_name: str,
    ) -> DataFrame:
        """Parse JSON arrays while supporting existing array columns."""

        source_type = df.schema[column].dataType

        if isinstance(source_type, T.StringType):

            parsed_column = F.from_json(
                F.col(column),
                array_schema,
            )

        elif isinstance(source_type, T.ArrayType):

            parsed_column = F.col(column).cast(array_schema)

        else:
            raise TypeError(
                f"Unsupported data type for {dataset_name}.{column}: "
                f"{source_type}"
            )

        df = df.withColumn(
            column,
            parsed_column,
        )

        self._assert_no_rows(
            df,
            F.col(column).isNull(),
            f"{dataset_name} contains null or invalid JSON arrays "
            f"in column {column!r}.",
        )

        return df

    # ========================================================
    # 7. PERSON WORKER DISCOVERY
    # ========================================================

    def _discover_person_workers(
        self,
    ) -> list[tuple[int, str]]:

        discovered = []

        tables = self.spark.catalog.listTables(
            self.source_database
        )

        prefix = "person_detections_worker"

        for table in tables:

            table_name = table.name

            if not table_name.startswith(prefix):
                continue

            match = re.fullmatch(
                r"person_detections_worker([1-9]\d*)",
                table_name,
            )

            if not match:
                raise ValueError(
                    f"Invalid person worker table name: {table_name}"
                )

            worker_number = int(match.group(1))

            discovered.append(
                (worker_number, table_name)
            )

        discovered.sort(
            key=lambda item: item[0]
        )

        worker_numbers = [
            number
            for number, _ in discovered
        ]

        if len(worker_numbers) != len(set(worker_numbers)):
            raise ValueError(
                "Duplicate person worker numbers were discovered."
            )

        if not discovered:
            raise ValueError(
                "No person worker tables found in Bronze."
            )

        logger.info(
            "Discovered %s person worker tables: %s",
            len(discovered),
            worker_numbers,
        )

        return discovered

    # ========================================================
    # 8. PERSON DETECTIONS
    # ========================================================

    def transform_person_detections(self) -> DataFrame:
        """
        Combine all person workers into one frame-level dataset.

        Empty detection arrays are deliberately preserved.
        """

        worker_tables = self._discover_person_workers()

        worker_dataframes = []

        for worker_number, table_name in worker_tables:

            logger.info(
                "Processing person worker %s.",
                worker_number,
            )

            df = self._read_bronze_table(table_name)

            self._require_columns(
                df,
                self.PERSON_COLUMNS,
                table_name,
            )

            df = df.select(
                *self.PERSON_COLUMNS
            )

            df = self._normalize_event_context(
                df,
                table_name,
            )

            df = self._parse_array_column(
                df,
                "detected_persons_id",
                self.person_ids_schema,
                table_name,
            )

            df = self._parse_array_column(
                df,
                "authorized",
                self.authorization_schema,
                table_name,
            )

            # Preserve the positional relationship between
            # employee IDs and their authorization flags.

            self._assert_no_rows(
                df,
                F.size("detected_persons_id")
                != F.size("authorized"),
                f"Worker {worker_number} contains misaligned "
                "person and authorization arrays.",
            )

            df = df.withColumn(
                "detected_persons_id",
                F.transform(
                    F.col("detected_persons_id"),
                    lambda value: F.trim(value),
                ),
            )

            worker_dataframes.append(df)

        # Combine workers without losing source columns.

        combined_df = worker_dataframes[0]

        for worker_df in worker_dataframes[1:]:

            combined_df = combined_df.unionByName(
                worker_df
            )

        self._write_silver_table(
            combined_df,
            "person_detections",
            key_columns=("event_id",),
        )

        logger.info(
            "Successfully combined %s person workers.",
            len(worker_tables),
        )

        return combined_df

    # ========================================================
    # 9. PERSON-LEVEL DETECTION DETAILS
    # ========================================================

    def transform_person_detection_details(self) -> DataFrame:
        """
        Create individual person-level records.

        arrays_zip preserves employee/authorization alignment.
        """

        person_df = self.spark.table(
            f"{self.target_database}.person_detections"
        )

        # explode intentionally produces zero detail rows
        # when the original frame contains empty arrays.

        details_df = (
            person_df

            .withColumn(
                "_person_detail",
                F.explode(
                    F.arrays_zip(
                        F.col("detected_persons_id"),
                        F.col("authorized"),
                    )
                ),
            )

            .select(
                "Time_stamp",
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",

                F.col(
                    "_person_detail.detected_persons_id"
                ).alias("employee_id"),

                F.col(
                    "_person_detail.authorized"
                ).alias("authorized"),
            )
        )

        self._write_silver_table(
            details_df,
            "person_detection_details",
            key_columns=("event_id", "employee_id"),
        )

        return details_df

    # ========================================================
    # 10. PLATE DETECTIONS
    # ========================================================

    def transform_plate_detections(self) -> DataFrame:
        """Normalize frame-level license-plate detections."""

        df = self._read_bronze_table(
            "plate_detections"
        )

        self._require_columns(
            df,
            self.PLATE_COLUMNS,
            "plate_detections",
        )

        df = df.select(
            *self.PLATE_COLUMNS
        )

        df = self._normalize_event_context(
            df,
            "plate_detections",
        )

        df = self._parse_array_column(
            df,
            "detected_plates",
            self.plates_schema,
            "plate_detections",
        )

        # Standardize plate formatting for downstream matching.

        df = df.withColumn(
            "detected_plates",
            F.transform(
                F.col("detected_plates"),
                lambda plate: F.upper(F.trim(plate)),
            ),
        )

        self._write_silver_table(
            df,
            "plate_detections",
            key_columns=("event_id",),
        )

        return df

    # ========================================================
    # 11. EMPLOYEES
    # ========================================================

    def transform_employees(self) -> DataFrame:
        """Normalize employee reference data."""

        df = self._read_bronze_table(
            "employees"
        )

        self._require_columns(
            df,
            self.EMPLOYEE_COLUMNS,
            "employees",
        )

        df = df.select(
            *self.EMPLOYEE_COLUMNS
        )

        df = df.withColumn(
            "employee_id",
            F.trim(F.col("employee_id")),
        )

        self._validate_text_columns(
            df,
            ("employee_id",),
            "employees",
        )

        normalized_plate = F.upper(
            F.trim(F.col("plate_number"))
        )

        df = df.withColumn(
            "plate_number",
            F.when(
                normalized_plate.isNull()
                | (normalized_plate == "")
                | (normalized_plate == "NA"),
                F.lit(None).cast("string"),
            ).otherwise(normalized_plate),
        )

        self._write_silver_table(
            df,
            "employees",
            mode="overwrite",
        )

        return df

    # ========================================================
    # 12. CAMERAS
    # ========================================================

    def transform_cameras(self) -> DataFrame:
        """Normalize camera configuration and data types."""

        df = self._read_bronze_table(
            "cameras"
        )

        self._require_columns(
            df,
            self.CAMERA_COLUMNS,
            "cameras",
        )

        df = df.select(
            *self.CAMERA_COLUMNS
        )

        for column in (
            "camera_id",
            "zone_id",
            "detector_type",
        ):
            df = df.withColumn(
                column,
                F.trim(F.col(column).cast("string")),
            )

        self._validate_text_columns(
            df,
            (
                "camera_id",
                "zone_id",
                "detector_type",
            ),
            "cameras",
        )

        df = df.withColumn(
            "detector_type",
            F.lower(F.col("detector_type")),
        )

        self._assert_no_rows(
            df,
            ~F.col("detector_type").isin("person", "plate"),
            "Cameras contain an unsupported detector type.",
        )

        enabled_value = F.upper(
            F.trim(F.col("enabled"))
        )

        self._assert_no_rows(
            df,
            enabled_value.isNull()
            | (~enabled_value.isin("TRUE", "FALSE")),
            "Camera enabled column contains invalid Boolean values.",
        )

        df = df.withColumn(
            "enabled",
            enabled_value == "TRUE",
        )

        # try_cast prevents malformed values from being
        # silently converted into an incorrect partition.

        df = df.withColumn(
            "kafka_partition",
            F.expr("try_cast(kafka_partition AS INT)"),
        )

        self._assert_no_rows(
            df,
            F.col("kafka_partition").isNull()
            | (F.col("kafka_partition") < 0),
            "Camera Kafka partition contains invalid values.",
        )

        self._write_silver_table(
            df,
            "cameras",
            mode="overwrite",
        )

        return df

    # ========================================================
    # 13. RESTRICTED ACCESS
    # ========================================================

    def transform_restricted_access(self) -> DataFrame:
        """Normalize employee and zone reference values."""

        df = self._read_bronze_table(
            "restricted_access"
        )

        self._require_columns(
            df,
            self.RESTRICTED_ACCESS_COLUMNS,
            "restricted_access",
        )

        df = df.select(
            *self.RESTRICTED_ACCESS_COLUMNS
        )

        for column in self.RESTRICTED_ACCESS_COLUMNS:

            df = df.withColumn(
                column,
                F.trim(F.col(column)),
            )

        self._validate_text_columns(
            df,
            self.RESTRICTED_ACCESS_COLUMNS,
            "restricted_access",
        )

        # No authorization or violation interpretation
        # is performed at the Silver layer.

        self._write_silver_table(
            df,
            "restricted_access",
            mode="overwrite",
        )

        return df

    # ========================================================
    # 14. COMPLETE SILVER PIPELINE
    # ========================================================

    def run(self) -> None:
        """Execute all Silver transformations."""

        logger.info("=" * 60)
        logger.info("STARTING SILVER TRANSFORMATION")
        logger.info("=" * 60)

        self._ensure_silver_database()

        logger.info("[1/6] Transforming person detections.")

        self.transform_person_detections()

        logger.info("[2/6] Creating person detection details.")

        self.transform_person_detection_details()

        logger.info("[3/6] Transforming plate detections.")

        self.transform_plate_detections()

        logger.info("[4/6] Transforming employees.")

        self.transform_employees()

        logger.info("[5/6] Transforming cameras.")

        self.transform_cameras()

        logger.info("[6/6] Transforming restricted access.")

        self.transform_restricted_access()

        logger.info("=" * 60)
        logger.info("SILVER TRANSFORMATION COMPLETED")
        logger.info("=" * 60)

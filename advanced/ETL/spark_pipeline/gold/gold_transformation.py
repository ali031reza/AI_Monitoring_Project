"""
Gold Transformation
===================

Transforms Silver analytical datasets into Gold business-ready tables.

Output tables:
    1. gold.employee_directory
    2. gold.monitoring_events
    3. gold.monitoring_detection_details
    4. gold.restricted_access

Features:
    - Automatic Gold database initialization
    - Strict input validation
    - Safe timestamp conversion
    - Scalable employee/license-plate mapping
    - Person and plate event integration
    - Detection-level normalization
    - Parquet table storage
    - Structured logging
"""

import logging
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


logger = logging.getLogger(__name__)


class GoldTransformation:
    """Transform Silver datasets into analytical Gold tables."""

    VALID_WRITE_MODES = {
        "overwrite",
        "append",
        "error",
        "errorifexists",
        "ignore",
    }

    DATABASE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    # ============================================================
    # 1. INITIALIZATION
    # ============================================================

    def __init__(
        self,
        spark: SparkSession,
        source_database: str = "silver",
        target_database: str = "gold",
    ) -> None:

        if not isinstance(spark, SparkSession):
            raise TypeError("spark must be a valid SparkSession.")

        self._validate_database_name(source_database)
        self._validate_database_name(target_database)

        self.spark = spark
        self.source_database = source_database
        self.target_database = target_database

        logger.info(
            "Gold transformation initialized | Source: %s | Target: %s",
            source_database,
            target_database,
        )

    # ============================================================
    # 2. VALIDATION HELPERS
    # ============================================================

    @classmethod
    def _validate_database_name(cls, name: str) -> None:
        """Validate a database identifier before using it in SQL."""

        if (
            not isinstance(name, str)
            or not cls.DATABASE_PATTERN.fullmatch(name)
        ):
            raise ValueError(f"Invalid database name: {name!r}")

    @classmethod
    def _validate_table_name(
        cls,
        name: str,
        context: str = "table",
    ) -> None:
        """Validate a table identifier and provide contextual errors."""

        if (
            not isinstance(name, str)
            or not cls.TABLE_PATTERN.fullmatch(name)
        ):
            if context.lower() == "table":
                message = f"Invalid table name: {name!r}"
            else:
                message = f"Invalid {context} table name: {name!r}"

            raise ValueError(message)

    @staticmethod
    def _require_columns(
        df: DataFrame,
        required_columns: list[str],
        context: str,
        *,
        missing_message: str | None = None,
    ) -> None:
        """Raise a clear ValueError when required columns are absent."""

        missing = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing:
            message = (
                missing_message
                or f"{context} is missing required columns"
            )

            raise ValueError(f"{message}: {missing}")

    @staticmethod
    def _assert_no_rows(
        df: DataFrame,
        condition,
        message: str,
    ) -> None:
        """Raise ValueError if at least one row matches a bad-data condition."""

        invalid_rows_exist = (
            df.filter(condition)
            .limit(1)
            .count()
            > 0
        )

        if invalid_rows_exist:
            raise ValueError(message)

    # ============================================================
    # 3. DATABASE MANAGEMENT
    # ============================================================

    def _ensure_gold_database(self) -> None:
        """Create the Gold database when it does not already exist."""

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS `{self.target_database}`"
        )

        logger.info(
            "Gold database is ready: %s",
            self.target_database,
        )

    # ============================================================
    # 4. SILVER READER
    # ============================================================

    def _read_silver_table(self, table_name: str) -> DataFrame:
        """Read a validated table from the Silver database."""

        self._validate_table_name(
            table_name,
            context="Silver",
        )

        full_table_name = (
            f"{self.source_database}.{table_name}"
        )

        if not self.spark.catalog.tableExists(full_table_name):
            raise ValueError(
                f"Silver table does not exist: {full_table_name}"
            )

        logger.info("Reading Silver table: %s", full_table_name)

        return self.spark.table(full_table_name)

    # ============================================================
    # 5. GOLD WRITER
    # ============================================================

    def _write_gold_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
    ) -> None:
        """Write a DataFrame into an existing Gold database as Parquet."""

        self._validate_table_name(
            table_name,
            context="Gold",
        )

        if not isinstance(mode, str):
            raise ValueError(f"Invalid write mode: {mode!r}")

        normalized_mode = mode.lower()

        if normalized_mode not in self.VALID_WRITE_MODES:
            raise ValueError(f"Invalid write mode: {mode!r}")

        # Verify that the target database already exists.
        # Do not silently create it here.
        existing_databases = {
            database.name.casefold()
            for database in self.spark.catalog.listDatabases()
        }

        if self.target_database.casefold() not in existing_databases:
            raise ValueError(
                f"Gold database does not exist: {self.target_database}"
            )

        full_table_name = (
            f"{self.target_database}.{table_name}"
        )

        logger.info("Writing Gold table: %s", full_table_name)

        (
            df.write
            .format("parquet")
            .mode(normalized_mode)
            .saveAsTable(full_table_name)
        )

        logger.info(
            "Successfully written Gold table: %s",
            full_table_name,
        )

    # ============================================================
    # 6. TIMESTAMP NORMALIZATION
    # ============================================================

    def _normalize_timestamp(
        self,
        df: DataFrame,
        source_column: str = "Time_stamp",
        context: str = "Dataset",
    ) -> DataFrame:
        """
        Convert the source timestamp to a timestamp column.

        Invalid or missing timestamps raise an application-level
        ValueError rather than being silently retained as null.
        """

        if source_column not in df.columns:
            raise ValueError(
                f"{context} is missing timestamp column "
                f"'{source_column}'."
            )

        normalized = df.withColumn(
            "_gold_timestamp",
            F.try_to_timestamp(F.col(source_column)),
        )

        self._assert_no_rows(
            normalized,
            F.col(source_column).isNull()
            | F.col("_gold_timestamp").isNull(),
            f"{context} contains invalid timestamps.",
        )

        return (
            normalized
            .drop(source_column)
            .withColumnRenamed(
                "_gold_timestamp",
                "timestamp",
            )
        )

    # ============================================================
    # 7. ARRAY NORMALIZATION
    # ============================================================

    def _normalize_array(
        self,
        df: DataFrame,
        column_name: str,
        element_type: T.DataType,
        context: str,
    ) -> DataFrame:
        """Normalize a JSON string or an existing Spark array column."""

        if column_name not in df.columns:
            raise ValueError(
                f"{context} is missing array column '{column_name}'."
            )

        data_type = df.schema[column_name].dataType
        expected_type = T.ArrayType(element_type)

        if isinstance(data_type, T.StringType):

            parsed = F.from_json(
                F.col(column_name),
                expected_type,
            )

            self._assert_no_rows(
                df,
                F.col(column_name).isNotNull() & parsed.isNull(),
                (
                    f"{context} contains invalid JSON arrays "
                    f"in '{column_name}'."
                ),
            )

            return df.withColumn(column_name, parsed)

        if isinstance(data_type, T.ArrayType):

            return df.withColumn(
                column_name,
                F.col(column_name).cast(expected_type),
            )

        raise TypeError(
            f"Unsupported {column_name} data type: "
            f"{data_type.simpleString()}"
        )

    # ============================================================
    # 8. EMPLOYEE DIRECTORY
    # ============================================================

    def transform_employee_directory(self) -> DataFrame:
        """Validate and persist the Gold employee directory."""

        logger.info("Starting employee directory transformation.")

        employees_df = self._read_silver_table("employees")

        self._require_columns(
            employees_df,
            ["employee_id", "plate_number"],
            "Silver employees",
            missing_message="Missing employee columns",
        )

        # Keep the source values unchanged.
        # Normalized values are used separately for validation and lookup.
        employee_directory_df = employees_df.select(
            F.col("employee_id")
            .cast("string")
            .alias("employee_id"),

            F.col("plate_number")
            .cast("string")
            .alias("plate_number"),
        )

        self._assert_no_rows(
            employee_directory_df,
            F.col("employee_id").isNull()
            | (F.trim(F.col("employee_id")) == ""),
            "Employee directory contains invalid employee IDs.",
        )

        # Employee IDs are checked using trimmed keys.
        # The output retains the original employee_id values.
        duplicate_employee_ids = (
            employee_directory_df
            .withColumn(
                "_employee_id_key",
                F.trim(F.col("employee_id")),
            )
            .groupBy("_employee_id_key")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate_employee_ids:
            raise ValueError("Duplicate employee IDs found.")

        # Plate values are normalized only for uniqueness validation.
        normalized_plates_df = (
            employee_directory_df
            .select(
                F.upper(
                    F.trim(F.col("plate_number"))
                ).alias("_plate_key")
            )
            .filter(
                F.col("_plate_key").isNotNull()
                & (
                    ~F.col("_plate_key").isin(
                        "",
                        "NA",
                        "N/A",
                        "NULL",
                    )
                )
            )
        )

        duplicate_plates = (
            normalized_plates_df
            .groupBy("_plate_key")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate_plates:
            raise ValueError(
                "Duplicate registered license plates found."
            )

        self._write_gold_table(
            employee_directory_df,
            "employee_directory",
        )

        logger.info("Employee directory transformation completed.")

        return employee_directory_df

    # ============================================================
    # 9. EMPLOYEE / PLATE LOOKUP
    # ============================================================

    def _build_employee_plate_lookup(self) -> DataFrame:
        """
        Build a normalized relational employee/plate lookup.

        The production transformation uses this DataFrame join instead
        of collecting all employee mappings onto the Spark driver.
        """

        employees_df = self._read_silver_table("employees")

        # FIX:
        # Use the standard missing-required-columns validation message.
        # This makes validation consistent with the test expectations.
        self._require_columns(
            employees_df,
            ["employee_id", "plate_number"],
            "Silver employees",
        )

        employee_lookup_df = (
            employees_df
            .select(
                F.trim(
                    F.col("employee_id").cast("string")
                ).alias("employee_id"),

                F.upper(
                    F.trim(
                        F.col("plate_number").cast("string")
                    )
                ).alias("_plate_key"),
            )
            .filter(
                F.col("employee_id").isNotNull()
                & (F.col("employee_id") != "")
                & F.col("_plate_key").isNotNull()
                & (
                    ~F.col("_plate_key").isin(
                        "",
                        "NA",
                        "N/A",
                        "NULL",
                    )
                )
            )
        )

        duplicate_plates = (
            employee_lookup_df
            .groupBy("_plate_key")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate_plates:
            raise ValueError(
                "Duplicate registered license plates found."
            )

        return employee_lookup_df

    def _build_employee_plate_map(self) -> DataFrame:
        """
        Build a one-row Spark map for compatibility and inspection.

        This helper is not used by the production event transformation;
        the production path uses _build_employee_plate_lookup() and a join.
        """

        employee_lookup_df = self._build_employee_plate_lookup()

        return employee_lookup_df.agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("_plate_key"),
                        F.col("employee_id"),
                    )
                )
            ).alias("_plate_to_employee")
        )

    # ============================================================
    # 10. PLATE EVENT TRANSFORMATION
    # ============================================================

    def _transform_plate_events(self) -> DataFrame:
        """
        Transform frame-level plate detections.

        Registered plates are associated with employee IDs using a
        normalized join key. Original detected plate values and array
        order are preserved. Unknown plates receive a null employee ID.
        """

        logger.info("Starting plate event transformation.")

        plate_df = self._read_silver_table("plate_detections")

        self._require_columns(
            plate_df,
            [
                "Time_stamp",
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",
                "detected_plates",
            ],
            "Silver plate detections",
            missing_message="Missing plate columns",
        )

        plate_df = self._normalize_array(
            plate_df,
            "detected_plates",
            T.StringType(),
            "Plate detections",
        )

        plate_df = self._normalize_timestamp(
            plate_df,
            context="Plate detections",
        )

        # Give each source row a temporary identifier so that detections
        # from different frames cannot be combined during reconstruction.
        plate_df = plate_df.withColumn(
            "_row_id",
            F.monotonically_increasing_id(),
        )

        # posexplode_outer retains rows where detected_plates is null
        # or empty, and captures each plate's original array position.
        exploded_df = plate_df.select(
            "*",
            F.posexplode_outer("detected_plates").alias(
                "_plate_position",
                "_plate_value",
            ),
        )

        employee_lookup_df = (
            self._build_employee_plate_lookup()
            .select(
                "_plate_key",
                F.col("employee_id").alias(
                    "_mapped_employee_id"
                ),
            )
        )

        joined_df = exploded_df.join(
            employee_lookup_df,
            F.upper(
                F.trim(F.col("_plate_value"))
            ) == F.col("_plate_key"),
            "left",
        )

        grouping_columns = [
            "timestamp",
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
            "detected_plates",
        ]

        aggregations = [
            F.first(
                F.col(column),
                ignorenulls=False,
            ).alias(column)
            for column in grouping_columns
        ]

        aggregations.append(
            F.collect_list(
                F.struct(
                    F.col("_plate_position").alias("position"),
                    F.col("_mapped_employee_id").alias("employee_id"),
                )
            ).alias("_employee_pairs")
        )

        reconstructed_df = (
            joined_df
            .groupBy("_row_id")
            .agg(*aggregations)
        )

        ordered_pairs = F.filter(
            F.array_sort(F.col("_employee_pairs")),
            lambda pair: pair["position"].isNotNull(),
        )

        reconstructed_df = reconstructed_df.withColumn(
            "detected_employee_ids",
            F.when(
                F.col("detected_plates").isNull(),
                F.lit(None).cast(
                    T.ArrayType(T.StringType())
                ),
            ).otherwise(
                F.transform(
                    ordered_pairs,
                    lambda pair: pair["employee_id"],
                )
            ),
        )

        plate_events_df = reconstructed_df.select(
            "timestamp",

            F.col("Zone")
            .cast("string")
            .alias("Zone"),

            F.col("camera_id")
            .cast("string")
            .alias("camera_id"),

            F.col("event_id")
            .cast("string")
            .alias("event_id"),

            F.col("detector_type")
            .cast("string")
            .alias("detector_type"),

            F.lit("plate").alias("detection_source"),

            "detected_employee_ids",
            "detected_plates",

            F.lit(None).cast(
                T.ArrayType(T.BooleanType())
            ).alias("authorized"),
        )

        logger.info("Plate event transformation completed.")

        return plate_events_df

    # ============================================================
    # 11. UNIFIED MONITORING EVENTS
    # ============================================================

    def transform_monitoring_events(self) -> DataFrame:
        """
        Combine person and plate frame-level detections.

        Person and plate observations remain distinguishable through
        detection_source. A plate/employee association is not treated as
        proof that the employee was physically present.
        """

        logger.info(
            "Starting unified monitoring event transformation."
        )

        person_df = self._read_silver_table("person_detections")

        self._require_columns(
            person_df,
            [
                "Time_stamp",
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",
                "detected_persons_id",
                "authorized",
            ],
            "Silver person detections",
            missing_message="Missing person columns",
        )

        person_df = self._normalize_timestamp(
            person_df,
            context="Person detections",
        )

        person_df = self._normalize_array(
            person_df,
            "detected_persons_id",
            T.StringType(),
            "Person detections",
        )

        person_df = self._normalize_array(
            person_df,
            "authorized",
            T.BooleanType(),
            "Person detections",
        )

        # Keep the employee ID and authorization arrays positionally aligned.
        person_ids = F.col("detected_persons_id")
        authorization = F.col("authorized")

        alignment_error = (
            (
                person_ids.isNull()
                & authorization.isNotNull()
            )
            |
            (
                person_ids.isNotNull()
                & authorization.isNull()
            )
            |
            (
                person_ids.isNotNull()
                & authorization.isNotNull()
                & (
                    F.size(person_ids)
                    != F.size(authorization)
                )
            )
        )

        self._assert_no_rows(
            person_df,
            alignment_error,
            "Person and authorization arrays are not aligned.",
        )

        person_events_df = person_df.select(
            "timestamp",

            F.col("Zone")
            .cast("string")
            .alias("Zone"),

            F.col("camera_id")
            .cast("string")
            .alias("camera_id"),

            F.col("event_id")
            .cast("string")
            .alias("event_id"),

            F.col("detector_type")
            .cast("string")
            .alias("detector_type"),

            F.lit("person").alias("detection_source"),

            F.col("detected_persons_id").alias(
                "detected_employee_ids"
            ),

            F.lit(None).cast(
                T.ArrayType(T.StringType())
            ).alias("detected_plates"),

            F.col("authorized"),
        )

        plate_events_df = self._transform_plate_events()

        monitoring_events_df = person_events_df.unionByName(
            plate_events_df
        )

        self._write_gold_table(
            monitoring_events_df,
            "monitoring_events",
        )

        logger.info("Unified monitoring events completed.")

        return monitoring_events_df

    # ============================================================
    # 12. DETECTION-LEVEL DETAILS
    # ============================================================

    def transform_monitoring_detection_details(self) -> DataFrame:
        """
        Combine individual person and plate detections.

        Each output row represents one person detection or one plate
        detection. Unknown plate-to-employee matches remain null.
        """

        logger.info(
            "Starting monitoring detection details transformation."
        )

        # --------------------------------------------------------
        # PERSON DETECTION DETAILS
        # --------------------------------------------------------

        person_df = self._read_silver_table(
            "person_detection_details"
        )

        self._require_columns(
            person_df,
            [
                "Time_stamp",
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",
                "employee_id",
                "authorized",
            ],
            "Silver person detection details",
            missing_message="Missing person detail columns",
        )

        person_df = self._normalize_timestamp(
            person_df,
            context="Person detection details",
        )

        person_details_df = person_df.select(
            "timestamp",

            F.col("Zone")
            .cast("string")
            .alias("Zone"),

            F.col("camera_id")
            .cast("string")
            .alias("camera_id"),

            F.col("event_id")
            .cast("string")
            .alias("event_id"),

            F.col("detector_type")
            .cast("string")
            .alias("detector_type"),

            F.lit("person").alias("detection_source"),

            F.col("employee_id")
            .cast("string")
            .alias("employee_id"),

            F.lit(None).cast("string").alias("plate_number"),

            F.col("authorized")
            .cast("boolean")
            .alias("authorized"),
        )

        # --------------------------------------------------------
        # PLATE DETECTION DETAILS
        # --------------------------------------------------------

        plate_events_df = self._transform_plate_events()

        alignment_error = ~F.size(
            F.col("detected_plates")
        ).eqNullSafe(
            F.size(F.col("detected_employee_ids"))
        )

        self._assert_no_rows(
            plate_events_df,
            alignment_error,
            "Plate and employee ID arrays are not aligned.",
        )

        # explode (not explode_outer) intentionally creates no detail
        # rows for an empty or null plate array.
        plate_details_df = (
            plate_events_df
            .withColumn(
                "_plate_detail",
                F.explode(
                    F.arrays_zip(
                        F.col("detected_plates"),
                        F.col("detected_employee_ids"),
                    )
                ),
            )
            .select(
                "timestamp",
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",

                F.lit("plate").alias("detection_source"),

                F.col(
                    "_plate_detail.detected_employee_ids"
                ).cast("string").alias("employee_id"),

                F.col(
                    "_plate_detail.detected_plates"
                ).cast("string").alias("plate_number"),

                F.lit(None).cast("boolean").alias("authorized"),
            )
        )

        # --------------------------------------------------------
        # UNION PERSON AND PLATE DETAILS
        # --------------------------------------------------------

        monitoring_detection_details_df = (
            person_details_df
            .unionByName(plate_details_df)
        )

        self._write_gold_table(
            monitoring_detection_details_df,
            "monitoring_detection_details",
        )

        logger.info(
            "Monitoring detection details completed."
        )

        return monitoring_detection_details_df

    # ============================================================
    # 13. RESTRICTED ACCESS
    # ============================================================

    def transform_restricted_access(self) -> DataFrame:
        """Validate and persist restricted-access reference data."""

        logger.info("Starting restricted access transformation.")

        restricted_df = self._read_silver_table(
            "restricted_access"
        )

        self._require_columns(
            restricted_df,
            ["employee_id", "zone_id"],
            "Silver restricted access",
            missing_message="Missing restricted access columns",
        )

        restricted_df = (
            restricted_df
            .withColumn(
                "employee_id",
                F.trim(
                    F.col("employee_id").cast("string")
                ),
            )
            .withColumn(
                "zone_id",
                F.trim(
                    F.col("zone_id").cast("string")
                ),
            )
        )

        self._assert_no_rows(
            restricted_df,
            F.col("employee_id").isNull()
            | (F.col("employee_id") == "")
            | F.col("zone_id").isNull()
            | (F.col("zone_id") == ""),
            (
                "Restricted access contains missing "
                "employee or zone IDs."
            ),
        )

        self._write_gold_table(
            restricted_df,
            "restricted_access",
        )

        logger.info("Restricted access transformation completed.")

        return restricted_df

    # ============================================================
    # 14. COMPLETE GOLD PIPELINE
    # ============================================================

    def run(self) -> None:
        """Execute all Gold transformations in dependency order."""

        logger.info("=" * 60)
        logger.info("STARTING GOLD TRANSFORMATION")
        logger.info("=" * 60)

        self._ensure_gold_database()

        logger.info("[1/4] Transforming employee directory.")
        self.transform_employee_directory()

        logger.info("[2/4] Transforming monitoring events.")
        self.transform_monitoring_events()

        logger.info(
            "[3/4] Transforming monitoring detection details."
        )
        self.transform_monitoring_detection_details()

        logger.info("[4/4] Transforming restricted access.")
        self.transform_restricted_access()

        logger.info("=" * 60)
        logger.info("GOLD TRANSFORMATION COMPLETED")
        logger.info("=" * 60)
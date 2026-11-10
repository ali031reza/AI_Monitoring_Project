"""Employee monitoring analytics backed by Gold monitoring tables.

The service distinguishes person observations from plate-to-employee
associations. A plate association is not evidence of physical presence.
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class PersonMonitoringService:
    """Provide employee history, directory, and detection summaries."""

    # ============================================================
    # 1. INITIALIZATION
    # ============================================================

    def __init__(
        self,
        spark: SparkSession,
        gold_database: str = "gold",
    ) -> None:
        self.spark = spark
        self.gold_database = gold_database
        self.monitoring_details_table = (
            f"{self.gold_database}.monitoring_detection_details"
        )

    # ============================================================
    # 2. EMPLOYEE HISTORY
    # ============================================================

    def get_employee_history(
        self,
        employee_id: str,
        start_time: str = None,
        end_time: str = None,
        camera_id: str = None,
        zone: str = None,
        detection_source: str = None,
        limit: int = 50,
    ) -> DataFrame:
        """Retrieve an employee's latest monitoring observations.

        Optionally filter by a half-open time window, camera, zone,
        and detection source. Select the latest ``limit`` records,
        then return that selection in chronological order.

        Plate records identify an association with a registered plate;
        they do not establish that the employee was observed in person.
        """

        # --------------------------------------------------------
        # 2.1. VALIDATE INPUTS
        # --------------------------------------------------------

        if not isinstance(employee_id, str) or not employee_id.strip():
            raise ValueError("A valid employee_id is required.")

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        if detection_source is not None:
            if not isinstance(detection_source, str):
                raise TypeError("detection_source must be a string.")

            detection_source = detection_source.strip().lower()
            if detection_source not in ("person", "plate"):
                raise ValueError(
                    "detection_source must be 'person' or 'plate'."
                )

        for name, value in (
            ("camera_id", camera_id),
            ("zone", zone),
        ):
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{name} must be a non-empty string.")

        start_datetime = None
        end_datetime = None

        for name, value in (
            ("start_time", start_time),
            ("end_time", end_time),
        ):
            if value is not None:
                if not isinstance(value, str):
                    raise TypeError(f"{name} must be a string.")

                try:
                    parsed_time = datetime.strptime(
                        value, "%Y-%m-%d %H:%M:%S"
                    )
                except ValueError as exc:
                    raise ValueError(
                        f"{name} must follow YYYY-MM-DD HH:MM:SS."
                    ) from exc

                if name == "start_time":
                    start_datetime = parsed_time
                else:
                    end_datetime = parsed_time

        if (
            start_datetime is not None
            and end_datetime is not None
            and start_datetime >= end_datetime
        ):
            raise ValueError("start_time must be earlier than end_time.")

        # --------------------------------------------------------
        # 2.2. LOAD AND VALIDATE GOLD TABLE
        # --------------------------------------------------------

        monitoring_df = self.spark.table(self.monitoring_details_table)

        required_columns = [
            "timestamp",
            "employee_id",
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
            "detection_source",
            "plate_number",
            "authorized",
        ]

        missing_columns = set(required_columns) - set(monitoring_df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # --------------------------------------------------------
        # 2.3. APPLY FILTERS
        # --------------------------------------------------------

        history_df = monitoring_df.filter(
            F.col("employee_id") == employee_id.strip()
        )

        if start_datetime is not None:
            history_df = history_df.filter(
                F.col("timestamp") >= F.lit(start_datetime)
            )

        if end_datetime is not None:
            history_df = history_df.filter(
                F.col("timestamp") < F.lit(end_datetime)
            )

        if camera_id is not None:
            history_df = history_df.filter(
                F.col("camera_id") == camera_id.strip()
            )

        if zone is not None:
            history_df = history_df.filter(
                F.col("Zone") == zone.strip()
            )

        if detection_source is not None:
            history_df = history_df.filter(
                F.col("detection_source") == detection_source
            )

        # --------------------------------------------------------
        # 2.4. SELECT LATEST N, THEN DISPLAY CHRONOLOGICALLY
        # --------------------------------------------------------

        latest_df = (
            history_df
            .select(*required_columns)
            .orderBy(
                F.col("timestamp").desc_nulls_last(),
                F.col("event_id").desc_nulls_last(),
                F.col("camera_id").desc_nulls_last(),
            )
            .limit(limit)
        )

        return latest_df.orderBy(
            F.col("timestamp").asc_nulls_last(),
            F.col("event_id").asc_nulls_last(),
            F.col("camera_id").asc_nulls_last(),
        )

    # ============================================================
    # 3. EMPLOYEE DIRECTORY
    # ============================================================

    def get_employee_directory(
        self,
        search_term: str = None,
        limit: int = 100,
    ) -> DataFrame:
        """Return the employee directory, optionally filtered by ID or plate."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        if search_term is not None:
            if not isinstance(search_term, str):
                raise TypeError("Search term must be a string.")

            search_term = search_term.strip()
            if not search_term:
                raise ValueError("Search term cannot be empty.")

        employee_df = self.spark.table(f"{self.gold_database}.employee_directory")

        required_columns = ["employee_id", "plate_number"]
        missing_columns = set(required_columns) - set(employee_df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        employee_df = employee_df.select(*required_columns)

        if search_term is not None:
            search_value = search_term.lower()

            employee_id_match = F.lower(
                F.coalesce(F.trim(F.col("employee_id")), F.lit(""))
            ).contains(search_value)
            plate_match = F.lower(
                F.coalesce(F.trim(F.col("plate_number")), F.lit(""))
            ).contains(search_value)

            employee_df = employee_df.filter(employee_id_match | plate_match)

        return employee_df.orderBy(
            F.col("employee_id").asc_nulls_last()
        ).limit(limit)

    # ============================================================
    # 4. EMPLOYEE DETECTION SUMMARY
    # ============================================================

    def get_employee_detection_summary(
        self,
        employee_id: str,
    ) -> DataFrame:
        """Summarize person and associated-plate observations separately."""

        if not isinstance(employee_id, str) or not employee_id.strip():
            raise ValueError("A valid employee_id must be provided.")

        employee_id = employee_id.strip()
        monitoring_df = self.spark.table(self.monitoring_details_table)

        required_columns = [
            "employee_id",
            "timestamp",
            "camera_id",
            "Zone",
            "detection_source",
        ]
        missing_columns = set(required_columns) - set(monitoring_df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        employee_df = monitoring_df.filter(
            F.col("employee_id") == employee_id
        )

        person_condition = F.col("detection_source") == "person"
        plate_condition = F.col("detection_source") == "plate"

        summary_df = employee_df.agg(
            F.count(F.when(person_condition, 1)).alias(
                "person_detection_count"
            ),
            F.count(F.when(plate_condition, 1)).alias(
                "plate_detection_count"
            ),
            F.countDistinct(
                F.when(person_condition, F.col("camera_id"))
            ).alias("distinct_person_cameras"),
            F.countDistinct(
                F.when(person_condition, F.col("Zone"))
            ).alias("distinct_person_zones"),
            F.min(
                F.when(person_condition, F.col("timestamp"))
            ).alias("first_person_detection"),
            F.max(
                F.when(person_condition, F.col("timestamp"))
            ).alias("last_person_detection"),
            F.min(
                F.when(plate_condition, F.col("timestamp"))
            ).alias("first_plate_detection"),
            F.max(
                F.when(plate_condition, F.col("timestamp"))
            ).alias("last_plate_detection"),
        )

        return summary_df.withColumn("employee_id", F.lit(employee_id))

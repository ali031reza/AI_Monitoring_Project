from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime



class PersonMonitoringService:

    def __init__(
        self,
        spark: SparkSession,
        gold_database: str = "gold"
    ):

        self.spark = spark
        self.gold_database = gold_database

        self.monitoring_details_table = (
            f"{self.gold_database}."
            "monitoring_detection_details"
        )

    def get_employee_history(
        self,
        employee_id: str,
        start_time: str = None,
        end_time: str = None,
        camera_id: str = None,
        zone: str = None,
        detection_source: str = None,
        limit: int = 50
    ) -> DataFrame:

        """
        Retrieve an employee's monitoring history
        with optional filtering.

        Supported filters:
        - Time range
        - Camera
        - Zone
        - Detection source
        """

        # ==========================================
        # 1. VALIDATE INPUTS
        # ==========================================

        if not isinstance(employee_id, str) or not employee_id.strip():
            raise ValueError("A valid employee_id is required.")

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        if detection_source is not None:

            if not isinstance(detection_source, str):
                raise TypeError(
                    "detection_source must be a string."
                )

            detection_source = detection_source.strip().lower()

            if detection_source not in ["person", "plate"]:
                raise ValueError(
                    "detection_source must be 'person' or 'plate'."
                )

        # Validate optional string filters
        for name, value in [
            ("camera_id", camera_id),
            ("zone", zone)
        ]:

            if value is not None:

                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"{name} must be a non-empty string."
                    )

        # Validate timestamps
        start_datetime = None
        end_datetime = None

        for name, value in [
            ("start_time", start_time),
            ("end_time", end_time)
        ]:

            if value is not None:

                if not isinstance(value, str):
                    raise TypeError(
                        f"{name} must be a string."
                    )

                try:
                    parsed_time = datetime.strptime(
                        value,
                        "%Y-%m-%d %H:%M:%S"
                    )

                except ValueError:
                    raise ValueError(
                        f"{name} must follow YYYY-MM-DD HH:MM:SS."
                    )

                if name == "start_time":
                    start_datetime = parsed_time
                else:
                    end_datetime = parsed_time

        if (
            start_datetime is not None
            and end_datetime is not None
            and start_datetime >= end_datetime
        ):
            raise ValueError(
                "start_time must be earlier than end_time."
            )

        # ==========================================
        # 2. LOAD GOLD TABLE
        # ==========================================

        monitoring_df = self.spark.table(
            self.monitoring_details_table
        )

        required_columns = [
            "timestamp",
            "employee_id",
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
            "detection_source",
            "plate_number",
            "authorized"
        ]

        missing_columns = (
            set(required_columns) - set(monitoring_df.columns)
        )

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # ==========================================
        # 3. FILTER EMPLOYEE
        # ==========================================

        history_df = monitoring_df.filter(
            F.col("employee_id") == employee_id.strip()
        )

        # ==========================================
        # 4. APPLY OPTIONAL FILTERS
        # ==========================================

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

        # ==========================================
        # 5. SELECT AND SORT RESULTS
        # ==========================================

        history_df = (
            history_df
            .select(*required_columns)
            .orderBy(
                F.col("timestamp").asc_nulls_last(),
                F.col("event_id").asc_nulls_last(),
                F.col("camera_id").asc_nulls_last()
            )
            .limit(limit)
        )

        return history_df
    def get_employee_directory(
        self,
        search_term: str = None,
        limit: int = 100
    ) -> DataFrame:
        """
        Retrieve the employee directory.

        Supports searching by employee ID or registered
        license plate number.
        """

        # Validate limit
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError(
                "Limit must be a positive integer."
            )

        # Validate search term
        if search_term is not None:

            if not isinstance(search_term, str):
                raise TypeError(
                    "Search term must be a string."
                )

            search_term = search_term.strip()

            if not search_term:
                raise ValueError(
                    "Search term cannot be empty."
                )

        # Read Gold employee directory
        employee_df = self.spark.table(
            f"{self.gold_database}.employee_directory"
        )

        # Validate schema
        required_columns = [
            "employee_id",
            "plate_number"
        ]

        missing_columns = (
            set(required_columns) - set(employee_df.columns)
        )

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # Select required columns
        employee_df = employee_df.select(
            *required_columns
        )

        # Apply search if provided
        if search_term is not None:

            search_value = search_term.lower()

            employee_df = employee_df.filter(
                F.lower(
                    F.coalesce(
                        F.trim(F.col("employee_id")),
                        F.lit("")
                    )
                ).contains(search_value)
                |
                F.lower(
                    F.coalesce(
                        F.trim(F.col("plate_number")),
                        F.lit("")
                    )
                ).contains(search_value)
            )

        # Sort and limit results
        employee_df = (
            employee_df
            .orderBy(F.col("employee_id").asc_nulls_last())
            .limit(limit)
        )

        return employee_df

    def get_employee_detection_summary(
        self,
        employee_id: str
    ) -> DataFrame:
        """
        Retrieve monitoring statistics for an employee.

        Person detections and plate detections are
        calculated separately.
        """

        # Validate employee ID
        if not isinstance(employee_id, str) or not employee_id.strip():
            raise ValueError(
                "A valid employee_id must be provided."
            )

        employee_id = employee_id.strip()

        # Read Gold table
        monitoring_df = self.spark.table(
            self.monitoring_details_table
        )

        # Validate required columns
        required_columns = [
            "employee_id",
            "timestamp",
            "camera_id",
            "Zone",
            "detection_source"
        ]

        missing_columns = (
            set(required_columns) - set(monitoring_df.columns)
        )

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # Filter employee observations
        employee_df = monitoring_df.filter(
            F.col("employee_id") == employee_id
        )

        # Define detection types
        person_condition = (
            F.col("detection_source") == "person"
        )

        plate_condition = (
            F.col("detection_source") == "plate"
        )

        # Calculate statistics
        summary_df = employee_df.agg(

            F.count(
                F.when(person_condition, 1)
            ).alias("person_detection_count"),

            F.count(
                F.when(plate_condition, 1)
            ).alias("plate_detection_count"),

            F.countDistinct(
                F.when(
                    person_condition,
                    F.col("camera_id")
                )
            ).alias("distinct_person_cameras"),

            F.countDistinct(
                F.when(
                    person_condition,
                    F.col("Zone")
                )
            ).alias("distinct_person_zones"),

            F.min(
                F.when(
                    person_condition,
                    F.col("timestamp")
                )
            ).alias("first_person_detection"),

            F.max(
                F.when(
                    person_condition,
                    F.col("timestamp")
                )
            ).alias("last_person_detection"),

            F.min(
                F.when(
                    plate_condition,
                    F.col("timestamp")
                )
            ).alias("first_plate_detection"),

            F.max(
                F.when(
                    plate_condition,
                    F.col("timestamp")
                )
            ).alias("last_plate_detection")
        )

        return summary_df.withColumn(
            "employee_id",
            F.lit(employee_id)
        )
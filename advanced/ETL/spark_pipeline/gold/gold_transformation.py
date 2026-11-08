from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

class GoldTransformation:

    def __init__(
        self,
        spark: SparkSession,
        source_database: str = "silver",
        target_database: str = "gold"
    ):

        self.spark = spark

        self.source_database = source_database
        self.target_database = target_database


    def _read_silver_table(
        self,
        table_name: str
    ) -> DataFrame:
        """
        Read a table from the Silver database.
        """

        if not table_name.isidentifier():
            raise ValueError(
                f"Invalid Silver table name: {table_name}"
            )

        full_table_name = (
            f"{self.source_database}.{table_name}"
        )

        if not self.spark.catalog.tableExists(
            full_table_name
        ):
            raise ValueError(
                f"Silver table does not exist: "
                f"{full_table_name}"
            )

        return self.spark.table(full_table_name)


    def _write_gold_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Write a DataFrame into the Gold database using Parquet.
        """

        if not table_name.isidentifier():
            raise ValueError(
                f"Invalid Gold table name: {table_name}"
            )

        if not self.spark.catalog.databaseExists(
            self.target_database
        ):
            raise ValueError(
                f"Gold database does not exist: "
                f"{self.target_database}"
            )

        full_table_name = (
            f"{self.target_database}.{table_name}"
        )

        (
            df.write
            .format("parquet")
            .mode(mode)
            .saveAsTable(full_table_name)
        )

        print(
            f"Successfully written: {full_table_name}"
        )

    def transform_employee_directory(self) -> DataFrame:
        """
        Prepare and write the Gold employee directory.
        """

        # Read Silver employees
        employees_df = self._read_silver_table(
            "employees"
        )

        # Validate required columns
        required_columns = [
            "employee_id",
            "plate_number"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in employees_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing employee columns: {missing_columns}"
            )

        # Select required columns
        employee_directory_df = employees_df.select(
            "employee_id",
            "plate_number"
        )

        # Validate employee IDs
        invalid_ids = (
            employee_directory_df
            .filter(
                F.col("employee_id").isNull() |
                (F.trim(F.col("employee_id")) == "")
            )
            .limit(1)
            .count()
        )

        if invalid_ids:
            raise ValueError(
                "Employee directory contains invalid employee IDs."
            )

        # Validate employee ID uniqueness
        duplicate_ids = (
            employee_directory_df
            .groupBy("employee_id")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate_ids:
            raise ValueError(
                "Duplicate employee IDs found."
            )

        # Validate registered plate uniqueness
        duplicate_plates = (
            employee_directory_df
            .filter(
                F.col("plate_number").isNotNull() &
                (F.trim(F.col("plate_number")) != "")
            )
            .groupBy(
                F.upper(
                    F.trim(F.col("plate_number"))
                )
            )
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate_plates:
            raise ValueError(
                "Duplicate registered license plates found."
            )

        # Write Gold table
        self._write_gold_table(
            df=employee_directory_df,
            table_name="employee_directory"
        )

        return employee_directory_df

    def _build_employee_plate_map(self) -> DataFrame:
        """
        Build a mapping between registered license plates
        and employee IDs.
        """

        employees_df = self._read_silver_table("employees")

        required_columns = [
            "employee_id",
            "plate_number"
        ]

        if not all(
            column in employees_df.columns
            for column in required_columns
        ):
            raise ValueError(
                "Employee table is missing required columns."
            )

        # Prepare normalized lookup
        employee_lookup_df = (
            employees_df

            .filter(
                F.col("employee_id").isNotNull() &
                F.col("plate_number").isNotNull() &
                (F.trim(F.col("plate_number")) != "")
            )

            .select(
                F.upper(
                    F.trim(F.col("plate_number"))
                ).alias("_plate_key"),

                F.col("employee_id").cast("string")
            )
        )

        # Validate plate uniqueness
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

        # Create a Spark map
        employee_plate_map_df = (
            employee_lookup_df
            .agg(
                F.map_from_entries(
                    F.collect_list(
                        F.struct(
                            F.col("_plate_key"),
                            F.col("employee_id")
                        )
                    )
                ).alias("_plate_to_employee")
            )
        )

        return employee_plate_map_df


    def _transform_plate_events(self) -> DataFrame:
        """
        Transform frame-level plate detections and
        map detected plates to registered employees.
        """

        # Read Silver plate detections
        plate_detections_df = self._read_silver_table(
            "plate_detections"
        )

        required_columns = [
            "Time_stamp",
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
            "detected_plates"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in plate_detections_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing plate columns: {missing_columns}"
            )

        # Parse detected plates
        plate_type = plate_detections_df.schema[
            "detected_plates"
        ].dataType

        if isinstance(plate_type, T.StringType):

            plate_detections_df = (
                plate_detections_df
                .withColumn(
                    "detected_plates",
                    F.from_json(
                        F.col("detected_plates"),
                        T.ArrayType(T.StringType())
                    )
                )
            )

        elif isinstance(plate_type, T.ArrayType):

            plate_detections_df = (
                plate_detections_df
                .withColumn(
                    "detected_plates",
                    F.col("detected_plates").cast(
                        T.ArrayType(T.StringType())
                    )
                )
            )

        else:
            raise TypeError(
                "Unsupported detected_plates data type."
            )

        # Build employee lookup
        employee_plate_map_df = (
            self._build_employee_plate_map()
        )

        # Map detected plates to employee IDs
        plate_events_df = (
            plate_detections_df

            .crossJoin(employee_plate_map_df)

            .withColumn(
                "detected_employee_ids",
                F.transform(
                    F.col("detected_plates"),

                    lambda plate: F.element_at(
                        F.col("_plate_to_employee"),
                        F.upper(F.trim(plate))
                    )
                )
            )

            .drop("_plate_to_employee")
        )

        # Standardize the frame-level schema
        plate_events_df = plate_events_df.select(

            F.to_timestamp(
                F.col("Time_stamp")
            ).alias("timestamp"),

            F.col("Zone").cast("string").alias("Zone"),

            F.col("camera_id").cast("string").alias("camera_id"),

            F.col("event_id").cast("string").alias("event_id"),

            F.col("detector_type").cast("string").alias(
                "detector_type"
            ),

            F.lit("plate").alias("detection_source"),

            F.col("detected_employee_ids"),

            F.col("detected_plates"),

            F.lit(None).cast(
                T.ArrayType(T.BooleanType())
            ).alias("authorized")
        )

        return plate_events_df

    def transform_monitoring_events(self) -> DataFrame:
        """
        Combine person and plate detector events
        into a unified frame-level monitoring table.
        """

        # Read Silver person detections
        person_detections_df = self._read_silver_table(
            "person_detections"
        )

        required_columns = [
            "Time_stamp",
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
            "detected_persons_id",
            "authorized"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in person_detections_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing person columns: {missing_columns}"
            )

        # Standardize person detection schema
        person_events_df = person_detections_df.select(

            F.to_timestamp(
                F.col("Time_stamp")
            ).alias("timestamp"),

            F.col("Zone").cast("string").alias("Zone"),

            F.col("camera_id").cast("string").alias("camera_id"),

            F.col("event_id").cast("string").alias("event_id"),

            F.col("detector_type").cast("string").alias(
                "detector_type"
            ),

            F.lit("person").alias("detection_source"),

            F.col("detected_persons_id").cast(
                T.ArrayType(T.StringType())
            ).alias("detected_employee_ids"),

            F.lit(None).cast(
                T.ArrayType(T.StringType())
            ).alias("detected_plates"),

            F.col("authorized").cast(
                T.ArrayType(T.BooleanType())
            ).alias("authorized")
        )

        # Transform plate events
        plate_events_df = self._transform_plate_events()

        # Union both detector datasets
        monitoring_events_df = (
            person_events_df
            .unionByName(plate_events_df)
        )

        # Write Gold table
        self._write_gold_table(
            df=monitoring_events_df,
            table_name="monitoring_events"
        )

        return monitoring_events_df

    def transform_monitoring_detection_details(self) -> DataFrame:
        """
        Combine individual person and plate detections
        into a unified detection-level Gold table.
        """

        # ==========================================
        # 1. READ PERSON DETECTION DETAILS
        # ==========================================

        person_details_df = self._read_silver_table(
            "person_detection_details"
        )

        required_columns = [
            "Time_stamp",
            "Zone",
            "camera_id",
            "event_id",
            "detector_type",
            "employee_id",
            "authorized"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in person_details_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing person detail columns: {missing_columns}"
            )

        # ==========================================
        # 2. STANDARDIZE PERSON DETAILS
        # ==========================================

        person_details_normalized_df = (
            person_details_df.select(

                F.to_timestamp(
                    F.col("Time_stamp")
                ).alias("timestamp"),

                F.col("Zone").cast("string").alias("Zone"),

                F.col("camera_id").cast("string").alias(
                    "camera_id"
                ),

                F.col("event_id").cast("string").alias(
                    "event_id"
                ),

                F.col("detector_type").cast("string").alias(
                    "detector_type"
                ),

                F.lit("person").alias("detection_source"),

                F.col("employee_id").cast("string").alias(
                    "employee_id"
                ),

                F.lit(None).cast("string").alias(
                    "plate_number"
                ),

                F.col("authorized").cast("boolean").alias(
                    "authorized"
                )
            )
        )

        # ==========================================
        # 3. TRANSFORM PLATE EVENTS
        # ==========================================

        plate_events_df = self._transform_plate_events()

        # ==========================================
        # 4. VALIDATE PLATE-EMPLOYEE ALIGNMENT
        # ==========================================

        alignment_errors = (
            plate_events_df
            .filter(
                ~F.size("detected_plates").eqNullSafe(
                    F.size("detected_employee_ids")
                )
            )
            .limit(1)
            .count()
        )

        if alignment_errors:
            raise ValueError(
                "Plate and employee ID arrays are not aligned."
            )

        # ==========================================
        # 5. EXPLODE PLATE ARRAYS
        # ==========================================

        plate_details_df = (
            plate_events_df

            .withColumn(
                "_plate_detail",
                F.explode(
                    F.arrays_zip(
                        F.col("detected_plates"),
                        F.col("detected_employee_ids")
                    )
                )
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

                F.lit(None).cast("boolean").alias(
                    "authorized"
                )
            )
        )

        # ==========================================
        # 6. UNION PERSON AND PLATE DETAILS
        # ==========================================

        monitoring_detection_details_df = (
            person_details_normalized_df
            .unionByName(plate_details_df)
        )

        # ==========================================
        # 7. WRITE GOLD TABLE
        # ==========================================

        self._write_gold_table(
            df=monitoring_detection_details_df,
            table_name="monitoring_detection_details"
        )

        return monitoring_detection_details_df

    def transform_restricted_access(self) -> DataFrame:
        """
        Transfer restricted access reference data
        from Silver to Gold.
        """

        # Read Silver table
        restricted_access_df = self._read_silver_table(
            "restricted_access"
        )

        # Write Gold table
        self._write_gold_table(
            df=restricted_access_df,
            table_name="restricted_access"
        )

        return restricted_access_df

    def run(self) -> None:
        """
        Execute the complete Gold transformation pipeline.
        """

        print("=" * 60)
        print("STARTING GOLD TRANSFORMATION")
        print("=" * 60)

        # Employee reference data
        print("\n[1/4] Transforming employee directory...")

        self.transform_employee_directory()

        # Unified monitoring events
        print("\n[2/4] Transforming monitoring events...")

        self.transform_monitoring_events()

        # Individual detection records
        print("\n[3/4] Transforming monitoring detection details...")

        self.transform_monitoring_detection_details()

        # Security reference data
        print("\n[4/4] Transforming restricted access...")

        self.transform_restricted_access()

        print("\n" + "=" * 60)
        print("GOLD TRANSFORMATION COMPLETED")
        print("=" * 60)
        
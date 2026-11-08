from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import re

class SilverTransformation:

    def __init__(self, spark: SparkSession):

        self.spark = spark

        self.source_database = "bronze"
        self.target_database = "silver"

        # JSON schemas
        self.person_ids_schema = T.ArrayType(
            T.StringType()
        )

        self.authorization_schema = T.ArrayType(
            T.BooleanType()
        )
    def _read_bronze_table(
        self,
        table_name: str
    ) -> DataFrame:
        """
        Read a table from the Bronze database.
        """

        if not table_name.isidentifier():
            raise ValueError(
                f"Invalid Bronze table name: {table_name}"
            )

        full_table_name = (
            f"{self.source_database}.{table_name}"
        )

        if not self.spark.catalog.tableExists(full_table_name):
            raise ValueError(
                f"Bronze table does not exist: {full_table_name}"
            )

        return self.spark.table(full_table_name)
    def _write_silver_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Write a DataFrame into the Silver database using Parquet.
        """

        if not table_name.isidentifier():
            raise ValueError(
                f"Invalid Silver table name: {table_name}"
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
    def transform_person_detections(self) -> DataFrame:
        """
        Read all person worker tables, correct their schemas,
        combine them, and write the unified Silver table.
        """

        # Discover person worker tables dynamically
        worker_tables = [
            table.name
            for table in self.spark.catalog.listTables(
                self.source_database
            )
            if re.fullmatch(
                r"person_detections_worker\d+",
                table.name
            )
        ]

        worker_tables.sort(
            key=lambda name: int(
                re.search(r"worker(\d+)$", name).group(1)
            )
        )

        if not worker_tables:
            raise ValueError(
                "No person worker tables found in Bronze."
            )

        person_dataframes = []

        # Read and transform each worker
        for table_name in worker_tables:

            print(f"Processing {table_name}...")

            df = self._read_bronze_table(table_name)

            # Correct timestamp schema
            df = df.withColumn(
                "Time_stamp",
                F.to_timestamp("Time_stamp")
            )

            # Parse detected employee IDs
            df = df.withColumn(
                "detected_persons_id",
                F.from_json(
                    F.col("detected_persons_id"),
                    self.person_ids_schema
                )
            )

            # Parse authorization flags
            df = df.withColumn(
                "authorized",
                F.from_json(
                    F.col("authorized"),
                    self.authorization_schema
                )
            )

            person_dataframes.append(df)

        # Combine all workers
        person_detections_df = person_dataframes[0]

        for df in person_dataframes[1:]:
            person_detections_df = (
                person_detections_df.unionByName(df)
            )

        # Write Silver table
        self._write_silver_table(
            df=person_detections_df,
            table_name="person_detections"
        )

        print(
            f"Successfully combined {len(worker_tables)} workers."
        )

        return person_detections_df

    def transform_person_detection_details(self) -> DataFrame:
        """
        Transform frame-level detections into person-level records.
        """

        # Read unified person detections
        person_detections_df = self.spark.table(
            "silver.person_detections"
        )

        # Pair employee IDs with authorization statuses
        person_detection_details_df = (
            person_detections_df

            .withColumn(
                "_person_details",
                F.explode(
                    F.arrays_zip(
                        F.col("detected_persons_id"),
                        F.col("authorized")
                    )
                )
            )

            .select(
                "Time_stamp",
                "Zone",
                "camera_id",
                "event_id",
                "detector_type",

                F.col(
                    "_person_details.detected_persons_id"
                ).alias("employee_id"),

                F.col(
                    "_person_details.authorized"
                ).alias("authorized")
            )
        )

        # Write Silver table
        self._write_silver_table(
            df=person_detection_details_df,
            table_name="person_detection_details"
        )

        return person_detection_details_df

    def transform_plate_detections(self) -> DataFrame:
        """
        Convert plate detection timestamps and write the Silver table.
        """

        # Read Bronze table
        plate_detections_df = self._read_bronze_table(
            "plate_detections"
        )

        # Correct timestamp type
        plate_detections_silver_df = (
            plate_detections_df
            .withColumn(
                "Time_stamp",
                F.to_timestamp("Time_stamp")
            )
        )

        # Write Silver table
        self._write_silver_table(
            df=plate_detections_silver_df,
            table_name="plate_detections"
        )

        return plate_detections_silver_df

    def transform_employees(self) -> DataFrame:
        """
        Clean employee reference data and write the Silver table.
        """

        # Read Bronze table
        employees_df = self._read_bronze_table("employees")

        # Replace NA with NULL
        employees_silver_df = (
            employees_df
            .withColumn(
                "plate_number",
                F.when(
                    F.upper(F.trim(F.col("plate_number"))) == "NA",
                    F.lit(None).cast("string")
                ).otherwise(
                    F.trim(F.col("plate_number"))
                )
            )
        )

        # Write Silver table
        self._write_silver_table(
            df=employees_silver_df,
            table_name="employees"
        )

        return employees_silver_df

    def transform_cameras(self) -> DataFrame:
        """
        Transfer camera reference data from Bronze to Silver.
        """

        # Read Bronze table
        cameras_df = self._read_bronze_table("cameras")

        # Write Silver table
        self._write_silver_table(
            df=cameras_df,
            table_name="cameras"
        )

        return cameras_df

    def transform_restricted_access(self) -> DataFrame:
        """
        Transfer restricted access data from Bronze to Silver.
        """

        # Read Bronze table
        restricted_access_df = self._read_bronze_table(
            "restricted_access"
        )

        # Write Silver table
        self._write_silver_table(
            df=restricted_access_df,
            table_name="restricted_access"
        )

        return restricted_access_df


    def run(self) -> None:
        """
        Execute the complete Silver transformation pipeline.
        """

        print("=" * 60)
        print("STARTING SILVER TRANSFORMATION")
        print("=" * 60)

        # Person detections
        self.transform_person_detections()

        self.transform_person_detection_details()

        # Plate detections
        self.transform_plate_detections()

        # Reference tables
        self.transform_employees()

        self.transform_cameras()

        self.transform_restricted_access()

        print("=" * 60)
        print("SILVER TRANSFORMATION COMPLETED")
        print("=" * 60)

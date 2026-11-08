from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
import re

class BronzeIngestion:

    def __init__(
        self,
        spark: SparkSession,
        data_dir: Path,
        starter_sources_dir: Path
    ):

        self.spark = spark
        self.data_dir = Path(data_dir)
        self.starter_sources_dir = Path(starter_sources_dir)

        self.database = "bronze"

    def _read_csv(self, file_path: Path) -> DataFrame:
           
            """
            Read a CSV file without applying transformations.
            All columns are preserved as strings.
            """

            file_path = Path(file_path)

            if not file_path.exists():
                raise FileNotFoundError(
                    f"Source file not found: {file_path}"
                )

            df = (
                self.spark.read
                .format("csv")
                .option("header", True)
                .option("inferSchema", False)
                .option("quote", '"')
                .option("escape", '"')
                .option("mode", "FAILFAST")
                .load(str(file_path))
            )

            return df
    def _write_table (self,df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
           
            """
            Write a DataFrame to the Bronze database in Parquet format.
            """

            if not table_name or "." in table_name:
                raise ValueError(
                    "Provide a valid table name without a database prefix."
                )

            full_table_name = f"{self.database}.{table_name}"

            (
                df.write
                .format("parquet")
                .mode(mode)
                .saveAsTable(full_table_name)
            )

            print(
                f"Successfully written: {full_table_name}"
            )
    def ingest_person_workers(self) -> None:
           
            """
            Dynamically discover and ingest all person worker CSV files.

            Each worker is stored in an independent Bronze table.
            """

            worker_files = list(
                self.data_dir.glob("persons_detected_worker*.csv")
            )

            # Validate and sort worker files numerically
            valid_files = []

            for file_path in worker_files:

                match = re.fullmatch(
                    r"persons_detected_worker(\d+)\.csv",
                    file_path.name
                )

                if match:
                    worker_number = int(match.group(1))
                    valid_files.append((worker_number, file_path))

            valid_files.sort(key=lambda item: item[0])

            if not valid_files:
                raise FileNotFoundError(
                    "No person worker CSV files were found."
                )

            # Ingest each worker independently
            for worker_number, file_path in valid_files:

                table_name = (
                    f"person_detections_worker{worker_number}"
                )

                print(f"Ingesting Worker {worker_number}...")

                df = self._read_csv(file_path)

                self._write_table(
                    df=df,
                    table_name=table_name
                )

            print(
                f"Successfully ingested {len(valid_files)} workers."
            )
    def ingest_plate_detections(self) -> None:
        """
        Ingest plate detection records into Bronze.
        """

        file_path = self.data_dir / "plates_detected.csv"

        df = self._read_csv(file_path)

        self._write_table(
            df=df,
            table_name="plate_detections"
        )

    def ingest_employees(self) -> None:
        """
        Ingest employee reference data into Bronze.
        """

        file_path = self.starter_sources_dir / "employees.csv"

        df = self._read_csv(file_path)

        self._write_table(
            df=df,
            table_name="employees"
        )
    
    def ingest_cameras(self) -> None:
        """
        Ingest camera configuration data into Bronze.
        """

        file_path = self.starter_sources_dir / "cameras.csv"

        df = self._read_csv(file_path)

        self._write_table(
            df=df,
            table_name="cameras"
        )

    def ingest_restricted_access(self) -> None:
        """
        Ingest employee restricted-access permissions into Bronze.
        """

        file_path = (
            self.starter_sources_dir / "restricted_access.csv"
        )

        df = self._read_csv(file_path)

        self._write_table(
            df=df,
            table_name="restricted_access"
        )
    def run(self) -> None:
        """
        Execute the complete Bronze ingestion pipeline.
        """

        print("=" * 60)
        print("STARTING BRONZE INGESTION")
        print("=" * 60)

        self.ingest_person_workers()

        self.ingest_plate_detections()

        self.ingest_employees()

        self.ingest_cameras()

        self.ingest_restricted_access()

        print("=" * 60)
        print("BRONZE INGESTION COMPLETED")
        print("=" * 60)





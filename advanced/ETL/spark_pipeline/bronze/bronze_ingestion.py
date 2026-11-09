
"""
Bronze layer ingestion.

Responsibilities:
    - Discover person detection worker CSV files.
    - Validate all source files before ingestion.
    - Read raw CSV data without applying transformations.
    - Store source data as Parquet tables.
    - Manage the Bronze database.
"""

from __future__ import annotations

import csv
import logging
import re

from collections import Counter
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from advanced.config import ApplicationSettings


logger = logging.getLogger(__name__)


class BronzeIngestion:
    """
    Ingest raw application data into the Bronze layer.

    All source columns are preserved as strings.
    No business transformations are performed.
    """

    DATABASE = "bronze"

    WORKER_PATTERN = re.compile(
        r"persons_detected_worker([1-9]\d*)\.csv"
    )

    REQUIRED_COLUMNS = {
        "person": (
            "Time_stamp",
            "Zone",
            "detected_persons_id",
            "authorized",
            "camera_id",
            "event_id",
            "detector_type",
        ),

        "plate": (
            "Time_stamp",
            "Zone",
            "detected_plates",
            "camera_id",
            "event_id",
            "detector_type",
        ),

        "employees": (
            "employee_id",
            "plate_number",
        ),

        "cameras": (
            "camera_id",
            "zone_id",
            "detector_type",
            "enabled",
            "kafka_partition",
        ),

        "restricted_access": (
            "employee_id",
            "zone_id",
        ),
    }

    VALID_WRITE_MODES = {
        "overwrite",
        "append",
        "ignore",
        "error",
        "errorifexists",
    }

    # ========================================================
    # 1. INITIALIZATION
    # ========================================================

    def __init__(
        self,
        spark: SparkSession,
        settings: ApplicationSettings,
    ) -> None:

        if not isinstance(settings, ApplicationSettings):
            raise TypeError(
                "settings must be an ApplicationSettings instance"
            )

        self.spark = spark
        self.settings = settings

        self.data_dir = Path(
            settings.paths.data_dir
        )

        self.starter_sources_dir = Path(
            settings.paths.starter_sources_dir
        )

        self.database = self.DATABASE

        logger.info(
            "Bronze ingestion initialized | "
            "Data directory: %s | "
            "Starter sources: %s",
            self.data_dir,
            self.starter_sources_dir,
        )

    # ========================================================
    # 2. CSV VALIDATION
    # ========================================================

    @staticmethod
    def _validate_file(file_path: Path) -> None:
        """
        Validate that a source path points to an existing file.
        """

        if not file_path.exists():
            raise FileNotFoundError(
                f"Source file not found: {file_path}"
            )

        if not file_path.is_file():
            raise IsADirectoryError(
                f"Expected a file, received a directory: {file_path}"
            )

    @staticmethod
    def _read_header(file_path: Path) -> tuple[str, ...]:
        """
        Read the CSV header using Python's CSV parser.

        UTF-8 BOM is supported.
        """

        file_path = Path(file_path)

        BronzeIngestion._validate_file(file_path)

        try:
            with file_path.open(
                mode="r",
                encoding="utf-8-sig",
                newline="",
            ) as file:

                reader = csv.reader(file)

                header = next(reader, None)

        except (UnicodeDecodeError, csv.Error) as exc:

            raise ValueError(
                f"Unable to read CSV header: {file_path}"
            ) from exc

        if not header or not any(column.strip() for column in header):
            raise ValueError(
                f"CSV file has no valid header: {file_path}"
            )

        if any(not column.strip() for column in header):
            raise ValueError(
                f"CSV contains an empty column name: {file_path}"
            )

        duplicates = [
            name
            for name, count in Counter(header).items()
            if count > 1
        ]

        if duplicates:
            raise ValueError(
                f"Duplicate CSV columns in {file_path}: "
                f"{duplicates}"
            )

        return tuple(header)

    @classmethod
    def _validate_columns(
        cls,
        file_path: Path,
        required_columns: tuple[str, ...],
    ) -> None:
        """
        Validate required CSV columns.

        Additional source columns are allowed and preserved.
        """

        actual_columns = set(
            cls._read_header(file_path)
        )

        missing_columns = (
            set(required_columns) - actual_columns
        )

        if missing_columns:
            raise ValueError(
                f"Missing required columns in {file_path}: "
                f"{sorted(missing_columns)}"
            )

    # ========================================================
    # 3. WORKER FILE DISCOVERY
    # ========================================================

    def _discover_person_workers(
        self,
    ) -> list[tuple[int, Path]]:
        """
        Discover person detection CSV files.

        Supported filenames:
            persons_detected_worker1.csv
            persons_detected_worker2.csv
            persons_detected_worker3.csv

        Workers are sorted numerically.
        """

        if not self.data_dir.is_dir():
            raise FileNotFoundError(
                f"Data directory not found: {self.data_dir}"
            )

        candidate_files = list(
            self.data_dir.glob(
                "persons_detected_worker*.csv"
            )
        )

        if not candidate_files:
            raise FileNotFoundError(
                "No person worker CSV files were found "
                f"in {self.data_dir}"
            )

        workers = []

        for file_path in candidate_files:

            match = self.WORKER_PATTERN.fullmatch(
                file_path.name
            )

            if not match:
                raise ValueError(
                    f"Invalid worker filename: {file_path.name}. "
                    "Expected persons_detected_worker1.csv, "
                    "persons_detected_worker2.csv, etc."
                )

            worker_number = int(match.group(1))

            workers.append(
                (worker_number, file_path)
            )

        workers.sort(
            key=lambda item: item[0]
        )

        return workers

    # ========================================================
    # 4. PIPELINE PREFLIGHT
    # ========================================================

    def _preflight(self) -> list[tuple[int, Path]]:
        """
        Validate every source before writing any Bronze table.

        Returns:
            Discovered and numerically sorted worker files.
        """

        logger.info(
            "Starting Bronze ingestion preflight."
        )

        worker_files = self._discover_person_workers()

        # Validate worker CSV files.

        for worker_number, file_path in worker_files:

            self._validate_columns(
                file_path,
                self.REQUIRED_COLUMNS["person"],
            )

            logger.info(
                "Validated person worker %d: %s",
                worker_number,
                file_path.name,
            )

        # Validate fixed source files.

        fixed_sources = (
            (
                self.settings.paths.plate_detections_csv,
                self.REQUIRED_COLUMNS["plate"],
            ),
            (
                self.settings.paths.employees_csv,
                self.REQUIRED_COLUMNS["employees"],
            ),
            (
                self.settings.paths.cameras_csv,
                self.REQUIRED_COLUMNS["cameras"],
            ),
            (
                self.settings.paths.restricted_access_csv,
                self.REQUIRED_COLUMNS["restricted_access"],
            ),
        )

        for file_path, required_columns in fixed_sources:

            self._validate_columns(
                Path(file_path),
                required_columns,
            )

            logger.info(
                "Validated source: %s",
                file_path,
            )

        logger.info(
            "Bronze preflight completed successfully. "
            "Discovered %d person workers.",
            len(worker_files),
        )

        return worker_files

    # ========================================================
    # 5. DATABASE MANAGEMENT
    # ========================================================

    def _ensure_database(self) -> None:
        """
        Create the Bronze database if it does not exist.
        """

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.DATABASE}"
        )

        logger.info(
            "Bronze database is ready."
        )

    # ========================================================
    # 6. GENERIC CSV READER
    # ========================================================

    def _read_csv(
        self,
        file_path: Path,
        required_columns: tuple[str, ...],
    ) -> DataFrame:
        """
        Read a CSV file without applying transformations.

        Schema inference is disabled so source values
        remain strings.
        """

        file_path = Path(file_path)

        self._validate_columns(
            file_path,
            required_columns,
        )

        logger.info(
            "Reading CSV source: %s",
            file_path,
        )

        dataframe = (
            self.spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .option("encoding", "UTF-8")
            .load(str(file_path))
        )

        return dataframe

    # ========================================================
    # 7. GENERIC TABLE WRITER
    # ========================================================

    def _write_table(
        self,
        dataframe: DataFrame,
        table_name: str,
        mode: str = "overwrite",
    ) -> None:
        """
        Write a DataFrame into the Bronze database.

        Storage format: Parquet.
        """

        if not isinstance(table_name, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*",
            table_name,
        ):
            raise ValueError(
                f"Invalid Bronze table name: {table_name!r}"
            )

        if mode not in self.VALID_WRITE_MODES:
            raise ValueError(
                f"Invalid write mode: {mode!r}"
            )

        self._ensure_database()

        full_table_name = (
            f"{self.DATABASE}.{table_name}"
        )

        logger.info(
            "Writing Bronze table: %s",
            full_table_name,
        )

        (
            dataframe.write
            .format("parquet")
            .mode(mode)
            .saveAsTable(full_table_name)
        )

        logger.info(
            "Successfully written Bronze table: %s",
            full_table_name,
        )

    # ========================================================
    # 8. PERSON WORKER INGESTION
    # ========================================================

    def _ingest_worker_files(
        self,
        worker_files: list[tuple[int, Path]],
    ) -> None:
        """
        Ingest each person worker into its own Bronze table.
        """

        for worker_number, file_path in worker_files:

            logger.info(
                "Ingesting person worker %d.",
                worker_number,
            )

            dataframe = self._read_csv(
                file_path,
                self.REQUIRED_COLUMNS["person"],
            )

            table_name = (
                f"person_detections_worker{worker_number}"
            )

            self._write_table(
                dataframe=dataframe,
                table_name=table_name,
            )

        logger.info(
            "Successfully ingested %d person workers.",
            len(worker_files),
        )

    def ingest_person_workers(self) -> None:
        """
        Discover and ingest all person worker CSV files.
        """

        worker_files = self._discover_person_workers()

        self._ingest_worker_files(worker_files)

    # ========================================================
    # 9. PLATE DETECTION INGESTION
    # ========================================================

    def ingest_plate_detections(self) -> None:
        """
        Ingest plate detection records.
        """

        file_path = Path(
            self.settings.paths.plate_detections_csv
        )

        dataframe = self._read_csv(
            file_path,
            self.REQUIRED_COLUMNS["plate"],
        )

        self._write_table(
            dataframe=dataframe,
            table_name="plate_detections",
        )

    # ========================================================
    # 10. EMPLOYEE INGESTION
    # ========================================================

    def ingest_employees(self) -> None:
        """
        Ingest employee reference data.
        """

        file_path = Path(
            self.settings.paths.employees_csv
        )

        dataframe = self._read_csv(
            file_path,
            self.REQUIRED_COLUMNS["employees"],
        )

        self._write_table(
            dataframe=dataframe,
            table_name="employees",
        )

    # ========================================================
    # 11. CAMERA INGESTION
    # ========================================================

    def ingest_cameras(self) -> None:
        """
        Ingest camera configuration data.
        """

        file_path = Path(
            self.settings.paths.cameras_csv
        )

        dataframe = self._read_csv(
            file_path,
            self.REQUIRED_COLUMNS["cameras"],
        )

        self._write_table(
            dataframe=dataframe,
            table_name="cameras",
        )

    # ========================================================
    # 12. RESTRICTED ACCESS INGESTION
    # ========================================================

    def ingest_restricted_access(self) -> None:
        """
        Ingest employee restricted-access permissions.
        """

        file_path = Path(
            self.settings.paths.restricted_access_csv
        )

        dataframe = self._read_csv(
            file_path,
            self.REQUIRED_COLUMNS["restricted_access"],
        )

        self._write_table(
            dataframe=dataframe,
            table_name="restricted_access",
        )

    # ========================================================
    # 13. PIPELINE ORCHESTRATION
    # ========================================================

    def run(self) -> None:
        """
        Execute the complete Bronze ingestion pipeline.

        All source files are validated before ingestion starts.
        """

        logger.info("=" * 60)
        logger.info("STARTING BRONZE INGESTION")
        logger.info("=" * 60)

        # Step 1: Validate all sources.
        worker_files = self._preflight()

        # Step 2: Prepare the database.
        self._ensure_database()

        # Step 3: Ingest person workers.
        self._ingest_worker_files(worker_files)

        # Step 4: Ingest plate detections.
        self.ingest_plate_detections()

        # Step 5: Ingest reference data.
        self.ingest_employees()

        self.ingest_cameras()

        self.ingest_restricted_access()

        logger.info("=" * 60)
        logger.info("BRONZE INGESTION COMPLETED")
        logger.info("=" * 60)

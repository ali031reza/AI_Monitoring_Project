
import csv
from pathlib import Path
from unittest.mock import MagicMock, call, Mock

import pytest

from advanced.config import ApplicationSettings, ProjectPaths

from advanced.ETL.spark_pipeline.bronze.bronze_ingestion import (
    BronzeIngestion,
)


# ============================================================
# TEST FIXTURES AND HELPERS
# ============================================================

@pytest.fixture
def bronze(tmp_path):
    """Create a BronzeIngestion instance with isolated test paths."""

    advanced_dir = tmp_path / "advanced"

    settings = ApplicationSettings(
        paths=ProjectPaths(advanced_dir=advanced_dir)
    )

    settings.paths.data_dir.mkdir(parents=True)
    settings.paths.starter_sources_dir.mkdir(parents=True)

    spark = MagicMock()

    return BronzeIngestion(
        spark=spark,
        settings=settings,
    )


def write_csv(
    file_path: Path,
    columns: tuple[str, ...],
) -> Path:
    """Create a CSV containing the specified header."""

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with file_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:

        writer = csv.writer(file)
        writer.writerow(columns)

    return file_path


def create_valid_sources(
    bronze,
    worker_ids=(1, 2),
):
    """Create all required source CSV files."""

    for worker_id in worker_ids:

        write_csv(
            bronze.data_dir
            / f"persons_detected_worker{worker_id}.csv",
            bronze.REQUIRED_COLUMNS["person"],
        )

    paths = bronze.settings.paths

    write_csv(
        paths.plate_detections_csv,
        bronze.REQUIRED_COLUMNS["plate"],
    )

    write_csv(
        paths.employees_csv,
        bronze.REQUIRED_COLUMNS["employees"],
    )

    write_csv(
        paths.cameras_csv,
        bronze.REQUIRED_COLUMNS["cameras"],
    )

    write_csv(
        paths.restricted_access_csv,
        bronze.REQUIRED_COLUMNS["restricted_access"],
    )


# ============================================================
# 1. INITIALIZATION TESTS
# ============================================================

def test_initialization(bronze):

    assert bronze.database == "bronze"
    assert bronze.data_dir.is_dir()
    assert bronze.starter_sources_dir.is_dir()


def test_initialization_rejects_invalid_settings():

    with pytest.raises(
        TypeError,
        match="ApplicationSettings",
    ):

        BronzeIngestion(
            spark=MagicMock(),
            settings=object(),
        )


# ============================================================
# 2. WORKER DISCOVERY TESTS
# ============================================================

def test_discover_workers_numeric_order(bronze):

    create_valid_sources(
        bronze,
        worker_ids=(10, 2, 1),
    )

    workers = bronze._discover_person_workers()

    worker_numbers = [
        number
        for number, _ in workers
    ]

    assert worker_numbers == [1, 2, 10]


@pytest.mark.parametrize(
    "filename",
    [
        "persons_detected_worker0.csv",
        "persons_detected_worker01.csv",
        "persons_detected_worker_east.csv",
        "persons_detected_worker-1.csv",
    ],
)
def test_invalid_worker_filename(bronze, filename):

    write_csv(
        bronze.data_dir / filename,
        bronze.REQUIRED_COLUMNS["person"],
    )

    with pytest.raises(
        ValueError,
        match="Invalid worker filename",
    ):

        bronze._discover_person_workers()


def test_discover_workers_without_files(bronze):

    with pytest.raises(
        FileNotFoundError,
        match="No person worker CSV files",
    ):

        bronze._discover_person_workers()


def test_discover_workers_missing_directory(bronze):

    bronze.data_dir.rmdir()

    with pytest.raises(
        FileNotFoundError,
        match="Data directory not found",
    ):

        bronze._discover_person_workers()


# ============================================================
# 3. CSV HEADER VALIDATION TESTS
# ============================================================

def test_valid_csv_header(bronze, tmp_path):

    file_path = write_csv(
        tmp_path / "valid.csv",
        bronze.REQUIRED_COLUMNS["person"],
    )

    header = bronze._read_header(file_path)

    assert header == bronze.REQUIRED_COLUMNS["person"]


def test_missing_required_columns(bronze, tmp_path):

    file_path = write_csv(
        tmp_path / "invalid.csv",
        ("camera_id", "zone_id"),
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):

        bronze._validate_columns(
            file_path,
            bronze.REQUIRED_COLUMNS["person"],
        )


def test_duplicate_csv_columns(bronze, tmp_path):

    file_path = tmp_path / "duplicate.csv"

    file_path.write_text(
        "camera_id,zone_id,camera_id\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Duplicate CSV columns",
    ):

        bronze._read_header(file_path)


def test_missing_csv_file(bronze, tmp_path):

    file_path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):

        bronze._read_header(file_path)


# ============================================================
# 4. PREFLIGHT TESTS
# ============================================================

def test_preflight_success(bronze):

    create_valid_sources(
        bronze,
        worker_ids=(1, 2),
    )

    workers = bronze._preflight()

    assert len(workers) == 2

    assert [
        number
        for number, _ in workers
    ] == [1, 2]


def test_preflight_fails_when_source_missing(bronze):

    create_valid_sources(bronze)

    bronze.settings.paths.employees_csv.unlink()

    with pytest.raises(FileNotFoundError):

        bronze._preflight()


# ============================================================
# 5. DATABASE MANAGEMENT TESTS
# ============================================================

def test_ensure_database(bronze):

    bronze._ensure_database()

    bronze.spark.sql.assert_called_once_with(
        "CREATE DATABASE IF NOT EXISTS bronze"
    )


# ============================================================
# 6. SPARK CSV READER TESTS
# ============================================================

def test_read_csv_configuration(bronze, tmp_path):

    file_path = write_csv(
        tmp_path / "person.csv",
        bronze.REQUIRED_COLUMNS["person"],
    )

    dataframe = MagicMock()
    reader = MagicMock()

    bronze.spark.read.format.return_value = reader

    reader.option.return_value = reader
    reader.load.return_value = dataframe

    result = bronze._read_csv(
        file_path,
        bronze.REQUIRED_COLUMNS["person"],
    )

    assert result is dataframe

    bronze.spark.read.format.assert_called_once_with("csv")

    expected_options = [
        call("header", "true"),
        call("inferSchema", "false"),
        call("quote", '"'),
        call("escape", '"'),
        call("mode", "FAILFAST"),
        call("encoding", "UTF-8"),
    ]

    assert reader.option.call_args_list == expected_options

    reader.load.assert_called_once_with(str(file_path))


# ============================================================
# 7. PARQUET WRITER TESTS
# ============================================================

def test_write_table(bronze):

    dataframe = MagicMock()
    writer = MagicMock()

    dataframe.write.format.return_value = writer
    writer.mode.return_value = writer

    bronze._write_table(
        dataframe=dataframe,
        table_name="test_table",
        mode="overwrite",
    )

    dataframe.write.format.assert_called_once_with("parquet")

    writer.mode.assert_called_once_with("overwrite")

    writer.saveAsTable.assert_called_once_with(
        "bronze.test_table"
    )


def test_append_mode_requires_stable_keys(bronze):

    with pytest.raises(
        ValueError,
        match="key_columns are required",
    ):

        bronze._write_table(
            dataframe=MagicMock(),
            table_name="event_table",
        )


def test_write_table_rejects_invalid_name(bronze):

    with pytest.raises(
        ValueError,
        match="Invalid Bronze table name",
    ):

        bronze._write_table(
            dataframe=MagicMock(),
            table_name="bronze.invalid.table",
        )


def test_write_table_rejects_invalid_mode(bronze):

    with pytest.raises(
        ValueError,
        match="Invalid write mode",
    ):

        bronze._write_table(
            dataframe=MagicMock(),
            table_name="test_table",
            mode="invalid",
        )


# ============================================================
# 8. PERSON WORKER INGESTION TESTS
# ============================================================

def test_worker_ingestion_uses_correct_table_names(
    bronze,
    monkeypatch,
):

    file1 = bronze.data_dir / "persons_detected_worker1.csv"
    file2 = bronze.data_dir / "persons_detected_worker2.csv"

    dataframe = MagicMock()

    read_mock = Mock(return_value=dataframe)
    write_mock = Mock()

    monkeypatch.setattr(
        bronze,
        "_read_csv",
        read_mock,
    )

    monkeypatch.setattr(
        bronze,
        "_write_table",
        write_mock,
    )

    bronze._ingest_worker_files(
        [
            (1, file1),
            (2, file2),
        ]
    )

    assert write_mock.call_count == 2

    written_tables = [
        item.kwargs["table_name"]
        for item in write_mock.call_args_list
    ]

    assert written_tables == [
        "person_detections_worker1",
        "person_detections_worker2",
    ]

    assert all(
        item.kwargs["key_columns"] == ("event_id",)
        for item in write_mock.call_args_list
    )


# ============================================================
# 9. COMPLETE PIPELINE ORCHESTRATION TESTS
# ============================================================

def test_run_executes_all_ingestion_steps(
    bronze,
    monkeypatch,
):

    worker_files = [
        (1, bronze.data_dir / "persons_detected_worker1.csv"),
        (2, bronze.data_dir / "persons_detected_worker2.csv"),
    ]

    preflight = Mock(return_value=worker_files)
    database = Mock()
    workers = Mock()
    plates = Mock()
    employees = Mock()
    cameras = Mock()
    restricted = Mock()

    monkeypatch.setattr(bronze, "_preflight", preflight)
    monkeypatch.setattr(bronze, "_ensure_database", database)
    monkeypatch.setattr(bronze, "_ingest_worker_files", workers)
    monkeypatch.setattr(bronze, "ingest_plate_detections", plates)
    monkeypatch.setattr(bronze, "ingest_employees", employees)
    monkeypatch.setattr(bronze, "ingest_cameras", cameras)
    monkeypatch.setattr(bronze, "ingest_restricted_access", restricted)

    bronze.run()

    preflight.assert_called_once()
    database.assert_called_once()
    workers.assert_called_once_with(worker_files)

    plates.assert_called_once()
    employees.assert_called_once()
    cameras.assert_called_once()
    restricted.assert_called_once()


def test_run_stops_when_preflight_fails(
    bronze,
    monkeypatch,
):

    preflight = Mock(
        side_effect=ValueError("Invalid source CSV")
    )

    database = Mock()
    workers = Mock()

    monkeypatch.setattr(
        bronze,
        "_preflight",
        preflight,
    )

    monkeypatch.setattr(
        bronze,
        "_ensure_database",
        database,
    )

    monkeypatch.setattr(
        bronze,
        "_ingest_worker_files",
        workers,
    )

    with pytest.raises(
        ValueError,
        match="Invalid source CSV",
    ):

        bronze.run()

    database.assert_not_called()
    workers.assert_not_called()

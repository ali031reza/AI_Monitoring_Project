
"""
Tests for the Silver transformation pipeline.

Covers:
- Initialization and configuration
- Bronze table discovery
- Missing source tables
- Person detection transformations
- JSON array parsing
- Timestamp conversion
- Person/authorization alignment
- Empty detection preservation
- Plate normalization
- Employee normalization
- Camera normalization
- Restricted access normalization
- Parquet table writing
- Pipeline orchestration
"""

import uuid
from unittest.mock import Mock

import pytest

from pyspark.sql import functions as F
from pyspark.sql import types as T

from advanced.config.spark_session import create_spark_session

from advanced.ETL.spark_pipeline.silver.silver_transformation import (
    SilverTransformation,
)


# ============================================================
# CONSTANTS
# ============================================================

PERSON_COLUMNS = [
    "Time_stamp",
    "Zone",
    "detected_persons_id",
    "authorized",
    "camera_id",
    "event_id",
    "detector_type",
]

PLATE_COLUMNS = [
    "Time_stamp",
    "Zone",
    "detected_plates",
    "camera_id",
    "event_id",
    "detector_type",
]

EMPLOYEE_COLUMNS = [
    "employee_id",
    "plate_number",
]

CAMERA_COLUMNS = [
    "camera_id",
    "zone_id",
    "detector_type",
    "enabled",
    "kafka_partition",
]

RESTRICTED_ACCESS_COLUMNS = [
    "employee_id",
    "zone_id",
]


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture(scope="session")
def spark():
    """Create one local Spark session for the test suite."""

    session = create_spark_session()

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def pipeline(spark):
    """Create isolated Bronze and Silver databases per test."""

    suffix = uuid.uuid4().hex[:10]

    source_database = f"bronze_test_{suffix}"
    target_database = f"silver_test_{suffix}"

    spark.sql(
        f"CREATE DATABASE {source_database}"
    )

    instance = SilverTransformation(
        spark=spark,
        source_database=source_database,
        target_database=target_database,
    )

    yield instance

    spark.sql(
        f"DROP DATABASE IF EXISTS {target_database} CASCADE"
    )

    spark.sql(
        f"DROP DATABASE IF EXISTS {source_database} CASCADE"
    )


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def write_table(
    spark,
    database,
    table_name,
    columns,
    rows,
):
    """Create a test Parquet table."""

    df = spark.createDataFrame(
        rows,
        schema=columns,
    )

    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(f"{database}.{table_name}")
    )

    return df


def create_person_worker(
    pipeline,
    worker_id,
    rows,
):
    """Create a Bronze person worker table."""

    return write_table(
        pipeline.spark,
        pipeline.source_database,
        f"person_detections_worker{worker_id}",
        PERSON_COLUMNS,
        rows,
    )


# ============================================================
# 1. INITIALIZATION TESTS
# ============================================================

def test_initialization(pipeline):

    assert pipeline.source_database.startswith("bronze_test_")

    assert pipeline.target_database.startswith("silver_test_")

    assert pipeline.spark is not None


def test_initialization_rejects_missing_spark():

    with pytest.raises(
        TypeError,
        match="SparkSession",
    ):

        SilverTransformation(
            spark=None,
        )


@pytest.mark.parametrize(
    "database",
    [
        "",
        "silver-test",
        "123silver",
        "silver.test",
    ],
)
def test_initialization_rejects_invalid_database(
    spark,
    database,
):

    with pytest.raises(
        ValueError,
        match="Invalid",
    ):

        SilverTransformation(
            spark=spark,
            target_database=database,
        )


# ============================================================
# 2. BRONZE TABLE DISCOVERY TESTS
# ============================================================

def test_discover_workers_numeric_order(
    pipeline,
):

    for worker_id in (10, 2, 1):

        write_table(
            pipeline.spark,
            pipeline.source_database,
            f"person_detections_worker{worker_id}",
            ["test_column"],
            [("test",)],
        )

    workers = pipeline._discover_person_workers()

    worker_numbers = [
        worker_id
        for worker_id, _ in workers
    ]

    assert worker_numbers == [1, 2, 10]


def test_discover_workers_without_tables(
    pipeline,
):

    with pytest.raises(
        ValueError,
        match="No person worker tables",
    ):

        pipeline._discover_person_workers()


def test_discover_workers_rejects_invalid_name(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "person_detections_worker01",
        ["test_column"],
        [("test",)],
    )

    with pytest.raises(
        ValueError,
        match="Invalid person worker table name",
    ):

        pipeline._discover_person_workers()


# ============================================================
# 3. BRONZE READER TESTS
# ============================================================

def test_read_bronze_table_success(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "test_table",
        ["employee_id"],
        [("001",)],
    )

    df = pipeline._read_bronze_table(
        "test_table"
    )

    assert df.count() == 1

    assert df.first()["employee_id"] == "001"


def test_read_bronze_table_missing(
    pipeline,
):

    with pytest.raises(
        ValueError,
        match="does not exist",
    ):

        pipeline._read_bronze_table(
            "missing_table"
        )


def test_read_bronze_table_rejects_invalid_name(
    pipeline,
):

    with pytest.raises(
        ValueError,
        match="Invalid table name",
    ):

        pipeline._read_bronze_table(
            "invalid.table"
        )


# ============================================================
# 4. PERSON DETECTION TRANSFORMATION TESTS
# ============================================================

def test_person_detections_combine_workers(
    pipeline,
):

    create_person_worker(
        pipeline,
        1,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                '["001", "002"]',
                "[true, false]",
                "cam1",
                "event1",
                "person",
            ),
        ],
    )

    create_person_worker(
        pipeline,
        2,
        [
            (
                "2026-11-09 10:01:00",
                "B",
                '["003"]',
                "[true]",
                "cam2",
                "event2",
                "person",
            ),
        ],
    )

    result = pipeline.transform_person_detections()

    assert result.count() == 2

    assert isinstance(
        result.schema["Time_stamp"].dataType,
        T.TimestampType,
    )

    assert isinstance(
        result.schema["detected_persons_id"].dataType,
        T.ArrayType,
    )

    assert isinstance(
        result.schema["authorized"].dataType,
        T.ArrayType,
    )

    assert (
        pipeline.spark.table(
            f"{pipeline.target_database}.person_detections"
        ).count()
        == 2
    )


def test_person_detection_preserves_empty_frames(
    pipeline,
):

    create_person_worker(
        pipeline,
        1,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                "[]",
                "[]",
                "cam1",
                "event1",
                "person",
            ),
            (
                "2026-11-09 10:01:00",
                "A",
                '["001"]',
                "[true]",
                "cam1",
                "event2",
                "person",
            ),
        ],
    )

    result = pipeline.transform_person_detections()

    assert result.count() == 2

    empty_frames = result.filter(
        F.size("detected_persons_id") == 0
    ).count()

    assert empty_frames == 1


def test_person_authorization_arrays_must_align(
    pipeline,
):

    create_person_worker(
        pipeline,
        1,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                '["001", "002"]',
                "[true]",
                "cam1",
                "event1",
                "person",
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="misaligned",
    ):

        pipeline.transform_person_detections()


def test_person_detection_rejects_invalid_timestamp(
    pipeline,
):

    create_person_worker(
        pipeline,
        1,
        [
            (
                "not-a-timestamp",
                "A",
                '["001"]',
                "[true]",
                "cam1",
                "event1",
                "person",
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid timestamps",
    ):

        pipeline.transform_person_detections()


def test_person_detection_rejects_invalid_json(
    pipeline,
):

    create_person_worker(
        pipeline,
        1,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                "invalid-json",
                "[true]",
                "cam1",
                "event1",
                "person",
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid JSON arrays",
    ):

        pipeline.transform_person_detections()


# ============================================================
# 5. PERSON DETECTION DETAILS TESTS
# ============================================================

def test_person_detection_details_preserve_alignment(
    pipeline,
):

    create_person_worker(
        pipeline,
        1,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                '["001", "002"]',
                "[true, false]",
                "cam1",
                "event1",
                "person",
            ),
            (
                "2026-11-09 10:01:00",
                "A",
                "[]",
                "[]",
                "cam1",
                "event2",
                "person",
            ),
        ],
    )

    pipeline.transform_person_detections()

    details = pipeline.transform_person_detection_details()

    assert details.count() == 2

    authorization = {
        row.employee_id: row.authorized
        for row in details.collect()
    }

    assert authorization == {
        "001": True,
        "002": False,
    }


def test_person_details_require_parent_table(
    pipeline,
):

    with pytest.raises(Exception):

        pipeline.transform_person_detection_details()


# ============================================================
# 6. PLATE DETECTION TESTS
# ============================================================

def test_plate_detection_normalization(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "plate_detections",
        PLATE_COLUMNS,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                '["ab123", "xy456"]',
                "cam1",
                "event1",
                "plate",
            ),
        ],
    )

    result = pipeline.transform_plate_detections()

    assert result.count() == 1

    plates = result.first()["detected_plates"]

    assert plates == ["AB123", "XY456"]

    assert isinstance(
        result.schema["Time_stamp"].dataType,
        T.TimestampType,
    )


def test_plate_detection_rejects_invalid_json(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "plate_detections",
        PLATE_COLUMNS,
        [
            (
                "2026-11-09 10:00:00",
                "A",
                "invalid-json",
                "cam1",
                "event1",
                "plate",
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid JSON arrays",
    ):

        pipeline.transform_plate_detections()


# ============================================================
# 7. EMPLOYEE TRANSFORMATION TESTS
# ============================================================

def test_employee_normalization(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "employees",
        EMPLOYEE_COLUMNS,
        [
            ("001", "na"),
            ("002", " ab123 "),
            ("003", None),
        ],
    )

    result = pipeline.transform_employees()

    records = {
        row.employee_id: row.plate_number
        for row in result.collect()
    }

    assert records["001"] is None

    assert records["002"] == "AB123"

    assert records["003"] is None


def test_employee_missing_id_is_rejected(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "employees",
        EMPLOYEE_COLUMNS,
        [
            ("", "AB123"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="missing or empty",
    ):

        pipeline.transform_employees()


# ============================================================
# 8. CAMERA TRANSFORMATION TESTS
# ============================================================

def test_camera_normalization(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "cameras",
        CAMERA_COLUMNS,
        [
            ("cam1", " A ", " PERSON ", "TRUE", "0"),
            ("cam2", " B ", " PLATE ", "false", "1"),
        ],
    )

    result = pipeline.transform_cameras()

    assert isinstance(
        result.schema["enabled"].dataType,
        T.BooleanType,
    )

    assert isinstance(
        result.schema["kafka_partition"].dataType,
        T.IntegerType,
    )

    cameras = {
        row.camera_id: row
        for row in result.collect()
    }

    assert cameras["cam1"].enabled is True

    assert cameras["cam2"].enabled is False

    assert cameras["cam1"].zone_id == "A"

    assert cameras["cam1"].detector_type == "person"

    assert cameras["cam2"].kafka_partition == 1


def test_camera_rejects_invalid_enabled_value(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "cameras",
        CAMERA_COLUMNS,
        [
            ("cam1", "A", "person", "YES", "0"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid Boolean",
    ):

        pipeline.transform_cameras()


def test_camera_rejects_invalid_partition(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "cameras",
        CAMERA_COLUMNS,
        [
            ("cam1", "A", "person", "TRUE", "invalid"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid values",
    ):

        pipeline.transform_cameras()


# ============================================================
# 9. RESTRICTED ACCESS TESTS
# ============================================================

def test_restricted_access_normalization(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "restricted_access",
        RESTRICTED_ACCESS_COLUMNS,
        [
            (" 001 ", " ZONE_A "),
            ("002", "ZONE_B"),
        ],
    )

    result = pipeline.transform_restricted_access()

    records = result.collect()

    assert records[0]["employee_id"] == "001"

    assert records[0]["zone_id"] == "ZONE_A"

    assert result.count() == 2


def test_restricted_access_rejects_empty_employee(
    pipeline,
):

    write_table(
        pipeline.spark,
        pipeline.source_database,
        "restricted_access",
        RESTRICTED_ACCESS_COLUMNS,
        [
            ("", "ZONE_A"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="missing or empty",
    ):

        pipeline.transform_restricted_access()


# ============================================================
# 10. SILVER WRITER TESTS
# ============================================================

def test_write_silver_table(
    pipeline,
):

    df = pipeline.spark.createDataFrame(
        [("001",)],
        ["employee_id"],
    )

    pipeline._write_silver_table(
        df,
        "test_table",
    )

    assert pipeline.spark.catalog.tableExists(
        f"{pipeline.target_database}.test_table"
    )

    stored = pipeline.spark.table(
        f"{pipeline.target_database}.test_table"
    )

    assert stored.count() == 1


def test_write_silver_table_rejects_invalid_name(
    pipeline,
):

    df = pipeline.spark.createDataFrame(
        [("001",)],
        ["employee_id"],
    )

    with pytest.raises(
        ValueError,
        match="Invalid table name",
    ):

        pipeline._write_silver_table(
            df,
            "invalid.table",
        )


def test_write_silver_table_rejects_invalid_mode(
    pipeline,
):

    df = pipeline.spark.createDataFrame(
        [("001",)],
        ["employee_id"],
    )

    with pytest.raises(
        ValueError,
        match="Invalid write mode",
    ):

        pipeline._write_silver_table(
            df,
            "test_table",
            mode="invalid",
        )


# ============================================================
# 11. PIPELINE ORCHESTRATION TESTS
# ============================================================

def test_run_executes_all_transformations(
    pipeline,
    monkeypatch,
):

    ensure_database = Mock()

    persons = Mock()

    details = Mock()

    plates = Mock()

    employees = Mock()

    cameras = Mock()

    restricted = Mock()

    monkeypatch.setattr(
        pipeline,
        "_ensure_silver_database",
        ensure_database,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_person_detections",
        persons,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_person_detection_details",
        details,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_plate_detections",
        plates,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_employees",
        employees,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_cameras",
        cameras,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_restricted_access",
        restricted,
    )

    pipeline.run()

    ensure_database.assert_called_once()

    persons.assert_called_once()

    details.assert_called_once()

    plates.assert_called_once()

    employees.assert_called_once()

    cameras.assert_called_once()

    restricted.assert_called_once()


def test_run_stops_when_transformation_fails(
    pipeline,
    monkeypatch,
):

    monkeypatch.setattr(
        pipeline,
        "_ensure_silver_database",
        Mock(),
    )

    monkeypatch.setattr(
        pipeline,
        "transform_person_detections",
        Mock(
            side_effect=ValueError(
                "Invalid person source"
            )
        ),
    )

    details = Mock()

    monkeypatch.setattr(
        pipeline,
        "transform_person_detection_details",
        details,
    )

    with pytest.raises(
        ValueError,
        match="Invalid person source",
    ):

        pipeline.run()

    details.assert_not_called()

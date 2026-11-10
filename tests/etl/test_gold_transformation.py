
"""
Tests for the Gold transformation pipeline.

Covers:
- Initialization and configuration
- Silver table reading
- Gold table writing
- Employee directory validation
- Employee/license-plate mapping
- Plate event transformation
- Monitoring event integration
- Detection-level details
- Unknown license plate handling
- Restricted access transformation
- Pipeline orchestration
"""

import uuid
from datetime import datetime
from unittest.mock import Mock

import pytest

from pyspark.sql import functions as F
from pyspark.sql import types as T

from advanced.config.spark_session import create_spark_session

from advanced.ETL.spark_pipeline.gold.gold_transformation import (
    GoldTransformation,
)


# ============================================================
# CONSTANTS AND SCHEMAS
# ============================================================

EMPLOYEE_SCHEMA = T.StructType([
    T.StructField("employee_id", T.StringType(), True),
    T.StructField("plate_number", T.StringType(), True),
])

PERSON_EVENT_SCHEMA = T.StructType([
    T.StructField("Time_stamp", T.TimestampType(), True),
    T.StructField("Zone", T.StringType(), True),
    T.StructField(
        "detected_persons_id",
        T.ArrayType(T.StringType()),
        True,
    ),
    T.StructField(
        "authorized",
        T.ArrayType(T.BooleanType()),
        True,
    ),
    T.StructField("camera_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("detector_type", T.StringType(), True),
])

PLATE_EVENT_SCHEMA = T.StructType([
    T.StructField("Time_stamp", T.TimestampType(), True),
    T.StructField("Zone", T.StringType(), True),
    T.StructField(
        "detected_plates",
        T.ArrayType(T.StringType()),
        True,
    ),
    T.StructField("camera_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("detector_type", T.StringType(), True),
])

PLATE_JSON_SCHEMA = T.StructType([
    T.StructField("Time_stamp", T.TimestampType(), True),
    T.StructField("Zone", T.StringType(), True),
    T.StructField("detected_plates", T.StringType(), True),
    T.StructField("camera_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("detector_type", T.StringType(), True),
])

PERSON_DETAIL_SCHEMA = T.StructType([
    T.StructField("Time_stamp", T.TimestampType(), True),
    T.StructField("Zone", T.StringType(), True),
    T.StructField("camera_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("detector_type", T.StringType(), True),
    T.StructField("employee_id", T.StringType(), True),
    T.StructField("authorized", T.BooleanType(), True),
])

RESTRICTED_ACCESS_SCHEMA = T.StructType([
    T.StructField("employee_id", T.StringType(), True),
    T.StructField("zone_id", T.StringType(), True),
])


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
    """Create isolated Silver and Gold databases per test."""

    suffix = uuid.uuid4().hex[:10]

    source_database = f"silver_test_{suffix}"
    target_database = f"gold_test_{suffix}"

    spark.sql(
        f"CREATE DATABASE {source_database}"
    )

    spark.sql(
        f"CREATE DATABASE {target_database}"
    )

    instance = GoldTransformation(
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

def timestamp(minute=0):
    """Return a deterministic test timestamp."""

    return datetime(2026, 11, 9, 10, minute, 0)


def write_silver_table(
    pipeline,
    table_name,
    schema,
    rows,
):
    """Create a Silver Parquet table."""

    df = pipeline.spark.createDataFrame(
        rows,
        schema=schema,
    )

    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{pipeline.source_database}.{table_name}"
        )
    )

    return df


def create_employees(pipeline, rows):
    return write_silver_table(
        pipeline,
        "employees",
        EMPLOYEE_SCHEMA,
        rows,
    )


def create_person_events(pipeline, rows):
    return write_silver_table(
        pipeline,
        "person_detections",
        PERSON_EVENT_SCHEMA,
        rows,
    )


def create_plate_events(pipeline, rows):
    return write_silver_table(
        pipeline,
        "plate_detections",
        PLATE_EVENT_SCHEMA,
        rows,
    )


def create_person_details(pipeline, rows):
    return write_silver_table(
        pipeline,
        "person_detection_details",
        PERSON_DETAIL_SCHEMA,
        rows,
    )


# ============================================================
# 1. INITIALIZATION TESTS
# ============================================================

def test_initialization(pipeline):

    assert pipeline.source_database.startswith(
        "silver_test_"
    )

    assert pipeline.target_database.startswith(
        "gold_test_"
    )

    assert pipeline.spark is not None


def test_initialization_with_default_databases(spark):

    instance = GoldTransformation(
        spark=spark,
    )

    assert instance.source_database == "silver"

    assert instance.target_database == "gold"


# ============================================================
# 2. SILVER TABLE READER TESTS
# ============================================================

def test_read_silver_table_success(pipeline):

    write_silver_table(
        pipeline,
        "test_table",
        T.StructType([
            T.StructField(
                "employee_id",
                T.StringType(),
                True,
            ),
        ]),
        [("001",)],
    )

    result = pipeline._read_silver_table(
        "test_table"
    )

    assert result.count() == 1

    assert result.first()["employee_id"] == "001"


def test_read_silver_table_missing(pipeline):

    with pytest.raises(
        ValueError,
        match="Silver table does not exist",
    ):

        pipeline._read_silver_table(
            "missing_table"
        )


def test_read_silver_table_rejects_invalid_name(
    pipeline,
):

    with pytest.raises(
        ValueError,
        match="Invalid Silver table name",
    ):

        pipeline._read_silver_table(
            "invalid.table"
        )


# ============================================================
# 3. GOLD TABLE WRITER TESTS
# ============================================================

def test_write_gold_table_success(pipeline):

    df = pipeline.spark.createDataFrame(
        [("001",)],
        ["employee_id"],
    )

    pipeline._write_gold_table(
        df,
        "test_table",
        mode="overwrite",
    )

    table_name = (
        f"{pipeline.target_database}.test_table"
    )

    assert pipeline.spark.catalog.tableExists(
        table_name
    )

    stored = pipeline.spark.table(table_name)

    assert stored.count() == 1

    assert stored.first()["employee_id"] == "001"


def test_append_mode_requires_stable_keys(pipeline):

    df = pipeline.spark.createDataFrame(
        [("event-001",)],
        ["event_id"],
    )

    with pytest.raises(
        ValueError,
        match="key_columns are required",
    ):

        pipeline._write_gold_table(
            df,
            "event_table",
        )


def test_write_gold_table_rejects_invalid_name(
    pipeline,
):

    df = pipeline.spark.createDataFrame(
        [("001",)],
        ["employee_id"],
    )

    with pytest.raises(
        ValueError,
        match="Invalid Gold table name",
    ):

        pipeline._write_gold_table(
            df,
            "invalid.table",
        )


def test_write_gold_table_rejects_missing_database(
    pipeline,
):

    df = pipeline.spark.createDataFrame(
        [("001",)],
        ["employee_id"],
    )

    missing_database_pipeline = GoldTransformation(
        spark=pipeline.spark,
        source_database=pipeline.source_database,
        target_database="gold_missing_database",
    )

    with pytest.raises(
        ValueError,
        match="Gold database does not exist",
    ):

        missing_database_pipeline._write_gold_table(
            df,
            "test_table",
            mode="overwrite",
        )


# ============================================================
# 4. EMPLOYEE DIRECTORY TESTS
# ============================================================

def test_employee_directory_success(pipeline):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
            ("002", None),
            ("003", "CD456"),
        ],
    )

    result = pipeline.transform_employee_directory()

    assert result.count() == 3

    assert result.columns == [
        "employee_id",
        "plate_number",
    ]

    assert pipeline.spark.catalog.tableExists(
        f"{pipeline.target_database}.employee_directory"
    )


def test_employee_directory_preserves_source_values(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", " ab123 "),
        ],
    )

    result = pipeline.transform_employee_directory()

    # The current implementation validates normalized
    # plate uniqueness but does not rewrite source values.

    assert result.first()["plate_number"] == " ab123 "


def test_employee_directory_rejects_missing_columns(
    pipeline,
):

    write_silver_table(
        pipeline,
        "employees",
        T.StructType([
            T.StructField(
                "employee_id",
                T.StringType(),
                True,
            ),
        ]),
        [("001",)],
    )

    with pytest.raises(
        ValueError,
        match="Missing employee columns",
    ):

        pipeline.transform_employee_directory()


def test_employee_directory_rejects_empty_employee_id(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("", "AB123"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid employee IDs",
    ):

        pipeline.transform_employee_directory()


def test_employee_directory_rejects_null_employee_id(
    pipeline,
):

    create_employees(
        pipeline,
        [
            (None, "AB123"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="invalid employee IDs",
    ):

        pipeline.transform_employee_directory()


def test_employee_directory_rejects_duplicate_employee_ids(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
            ("001", "CD456"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate employee IDs",
    ):

        pipeline.transform_employee_directory()


def test_employee_directory_rejects_duplicate_plates(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
            ("002", " ab123 "),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate registered license plates",
    ):

        pipeline.transform_employee_directory()


def test_employee_directory_allows_multiple_null_plates(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", None),
            ("002", None),
            ("003", ""),
        ],
    )

    result = pipeline.transform_employee_directory()

    assert result.count() == 3


# ============================================================
# 5. EMPLOYEE / PLATE MAPPING TESTS
# ============================================================

def test_build_employee_plate_map(pipeline):

    create_employees(
        pipeline,
        [
            ("001", " ab123 "),
            ("002", "CD456"),
            ("003", None),
        ],
    )

    result = pipeline._build_employee_plate_map()

    assert result.count() == 1

    mapping = result.first()["_plate_to_employee"]

    assert mapping == {
        "AB123": "001",
        "CD456": "002",
    }


def test_build_employee_plate_map_ignores_empty_plates(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", None),
            ("002", ""),
            ("003", "AB123"),
        ],
    )

    result = pipeline._build_employee_plate_map()

    mapping = result.first()["_plate_to_employee"]

    assert mapping == {
        "AB123": "003",
    }


def test_build_employee_plate_map_rejects_duplicate_plates(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
            ("002", " ab123 "),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate registered license plates",
    ):

        pipeline._build_employee_plate_map()


def test_build_employee_plate_map_rejects_missing_columns(
    pipeline,
):

    write_silver_table(
        pipeline,
        "employees",
        T.StructType([
            T.StructField(
                "employee_id",
                T.StringType(),
                True,
            ),
        ]),
        [("001",)],
    )

    with pytest.raises(
        ValueError,
        match="missing required columns",
    ):

        pipeline._build_employee_plate_map()


# ============================================================
# 6. PLATE EVENT TRANSFORMATION TESTS
# ============================================================

def test_transform_plate_events_maps_registered_employees(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
            ("002", "CD456"),
        ],
    )

    create_plate_events(
        pipeline,
        [
            (
                timestamp(),
                "A",
                ["ab123", "CD456", "UNKNOWN"],
                "cam1",
                "event1",
                "plate",
            ),
        ],
    )

    result = pipeline._transform_plate_events()

    assert result.count() == 1

    row = result.first()

    assert row["detection_source"] == "plate"

    assert row["detected_employee_ids"] == [
        "001",
        "002",
        None,
    ]

    assert row["detected_plates"] == [
        "ab123",
        "CD456",
        "UNKNOWN",
    ]

    assert row["authorized"] is None

    assert isinstance(
        result.schema["timestamp"].dataType,
        T.TimestampType,
    )


def test_transform_plate_events_preserves_empty_arrays(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_plate_events(
        pipeline,
        [
            (
                timestamp(),
                "A",
                [],
                "cam1",
                "event1",
                "plate",
            ),
        ],
    )

    result = pipeline._transform_plate_events()

    row = result.first()

    assert row["detected_plates"] == []

    assert row["detected_employee_ids"] == []


def test_transform_plate_events_supports_json_arrays(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    write_silver_table(
        pipeline,
        "plate_detections",
        PLATE_JSON_SCHEMA,
        [
            (
                timestamp(),
                "A",
                '[" ab123 ", "UNKNOWN"]',
                "cam1",
                "event1",
                "plate",
            ),
        ],
    )

    result = pipeline._transform_plate_events()

    row = result.first()

    assert row["detected_employee_ids"] == [
        "001",
        None,
    ]


def test_transform_plate_events_rejects_missing_columns(
    pipeline,
):

    write_silver_table(
        pipeline,
        "plate_detections",
        T.StructType([
            T.StructField(
                "event_id",
                T.StringType(),
                True,
            ),
        ]),
        [("event1",)],
    )

    with pytest.raises(
        ValueError,
        match="Missing plate columns",
    ):

        pipeline._transform_plate_events()


def test_transform_plate_events_rejects_unsupported_array_type(
    pipeline,
):

    unsupported_schema = T.StructType([
        T.StructField(
            "Time_stamp",
            T.TimestampType(),
            True,
        ),
        T.StructField("Zone", T.StringType(), True),
        T.StructField(
            "detected_plates",
            T.IntegerType(),
            True,
        ),
        T.StructField("camera_id", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("detector_type", T.StringType(), True),
    ])

    write_silver_table(
        pipeline,
        "plate_detections",
        unsupported_schema,
        [
            (
                timestamp(),
                "A",
                123,
                "cam1",
                "event1",
                "plate",
            ),
        ],
    )

    with pytest.raises(
        TypeError,
        match="Unsupported detected_plates data type",
    ):

        pipeline._transform_plate_events()


# ============================================================
# 7. MONITORING EVENTS TESTS
# ============================================================

def test_monitoring_events_combine_person_and_plate(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_person_events(
        pipeline,
        [
            (
                timestamp(),
                "A",
                ["001"],
                [True],
                "cam1",
                "event1",
                "person",
            ),
        ],
    )

    create_plate_events(
        pipeline,
        [
            (
                timestamp(1),
                "E",
                ["AB123"],
                "cam8",
                "event2",
                "plate",
            ),
        ],
    )

    result = pipeline.transform_monitoring_events()

    assert result.count() == 2

    sources = {
        row["detection_source"]
        for row in result.collect()
    }

    assert sources == {
        "person",
        "plate",
    }

    assert pipeline.spark.catalog.tableExists(
        f"{pipeline.target_database}.monitoring_events"
    )



def test_monitoring_events_append_new_events_without_replacing_history(
    pipeline,
):
    create_employees(pipeline, [("001", "AB123")])

    first_rows = [
        (
            timestamp(),
            "A",
            ["001"],
            [True],
            "cam1",
            "person-event-001",
            "person",
        ),
    ]
    create_person_events(pipeline, first_rows)
    create_plate_events(pipeline, [])

    assert pipeline.transform_monitoring_events().count() == 1

    second_rows = first_rows + [
        (
            timestamp(1),
            "B",
            ["001"],
            [False],
            "cam2",
            "person-event-002",
            "person",
        ),
    ]
    create_person_events(pipeline, second_rows)

    assert pipeline.transform_monitoring_events().count() == 2
    pipeline.transform_monitoring_events()

    stored = pipeline.spark.table(
        f"{pipeline.target_database}.monitoring_events"
    )
    assert stored.count() == 2
    assert {
        row.event_id for row in stored.select("event_id").collect()
    } == {"person-event-001", "person-event-002"}


def test_monitoring_events_person_schema(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_person_events(
        pipeline,
        [
            (
                timestamp(),
                "A",
                ["001"],
                [True],
                "cam1",
                "event1",
                "person",
            ),
        ],
    )

    create_plate_events(
        pipeline,
        [],
    )

    result = pipeline.transform_monitoring_events()

    person_event = result.first()

    assert person_event["detection_source"] == "person"

    assert person_event["detected_employee_ids"] == ["001"]

    assert person_event["detected_plates"] is None

    assert person_event["authorized"] == [True]


def test_monitoring_events_reject_missing_person_columns(
    pipeline,
):

    write_silver_table(
        pipeline,
        "person_detections",
        T.StructType([
            T.StructField(
                "event_id",
                T.StringType(),
                True,
            ),
        ]),
        [("event1",)],
    )

    with pytest.raises(
        ValueError,
        match="Missing person columns",
    ):

        pipeline.transform_monitoring_events()


# ============================================================
# 8. MONITORING DETECTION DETAILS TESTS
# ============================================================

def test_monitoring_detection_details_combine_sources(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_person_details(
        pipeline,
        [
            (
                timestamp(),
                "A",
                "cam1",
                "event1",
                "person",
                "001",
                True,
            ),
        ],
    )

    create_plate_events(
        pipeline,
        [
            (
                timestamp(1),
                "E",
                ["AB123", "UNKNOWN"],
                "cam8",
                "event2",
                "plate",
            ),
        ],
    )

    result = pipeline.transform_monitoring_detection_details()

    assert result.count() == 3

    assert pipeline.spark.catalog.tableExists(
        f"{pipeline.target_database}."
        "monitoring_detection_details"
    )

    sources = [
        row["detection_source"]
        for row in result.collect()
    ]

    assert sources.count("person") == 1

    assert sources.count("plate") == 2



def test_monitoring_detection_details_are_idempotent_for_nullable_keys(
    pipeline,
):
    create_employees(pipeline, [("001", "AB123")])

    create_person_details(
        pipeline,
        [
            (
                timestamp(),
                "A",
                "cam1",
                "person-event-001",
                "person",
                "001",
                True,
            ),
        ],
    )
    create_plate_events(
        pipeline,
        [
            (
                timestamp(1),
                "E",
                ["AB123", "UNKNOWN"],
                "cam8",
                "plate-event-001",
                "plate",
            ),
        ],
    )

    assert pipeline.transform_monitoring_detection_details().count() == 3
    pipeline.transform_monitoring_detection_details()

    stored = pipeline.spark.table(
        f"{pipeline.target_database}.monitoring_detection_details"
    )
    assert stored.count() == 3

    unknown_plate = stored.filter(
        (F.col("detection_source") == "plate")
        & (F.col("plate_number") == "UNKNOWN")
    )
    assert unknown_plate.count() == 1


def test_monitoring_detection_details_person_record(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_person_details(
        pipeline,
        [
            (
                timestamp(),
                "A",
                "cam1",
                "event1",
                "person",
                "001",
                True,
            ),
        ],
    )

    create_plate_events(
        pipeline,
        [],
    )

    result = pipeline.transform_monitoring_detection_details()

    person = result.first()

    assert person["employee_id"] == "001"

    assert person["plate_number"] is None

    assert person["authorized"] is True

    assert person["detection_source"] == "person"


def test_monitoring_detection_details_preserve_unknown_plates(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_person_details(
        pipeline,
        [
            (
                timestamp(),
                "A",
                "cam1",
                "event1",
                "person",
                "001",
                True,
            ),
        ],
    )

    create_plate_events(
        pipeline,
        [
            (
                timestamp(1),
                "E",
                ["AB123", "UNKNOWN"],
                "cam8",
                "event2",
                "plate",
            ),
        ],
    )

    result = pipeline.transform_monitoring_detection_details()

    unknown = (
        result
        .filter(
            (F.col("detection_source") == "plate")
            & (F.col("plate_number") == "UNKNOWN")
        )
        .first()
    )

    assert unknown is not None

    assert unknown["employee_id"] is None

    assert unknown["authorized"] is None


def test_monitoring_detection_details_preserve_empty_plate_events(
    pipeline,
):

    create_employees(
        pipeline,
        [
            ("001", "AB123"),
        ],
    )

    create_person_details(
        pipeline,
        [
            (
                timestamp(),
                "A",
                "cam1",
                "event1",
                "person",
                "001",
                True,
            ),
        ],
    )

    create_plate_events(
        pipeline,
        [
            (
                timestamp(1),
                "E",
                [],
                "cam8",
                "event2",
                "plate",
            ),
        ],
    )

    result = pipeline.transform_monitoring_detection_details()

    assert result.count() == 1

    assert result.first()["detection_source"] == "person"


def test_monitoring_detection_details_reject_missing_columns(
    pipeline,
):

    write_silver_table(
        pipeline,
        "person_detection_details",
        T.StructType([
            T.StructField(
                "employee_id",
                T.StringType(),
                True,
            ),
        ]),
        [("001",)],
    )

    with pytest.raises(
        ValueError,
        match="Missing person detail columns",
    ):

        pipeline.transform_monitoring_detection_details()


def test_monitoring_detection_details_reject_array_misalignment(
    pipeline,
    monkeypatch,
):

    create_person_details(
        pipeline,
        [],
    )

    mismatched_schema = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("Zone", T.StringType(), True),
        T.StructField("camera_id", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("detector_type", T.StringType(), True),
        T.StructField("detection_source", T.StringType(), True),
        T.StructField(
            "detected_plates",
            T.ArrayType(T.StringType()),
            True,
        ),
        T.StructField(
            "detected_employee_ids",
            T.ArrayType(T.StringType()),
            True,
        ),
        T.StructField(
            "authorized",
            T.ArrayType(T.BooleanType()),
            True,
        ),
    ])

    mismatched_df = pipeline.spark.createDataFrame(
        [
            (
                timestamp(),
                "E",
                "cam8",
                "event1",
                "plate",
                "plate",
                ["AB123", "CD456"],
                ["001"],
                None,
            ),
        ],
        mismatched_schema,
    )

    monkeypatch.setattr(
        pipeline,
        "_transform_plate_events",
        lambda: mismatched_df,
    )

    with pytest.raises(
        ValueError,
        match="Plate and employee ID arrays are not aligned",
    ):

        pipeline.transform_monitoring_detection_details()


# ============================================================
# 9. RESTRICTED ACCESS TESTS
# ============================================================

def test_restricted_access_success(pipeline):

    write_silver_table(
        pipeline,
        "restricted_access",
        RESTRICTED_ACCESS_SCHEMA,
        [
            ("001", "A"),
            ("002", "B"),
            ("003", "C"),
        ],
    )

    result = pipeline.transform_restricted_access()

    assert result.count() == 3

    assert pipeline.spark.catalog.tableExists(
        f"{pipeline.target_database}.restricted_access"
    )

    records = {
        (row["employee_id"], row["zone_id"])
        for row in result.collect()
    }

    assert records == {
        ("001", "A"),
        ("002", "B"),
        ("003", "C"),
    }


def test_restricted_access_rejects_missing_source(
    pipeline,
):

    with pytest.raises(
        ValueError,
        match="Silver table does not exist",
    ):

        pipeline.transform_restricted_access()


# ============================================================
# 10. PIPELINE ORCHESTRATION TESTS
# ============================================================

def test_run_executes_all_transformations(
    pipeline,
    monkeypatch,
):

    employees = Mock()

    events = Mock()

    details = Mock()

    restricted = Mock()

    monkeypatch.setattr(
        pipeline,
        "transform_employee_directory",
        employees,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_monitoring_events",
        events,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_monitoring_detection_details",
        details,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_restricted_access",
        restricted,
    )

    pipeline.run()

    employees.assert_called_once()

    events.assert_called_once()

    details.assert_called_once()

    restricted.assert_called_once()


def test_run_executes_transformations_in_correct_order(
    pipeline,
    monkeypatch,
):

    execution_order = []

    monkeypatch.setattr(
        pipeline,
        "transform_employee_directory",
        lambda: execution_order.append("employees"),
    )

    monkeypatch.setattr(
        pipeline,
        "transform_monitoring_events",
        lambda: execution_order.append("events"),
    )

    monkeypatch.setattr(
        pipeline,
        "transform_monitoring_detection_details",
        lambda: execution_order.append("details"),
    )

    monkeypatch.setattr(
        pipeline,
        "transform_restricted_access",
        lambda: execution_order.append("restricted"),
    )

    pipeline.run()

    assert execution_order == [
        "employees",
        "events",
        "details",
        "restricted",
    ]


def test_run_stops_when_transformation_fails(
    pipeline,
    monkeypatch,
):

    employees = Mock(
        side_effect=ValueError(
            "Invalid employee source"
        )
    )

    events = Mock()

    monkeypatch.setattr(
        pipeline,
        "transform_employee_directory",
        employees,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_monitoring_events",
        events,
    )

    with pytest.raises(
        ValueError,
        match="Invalid employee source",
    ):

        pipeline.run()

    employees.assert_called_once()

    events.assert_not_called()

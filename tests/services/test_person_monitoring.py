
"""
Tests for PersonMonitoringService.

Covers:
    - Service initialization
    - Employee monitoring history
    - Employee history filtering
    - Timestamp validation
    - Recent history selection
    - Employee directory retrieval
    - Employee and plate searching
    - Employee detection summaries
    - Person and plate observation separation
    - Gold table schema validation
    - Input validation and edge cases
"""

import uuid

from datetime import datetime

import pytest

from pyspark.sql import types as T

from advanced.config.spark_session import create_spark_session

from advanced.ETL.analytics.services.person_monitoring import (
    PersonMonitoringService,
)


# ============================================================
# 1. CONSTANTS AND TEST SCHEMAS
# ============================================================

MONITORING_DETAILS_SCHEMA = T.StructType([
    T.StructField("timestamp", T.TimestampType(), True),
    T.StructField("Zone", T.StringType(), True),
    T.StructField("camera_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("detector_type", T.StringType(), True),
    T.StructField("detection_source", T.StringType(), True),
    T.StructField("employee_id", T.StringType(), True),
    T.StructField("plate_number", T.StringType(), True),
    T.StructField("authorized", T.BooleanType(), True),
])


EMPLOYEE_DIRECTORY_SCHEMA = T.StructType([
    T.StructField("employee_id", T.StringType(), True),
    T.StructField("plate_number", T.StringType(), True),
])


# ============================================================
# 2. DETERMINISTIC TEST DATA
# ============================================================

MONITORING_ROWS = [

    # --------------------------------------------------------
    # EMPLOYEE 001: PERSON OBSERVATIONS
    # --------------------------------------------------------

    (
        datetime(2026, 11, 9, 9, 0),
        "A",
        "cam-01",
        "ev-001",
        "synthetic",
        "person",
        "EMP-001",
        None,
        True,
    ),

    (
        datetime(2026, 11, 9, 10, 0),
        "A",
        "cam-01",
        "ev-002",
        "synthetic",
        "person",
        "EMP-001",
        None,
        True,
    ),

    (
        datetime(2026, 11, 9, 12, 0),
        "B",
        "cam-02",
        "ev-004",
        "synthetic",
        "person",
        "EMP-001",
        None,
        False,
    ),

    (
        datetime(2026, 11, 9, 13, 0),
        "C",
        "cam-03",
        "ev-005",
        "synthetic",
        "person",
        "EMP-001",
        None,
        True,
    ),

    # --------------------------------------------------------
    # EMPLOYEE 001: PLATE OBSERVATION
    # --------------------------------------------------------

    (
        datetime(2026, 11, 9, 11, 0),
        "E",
        "cam-02",
        "ev-003",
        "synthetic",
        "plate",
        "EMP-001",
        "ABC123",
        None,
    ),

    # --------------------------------------------------------
    # EMPLOYEE 002: PERSON OBSERVATION
    # --------------------------------------------------------

    (
        datetime(2026, 11, 9, 10, 30),
        "A",
        "cam-04",
        "ev-006",
        "synthetic",
        "person",
        "EMP-002",
        None,
        True,
    ),

    # --------------------------------------------------------
    # EMPLOYEE 002: PLATE OBSERVATION
    # --------------------------------------------------------

    (
        datetime(2026, 11, 9, 11, 30),
        "E",
        "cam-05",
        "ev-007",
        "synthetic",
        "plate",
        "EMP-002",
        "XYZ789",
        None,
    ),
]


EMPLOYEE_DIRECTORY_ROWS = [
    ("EMP-003", "QWE456"),
    ("EMP-001", "ABC123"),
    ("EMP-002", "XYZ789"),
    ("EMP-004", None),
]


# ============================================================
# 3. FIXTURES
# ============================================================

@pytest.fixture(scope="session")
def spark():
    """Create the project's centralized SparkSession."""

    session = create_spark_session()

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def gold_database(spark):
    """Create an isolated Gold database for each test."""

    database_name = (
        f"gold_person_test_{uuid.uuid4().hex[:10]}"
    )

    spark.sql(
        f"CREATE DATABASE `{database_name}`"
    )

    yield database_name

    spark.sql(
        f"DROP DATABASE IF EXISTS `{database_name}` CASCADE"
    )


@pytest.fixture
def person_monitoring_service(
    spark,
    gold_database,
):
    """Create PersonMonitoringService with isolated Gold tables."""

    monitoring_df = spark.createDataFrame(
        MONITORING_ROWS,
        schema=MONITORING_DETAILS_SCHEMA,
    )

    directory_df = spark.createDataFrame(
        EMPLOYEE_DIRECTORY_ROWS,
        schema=EMPLOYEE_DIRECTORY_SCHEMA,
    )

    (
        monitoring_df.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{gold_database}.monitoring_detection_details"
        )
    )

    (
        directory_df.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{gold_database}.employee_directory"
        )
    )

    return PersonMonitoringService(
        spark=spark,
        gold_database=gold_database,
    )


# ============================================================
# 4. INITIALIZATION TESTS
# ============================================================

def test_person_monitoring_initialization(
    person_monitoring_service,
    gold_database,
):
    """Verify service initialization."""

    assert person_monitoring_service is not None

    assert (
        person_monitoring_service.gold_database
        == gold_database
    )

    assert (
        person_monitoring_service.monitoring_details_table
        == f"{gold_database}.monitoring_detection_details"
    )


# ============================================================
# 5. EMPLOYEE HISTORY TESTS
# ============================================================

def test_employee_history_with_multiple_filters(
    person_monitoring_service,
):
    """
    Verify employee history with simultaneous filtering.

    Filters:
        Employee: EMP-001
        Time: 10:00 to 12:00
        Camera: cam-02
        Zone: E
        Source: plate

    Expected:
        Only ev-003.
    """

    result = (
        person_monitoring_service
        .get_employee_history(
            employee_id="EMP-001",
            start_time="2026-11-09 10:00:00",
            end_time="2026-11-09 12:00:00",
            camera_id="cam-02",
            zone="E",
            detection_source="plate",
            limit=50,
        )
        .collect()
    )

    assert len(result) == 1

    assert result[0]["event_id"] == "ev-003"

    assert result[0]["employee_id"] == "EMP-001"

    assert result[0]["detection_source"] == "plate"

    assert result[0]["plate_number"] == "ABC123"


def test_employee_history_returns_person_and_plate_observations(
    person_monitoring_service,
):
    """Keep person and registered-plate observations distinct."""

    result = (
        person_monitoring_service
        .get_employee_history(
            employee_id="EMP-001",
            limit=50,
        )
        .collect()
    )

    sources = {
        row["detection_source"]
        for row in result
    }

    assert sources == {"person", "plate"}

    person_count = sum(
        row["detection_source"] == "person"
        for row in result
    )

    plate_count = sum(
        row["detection_source"] == "plate"
        for row in result
    )

    assert person_count == 4

    assert plate_count == 1


def test_employee_history_normalizes_string_filters(
    person_monitoring_service,
):
    """Trim employee IDs and optional string filters."""

    result = (
        person_monitoring_service
        .get_employee_history(
            employee_id=" EMP-001 ",
            start_time="2026-11-09 10:00:00",
            end_time="2026-11-09 11:00:00",
            camera_id=" cam-01 ",
            zone=" A ",
            detection_source=" PERSON ",
        )
        .collect()
    )

    assert len(result) == 1

    assert result[0]["event_id"] == "ev-002"


def test_employee_history_returns_latest_limit_in_chronological_order(
    person_monitoring_service,
):
    """
    Verify that the latest N observations are selected first.

    The selected records should then be displayed chronologically.

    This is a regression test for the existing implementation,
    which currently sorts ascending before applying the limit.
    """

    result = (
        person_monitoring_service
        .get_employee_history(
            employee_id="EMP-001",
            limit=2,
        )
        .collect()
    )

    actual_event_ids = [
        row["event_id"]
        for row in result
    ]

    expected_event_ids = [
        "ev-004",
        "ev-005",
    ]

    assert actual_event_ids == expected_event_ids

    timestamps = [
        row["timestamp"]
        for row in result
    ]

    assert timestamps == sorted(timestamps)


# ============================================================
# 6. EMPLOYEE HISTORY INPUT VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "employee_id",
    [
        "",
        " ",
        None,
        123,
    ],
)
def test_employee_history_rejects_invalid_employee_id(
    person_monitoring_service,
    employee_id,
):
    """Reject invalid employee identifiers."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id=employee_id,
        )


@pytest.mark.parametrize(
    "limit",
    [
        0,
        -1,
        True,
        1.5,
        "50",
        None,
    ],
)
def test_employee_history_rejects_invalid_limits(
    person_monitoring_service,
    limit,
):
    """Reject invalid history limits."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            limit=limit,
        )


def test_employee_history_rejects_invalid_detection_source(
    person_monitoring_service,
):
    """Reject unsupported detection sources."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            detection_source="vehicle",
        )


def test_employee_history_rejects_non_string_detection_source(
    person_monitoring_service,
):
    """Reject invalid detection-source types."""

    with pytest.raises(TypeError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            detection_source=123,
        )


@pytest.mark.parametrize(
    "filter_name",
    [
        "camera_id",
        "zone",
    ],
)
def test_employee_history_rejects_empty_optional_filters(
    person_monitoring_service,
    filter_name,
):
    """Reject empty camera and zone filters."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            **{filter_name: " "},
        )


def test_employee_history_rejects_non_string_timestamp(
    person_monitoring_service,
):
    """Reject non-string timestamp inputs."""

    with pytest.raises(TypeError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            start_time=20261109,
        )


def test_employee_history_rejects_invalid_timestamp_format(
    person_monitoring_service,
):
    """Require YYYY-MM-DD HH:MM:SS timestamp formatting."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            start_time="09-11-2026",
        )


def test_employee_history_rejects_reversed_time_window(
    person_monitoring_service,
):
    """Reject start timestamps later than end timestamps."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            start_time="2026-11-09 13:00:00",
            end_time="2026-11-09 10:00:00",
        )


def test_employee_history_rejects_equal_time_boundaries(
    person_monitoring_service,
):
    """Reject zero-duration time windows."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_history(
            employee_id="EMP-001",
            start_time="2026-11-09 10:00:00",
            end_time="2026-11-09 10:00:00",
        )


# ============================================================
# 7. EMPLOYEE DIRECTORY TESTS
# ============================================================

def test_employee_directory_returns_sorted_results(
    person_monitoring_service,
):
    """Verify employee directory ordering."""

    result = (
        person_monitoring_service
        .get_employee_directory()
        .collect()
    )

    employee_ids = [
        row["employee_id"]
        for row in result
    ]

    assert employee_ids == [
        "EMP-001",
        "EMP-002",
        "EMP-003",
        "EMP-004",
    ]


def test_employee_directory_searches_employee_id(
    person_monitoring_service,
):
    """Search employees by employee ID."""

    result = (
        person_monitoring_service
        .get_employee_directory(
            search_term="emp-002",
        )
        .collect()
    )

    assert len(result) == 1

    assert result[0]["employee_id"] == "EMP-002"


def test_employee_directory_searches_plate_number(
    person_monitoring_service,
):
    """Search employees by registered plate number."""

    result = (
        person_monitoring_service
        .get_employee_directory(
            search_term="xyz789",
        )
        .collect()
    )

    assert len(result) == 1

    assert result[0]["employee_id"] == "EMP-002"

    assert result[0]["plate_number"] == "XYZ789"


def test_employee_directory_respects_limit(
    person_monitoring_service,
):
    """Verify the directory result limit."""

    result = (
        person_monitoring_service
        .get_employee_directory(
            limit=2,
        )
        .collect()
    )

    assert len(result) == 2

    assert [
        row["employee_id"]
        for row in result
    ] == [
        "EMP-001",
        "EMP-002",
    ]


def test_employee_directory_returns_empty_result_for_no_match(
    person_monitoring_service,
):
    """Return an empty DataFrame when the search finds no match."""

    result = (
        person_monitoring_service
        .get_employee_directory(
            search_term="UNKNOWN-EMPLOYEE",
        )
        .collect()
    )

    assert len(result) == 0


def test_employee_directory_rejects_empty_search(
    person_monitoring_service,
):
    """Reject an empty search term."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_directory(
            search_term=" ",
        )


def test_employee_directory_rejects_invalid_search_type(
    person_monitoring_service,
):
    """Reject a non-string search term."""

    with pytest.raises(TypeError):
        person_monitoring_service.get_employee_directory(
            search_term=123,
        )


@pytest.mark.parametrize(
    "limit",
    [
        0,
        -1,
        True,
        1.5,
        "100",
        None,
    ],
)
def test_employee_directory_rejects_invalid_limits(
    person_monitoring_service,
    limit,
):
    """Reject invalid directory limits."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_directory(
            limit=limit,
        )


# ============================================================
# 8. EMPLOYEE DETECTION SUMMARY TESTS
# ============================================================

def test_employee_detection_summary(
    person_monitoring_service,
):
    """
    Verify employee detection statistics.

    EMP-001 expected:
        Person detections: 4
        Plate detections: 1
        Distinct person cameras: 3
        Distinct person zones: 3
        First person observation: 09:00
        Last person observation: 13:00
        First plate observation: 11:00
        Last plate observation: 11:00
    """

    result = (
        person_monitoring_service
        .get_employee_detection_summary(
            employee_id="EMP-001",
        )
        .first()
        .asDict()
    )

    assert result["employee_id"] == "EMP-001"

    assert result["person_detection_count"] == 4

    assert result["plate_detection_count"] == 1

    assert result["distinct_person_cameras"] == 3

    assert result["distinct_person_zones"] == 3

    assert result["first_person_detection"] == datetime(
        2026, 11, 9, 9, 0
    )

    assert result["last_person_detection"] == datetime(
        2026, 11, 9, 13, 0
    )

    assert result["first_plate_detection"] == datetime(
        2026, 11, 9, 11, 0
    )

    assert result["last_plate_detection"] == datetime(
        2026, 11, 9, 11, 0
    )


def test_employee_detection_summary_strips_employee_id(
    person_monitoring_service,
):
    """Normalize whitespace around the employee identifier."""

    result = (
        person_monitoring_service
        .get_employee_detection_summary(
            employee_id=" EMP-001 ",
        )
        .first()
    )

    assert result["employee_id"] == "EMP-001"

    assert result["person_detection_count"] == 4


def test_employee_detection_summary_for_unknown_employee(
    person_monitoring_service,
):
    """Return zero counts and null timestamps for an unknown employee."""

    result = (
        person_monitoring_service
        .get_employee_detection_summary(
            employee_id="EMP-999",
        )
        .first()
    )

    assert result["employee_id"] == "EMP-999"

    assert result["person_detection_count"] == 0

    assert result["plate_detection_count"] == 0

    assert result["distinct_person_cameras"] == 0

    assert result["distinct_person_zones"] == 0

    assert result["first_person_detection"] is None

    assert result["last_person_detection"] is None

    assert result["first_plate_detection"] is None

    assert result["last_plate_detection"] is None


def test_employee_detection_summary_rejects_invalid_employee_id(
    person_monitoring_service,
):
    """Reject empty employee identifiers."""

    with pytest.raises(ValueError):
        person_monitoring_service.get_employee_detection_summary(
            employee_id=" ",
        )


# ============================================================
# 9. GOLD SCHEMA VALIDATION TESTS
# ============================================================

def test_history_rejects_incomplete_gold_schema(
    spark,
    gold_database,
):
    """Reject a monitoring table missing required history columns."""

    incomplete_schema = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("employee_id", T.StringType(), True),
        T.StructField("Zone", T.StringType(), True),
        T.StructField("camera_id", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("detector_type", T.StringType(), True),
        T.StructField("detection_source", T.StringType(), True),
        T.StructField("plate_number", T.StringType(), True),
    ])

    dataframe = spark.createDataFrame(
        [],
        schema=incomplete_schema,
    )

    (
        dataframe.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{gold_database}.monitoring_detection_details"
        )
    )

    service = PersonMonitoringService(
        spark=spark,
        gold_database=gold_database,
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        service.get_employee_history(
            employee_id="EMP-001",
        )


def test_employee_directory_rejects_incomplete_schema(
    spark,
    gold_database,
):
    """Reject an employee directory missing required columns."""

    incomplete_schema = T.StructType([
        T.StructField("employee_id", T.StringType(), True),
    ])

    dataframe = spark.createDataFrame(
        [],
        schema=incomplete_schema,
    )

    (
        dataframe.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{gold_database}.employee_directory"
        )
    )

    service = PersonMonitoringService(
        spark=spark,
        gold_database=gold_database,
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        service.get_employee_directory()


def test_employee_summary_rejects_incomplete_schema(
    spark,
    gold_database,
):
    """Reject a monitoring table missing summary columns."""

    incomplete_schema = T.StructType([
        T.StructField("employee_id", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("camera_id", T.StringType(), True),
        T.StructField("Zone", T.StringType(), True),
    ])

    dataframe = spark.createDataFrame(
        [],
        schema=incomplete_schema,
    )

    (
        dataframe.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{gold_database}.monitoring_detection_details"
        )
    )

    service = PersonMonitoringService(
        spark=spark,
        gold_database=gold_database,
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        service.get_employee_detection_summary(
            employee_id="EMP-001",
        )

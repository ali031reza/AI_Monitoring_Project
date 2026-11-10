"""
Tests for DashboardService.

Covers:
    - Initialization and database validation
    - Gold table existence and schema validation
    - UTC timestamp handling and window boundaries
    - Person and plate KPI correctness
    - Hourly and zone-level aggregations
    - Recent activity ordering and limits
    - Empty-result handling

Test data uses timezone-aware UTC datetimes. This is important because
PySpark interprets timezone-naive Python datetimes using the host's
local timezone when creating TimestampType values.
"""

from __future__ import annotations

import uuid

from datetime import datetime, timedelta, timezone

import pytest

from pyspark.sql import functions as F
from pyspark.sql import types as T

from advanced.config.spark_session import create_spark_session
from advanced.ETL.analytics.services.dashboard_service import (
    DashboardService,
)


# ============================================================
# 1. CONSTANTS AND TEST SCHEMA
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


def utc_timestamp(hour: int, minute: int = 0) -> datetime:
    """Return a timezone-aware UTC timestamp for the test day."""

    return datetime(
        2026, 11, 9, hour, minute, tzinfo=timezone.utc
    )


START_TIME = utc_timestamp(10)
END_TIME = utc_timestamp(13)

MONITORING_ROWS = [
    # Person observations: two events for EMP-001, one for EMP-002.
    (
        utc_timestamp(10, 5), "A", "cam-01", "person-ev-001",
        "synthetic", "person", "EMP-001", None, True,
    ),
    (
        utc_timestamp(10, 5), "A", "cam-01", "person-ev-002",
        "synthetic", "person", "EMP-001", None, True,
    ),
    (
        utc_timestamp(11, 15), "B", "cam-02", "person-ev-003",
        "synthetic", "person", "EMP-002", None, False,
    ),

    # EMP-003 appears only through a registered plate association.
    # It must NOT increase the distinct-person KPI.
    (
        utc_timestamp(10, 20), "E", "cam-03", "plate-ev-001",
        "synthetic", "plate", "EMP-003", "ABC123", None,
    ),

    # Unknown license plates remain valid plate observations.
    (
        utc_timestamp(12), "E", "cam-04", "plate-ev-002",
        "synthetic", "plate", None, "XYZ789", None,
    ),

    # The following two rows fall outside [START_TIME, END_TIME).
    (
        utc_timestamp(9, 59), "A", "cam-01", "outside-before",
        "synthetic", "person", "EMP-004", None, True,
    ),
    (
        utc_timestamp(13), "C", "cam-05", "outside-end",
        "synthetic", "person", "EMP-005", None, True,
    ),
]


# ============================================================
# 2. FIXTURES AND HELPERS
# ============================================================

@pytest.fixture(scope="session")
def spark():
    """Reuse the project's UTC-configured Hive-enabled SparkSession."""

    session = create_spark_session()
    session.sparkContext.setLogLevel("ERROR")

    assert session.conf.get("spark.sql.session.timeZone") == "UTC"

    yield session

    session.stop()


@pytest.fixture
def gold_database(spark):
    """Create and later remove a uniquely named test-only database."""

    name = f"gold_dashboard_test_{uuid.uuid4().hex[:10]}"

    spark.sql(f"CREATE DATABASE `{name}`")

    try:
        yield name
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def write_monitoring_table(
    spark,
    database_name: str,
    rows,
    schema=MONITORING_DETAILS_SCHEMA,
) -> None:
    """Write a small Parquet table using the production Gold table name."""

    dataframe = spark.createDataFrame(rows, schema=schema)

    (
        dataframe.write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(
            f"{database_name}.monitoring_detection_details"
        )
    )


@pytest.fixture
def dashboard_service(spark, gold_database):
    """Build a service without creating the monitoring table."""

    return DashboardService(
        spark=spark,
        gold_database=gold_database,
    )


@pytest.fixture
def populated_dashboard_service(spark, gold_database):
    """Build a service against deterministic UTC Gold observations."""

    write_monitoring_table(
        spark,
        gold_database,
        MONITORING_ROWS,
    )

    return DashboardService(
        spark=spark,
        gold_database=gold_database,
    )


def collect_kpis(service, start_time=START_TIME, end_time=END_TIME) -> dict:
    """Collect one KPI row, formatting its timestamp inside UTC Spark."""

    row = (
        service.get_detection_kpis(start_time, end_time)
        .select(
            "person_observation_count",
            "plate_observation_count",
            "distinct_employees_observed",
            "cameras_with_activity",
            F.date_format(
                "latest_detection_at",
                "yyyy-MM-dd HH:mm:ss",
            ).alias("latest_detection_utc"),
        )
        .first()
    )

    return row.asDict()


# ============================================================
# 3. INITIALIZATION AND CONFIGURATION
# ============================================================

def test_dashboard_service_initialization(dashboard_service, gold_database):
    """The service uses the injected session and Gold database."""

    assert dashboard_service.gold_database == gold_database
    assert dashboard_service.monitoring_details_table == (
        f"{gold_database}.monitoring_detection_details"
    )


def test_initialization_requires_valid_spark_session():
    """An invalid Spark dependency is rejected immediately."""

    with pytest.raises(TypeError):
        DashboardService(spark=None)


@pytest.mark.parametrize(
    "database_name",
    ["", "gold-test", "123gold", "gold.test", "gold test"],
)
def test_initialization_rejects_invalid_database_names(spark, database_name):
    """Database names follow the same identifier rules as Gold ETL."""

    with pytest.raises(ValueError):
        DashboardService(spark=spark, gold_database=database_name)


# ============================================================
# 4. TIMESTAMP AND TIME-WINDOW VALIDATION
# ============================================================

def test_utc_timestamp_normalization():
    """Offset-aware timestamps are correctly converted to UTC."""

    original = datetime(
        2026, 11, 9, 12, 0,
        tzinfo=timezone(timedelta(hours=2)),
    )

    result = DashboardService._as_utc_naive(original, "timestamp")

    assert result == datetime(2026, 11, 9, 10, 0)
    assert result.tzinfo is None


def test_timestamp_normalization_rejects_naive_datetime():
    """Reject timestamps whose timezone is unknown."""

    with pytest.raises(ValueError, match="timezone-aware"):
        DashboardService._as_utc_naive(
            datetime(2026, 11, 9, 10, 0),
            "timestamp",
        )


def test_timestamp_normalization_rejects_invalid_type():
    """Reject non-datetime timestamp inputs."""

    with pytest.raises(TypeError, match="datetime"):
        DashboardService._as_utc_naive(
            "2026-11-09 10:00:00",
            "timestamp",
        )


def test_time_window_rejects_naive_datetimes(populated_dashboard_service):
    """Time-window boundaries must be timezone-aware."""

    with pytest.raises(ValueError, match="timezone-aware"):
        populated_dashboard_service.get_detection_kpis(
            datetime(2026, 11, 9, 10, 0),
            datetime(2026, 11, 9, 13, 0),
        )


def test_time_window_rejects_invalid_order(populated_dashboard_service):
    """The start of the window must precede its end."""

    with pytest.raises(ValueError, match="earlier"):
        populated_dashboard_service.get_detection_kpis(
            END_TIME, START_TIME
        )


def test_time_window_rejects_equal_timestamps(populated_dashboard_service):
    """A zero-length window is invalid."""

    with pytest.raises(ValueError, match="earlier"):
        populated_dashboard_service.get_detection_kpis(
            START_TIME, START_TIME
        )


def test_offset_time_window_matches_utc(populated_dashboard_service):
    """Equivalent UTC and offset-aware windows select the same records."""

    offset = timezone(timedelta(hours=3, minutes=30))

    start_offset = START_TIME.astimezone(offset)
    end_offset = END_TIME.astimezone(offset)

    assert collect_kpis(
        populated_dashboard_service,
        start_offset,
        end_offset,
    ) == collect_kpis(populated_dashboard_service)


# ============================================================
# 5. GOLD TABLE VALIDATION
# ============================================================

def test_missing_gold_table_raises_runtime_error(dashboard_service):
    """A missing Gold table produces an actionable error."""

    with pytest.raises(RuntimeError, match="Gold table is unavailable"):
        dashboard_service.get_detection_kpis(START_TIME, END_TIME)


def test_missing_required_columns_raise_value_error(spark, gold_database):
    """Reject a monitoring table that violates the required schema."""

    incomplete_schema = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("camera_id", T.StringType(), True),
        T.StructField("detection_source", T.StringType(), True),
    ])

    write_monitoring_table(
        spark,
        gold_database,
        rows=[],
        schema=incomplete_schema,
    )

    service = DashboardService(spark=spark, gold_database=gold_database)

    with pytest.raises(ValueError, match="missing required columns"):
        service.get_detection_kpis(START_TIME, END_TIME)


# ============================================================
# 6. DASHBOARD KPIS
# ============================================================

def test_detection_kpis(populated_dashboard_service):
    """KPIs count detection rows and distinct person observations."""

    result = collect_kpis(populated_dashboard_service)

    assert result["person_observation_count"] == 3
    assert result["plate_observation_count"] == 2
    assert result["distinct_employees_observed"] == 2
    assert result["cameras_with_activity"] == 4
    assert result["latest_detection_utc"] == "2026-11-09 12:00:00"


def test_plate_employee_association_is_not_counted_as_person(
    populated_dashboard_service,
):
    """EMP-003 appears only in a plate record, not in person counts."""

    result = collect_kpis(populated_dashboard_service)

    assert result["plate_observation_count"] == 2
    assert result["distinct_employees_observed"] == 2


def test_kpis_respect_inclusive_start_exclusive_end(
    populated_dashboard_service,
):
    """Include the start time and exclude the end time exactly."""

    result = collect_kpis(
        populated_dashboard_service,
        start_time=utc_timestamp(10, 5),
        end_time=utc_timestamp(12),
    )

    assert result["person_observation_count"] == 3
    assert result["plate_observation_count"] == 1
    assert result["distinct_employees_observed"] == 2
    assert result["cameras_with_activity"] == 3
    assert result["latest_detection_utc"] == "2026-11-09 11:15:00"


def test_kpis_return_zero_for_empty_time_window(populated_dashboard_service):
    """A valid window without observations returns zero-valued KPIs."""

    result = collect_kpis(
        populated_dashboard_service,
        start_time=utc_timestamp(20),
        end_time=utc_timestamp(21),
    )

    assert result["person_observation_count"] == 0
    assert result["plate_observation_count"] == 0
    assert result["distinct_employees_observed"] == 0
    assert result["cameras_with_activity"] == 0
    assert result["latest_detection_utc"] is None


# ============================================================
# 7. HOURLY AND ZONE AGGREGATIONS
# ============================================================

def test_hourly_activity(populated_dashboard_service):
    """Spark groups records by UTC hour and detection source."""

    result = (
        populated_dashboard_service
        .get_hourly_activity(START_TIME, END_TIME)
        .select(
            F.date_format(
                "hour", "yyyy-MM-dd HH:mm:ss"
            ).alias("hour_utc"),
            "detection_source",
            "observation_count",
        )
        .collect()
    )

    actual = [
        (row["hour_utc"], row["detection_source"], row["observation_count"])
        for row in result
    ]

    expected = [
        ("2026-11-09 10:00:00", "person", 2),
        ("2026-11-09 10:00:00", "plate", 1),
        ("2026-11-09 11:00:00", "person", 1),
        ("2026-11-09 12:00:00", "plate", 1),
    ]

    assert actual == expected


def test_zone_activity(populated_dashboard_service):
    """Zone totals preserve person and plate source distinctions."""

    result = (
        populated_dashboard_service
        .get_zone_activity(START_TIME, END_TIME)
        .collect()
    )

    actual = [
        (row["Zone"], row["detection_source"], row["observation_count"])
        for row in result
    ]

    assert actual == [
        ("A", "person", 2),
        ("B", "person", 1),
        ("E", "plate", 2),
    ]


# ============================================================
# 8. RECENT ACTIVITY
# ============================================================

def test_recent_activity(populated_dashboard_service):
    """Return the latest records with deterministic same-time ordering."""

    result = (
        populated_dashboard_service
        .get_recent_activity(START_TIME, END_TIME, limit=5)
        .collect()
    )

    assert len(result) == 5

    assert [row["event_id"] for row in result] == [
        "plate-ev-002",
        "person-ev-003",
        "plate-ev-001",
        "person-ev-001",
        "person-ev-002",
    ]

    assert result[2]["employee_id"] == "EMP-003"
    assert result[2]["detection_source"] == "plate"
    assert result[0]["employee_id"] is None


@pytest.mark.parametrize(
    "limit",
    [0, -1, True, 1.5, "10", None],
)
def test_recent_activity_rejects_invalid_limits(
    populated_dashboard_service,
    limit,
):
    """Reject nonpositive, boolean and noninteger result limits."""

    with pytest.raises(ValueError, match="positive integer"):
        populated_dashboard_service.get_recent_activity(
            START_TIME,
            END_TIME,
            limit=limit,
        )

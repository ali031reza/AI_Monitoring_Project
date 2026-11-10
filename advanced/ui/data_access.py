"""Cached, read-only access to the project's Gold monitoring tables.

All dashboard observations are detection-detail rows, not camera frames.
The Spark session remains shared; materialized responses are deliberately
bounded before being converted to pandas for display in Streamlit.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from advanced.config.camera_loader import CameraConfig, load_cameras
from advanced.config.paths import ProjectPaths
from advanced.config.spark_session import create_spark_session
from advanced.ETL.analytics.services.dashboard_service import DashboardService
from advanced.ETL.analytics.services.person_monitoring import PersonMonitoringService


# ============================================================
# 1. CONFIGURATION AND SPARK RESOURCE LIFECYCLE
# ============================================================

TIME_RANGES = ("Last 24 hours", "Last 7 days", "Last 30 days")
_RANGE_DAYS = dict(zip(TIME_RANGES, (1, 7, 30)))
_GOLD_DETAILS_TABLE = "gold.monitoring_detection_details"
_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"


@st.cache_resource(show_spinner="Connecting to the Spark Gold layer…")
def get_spark_session():
    """Reuse the project's Hive-enabled SparkSession across UI reruns."""
    return create_spark_session()


@st.cache_resource(show_spinner=False)
def get_dashboard_service() -> DashboardService:
    return DashboardService(get_spark_session())


@st.cache_resource(show_spinner=False)
def get_person_service() -> PersonMonitoringService:
    return PersonMonitoringService(get_spark_session())


@st.cache_data(ttl=300, show_spinner=False)
def get_camera_configuration() -> tuple[CameraConfig, ...]:
    """Load validated camera inventory from the configured source file."""
    path = ProjectPaths.discover().cameras_csv
    return load_cameras(path)


def _utc_window(time_range: str) -> tuple[datetime, datetime]:
    """Validate a named, rolling UTC window."""
    if time_range not in _RANGE_DAYS:
        raise ValueError(f"Unsupported time range: {time_range!r}")
    end = datetime.now(timezone.utc)
    return end - timedelta(days=_RANGE_DAYS[time_range]), end


def _spark_utc(value: datetime):
    """Build offset-bearing Spark literals, independent of Windows timezone."""
    return F.to_timestamp(F.lit(value.astimezone(timezone.utc).isoformat()))


def _window_details(time_range: str) -> tuple[DataFrame, datetime, datetime]:
    """Read detection details in a half-open UTC time window."""
    start, end = _utc_window(time_range)
    spark = get_spark_session()
    if not spark.catalog.tableExists(_GOLD_DETAILS_TABLE):
        raise RuntimeError(
            f"Gold table is unavailable: {_GOLD_DETAILS_TABLE}. "
            "Run the Bronze, Silver, and Gold pipeline first."
        )
    df = spark.table(_GOLD_DETAILS_TABLE)
    required = {
        "timestamp", "Zone", "camera_id", "event_id", "employee_id",
        "detection_source", "plate_number", "authorized",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError("Gold table is missing columns: " + ", ".join(sorted(missing)))
    df = df.filter(
        (F.col("timestamp") >= _spark_utc(start))
        & (F.col("timestamp") < _spark_utc(end))
    )
    return df, start, end


def _display_timestamp(column: str = "timestamp"):
    """Format UTC values inside the UTC-configured SparkSession."""
    return F.date_format(F.col(column), _TIMESTAMP_PATTERN)


def _loaded_at() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================
# 2. DASHBOARD OVERVIEW
# ============================================================

@st.cache_data(ttl=25, show_spinner="Loading monitoring overview…")
def load_dashboard_data(time_range: str) -> dict:
    """Load four existing service queries and the camera configuration."""
    start, end = _utc_window(time_range)
    service = get_dashboard_service()
    kpi_row = (
        service.get_detection_kpis(start, end)
        .withColumn(
            "latest_detection_utc",
            F.date_format("latest_detection_at", _TIMESTAMP_PATTERN),
        )
        .first()
    )
    kpis = kpi_row.asDict() if kpi_row is not None else {}

    hourly = (
        service.get_hourly_activity(start, end)
        .withColumn("hour", F.date_format("hour", _TIMESTAMP_PATTERN))
        .toPandas()
    )
    zones = service.get_zone_activity(start, end).toPandas()
    recent = (
        service.get_recent_activity(start, end, limit=30)
        .withColumn("timestamp", _display_timestamp())
        .toPandas()
    )
    cameras = get_camera_configuration()
    return {
        "time_range": time_range,
        "window_start": start,
        "window_end": end,
        "loaded_at": _loaded_at(),
        "kpis": kpis,
        "hourly_activity": hourly,
        "zone_activity": zones,
        "recent_activity": recent,
        "enabled_camera_count": sum(cam.enabled for cam in cameras),
        "configured_camera_count": len(cameras),
    }


# ============================================================
# 3. CAMERA INVENTORY
# ============================================================

@st.cache_data(ttl=30, show_spinner="Loading camera activity…")
def load_camera_data(time_range: str) -> dict:
    """Join configured cameras to observed Gold activity, without health claims."""
    details, _, _ = _window_details(time_range)
    activity_rows = (
        details.groupBy("camera_id")
        .agg(
            F.count("*").alias("observations"),
            F.count(F.when(F.col("detection_source") == "person", 1)).alias("person_observations"),
            F.count(F.when(F.col("detection_source") == "plate", 1)).alias("plate_observations"),
            F.max("timestamp").alias("last_timestamp"),
        )
        .withColumn("last_observation_utc", _display_timestamp("last_timestamp"))
        .select("camera_id", "observations", "person_observations", "plate_observations", "last_observation_utc")
        .collect()
    )
    activity = {row["camera_id"]: row.asDict() for row in activity_rows}
    cameras = get_camera_configuration()
    records = []
    for camera in cameras:
        observed = activity.get(camera.camera_id, {})
        records.append({
            "Camera": camera.camera_id,
            "Zone": camera.zone_id,
            "Detector": camera.detector_type.title(),
            "Configured": "Enabled" if camera.enabled else "Disabled",
            "Observations": int(observed.get("observations", 0)),
            "Person": int(observed.get("person_observations", 0)),
            "Plate": int(observed.get("plate_observations", 0)),
            "Last observation (UTC)": observed.get("last_observation_utc") or "—",
        })
    inventory = pd.DataFrame(records)
    return {
        "loaded_at": _loaded_at(),
        "inventory": inventory,
        "configured": len(cameras),
        "enabled": sum(cam.enabled for cam in cameras),
        "active": sum(row["Observations"] > 0 for row in records),
        "observations": sum(row["Observations"] for row in records),
    }


# ============================================================
# 4. PERSON MONITORING
# ============================================================

@st.cache_data(ttl=45, show_spinner="Searching employee directory…")
def load_employee_directory(search_term: str = "", limit: int = 150) -> pd.DataFrame:
    """Delegate validated directory search to PersonMonitoringService."""
    return get_person_service().get_employee_directory(
        search_term=search_term.strip() or None,
        limit=limit,
    ).toPandas()


@st.cache_data(ttl=30, show_spinner="Loading employee summary…")
def load_person_summary(employee_id: str) -> dict:
    """Return all-time person and associated-plate summary values."""
    df = get_person_service().get_employee_detection_summary(employee_id)
    for column in (
        "first_person_detection", "last_person_detection",
        "first_plate_detection", "last_plate_detection",
    ):
        df = df.withColumn(column, _display_timestamp(column))
    result = df.first()
    return result.asDict() if result is not None else {}


@st.cache_data(ttl=30, show_spinner="Loading employee observations…")
def load_person_history(
    employee_id: str,
    time_range: str,
    detection_source: str = "All",
    limit: int = 150,
) -> pd.DataFrame:
    """Bound person history with timezone-safe Spark window filtering.

    The service's history method accepts UTC-naive strings, so the UI uses
    explicit UTC Spark timestamp literals for its rolling window to avoid
    Windows-local timezone interpretation.
    """
    if not employee_id.strip():
        raise ValueError("employee_id cannot be empty.")
    if detection_source not in ("All", "person", "plate"):
        raise ValueError("Invalid detection source.")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise ValueError("limit must be an integer from 1 to 500.")

    details, _, _ = _window_details(time_range)
    details = details.filter(F.col("employee_id") == employee_id.strip())
    if detection_source != "All":
        details = details.filter(F.col("detection_source") == detection_source)
    return (
        details.orderBy(
            F.col("timestamp").desc_nulls_last(),
            F.col("camera_id").asc_nulls_last(),
            F.col("event_id").asc_nulls_last(),
        )
        .limit(limit)
        .select(
            _display_timestamp().alias("timestamp"),
            "detection_source", "camera_id", "Zone", "employee_id",
            "plate_number", "authorized", "event_id",
        )
        .toPandas()
    )


# ============================================================
# 5. SECURITY ACCESS REVIEW
# ============================================================

@st.cache_data(ttl=30, show_spinner="Loading access review…")
def load_security_data(time_range: str, limit: int = 100) -> dict:
    """Return explicitly unauthorized person flags, not inferred incidents."""
    details, _, _ = _window_details(time_range)
    flagged = details.filter(
        (F.col("detection_source") == "person")
        & (F.col("authorized") == F.lit(False))
    )
    stats_row = flagged.agg(
        F.count("*").alias("observations"),
        F.countDistinct("employee_id").alias("employees"),
        F.countDistinct("camera_id").alias("cameras"),
        F.max("timestamp").alias("last_timestamp"),
    ).first()
    zones = (
        flagged.groupBy("Zone")
        .agg(F.count("*").alias("observation_count"))
        .orderBy("Zone")
        .toPandas()
    )
    recent = (
        flagged.orderBy(F.col("timestamp").desc_nulls_last(), F.col("event_id").asc())
        .limit(limit)
        .select(
            _display_timestamp().alias("timestamp"), "employee_id", "Zone", "camera_id", "event_id",
        )
        .toPandas()
    )
    return {
        "loaded_at": _loaded_at(),
        "stats": stats_row.asDict() if stats_row is not None else {},
        "zones": zones,
        "recent": recent,
    }


# ============================================================
# 6. EVENTS / OBSERVATION EXPLORER
# ============================================================

@st.cache_data(ttl=30, show_spinner="Searching monitoring observations…")
def load_event_data(
    time_range: str,
    source: str = "All",
    zone: str = "All",
    camera_id: str = "All",
    limit: int = 200,
) -> dict:
    """Search bounded raw observations without assigning alert severity."""
    if source not in ("All", "person", "plate"):
        raise ValueError("Unsupported detection source.")
    if zone not in ("All", "A", "B", "C", "D", "E"):
        raise ValueError("Unsupported zone.")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise ValueError("limit must be an integer from 1 to 500.")
    details, _, _ = _window_details(time_range)
    if source != "All":
        details = details.filter(F.col("detection_source") == source)
    if zone != "All":
        details = details.filter(F.col("Zone") == zone)
    if camera_id != "All":
        details = details.filter(F.col("camera_id") == camera_id)
    totals = details.agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col("detection_source") == "person", 1)).alias("person"),
        F.count(F.when(F.col("detection_source") == "plate", 1)).alias("plate"),
    ).first()
    recent = (
        details.orderBy(
            F.col("timestamp").desc_nulls_last(),
            F.col("camera_id").asc_nulls_last(),
            F.col("event_id").asc_nulls_last(),
        )
        .limit(limit)
        .select(
            _display_timestamp().alias("timestamp"), "detection_source", "Zone",
            "camera_id", "employee_id", "plate_number", "authorized", "event_id",
        )
        .toPandas()
    )
    return {
        "loaded_at": _loaded_at(),
        "totals": totals.asDict() if totals is not None else {},
        "recent": recent,
    }


# ============================================================
# 7. EMPLOYEE CO-OBSERVATION
# ============================================================

@st.cache_data(ttl=45, show_spinner="Analyzing shared-frame observations…")
def load_connection_data(time_range: str, limit: int = 100) -> dict:
    """Count employees co-observed in the same person camera frame.

    Uses the Gold detail event ID AND camera ID AND timestamp to avoid
    mixing observations from different frames or detectors. The result
    does not represent inferred relationships or physical interactions.
    """
    details, _, _ = _window_details(time_range)
    person = details.filter(
        (F.col("detection_source") == "person")
        & F.col("employee_id").isNotNull()
        & (F.trim("employee_id") != "")
    )
    frames = (
        person.groupBy("camera_id", "event_id", "timestamp", "Zone")
        .agg(F.array_sort(F.collect_set("employee_id")).alias("employees"))
        .filter(F.size("employees") >= 2)
    )
    pairs = (
        frames.withColumn("employee_a", F.explode("employees"))
        .withColumn("employee_b", F.explode("employees"))
        .filter(F.col("employee_a") < F.col("employee_b"))
    )
    summary = (
        pairs.groupBy("employee_a", "employee_b")
        .agg(
            F.count("*").alias("shared_frames"),
            F.countDistinct("camera_id").alias("cameras"),
            F.max("timestamp").alias("last_timestamp"),
        )
        .orderBy(
            F.col("shared_frames").desc(),
            F.col("employee_a").asc(),
            F.col("employee_b").asc(),
        )
        .limit(limit)
        .withColumn("last_observed_utc", _display_timestamp("last_timestamp"))
        .select("employee_a", "employee_b", "shared_frames", "cameras", "last_observed_utc")
        .toPandas()
    )
    return {"loaded_at": _loaded_at(), "pairs": summary}

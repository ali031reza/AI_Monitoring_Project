"""Streamlit resource and data caching for the UI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import streamlit as st

from advanced.ETL.analytics.services.dashboard_service import DashboardService
from advanced.config.camera_loader import load_cameras
from advanced.config.spark_session import create_spark_session


@st.cache_resource(show_spinner="Starting Spark and connecting to the Gold tables...")
def get_spark_session():
    """Keep one SparkSession alive across Streamlit reruns."""
    return create_spark_session()


@st.cache_resource(show_spinner=False)
def get_dashboard_service() -> DashboardService:
    """Keep one dashboard service bound to the cached SparkSession."""
    return DashboardService(get_spark_session())


@st.cache_data(ttl=25, show_spinner="Loading dashboard data...")
def load_dashboard_data(time_range: str) -> dict:
    """Load small, bounded dashboard results for the selected time range.

    The cache expires before the dashboard's 30-second fragment refresh.
    """
    range_days = {
        "Last 24 hours": 1,
        "Last 7 days": 7,
        "Last 30 days": 30,
    }

    if time_range not in range_days:
        raise ValueError(f"Unsupported dashboard time range: {time_range}")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=range_days[time_range])

    service = get_dashboard_service()

    kpi_row = service.get_detection_kpis(
        start_time,
        end_time,
    ).first()

    kpis = kpi_row.asDict() if kpi_row is not None else {}

    hourly_activity = service.get_hourly_activity(
        start_time,
        end_time,
    ).toPandas()

    zone_activity = service.get_zone_activity(
        start_time,
        end_time,
    ).toPandas()

    recent_activity = service.get_recent_activity(
        start_time,
        end_time,
        limit=20,
    ).toPandas()

    cameras = load_cameras()
    enabled_camera_count = sum(
        1 for camera in cameras if camera["enabled"]
    )

    return {
        "time_range": time_range,
        "window_start": start_time,
        "window_end": end_time,
        "loaded_at": datetime.now(timezone.utc),
        "kpis": kpis,
        "hourly_activity": hourly_activity,
        "zone_activity": zone_activity,
        "recent_activity": recent_activity,
        "enabled_camera_count": enabled_camera_count,
        "configured_camera_count": len(cameras),
    }

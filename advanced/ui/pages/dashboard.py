"""Polished operational overview backed by DashboardService."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.components.visuals import activity_chart, zone_chart
from advanced.ui.data_access import TIME_RANGES, load_dashboard_data
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error, source_caption,
)


def _count(kpis: dict, key: str) -> int:
    return int(kpis.get(key) or 0)


def _recent_table(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data
    renamed = data.rename(columns={
        "timestamp": "Timestamp (UTC)",
        "detection_source": "Source",
        "camera_id": "Camera",
        "Zone": "Zone",
        "employee_id": "Employee ID / plate match",
        "plate_number": "Plate",
        "event_id": "Event ID",
    }).copy()
    renamed["Source"] = renamed["Source"].replace({"person": "Person", "plate": "Plate"})
    return renamed[[
        "Timestamp (UTC)", "Source", "Camera", "Zone",
        "Employee ID / plate match", "Plate", "Event ID",
    ]].fillna("—")


@st.fragment(run_every="30s")
def _render_dashboard(time_range: str) -> None:
    try:
        data = load_dashboard_data(time_range)
    except Exception as exc:
        show_data_error(exc, "Dashboard")
        return

    source_caption(data["loaded_at"], time_range)
    kpis = data["kpis"]
    render_metric_cards([
        (
            "Person observations",
            f"{_count(kpis, 'person_observation_count'):,}",
            "Individual person detection detail rows, not camera frames.",
        ),
        (
            "Plate observations",
            f"{_count(kpis, 'plate_observation_count'):,}",
            "Individual license-plate detection detail rows.",
        ),
        (
            "Employees observed",
            f"{_count(kpis, 'distinct_employees_observed'):,}",
            "Distinct employee IDs from person observations only.",
        ),
        (
            "Cameras with activity",
            f"{_count(kpis, 'cameras_with_activity'):,}",
            "Cameras with at least one detection-detail row in the period.",
        ),
        (
            "Cameras enabled",
            f"{data['enabled_camera_count']}/{data['configured_camera_count']}",
            "Configuration state; this does not indicate that cameras are online.",
        ),
    ])

    latest = kpis.get("latest_detection_utc")
    if latest:
        st.caption(f"Most recent persisted detection in this window: {latest} UTC")

    st.write("")
    left, right = st.columns([1.6, 1], gap="large")
    with left:
        section_heading("Detection activity", "Observation counts by UTC hour")
        activity_chart(data["hourly_activity"])
    with right:
        section_heading("Coverage by zone", "Person and plate records")
        zone_chart(data["zone_activity"])

    st.write("")
    section_heading("Recent observations", "Latest 30 individual detections")
    recent = _recent_table(data["recent_activity"])
    if recent.empty:
        empty_state("No recent observations", "No Gold detection details match the selected time window.")
    else:
        st.dataframe(recent, use_container_width=True, hide_index=True, height=380)
    note(
        "A plate-to-employee match is an association with a registered plate; "
        "it does not establish that the employee was physically present. "
        "These are observations, not automatically classified security alerts."
    )


def render_dashboard() -> None:
    page_header(
        "Operations / Overview",
        "Monitoring overview",
        "Explore persisted person and vehicle-plate observations across zones and cameras. "
        "The console refreshes Gold-layer analytics automatically; it does not stream live video.",
    )
    left, middle, right = st.columns([2.1, 1.1, 1.0], vertical_alignment="bottom")
    with left:
        selected = st.selectbox("Time window", TIME_RANGES, key="overview_time_range")
    with middle:
        st.markdown('<span class="app-pill">● Gold analytics</span>', unsafe_allow_html=True)
    with right:
        if st.button("↻ Refresh data", use_container_width=True):
            load_dashboard_data.clear()
            st.rerun()
    _render_dashboard(selected)


render_dashboard()

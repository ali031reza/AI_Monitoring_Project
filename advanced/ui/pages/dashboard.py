from __future__ import annotations

import pandas as pd
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.data_access import load_dashboard_data


TIME_RANGES = [
    "Last 24 hours",
    "Last 7 days",
    "Last 30 days",
]


def _count(kpis: dict, key: str) -> int:
    value = kpis.get(key, 0)
    return int(value or 0)


def _build_hourly_chart(
    hourly_df: pd.DataFrame,
    window_start,
    window_end,
) -> pd.DataFrame:
    """Pivot hourly results and fill missing hours with zero counts."""
    if hourly_df.empty:
        return pd.DataFrame(
            columns=["Person detections", "Plate detections"]
        )

    chart_data = hourly_df.copy()
    chart_data["hour"] = pd.to_datetime(chart_data["hour"])

    chart_data = chart_data.pivot_table(
        index="hour",
        columns="detection_source",
        values="observation_count",
        aggfunc="sum",
        fill_value=0,
    )

    for source in ("person", "plate"):
        if source not in chart_data.columns:
            chart_data[source] = 0

    start = pd.Timestamp(window_start).tz_convert("UTC").tz_localize(None)
    end = pd.Timestamp(window_end).tz_convert("UTC").tz_localize(None)

    complete_hours = pd.date_range(
        start=start.floor("h"),
        end=end.floor("h"),
        freq="h",
        name="hour",
    )

    chart_data = chart_data.reindex(complete_hours, fill_value=0)
    return chart_data[["person", "plate"]].rename(
        columns={
            "person": "Person detections",
            "plate": "Plate detections",
        }
    )


def _build_zone_chart(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return pd.DataFrame(
            columns=["Person detections", "Plate detections"]
        )

    chart_data = zone_df.pivot_table(
        index="Zone",
        columns="detection_source",
        values="observation_count",
        aggfunc="sum",
        fill_value=0,
    )

    for source in ("person", "plate"):
        if source not in chart_data.columns:
            chart_data[source] = 0

    return chart_data[["person", "plate"]].rename(
        columns={
            "person": "Person detections",
            "plate": "Plate detections",
        }
    )


def _render_recent_activity(recent_df: pd.DataFrame) -> None:
    st.subheader("Recent monitoring activity")

    if recent_df.empty:
        st.info("No detection records were found in the selected period.")
        return

    display_df = recent_df.copy()
    display_df["timestamp"] = pd.to_datetime(
        display_df["timestamp"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    display_df = display_df.rename(
        columns={
            "timestamp": "Timestamp (UTC)",
            "detection_source": "Source",
            "camera_id": "Camera",
            "Zone": "Zone",
            "employee_id": "Employee ID / plate match",
            "plate_number": "Plate number",
        }
    )

    display_df = display_df[
        [
            "Timestamp (UTC)",
            "Source",
            "Camera",
            "Zone",
            "Employee ID / plate match",
            "Plate number",
        ]
    ]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "For plate records, an employee ID indicates a registered plate match; "
        "it does not establish that the employee was physically present."
    )


@st.fragment(run_every="30s")
def render_live_dashboard(time_range: str) -> None:
    try:
        data = load_dashboard_data(time_range)
    except Exception as exc:
        st.error("The Dashboard could not load data from the Gold layer.")
        st.info(
            "Confirm that the Bronze, Silver, and Gold pipelines have run, "
            "the Gold metastore is accessible, and the Spark runtime is configured."
        )
        with st.expander("Technical details"):
            st.exception(exc)
        return

    kpis = data["kpis"]

    st.caption(
        f"Period: {data['window_start']:%Y-%m-%d %H:%M} UTC — "
        f"{data['window_end']:%Y-%m-%d %H:%M} UTC"
        f"  ·  Last UI data load: {data['loaded_at']:%H:%M:%S} UTC"
        "  ·  UI refresh: every 30 seconds"
    )

    render_metric_cards(
        [
            (
                "Person observations",
                f"{_count(kpis, 'person_observation_count'):,}",
                "Individual person-detection records, not camera frames.",
            ),
            (
                "Plate observations",
                f"{_count(kpis, 'plate_observation_count'):,}",
                "Individual plate-detection records.",
            ),
            (
                "Distinct employees observed",
                f"{_count(kpis, 'distinct_employees_observed'):,}",
                "Distinct employee IDs from person detections only.",
            ),
            (
                "Cameras with activity",
                f"{_count(kpis, 'cameras_with_activity'):,}",
                "Distinct cameras with detection records in this period.",
            ),
            (
                "Enabled cameras",
                f"{data['enabled_camera_count']:,}",
                f"Configured as enabled: {data['enabled_camera_count']} "
                f"of {data['configured_camera_count']}. This is configuration, "
                "not a live camera-health check.",
            ),
        ]
    )

    latest_detection = kpis.get("latest_detection_at")
    if latest_detection is not None:
        latest_timestamp = pd.Timestamp(latest_detection)
        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.tz_localize("UTC")
        else:
            latest_timestamp = latest_timestamp.tz_convert("UTC")

        st.caption(
            f"Latest detection in selected period: "
            f"{latest_timestamp:%Y-%m-%d %H:%M:%S} UTC"
        )

    st.divider()

    activity_column, zone_column = st.columns([1.6, 1])

    with activity_column:
        st.subheader("Detection activity")
        hourly_chart = _build_hourly_chart(
            data["hourly_activity"],
            data["window_start"],
            data["window_end"],
        )
        if hourly_chart.empty:
            st.info("No hourly detection activity is available for this period.")
        else:
            st.line_chart(hourly_chart)

    with zone_column:
        st.subheader("Activity by zone")
        zone_chart = _build_zone_chart(data["zone_activity"])
        if zone_chart.empty:
            st.info("No zone activity is available for this period.")
        else:
            st.bar_chart(zone_chart)

    _render_recent_activity(data["recent_activity"])

    st.caption(
        "The UI refreshes every 30 seconds. It reads the latest persisted Gold tables; "
        "it does not start Kafka consumers or rerun the ETL pipeline."
    )


def render_dashboard() -> None:
    st.title("Dashboard / Overview")
    st.caption(
        "Operational overview of person observations, vehicle observations, "
        "camera activity, and monitoring events."
    )

    time_range = st.selectbox(
        "Time range",
        options=TIME_RANGES,
        index=0,
        key="dashboard_time_range",
    )

    render_live_dashboard(time_range)


render_dashboard()

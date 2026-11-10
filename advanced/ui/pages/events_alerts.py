"""Raw person and plate observation explorer; no fabricated alerts."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.data_access import (
    TIME_RANGES, get_camera_configuration, load_event_data,
)
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error, source_caption,
)


def _display_events(df: pd.DataFrame) -> pd.DataFrame:
    result = df.rename(columns={
        "timestamp": "Timestamp (UTC)",
        "detection_source": "Source",
        "Zone": "Zone",
        "camera_id": "Camera",
        "employee_id": "Employee ID / plate match",
        "plate_number": "Plate number",
        "authorized": "Authorization flag",
        "event_id": "Event ID",
    }).copy()
    result["Source"] = result["Source"].replace({"person": "Person", "plate": "Plate"})
    result["Authorization flag"] = result["Authorization flag"].map(
        {True: "Authorized", False: "Not authorized"}
    )
    return result.fillna("—")


def render_events() -> None:
    page_header(
        "Operations / Observations",
        "Observation explorer",
        "Investigate timestamped Gold detection records with source, zone, and camera filters. "
        "Raw observations remain separate from rule-generated security alerts.",
    )

    left, mid_left, mid_right, right = st.columns([1.25, 1, 1, 1.2])
    with left:
        period = st.selectbox("Time window", TIME_RANGES, key="events_period")
    with mid_left:
        source_label = st.selectbox("Source", ["All", "Person", "Plate"], key="events_source")
    with mid_right:
        zone = st.selectbox("Zone", ["All", "A", "B", "C", "D", "E"], key="events_zone")
    with right:
        try:
            configured = get_camera_configuration()
            cameras = ["All"] + sorted(c.camera_id for c in configured)
        except Exception as exc:
            show_data_error(exc, "Camera configuration")
            return
        camera = st.selectbox("Camera", cameras, key="events_camera")

    with st.expander("Result options", expanded=False):
        limit = st.selectbox("Max rows returned", [50, 100, 200, 500], index=2, key="events_limit")

    try:
        data = load_event_data(
            period,
            source="All" if source_label == "All" else source_label.lower(),
            zone=zone,
            camera_id=camera,
            limit=limit,
        )
    except Exception as exc:
        show_data_error(exc, "Observation explorer")
        return

    source_caption(data["loaded_at"], period)
    totals = data["totals"]
    render_metric_cards([
        ("Matching observations", f"{int(totals.get('total') or 0):,}", "All detail rows matching the current filters."),
        ("Person records", f"{int(totals.get('person') or 0):,}", "Matching person observation rows."),
        ("Plate records", f"{int(totals.get('plate') or 0):,}", "Matching plate observation rows."),
        ("Rows displayed", f"{len(data['recent']):,}", "Most recent rows, bounded by the selected maximum."),
    ])
    st.write("")
    section_heading("Monitoring records", "Most recent first · timestamps in UTC")
    if data["recent"].empty:
        empty_state("No matching observations", "Change one or more filters or select a longer time window.")
    else:
        display = _display_events(data["recent"])
        st.dataframe(display, use_container_width=True, hide_index=True, height=480)
        st.download_button(
            label="↓ Export displayed rows (CSV)",
            data=display.to_csv(index=False).encode("utf-8"),
            file_name="monitoring_observations.csv",
            mime="text/csv",
        )

    note(
        "Raw detections are not alerts. A person authorization flag of 'Not authorized' "
        "is an explicit data attribute; no event is automatically classified as a confirmed incident. "
        "Plate matches do not establish employee presence."
    )


render_events()

"""Human-review workflow for explicitly unauthorized person flags."""

from __future__ import annotations

import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.components.visuals import single_zone_chart
from advanced.ui.data_access import TIME_RANGES, load_security_data
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error, source_caption,
)


def render_security_access() -> None:
    page_header(
        "Investigations / Access",
        "Access review",
        "Review person observations carrying an explicit unauthorized flag. "
        "These are review candidates, not confirmed security incidents or inferred alert severities.",
    )
    period = st.selectbox("Time window", TIME_RANGES, key="access_time_window")
    try:
        data = load_security_data(period, limit=100)
    except Exception as exc:
        show_data_error(exc, "Access review")
        return

    source_caption(data["loaded_at"], period)
    values = data["stats"]
    render_metric_cards([
        (
            "Flagged person observations",
            f"{int(values.get('observations') or 0):,}",
            "Person detection rows where authorized is explicitly false.",
        ),
        (
            "Distinct employees",
            f"{int(values.get('employees') or 0):,}",
            "Distinct employee IDs among flagged person observations.",
        ),
        (
            "Cameras involved",
            f"{int(values.get('cameras') or 0):,}",
            "Distinct cameras recording an explicit false authorization flag.",
        ),
    ])

    st.write("")
    left, right = st.columns([1, 1.5], gap="large")
    with left:
        section_heading("Review flags by zone", "Person detections only")
        single_zone_chart(data["zones"])
    with right:
        section_heading("Interpretation", "Explicit data flags only")
        with st.container(border=True):
            st.write("**What is shown**")
            st.caption("Gold person-detection details for which `authorized` is `False`.")
            st.write("**What is not assumed**")
            st.caption("No entry attempt, breach, incident, or alert priority is inferred from this field alone.")
            st.write("**Where access rules come from**")
            st.caption("The pipeline's upstream person authorization logic and restricted-access configuration.")

    st.write("")
    section_heading("Observations for review", "Latest 100 flagged rows")
    if data["recent"].empty:
        empty_state("No flagged observations", "No person-detection records with an explicit unauthorized flag in this period.")
    else:
        table = data["recent"].rename(columns={
            "timestamp": "Timestamp (UTC)",
            "employee_id": "Employee ID",
            "Zone": "Zone",
            "camera_id": "Camera",
            "event_id": "Event ID",
        }).fillna("—")
        st.dataframe(table, use_container_width=True, hide_index=True, height=365)
    note("An authorization flag is an attribute of a recorded person observation. It is not automatically a confirmed violation.")


render_security_access()

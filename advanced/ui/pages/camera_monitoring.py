"""Camera configuration and observed Gold activity, without health inference."""

from __future__ import annotations

import altair as alt
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.data_access import TIME_RANGES, load_camera_data
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error, source_caption,
)


def render_camera_monitoring() -> None:
    page_header(
        "Operations / Cameras",
        "Camera inventory",
        "Reconcile the configured camera fleet with detection records persisted in the Gold layer. "
        "Activity is historical evidence—not a camera connectivity test.",
    )

    period = st.selectbox("Time window", TIME_RANGES, key="camera_time_range")
    try:
        data = load_camera_data(period)
    except Exception as exc:
        show_data_error(exc, "Camera inventory")
        return

    source_caption(data["loaded_at"], period)
    render_metric_cards([
        ("Configured cameras", str(data["configured"]), "Cameras defined in Starter_Sources/cameras.csv."),
        ("Enabled in config", str(data["enabled"]), "Enabled flag from the configuration, not a health indicator."),
        ("With observations", str(data["active"]), "Configured cameras having Gold detail records in the selected period."),
        ("Detection records", f"{data['observations']:,}", "Person and plate detail rows from configured cameras."),
    ])

    st.write("")
    left, right = st.columns([1, 1], gap="medium")
    inventory = data["inventory"]
    with left:
        zone = st.selectbox("Zone", ["All", "A", "B", "C", "D", "E"], key="camera_zone")
    with right:
        detector = st.selectbox("Detector", ["All", "Person", "Plate"], key="camera_detector")
    selected = inventory.copy()
    if zone != "All":
        selected = selected[selected["Zone"] == zone]
    if detector != "All":
        selected = selected[selected["Detector"] == detector]

    section_heading("Configured fleet", f"{len(selected)} cameras shown")
    if selected.empty:
        empty_state("No matching cameras", "Try changing the zone or detector filters.")
    else:
        st.dataframe(selected, use_container_width=True, hide_index=True, height=385)

    st.write("")
    section_heading("Observations by camera", "Only detection rows, not frames or uptime")
    if selected.empty or selected["Observations"].sum() == 0:
        empty_state("No camera observations", "There are no detections for this camera selection and time window.")
    else:
        plot = selected.copy()
        plot["Camera"] = "CAM " + plot["Camera"]
        chart = (
            alt.Chart(plot)
            .mark_bar(cornerRadiusEnd=5)
            .encode(
                x=alt.X("Observations:Q", title="Observation records"),
                y=alt.Y("Camera:N", sort="-x", title=None),
                color=alt.Color(
                    "Detector:N", title="Detector",
                    scale=alt.Scale(domain=["Person", "Plate"], range=["#54c9b6", "#7d9af9"]),
                ),
                tooltip=["Camera:N", "Zone:N", "Detector:N", alt.Tooltip("Observations:Q", format=",d")],
            )
            .properties(height=max(230, 39 * len(plot)))
        )
        st.altair_chart(chart, use_container_width=True)
    note("An enabled camera with zero detections is not necessarily offline. Empty frames do not create Gold detection-detail rows.")


render_camera_monitoring()

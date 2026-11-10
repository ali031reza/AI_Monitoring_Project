"""Employee directory, person observations and plate associations."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.components.visuals import zone_chart
from advanced.ui.data_access import (
    TIME_RANGES, load_employee_directory,
    load_person_history, load_person_summary,
)
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error,
)


def _display_history(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.rename(columns={
        "timestamp": "Timestamp (UTC)",
        "detection_source": "Source",
        "camera_id": "Camera",
        "Zone": "Zone",
        "employee_id": "Employee ID / plate match",
        "plate_number": "Plate",
        "authorized": "Authorization flag",
        "event_id": "Event ID",
    }).copy()
    result["Source"] = result["Source"].replace({"person": "Person", "plate": "Plate"})
    result["Authorization flag"] = result["Authorization flag"].map(
        {True: "Authorized", False: "Not authorized"}
    )
    return result.fillna("—")


def render_person_monitoring() -> None:
    page_header(
        "Investigations / People",
        "Person monitoring",
        "Search registered employees, inspect their person-detection history, "
        "and distinguish physical observations from registered-plate associations.",
    )

    search = st.text_input(
        "Search employee ID or registered plate",
        placeholder="e.g. EMP0001 or a license plate",
        key="person_directory_search",
    )
    try:
        directory = load_employee_directory(search, limit=150)
    except Exception as exc:
        show_data_error(exc, "Employee directory")
        return

    if directory.empty:
        empty_state("No matching employees", "Search for another employee ID or registered plate.")
        return

    people = directory.dropna(subset=["employee_id"]).copy()
    people["employee_id"] = people["employee_id"].astype(str)
    ids = people["employee_id"].tolist()
    if not ids:
        empty_state("No employee IDs", "The directory has no usable employee identifiers.")
        return
    plate_map = dict(zip(people["employee_id"], people["plate_number"]))
    selected = st.selectbox(
        "Employee",
        options=ids,
        format_func=lambda employee: (
            f"{employee}   ·   {plate_map.get(employee) if pd.notna(plate_map.get(employee)) else 'No registered plate'}"
        ),
        key="person_selected_employee",
    )

    try:
        summary = load_person_summary(selected)
    except Exception as exc:
        show_data_error(exc, "Employee summary")
        return

    st.caption("Summary metrics below are all-time across the persisted Gold table.")
    render_metric_cards([
        ("Person observations", f"{int(summary.get('person_detection_count') or 0):,}", "Individual person detail records."),
        ("Plate associations", f"{int(summary.get('plate_detection_count') or 0):,}", "Registered-plate matches, not proof of person presence."),
        ("Person cameras", f"{int(summary.get('distinct_person_cameras') or 0):,}", "Distinct cameras with person observations."),
        ("Person zones", f"{int(summary.get('distinct_person_zones') or 0):,}", "Distinct zones with person observations."),
    ])

    with st.expander("First and last observed timestamps (UTC)"):
        left, right = st.columns(2)
        with left:
            st.write("**Person observations**")
            st.caption(f"First: {summary.get('first_person_detection') or '—'} UTC")
            st.caption(f"Last: {summary.get('last_person_detection') or '—'} UTC")
        with right:
            st.write("**Plate associations**")
            st.caption(f"First: {summary.get('first_plate_detection') or '—'} UTC")
            st.caption(f"Last: {summary.get('last_plate_detection') or '—'} UTC")

    st.write("")
    left, right = st.columns(2)
    with left:
        period = st.selectbox("History window", TIME_RANGES, key="person_history_window")
    with right:
        source_label = st.selectbox("Observation source", ["All", "Person", "Plate"], key="person_history_source")

    source = "All" if source_label == "All" else source_label.lower()
    try:
        history = load_person_history(selected, period, source, limit=150)
    except Exception as exc:
        show_data_error(exc, "Employee history")
        return

    section_heading("Observation history", f"{len(history)} latest rows displayed · {period}")
    if history.empty:
        empty_state("No history in this window", "Try a longer time window or a different observation source.")
    else:
        st.dataframe(_display_history(history), use_container_width=True, hide_index=True, height=400)
        st.write("")
        section_heading("Displayed history by zone", "Counts based on the rows above")
        breakdown = (
            history.groupby(["Zone", "detection_source"], dropna=False)
            .size()
            .reset_index(name="observation_count")
        )
        zone_chart(breakdown)
    note(
        "Person observations and license-plate matches are different forms of evidence. "
        "A registered plate match does not confirm that the employee was inside the vehicle or present at the camera."
    )


render_person_monitoring()

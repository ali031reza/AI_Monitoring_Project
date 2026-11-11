"""Employee directory, person observations and plate associations."""

from __future__ import annotations

from pathlib import Path

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
from advanced.ui.pipeline_state import (
    get_pipeline_controller,
    load_pipeline_snapshot,
    resolve_run_directory,
)


def _metric_value(value, digits: int = 1) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "—"


@st.fragment(run_every="2s")
def _render_live_person_monitoring(
    run_id: str,
    run_directory: str,
) -> None:
    """Show person-topic throughput, worker health and camera assignments."""

    run_dir = Path(run_directory)
    snapshot = load_pipeline_snapshot(run_dir)
    config = snapshot["config"]
    if not config:
        st.warning("The saved pipeline configuration could not be read.")
        return

    controller = get_pipeline_controller()
    is_current_run = controller.active_run_id == run_id
    status = (
        "Running"
        if is_current_run and controller.is_running
        else "Stopped / saved run"
    )
    cameras = [
        camera
        for camera in config.get("cameras", [])
        if camera.get("detector_type") == "person"
    ]
    fps = float(config.get("fps_per_camera", 0))
    target_rate = len(cameras) * fps
    rates = snapshot["rates"]
    person_lag = [
        row
        for row in snapshot["lag_rows"]
        if row.get("Detector") == "Person"
    ]
    person_lag_total = None
    if person_lag and all(
        row.get("Lag (messages)") is not None
        for row in person_lag
    ):
        person_lag_total = sum(
            int(row["Lag (messages)"])
            for row in person_lag
        )

    st.subheader("Live person detector pipeline")
    st.caption(
        f"Run: `{run_id}` · Status: **{status}** · "
        f"{len(cameras)} person cameras at {fps:g} FPS each"
    )

    camera_col, target_col, produced_col, processed_col, lag_col = (
        st.columns(5)
    )
    camera_col.metric("Person cameras", f"{len(cameras)}")
    target_col.metric(
        "Target input · frames/s",
        _metric_value(target_rate),
    )
    produced_col.metric(
        "Person topic produced · msg/s",
        _metric_value(rates.get("producer_person_delivered")),
    )
    processed_col.metric(
        "Person processed · msg/s",
        _metric_value(rates.get("person_processed")),
    )
    lag_col.metric(
        "Person committed lag · messages",
        "—" if person_lag_total is None else f"{person_lag_total:,}",
    )

    worker_count, partition_count, delay_col = st.columns(3)
    worker_count.metric(
        "Person detector workers",
        f"{config.get('person_workers', '—')}",
    )
    partition_count.metric(
        "Person Kafka partitions",
        f"{config.get('person_partitions', '—')}",
    )
    delay_col.metric(
        "Mock processing delay · ms",
        _metric_value(
            config.get("processing_delay_ms", {}).get("person"),
            0,
        ),
    )

    section_heading(
        "Person worker performance",
        "Latest received, processed and committed rates for each configured worker",
    )
    if snapshot["person_workers"]:
        st.dataframe(
            pd.DataFrame(snapshot["person_workers"]).drop(
                columns=["Detector"],
                errors="ignore",
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("Waiting for person worker metric snapshots…")

    throughput = snapshot["throughput"]
    section_heading(
        "Person-topic throughput",
        "Person messages delivered by the producer versus processed by person workers",
    )
    if throughput.empty:
        st.caption("No person throughput history is available yet.")
    else:
        person_columns = [
            column
            for column in ("Person produced", "Person processed")
            if column in throughput.columns
        ]
        if person_columns:
            st.line_chart(throughput[person_columns])

    lag_history = snapshot["lag_history"]
    section_heading(
        "Person consumer lag",
        "Committed person-topic lag over time",
    )
    if lag_history.empty or "Person lag" not in lag_history.columns:
        st.caption("No person consumer-lag history is available yet.")
    else:
        st.line_chart(lag_history[["Person lag"]])

    section_heading(
        "Person Kafka partitions",
        "Committed offsets and lag for the person topic",
    )
    if person_lag:
        st.dataframe(
            pd.DataFrame(person_lag),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("No person partition snapshot is available yet.")

    section_heading(
        "Person camera assignments",
        "Camera-to-zone and Kafka partition mapping from this run configuration",
    )
    if cameras:
        camera_frame = pd.DataFrame([
            {
                "Camera": camera.get("camera_id"),
                "Zone": camera.get("zone_id"),
                "Kafka partition": camera.get("kafka_partition"),
                "Enabled": camera.get("enabled"),
            }
            for camera in cameras
        ])
        st.dataframe(
            camera_frame,
            use_container_width=True,
            hide_index=True,
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

    controller = get_pipeline_controller()
    run_dir = resolve_run_directory(controller)
    if run_dir is None:
        st.info(
            "No experiment run is available yet. Start the Kafka pipeline "
            "from the Kafka Pipeline page to populate live person monitoring."
        )
    else:
        _render_live_person_monitoring(run_dir.name, str(run_dir))

    st.divider()
    section_heading(
        "Persisted employee observations",
        "Gold-layer employee history and registered-plate associations",
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

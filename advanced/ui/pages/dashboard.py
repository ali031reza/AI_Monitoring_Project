"""Polished operational overview backed by DashboardService."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.components.visuals import activity_chart, zone_chart
from advanced.ui.data_access import TIME_RANGES, load_dashboard_data
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error, source_caption,
)
from advanced.ui.pipeline_state import (
    get_pipeline_controller,
    load_pipeline_snapshot,
    resolve_run_directory,
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


def _metric_value(value, digits: int = 1) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "—"


@st.fragment(run_every="2s")
def _render_pipeline_overview(run_id: str, run_directory: str) -> None:
    """Show the end-to-end run using its saved Kafka pipeline configuration."""

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

    cameras = config.get("cameras", [])
    person_cameras = [
        camera for camera in cameras
        if camera.get("detector_type") == "person"
    ]
    plate_cameras = [
        camera for camera in cameras
        if camera.get("detector_type") == "plate"
    ]
    camera_count = int(config.get("camera_count", len(cameras)))
    fps = float(config.get("fps_per_camera", 0))
    target_total = camera_count * fps
    target_person = len(person_cameras) * fps
    target_plate = len(plate_cameras) * fps
    rates = snapshot["rates"]

    st.subheader("End-to-end Kafka pipeline")
    st.caption(f"Run: `{run_id}` · Status: **{status}**")
    st.caption(
        f"Configuration: {camera_count} cameras at {fps:g} FPS each · "
        f"{config.get('person_workers', 0)} person workers · "
        f"{config.get('plate_workers', 0)} plate workers"
    )

    camera_col, target_col, producer_col, person_col, plate_col, lag_col = (
        st.columns(6)
    )
    camera_col.metric("Configured cameras", f"{camera_count}")
    target_col.metric("Target input · frames/s", _metric_value(target_total))
    producer_col.metric(
        "Producer delivered · msg/s",
        _metric_value(rates.get("producer_delivered")),
    )
    person_col.metric(
        "Person processed · msg/s",
        _metric_value(rates.get("person_processed")),
    )
    plate_col.metric(
        "Plate processed · msg/s",
        _metric_value(rates.get("plate_processed")),
    )
    lag_col.metric(
        "Committed lag · messages",
        "—" if snapshot["total_lag"] is None else f"{snapshot['total_lag']:,}",
    )

    person_in_col, plate_in_col, person_target_col, plate_target_col = (
        st.columns(4)
    )
    person_in_col.metric(
        "Person topic delivered · msg/s",
        _metric_value(rates.get("producer_person_delivered")),
    )
    plate_in_col.metric(
        "Plate topic delivered · msg/s",
        _metric_value(rates.get("producer_plate_delivered")),
    )
    person_target_col.metric(
        "Person target · frames/s",
        _metric_value(target_person),
    )
    plate_target_col.metric(
        "Plate target · frames/s",
        _metric_value(target_plate),
    )

    section_heading("Run configuration", "Values shared by the producer and all detector workers")
    config_rows = [
        {"Setting": "Camera count", "Value": camera_count},
        {"Setting": "Sampling rate per camera", "Value": f"{fps:g} FPS"},
        {"Setting": "Person cameras / workers / partitions", "Value": (
            f"{len(person_cameras)} / {config.get('person_workers', '—')} / "
            f"{config.get('person_partitions', '—')}"
        )},
        {"Setting": "Plate cameras / workers / partitions", "Value": (
            f"{len(plate_cameras)} / {config.get('plate_workers', '—')} / "
            f"{config.get('plate_partitions', '—')}"
        )},
        {"Setting": "Processing delay", "Value": (
            f"Person {config.get('processing_delay_ms', {}).get('person', '—')} ms · "
            f"Plate {config.get('processing_delay_ms', {}).get('plate', '—')} ms"
        )},
    ]
    st.dataframe(
        pd.DataFrame(config_rows),
        use_container_width=True,
        hide_index=True,
    )

    throughput = snapshot["throughput"]
    section_heading("End-to-end throughput", "Producer delivery compared with person and plate processing")
    if throughput.empty:
        empty_state(
            "Waiting for throughput metrics",
            "Start the Kafka pipeline and allow the producer and workers to publish snapshots.",
        )
    else:
        st.line_chart(throughput)

    lag_history = snapshot["lag_history"]
    section_heading("Consumer lag", "Committed message lag by detector topic")
    if lag_history.empty:
        st.caption("No consumer-lag history is available for this run yet.")
    else:
        st.line_chart(lag_history)

    section_heading("Detector worker performance", "Latest worker-level application metrics")
    if snapshot["worker_rows"]:
        st.dataframe(
            pd.DataFrame(snapshot["worker_rows"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("No worker metric snapshots are available yet.")

    section_heading("Kafka partition lag", "Committed offsets compared with broker high watermarks")
    if snapshot["lag_rows"]:
        st.dataframe(
            pd.DataFrame(snapshot["lag_rows"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("No partition-level lag snapshot is available yet.")

    yolo = snapshot["metadata"].get("yolo_reference", {})
    memory = yolo.get("model_weight_memory_estimate_mb", {})
    if yolo:
        section_heading("YOLO11n reference resources", "Model benchmark assumptions for this worker configuration · not executed")
        cpu_col, gpu_col, ram_col, vram_col = st.columns(4)
        cpu_col.metric(
            "CPU ONNX latency · ms/image",
            _metric_value(
                yolo.get("cpu_benchmark", {}).get("latency_ms_per_image")
            ),
        )
        gpu_col.metric(
            "T4 TensorRT10 latency · ms/image",
            _metric_value(
                yolo.get("gpu_benchmark", {}).get("latency_ms_per_image")
            ),
        )
        ram_col.metric(
            "Model weights in RAM · MB (FP32)",
            _metric_value(memory.get("fp32_all_workers")),
        )
        vram_col.metric(
            "Model weights in VRAM · MB (FP16)",
            _metric_value(memory.get("fp16_all_workers")),
        )
        st.caption(
            "RAM and VRAM are alternative precision/deployment assumptions. "
            "Memory values cover model weights only, not full runtime memory. "
            "The mock detectors do not execute YOLO and do not measure CPU/GPU utilization."
        )

    if is_current_run:
        with st.expander("Pipeline process status"):
            process_rows = controller.process_status()
            if process_rows:
                st.dataframe(
                    pd.DataFrame(process_rows),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption("No child process status is available.")


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
        "Follow the complete Kafka pipeline using the exact camera, FPS and worker "
        "configuration selected in the Kafka Pipeline page. Persisted Gold analytics "
        "remain available below.",
    )

    controller = get_pipeline_controller()
    run_dir = resolve_run_directory(controller)
    if run_dir is None:
        st.info(
            "No experiment run is available yet. Configure and start a run "
            "from the Kafka Pipeline page to populate this overview."
        )
    else:
        _render_pipeline_overview(run_dir.name, str(run_dir))

    st.divider()
    with st.expander("Persisted Gold observation analytics", expanded=False):
        st.caption(
            "Historical person and plate observation details from the Gold layer. "
            "These records are separate from the live Kafka throughput metrics above."
        )
        left, middle, right = st.columns([2.1, 1.1, 1.0], vertical_alignment="bottom")
        with left:
            selected = st.selectbox(
                "Time window",
                TIME_RANGES,
                key="overview_time_range",
            )
        with middle:
            st.markdown(
                '<span class="app-pill">● Gold analytics</span>',
                unsafe_allow_html=True,
            )
        with right:
            if st.button("↻ Refresh data", use_container_width=True):
                load_dashboard_data.clear()
                st.rerun()
        _render_dashboard(selected)


render_dashboard()

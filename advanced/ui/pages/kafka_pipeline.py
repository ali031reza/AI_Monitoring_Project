"""Live Kafka pipeline monitoring and scaling controls."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from advanced.config import ProjectPaths, load_cameras
from advanced.performance_monitoring.pipeline_controller import PipelineController


# ============================================================
# 1. SHARED UI RESOURCES AND METRIC READERS
# ============================================================

@st.cache_resource
def get_pipeline_controller() -> PipelineController:
    """Keep one process controller across Streamlit reruns."""

    return PipelineController()


@st.cache_data(ttl=60)
def get_camera_mix() -> float:
    """Read the configured person/plate camera ratio."""

    cameras = load_cameras(ProjectPaths.discover().cameras_csv)
    enabled = [camera for camera in cameras if camera.enabled]
    person_count = sum(camera.detector_type == "person" for camera in enabled)
    return person_count / len(enabled)


def _read_jsonl(path: Path, limit: int = 600) -> list[dict]:
    """Read the newest JSONL records without loading an entire run."""

    if not path.is_file():
        return []

    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
    except OSError:
        return []

    records = []
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)

    return records


def _latest_record(path: Path) -> dict | None:
    records = _read_jsonl(path, limit=5)
    return records[-1] if records else None


def _worker_metric_paths(run_dir: Path, detector: str) -> list[Path]:
    return sorted(run_dir.glob(f"{detector}_worker*_metrics.jsonl"))


def _number(value, digits: int = 1) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "—"


# ============================================================
# 2. TIME-SERIES AGGREGATION
# ============================================================

def _throughput_history(run_dir: Path) -> pd.DataFrame:
    """Aggregate producer and worker rates into one-second buckets."""

    buckets: dict[pd.Timestamp, dict[str, float]] = {}

    def add_record(path: Path, metric: str, label: str) -> None:
        for record in _read_jsonl(path):
            timestamp = record.get("timestamp_utc")
            if not timestamp:
                continue

            try:
                bucket = pd.to_datetime(timestamp, utc=True).floor("s")
            except (TypeError, ValueError):
                continue

            rates = record.get("rates_per_second", {})
            value = float(rates.get(metric, 0) or 0)
            buckets.setdefault(bucket, {})
            buckets[bucket][label] = buckets[bucket].get(label, 0.0) + value

    add_record(
        run_dir / "producer_metrics.jsonl",
        "frames_generated",
        "Frames generated",
    )
    add_record(
        run_dir / "producer_metrics.jsonl",
        "messages_delivered",
        "Producer delivered",
    )

    for path in _worker_metric_paths(run_dir, "person"):
        add_record(path, "messages_processed", "Person processed")

    for path in _worker_metric_paths(run_dir, "plate"):
        add_record(path, "messages_processed", "Plate processed")

    if not buckets:
        return pd.DataFrame()

    dataframe = pd.DataFrame.from_dict(buckets, orient="index").fillna(0.0)
    dataframe.index = pd.DatetimeIndex(dataframe.index, name="Timestamp (UTC)")
    dataframe = dataframe.sort_index()

    for column in (
        "Frames generated",
        "Producer delivered",
        "Person processed",
        "Plate processed",
    ):
        if column not in dataframe:
            dataframe[column] = 0.0

    return dataframe[[
        "Frames generated",
        "Producer delivered",
        "Person processed",
        "Plate processed",
    ]]


def _payload_history(run_dir: Path) -> pd.DataFrame:
    rows = []
    for record in _read_jsonl(run_dir / "producer_metrics.jsonl"):
        timestamp = record.get("timestamp_utc")
        if not timestamp:
            continue
        try:
            bucket = pd.to_datetime(timestamp, utc=True).floor("s")
        except (TypeError, ValueError):
            continue
        byte_rate = record.get("rates_per_second", {}).get("payload_bytes", 0) or 0
        rows.append({
            "Timestamp (UTC)": bucket,
            "Payload MB/s": float(byte_rate) / (1024 * 1024),
        })

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .drop_duplicates("Timestamp (UTC)", keep="last")
        .set_index("Timestamp (UTC)")
        .sort_index()
    )


def _lag_history(run_dir: Path) -> pd.DataFrame:
    rows = []
    for record in _read_jsonl(run_dir / "consumer_lag_metrics.jsonl"):
        timestamp = record.get("timestamp_utc")
        topics = record.get("topics", {})
        if not timestamp:
            continue

        person = topics.get("person", {}).get("summary", {}).get(
            "total_committed_lag_messages"
        )
        plate = topics.get("plate", {}).get("summary", {}).get(
            "total_committed_lag_messages"
        )
        if person is None or plate is None:
            continue

        try:
            bucket = pd.to_datetime(timestamp, utc=True)
        except (TypeError, ValueError):
            continue

        rows.append({
            "Timestamp (UTC)": bucket,
            "Person lag": int(person),
            "Plate lag": int(plate),
        })

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .drop_duplicates("Timestamp (UTC)", keep="last")
        .set_index("Timestamp (UTC)")
        .sort_index()
    )


# ============================================================
# 3. LATEST SNAPSHOTS
# ============================================================

def _latest_worker_rows(run_dir: Path) -> list[dict]:
    rows = []

    for detector in ("person", "plate"):
        for path in _worker_metric_paths(run_dir, detector):
            record = _latest_record(path)
            if record is None:
                continue

            rates = record.get("rates_per_second", {})
            latency = record.get("latency_ms", {})
            rows.append({
                "Worker": record.get("component", path.stem),
                "Received/s": rates.get("messages_received", 0),
                "Processed/s": rates.get("messages_processed", 0),
                "Processing p95 (ms)": (
                    latency.get("message_processing", {}).get("p95")
                ),
                "Commit p95 (ms)": (
                    latency.get("kafka_commit", {}).get("p95")
                ),
                "Failed": record.get("totals", {}).get("messages_failed", 0),
            })

    return rows


def _latest_lag_rows(run_dir: Path) -> tuple[list[dict], int | None]:
    record = _latest_record(run_dir / "consumer_lag_metrics.jsonl")
    if record is None:
        return [], None

    rows = []
    known_total = 0
    has_missing = False

    for detector in ("person", "plate"):
        topic_data = record.get("topics", {}).get(detector, {})
        summary = topic_data.get("summary", {})
        topic_total = summary.get("total_committed_lag_messages")

        if topic_total is None:
            has_missing = True
        else:
            known_total += int(topic_total)

        for partition in topic_data.get("partitions", []):
            rows.append({
                "Detector": detector.title(),
                "Topic": topic_data.get("topic", "—"),
                "Partition": partition.get("partition"),
                "Committed offset": partition.get("committed_offset"),
                "High watermark": partition.get("high_watermark"),
                "Lag (messages)": partition.get("committed_lag_messages"),
                "Status": partition.get("status", "unknown"),
            })

    return rows, (None if has_missing else known_total)


def _latest_rates(run_dir: Path) -> dict:
    producer = _latest_record(run_dir / "producer_metrics.jsonl") or {}
    producer_rates = producer.get("rates_per_second", {})
    producer_latency = producer.get("latency_ms", {})
    worker_records = []

    for detector in ("person", "plate"):
        for path in _worker_metric_paths(run_dir, detector):
            record = _latest_record(path)
            if record:
                worker_records.append((detector, record))

    person_rate = sum(
        (record.get("rates_per_second", {}).get("messages_processed", 0) or 0)
        for detector, record in worker_records
        if detector == "person"
    )
    plate_rate = sum(
        (record.get("rates_per_second", {}).get("messages_processed", 0) or 0)
        for detector, record in worker_records
        if detector == "plate"
    )

    person_p95 = [
        record.get("latency_ms", {}).get("message_processing", {}).get("p95")
        for detector, record in worker_records
        if detector == "person"
    ]
    plate_p95 = [
        record.get("latency_ms", {}).get("message_processing", {}).get("p95")
        for detector, record in worker_records
        if detector == "plate"
    ]

    person_p95 = [value for value in person_p95 if value is not None]
    plate_p95 = [value for value in plate_p95 if value is not None]

    return {
        "producer_frames": producer_rates.get("frames_generated"),
        "producer_delivered": producer_rates.get("messages_delivered"),
        "producer_bytes": producer_rates.get("payload_bytes"),
        "producer_queue": producer.get("gauges", {}).get("producer_queue_depth"),
        "producer_delivery_p95": (
            producer_latency.get("producer_delivery", {}).get("p95")
        ),
        "person_processed": person_rate,
        "plate_processed": plate_rate,
        "person_processing_p95": max(person_p95) if person_p95 else None,
        "plate_processing_p95": max(plate_p95) if plate_p95 else None,
    }


# ============================================================
# 4. LIVE METRICS FRAGMENT
# ============================================================

@st.fragment(run_every="2s")
def render_live_metrics(run_directory: Path, process_controller: PipelineController):
    rates = _latest_rates(run_directory)
    lag_rows, total_lag = _latest_lag_rows(run_directory)

    frames_col, producer_col, person_col, plate_col, lag_col = st.columns(5)
    frames_col.metric(
        "Frames generated",
        f"{_number(rates['producer_frames'])} frames/s",
    )
    producer_col.metric(
        "Producer delivered",
        f"{_number(rates['producer_delivered'])} msg/s",
    )
    person_col.metric(
        "Person processed",
        f"{_number(rates['person_processed'])} msg/s",
    )
    plate_col.metric(
        "Plate processed",
        f"{_number(rates['plate_processed'])} msg/s",
    )
    lag_col.metric(
        "Committed consumer lag",
        "—" if total_lag is None else f"{total_lag:,} messages",
    )

    bytes_col, queue_col, delivery_col, person_latency_col, plate_latency_col = st.columns(5)
    bytes_col.metric(
        "Payload queued",
        f"{_number((rates['producer_bytes'] or 0) / (1024 * 1024))} MB/s",
    )
    queue_col.metric(
        "Producer queue depth",
        "—" if rates["producer_queue"] is None else f"{int(rates['producer_queue']):,}",
    )
    delivery_col.metric(
        "Delivery p95",
        f"{_number(rates['producer_delivery_p95'])} ms",
    )
    person_latency_col.metric(
        "Worst person worker p95",
        f"{_number(rates['person_processing_p95'])} ms",
    )
    plate_latency_col.metric(
        "Worst plate worker p95",
        f"{_number(rates['plate_processing_p95'])} ms",
    )

    st.markdown("#### Throughput over time")
    throughput = _throughput_history(run_directory)
    if throughput.empty:
        st.caption("Waiting for producer and consumer metric snapshots…")
    else:
        st.line_chart(throughput)

    payload = _payload_history(run_directory)
    if not payload.empty:
        st.markdown("#### Producer queued payload rate")
        st.line_chart(payload)

    lag_history = _lag_history(run_directory)
    if not lag_history.empty:
        st.markdown("#### Consumer lag over time")
        st.line_chart(lag_history)

    st.markdown("#### Detector worker performance")
    worker_rows = _latest_worker_rows(run_directory)
    if worker_rows:
        st.dataframe(
            pd.DataFrame(worker_rows),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("Waiting for detector workers to publish metrics…")

    st.markdown("#### Kafka lag by partition")
    if lag_rows:
        st.dataframe(
            pd.DataFrame(lag_rows),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("Waiting for committed offsets and broker watermarks…")

    with st.expander("Pipeline process status"):
        process_rows = process_controller.process_status()
        if process_rows:
            st.dataframe(
                pd.DataFrame(process_rows),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No child processes have been launched.")

    with st.expander("Run files"):
        st.code(str(run_directory))
        st.caption(
            "Includes run_metadata.json, pipeline_config.json, JSONL metrics "
            "and individual process logs."
        )


# ============================================================
# 5. PAGE LAYOUT AND SCALING CONTROLS
# ============================================================

st.title("Kafka Pipeline")
st.caption(
    "End-to-end producer, Kafka and detector monitoring with live workload scaling."
)

controller = get_pipeline_controller()
person_ratio = get_camera_mix()

st.info(
    "The controls configure a complete run; stop and start a new run to compare "
    "settings. Stop any producer or detector processes you launched manually "
    "before starting here. Each dashboard run uses its own consumer groups. "
    "Kafka topics are created or expanded as needed; partition counts cannot "
    "be reduced."
)

with st.container(border=True):
    st.subheader("Experiment controls")

    camera_count = st.slider(
        "Number of active cameras",
        min_value=5,
        max_value=40,
        value=10,
        step=1,
        disabled=controller.is_running,
    )

    fps_per_camera = st.slider(
        "Sampling rate per camera (FPS)",
        min_value=1,
        max_value=20,
        value=12,
        step=1,
        disabled=controller.is_running,
    )

    person_camera_count = int(camera_count * person_ratio + 0.5)
    person_camera_count = max(1, min(camera_count - 1, person_camera_count))
    plate_camera_count = camera_count - person_camera_count

    worker_col1, worker_col2 = st.columns(2)

    with worker_col1:
        person_workers = st.slider(
            "Person detector workers",
            min_value=1,
            max_value=min(8, person_camera_count),
            value=min(2, person_camera_count),
            step=1,
            disabled=controller.is_running,
        )

    with worker_col2:
        plate_workers = st.slider(
            "Plate detector workers",
            min_value=1,
            max_value=min(4, plate_camera_count),
            value=1,
            step=1,
            disabled=controller.is_running,
        )

    target_total = camera_count * fps_per_camera
    target_person = person_camera_count * fps_per_camera
    target_plate = plate_camera_count * fps_per_camera

    st.caption(
        f"Target input: **{target_total:,} frames/s** "
        f"({target_person:,} person-topic + {target_plate:,} plate-topic). "
        f"Camera mix: {person_camera_count} person / {plate_camera_count} plate. "
        "Detector processing delay: 30 ms per message."
    )

    button_col1, button_col2 = st.columns(2)

    with button_col1:
        start_clicked = st.button(
            "Start pipeline",
            type="primary",
            use_container_width=True,
            disabled=controller.is_running,
        )

    with button_col2:
        stop_clicked = st.button(
            "Stop pipeline",
            use_container_width=True,
            disabled=not controller.is_running,
        )

    if start_clicked:
        try:
            run_id = controller.start(
                camera_count=camera_count,
                fps_per_camera=float(fps_per_camera),
                person_workers=person_workers,
                plate_workers=plate_workers,
            )
            st.success(f"Pipeline started. Run ID: {run_id}")
        except Exception as exc:
            st.error(f"Unable to start the pipeline: {exc}")

    if stop_clicked:
        controller.stop()
        st.success("Pipeline stop requested. Final metrics are being flushed.")

# ============================================================
# 6. LIVE PIPELINE VIEW
# ============================================================

run_id = controller.active_run_id
run_dir = controller.run_directory()

if run_id is None or run_dir is None:
    st.info("Choose a workload above and start the Kafka pipeline to view live metrics.")
else:
    status_text = "Running" if controller.is_running else "Stopped"
    st.subheader("Live pipeline")
    st.caption(f"Run: `{run_id}` · Status: **{status_text}**")
    render_live_metrics(run_dir, controller)

st.divider()
st.caption(
    "Measurement scope: application-level Kafka and detector metrics. "
    "The mock detectors simulate processing with a 30 ms delay; no YOLO inference "
    "is run. CPU, RAM, GPU, broker disk I/O and broker host network metrics are "
    "not collected by this dashboard."
)

"""Shared live Kafka pipeline state and metrics for Streamlit pages."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from advanced.performance_monitoring.pipeline_controller import PipelineController


# ============================================================
# 1. SHARED PIPELINE CONTROLLER
# ============================================================

@st.cache_resource
def get_pipeline_controller() -> PipelineController:
    """Return the single process controller shared by all UI pages."""

    return PipelineController()


# ============================================================
# 2. RUN FILE READERS
# ============================================================

def read_json(path: Path) -> dict:
    """Read a JSON object, returning an empty mapping when unavailable."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    return value if isinstance(value, dict) else {}


def read_jsonl(path: Path, limit: int = 600) -> list[dict]:
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


def latest_record(path: Path) -> dict | None:
    """Return the latest valid JSONL record."""

    records = read_jsonl(path, limit=5)
    return records[-1] if records else None


def worker_metric_paths(run_dir: Path, detector: str) -> list[Path]:
    """Return metric files for one detector type."""

    return sorted(run_dir.glob(f"{detector}_worker*_metrics.jsonl"))


# ============================================================
# 3. WORKER AND LAG SNAPSHOTS
# ============================================================

def latest_worker_rows(run_dir: Path, detector: str) -> list[dict]:
    """Return the latest metrics for each worker of one detector type."""

    rows = []
    for path in worker_metric_paths(run_dir, detector):
        record = latest_record(path)
        if record is None:
            continue

        rates = record.get("rates_per_second", {})
        latency = record.get("latency_ms", {})
        rows.append({
            "Detector": detector.title(),
            "Worker": record.get("component", path.stem),
            "Received/s": rates.get("messages_received", 0),
            "Processed/s": rates.get("messages_processed", 0),
            "Committed/s": rates.get("messages_committed", 0),
            "Processing p95 (ms)": (
                latency.get("message_processing", {}).get("p95")
            ),
            "Commit p95 (ms)": (
                latency.get("kafka_commit", {}).get("p95")
            ),
            "Failed": record.get("totals", {}).get("messages_failed", 0),
        })

    return rows


def latest_lag_rows(run_dir: Path) -> tuple[list[dict], int | None]:
    """Return the latest Kafka partition lag and its known total."""

    record = latest_record(run_dir / "consumer_lag_metrics.jsonl")
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
                "Expired offset gap": partition.get("expired_offset_gap", 0),
                "Status": partition.get("status", "unknown"),
            })

    return rows, (None if has_missing else known_total)


def latest_rates(run_dir: Path) -> dict:
    """Aggregate the latest producer and detector-worker rates."""

    producer = latest_record(run_dir / "producer_metrics.jsonl") or {}
    producer_rates = producer.get("rates_per_second", {})
    producer_latency = producer.get("latency_ms", {})

    person_records = [
        latest_record(path)
        for path in worker_metric_paths(run_dir, "person")
    ]
    plate_records = [
        latest_record(path)
        for path in worker_metric_paths(run_dir, "plate")
    ]
    person_records = [record for record in person_records if record]
    plate_records = [record for record in plate_records if record]

    def processed_rate(records: list[dict]) -> float:
        return sum(
            record.get("rates_per_second", {}).get(
                "messages_processed", 0
            ) or 0
            for record in records
        )

    def processing_p95(records: list[dict]):
        values = [
            record.get("latency_ms", {}).get(
                "message_processing", {}
            ).get("p95")
            for record in records
        ]
        values = [value for value in values if value is not None]
        return max(values) if values else None

    producer_counters = producer.get("counters", {})
    queued_messages = producer_counters.get("messages_queued", 0) or 0
    queued_bytes = producer_counters.get("payload_bytes", 0) or 0
    mean_payload_bytes = (
        queued_bytes / queued_messages
        if queued_messages
        else None
    )

    return {
        "producer_frames": producer_rates.get("frames_generated"),
        "producer_delivered": producer_rates.get("messages_delivered"),
        "producer_person_delivered": producer_rates.get(
            "messages_delivered_person"
        ),
        "producer_plate_delivered": producer_rates.get(
            "messages_delivered_plate"
        ),
        "producer_bytes": producer_rates.get("payload_bytes"),
        "producer_mean_payload_bytes": mean_payload_bytes,
        "producer_queue": producer.get("gauges", {}).get(
            "producer_queue_depth"
        ),
        "producer_frame_read_p95": (
            producer_latency.get("frame_read_encode", {}).get("p95")
        ),
        "producer_delivery_p95": (
            producer_latency.get("producer_delivery", {}).get("p95")
        ),
        "person_processed": processed_rate(person_records),
        "plate_processed": processed_rate(plate_records),
        "person_processing_p95": processing_p95(person_records),
        "plate_processing_p95": processing_p95(plate_records),
    }


# ============================================================
# 4. TIME-SERIES AGGREGATION
# ============================================================

def throughput_history(run_dir: Path) -> pd.DataFrame:
    """Aggregate producer and detector rates into one-second buckets."""

    buckets: dict[pd.Timestamp, dict[str, float]] = {}

    def add_record(path: Path, metric: str, label: str) -> None:
        for record in read_jsonl(path):
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
            buckets[bucket][label] = (
                buckets[bucket].get(label, 0.0) + value
            )

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
    add_record(
        run_dir / "producer_metrics.jsonl",
        "messages_delivered_person",
        "Person produced",
    )
    add_record(
        run_dir / "producer_metrics.jsonl",
        "messages_delivered_plate",
        "Plate produced",
    )

    for path in worker_metric_paths(run_dir, "person"):
        add_record(path, "messages_processed", "Person processed")

    for path in worker_metric_paths(run_dir, "plate"):
        add_record(path, "messages_processed", "Plate processed")

    if not buckets:
        return pd.DataFrame()

    dataframe = pd.DataFrame.from_dict(
        buckets,
        orient="index",
    ).fillna(0.0)
    dataframe.index = pd.DatetimeIndex(
        dataframe.index,
        name="Timestamp (UTC)",
    )
    dataframe = dataframe.sort_index()

    columns = (
        "Frames generated",
        "Producer delivered",
        "Person produced",
        "Plate produced",
        "Person processed",
        "Plate processed",
    )
    for column in columns:
        if column not in dataframe:
            dataframe[column] = 0.0

    return dataframe[list(columns)]


def lag_history(run_dir: Path) -> pd.DataFrame:
    """Build a time series of committed lag for person and plate topics."""

    rows = []
    for record in read_jsonl(
        run_dir / "consumer_lag_metrics.jsonl"
    ):
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
# 5. COMPLETE RUN SNAPSHOT
# ============================================================

def load_pipeline_snapshot(run_dir: Path) -> dict:
    """Load the saved run configuration and latest application metrics."""

    worker_rows = (
        latest_worker_rows(run_dir, "person")
        + latest_worker_rows(run_dir, "plate")
    )
    lag_rows, total_lag = latest_lag_rows(run_dir)

    return {
        "config": read_json(run_dir / "pipeline_config.json"),
        "metadata": read_json(run_dir / "run_metadata.json"),
        "rates": latest_rates(run_dir),
        "person_workers": latest_worker_rows(run_dir, "person"),
        "plate_workers": latest_worker_rows(run_dir, "plate"),
        "worker_rows": worker_rows,
        "lag_rows": lag_rows,
        "total_lag": total_lag,
        "throughput": throughput_history(run_dir),
        "lag_history": lag_history(run_dir),
        "producer_record": latest_record(
            run_dir / "producer_metrics.jsonl"
        ),
    }


"""
Performance metrics collector.

Responsibilities:
- Collect application counters.
- Measure processing and delivery latency.
- Calculate throughput over fixed measurement intervals.
- Calculate latency percentiles (p50, p95, p99).
- Record numeric performance gauges.
- Write periodic metrics snapshots to JSONL files.

The collector is independent of Kafka and can be injected
into producers and detector workers.

It does not collect hardware resource utilization.
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ============================================================
# 1. LOGGING AND CONSTANTS
# ============================================================

DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 1.0

METRIC_NAME_PATTERN = re.compile(
    r"^[a-z][a-z0-9_]*$"
)

RUN_ID_PATTERN = re.compile(
    r"^run_[A-Za-z0-9][A-Za-z0-9_-]*$"
)

COMPONENT_PATTERN = re.compile(
    r"^[A-Za-z][A-Za-z0-9_-]*$"
)


# ============================================================
# 2. METRICS COLLECTOR
# ============================================================

class MetricsCollector:
    """
    Collect and periodically persist application metrics.

    Dependencies:
        output_path:
            Destination JSONL file.

        run_id:
            Unique experimental run identifier.

        component:
            Name of the monitored component.
            Examples: producer, person_worker1, plate_worker.

        snapshot_interval_seconds:
            Time interval between metrics snapshots.

    Metric types:
        Counters:
            Cumulative event counts and byte counts.

        Latencies:
            Individual duration observations in milliseconds.

        Gauges:
            Latest numeric values, such as queue depth.
    """

    def __init__(
        self,
        output_path: str | Path,
        *,
        run_id: str,
        component: str,
        snapshot_interval_seconds: float = (
            DEFAULT_SNAPSHOT_INTERVAL_SECONDS
        ),
    ) -> None:

        # ----------------------------------------------------
        # Validate output path
        # ----------------------------------------------------

        if (
            not isinstance(output_path, (str, Path))
            or not str(output_path).strip()
        ):
            raise ValueError(
                "output_path must be a non-empty path"
            )

        self.output_path = Path(output_path)

        # ----------------------------------------------------
        # Validate run ID
        # ----------------------------------------------------

        if (
            not isinstance(run_id, str)
            or not RUN_ID_PATTERN.fullmatch(run_id)
        ):
            raise ValueError(
                f"Invalid run ID: {run_id!r}"
            )

        self.run_id = run_id

        # ----------------------------------------------------
        # Validate component name
        # ----------------------------------------------------

        if (
            not isinstance(component, str)
            or not COMPONENT_PATTERN.fullmatch(component)
        ):
            raise ValueError(
                f"Invalid component name: {component!r}"
            )

        self.component = component

        # ----------------------------------------------------
        # Validate snapshot interval
        # ----------------------------------------------------

        if (
            isinstance(snapshot_interval_seconds, bool)
            or not isinstance(
                snapshot_interval_seconds,
                (int, float),
            )
            or not math.isfinite(snapshot_interval_seconds)
            or snapshot_interval_seconds <= 0
        ):
            raise ValueError(
                "Snapshot interval must be a positive finite number"
            )

        self.snapshot_interval_seconds = float(
            snapshot_interval_seconds
        )

        # ----------------------------------------------------
        # Prepare output directory
        # ----------------------------------------------------

        self.output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # ----------------------------------------------------
        # Initialize metric storage
        # ----------------------------------------------------

        self._interval_counters: Counter[str] = Counter()

        self._total_counters: Counter[str] = Counter()

        self._interval_latencies: dict[
            str,
            list[float],
        ] = defaultdict(list)

        self._gauges: dict[str, int | float] = {}

        # ----------------------------------------------------
        # Initialize timing and synchronization
        # ----------------------------------------------------

        self._started_at = time.monotonic()

        self._last_snapshot_at = self._started_at

        self._total_elapsed_seconds = 0.0

        self._lock = threading.Lock()

        self._closed = False

    # ========================================================
    # 3. VALIDATION HELPERS
    # ========================================================

    @staticmethod
    def _validate_metric_name(name: str) -> None:
        """Validate a metric name."""

        if (
            not isinstance(name, str)
            or not METRIC_NAME_PATTERN.fullmatch(name)
        ):
            raise ValueError(
                f"Invalid metric name: {name!r}"
            )

    def _ensure_open(self) -> None:
        """Ensure the collector has not been closed."""

        if self._closed:
            raise RuntimeError(
                "Metrics collector is already closed"
            )

    # ========================================================
    # 4. COUNTER COLLECTION
    # ========================================================

    def increment_counter(
        self,
        name: str,
        amount: int = 1,
    ) -> None:
        """
        Increment an interval and cumulative counter.

        Examples:
            messages_received
            messages_processed
            messages_failed
            messages_committed
            payload_bytes
        """

        self._validate_metric_name(name)

        if (
            isinstance(amount, bool)
            or not isinstance(amount, int)
            or amount < 0
        ):
            raise ValueError(
                "Counter amount must be a non-negative integer"
            )

        with self._lock:

            self._ensure_open()

            self._interval_counters[name] += amount

            self._total_counters[name] += amount

    # ========================================================
    # 5. LATENCY COLLECTION
    # ========================================================

    def observe_latency_ms(
        self,
        name: str,
        duration_ms: float,
    ) -> None:
        """
        Record an individual latency observation in milliseconds.

        Examples:
            message_processing
            csv_write
            csv_flush
            kafka_commit
            producer_delivery
            frame_read
            jpeg_encode
        """

        self._validate_metric_name(name)

        if (
            isinstance(duration_ms, bool)
            or not isinstance(duration_ms, (int, float))
            or not math.isfinite(duration_ms)
            or duration_ms < 0
        ):
            raise ValueError(
                "Latency must be a non-negative finite number"
            )

        with self._lock:

            self._ensure_open()

            self._interval_latencies[name].append(
                float(duration_ms)
            )

    # ========================================================
    # 6. GAUGE COLLECTION
    # ========================================================

    def set_gauge(
        self,
        name: str,
        value: int | float,
    ) -> None:
        """
        Update the latest value of a numeric gauge.

        Examples:
            producer_queue_depth
            consumer_lag_messages
            pending_messages
        """

        self._validate_metric_name(name)

        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError(
                "Gauge value must be a finite number"
            )

        with self._lock:

            self._ensure_open()

            self._gauges[name] = value

    # ========================================================
    # 7. LATENCY STATISTICS
    # ========================================================

    @staticmethod
    def _percentile(
        sorted_values: list[float],
        percentile: float,
    ) -> float:
        """
        Calculate a nearest-rank percentile.

        The input values must already be sorted.
        """

        index = max(
            0,
            math.ceil(
                percentile * len(sorted_values)
            ) - 1,
        )

        return round(
            sorted_values[index],
            3,
        )

    @classmethod
    def _summarize_latencies(
        cls,
        values: list[float],
    ) -> dict[str, Any]:
        """Calculate latency distribution statistics."""

        if not values:
            return {
                "count": 0,
                "mean": None,
                "min": None,
                "p50": None,
                "p95": None,
                "p99": None,
                "max": None,
            }

        sorted_values = sorted(values)

        return {
            "count": len(sorted_values),

            "mean": round(
                sum(sorted_values) / len(sorted_values),
                3,
            ),

            "min": round(
                sorted_values[0],
                3,
            ),

            "p50": cls._percentile(
                sorted_values,
                0.50,
            ),

            "p95": cls._percentile(
                sorted_values,
                0.95,
            ),

            "p99": cls._percentile(
                sorted_values,
                0.99,
            ),

            "max": round(
                sorted_values[-1],
                3,
            ),
        }

    # ========================================================
    # 8. WRITE METRICS SNAPSHOT
    # ========================================================

    def _write_snapshot_locked(
        self,
        now: float,
    ) -> dict[str, Any]:
        """
        Build and persist one snapshot.

        The caller must hold the collector lock.
        """

        elapsed = max(
            now - self._last_snapshot_at,
            1e-9,
        )

        self._total_elapsed_seconds += elapsed

        interval_counters = dict(
            sorted(self._interval_counters.items())
        )

        total_counters = dict(
            sorted(self._total_counters.items())
        )

        rates = {
            name: round(
                count / elapsed,
                3,
            )
            for name, count in interval_counters.items()
        }

        latency_statistics = {
            name: self._summarize_latencies(values)
            for name, values in sorted(
                self._interval_latencies.items()
            )
        }

        record = {
            "timestamp_utc": datetime.now(
                timezone.utc
            ).isoformat(),

            "run_id": self.run_id,

            "component": self.component,

            "process_id": os.getpid(),

            "interval_seconds": round(
                elapsed,
                6,
            ),

            "elapsed_seconds": round(
                self._total_elapsed_seconds,
                6,
            ),

            "counters": interval_counters,

            "rates_per_second": rates,

            "totals": total_counters,

            "latency_ms": latency_statistics,

            "gauges": dict(
                sorted(self._gauges.items())
            ),
        }

        # Append one JSON object per line.
        with self.output_path.open(
            mode="a",
            encoding="utf-8",
            newline="",
        ) as file:

            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                )
            )

            file.write("\n")

        # Reset interval metrics only after a successful write.
        self._interval_counters.clear()

        self._interval_latencies.clear()

        self._last_snapshot_at = now

        return record

    # ========================================================
    # 9. PERIODIC SNAPSHOT
    # ========================================================

    def write_snapshot(
        self,
        *,
        force: bool = False,
    ) -> dict[str, Any] | None:
        """
        Write a metrics snapshot when the interval has elapsed.

        Args:
            force:
                Write immediately, regardless of the interval.

        Returns:
            The written snapshot, or None if not yet due.
        """

        with self._lock:

            self._ensure_open()

            now = time.monotonic()

            elapsed = (
                now - self._last_snapshot_at
            )

            if (
                not force
                and elapsed < self.snapshot_interval_seconds
            ):
                return None

            return self._write_snapshot_locked(now)

    # ========================================================
    # 10. COLLECTOR STATUS
    # ========================================================

    @property
    def is_closed(self) -> bool:
        """Return whether the collector has been closed."""

        with self._lock:
            return self._closed

    # ========================================================
    # 11. CLEAN SHUTDOWN
    # ========================================================

    def close(self) -> None:
        """
        Write the final snapshot and close the collector.

        Safe to call multiple times.
        """

        with self._lock:

            if self._closed:
                return

            self._write_snapshot_locked(
                time.monotonic()
            )

            self._closed = True

    # ========================================================
    # 12. CONTEXT MANAGER SUPPORT
    # ========================================================

    def __enter__(self) -> MetricsCollector:
        """Return the active collector."""

        self._ensure_open()

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        """Write final metrics and close the collector."""

        self.close()

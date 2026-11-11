
"""
Kafka consumer for synthetic person detection.

Responsibilities:
- Consume person-camera frames from Kafka.
- Validate camera metadata and message structure.
- Run the person movement simulator.
- Evaluate employee authorization.
- Persist detection events to CSV.
- Commit Kafka offsets after successful persistence.

Application configuration and camera definitions are injected
during initialization.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import logging

from pathlib import Path
from typing import Any

from confluent_kafka import Consumer

from advanced.config import (
    ApplicationSettings,
    CameraConfig,
    load_cameras,
    load_settings,
)

from advanced.performance_monitoring.metrics_collector import MetricsCollector

from advanced.Consumers.person_simulator import (
    PersonReferenceData,
    PersonMovementSimulator,
    is_authorized,
    load_reference_data,
)


# ============================================================
# 1. LOGGING AND CONSTANTS
# ============================================================

logger = logging.getLogger(__name__)

CSV_FIELDS = (
    "Time_stamp",
    "Zone",
    "detected_persons_id",
    "authorized",
    "camera_id",
    "event_id",
    "detector_type",
)
DETECTOR_PROCESSING_DELAY_SECONDS = 0.030

# ============================================================
# 2. KAFKA MESSAGE HELPERS
# ============================================================

def read_header(
    message: Any,
    name: str,
) -> str:
    """Read and decode a required Kafka message header."""

    headers = message.headers() or []

    matching_values = [
        value
        for key, value in headers
        if key == name
    ]

    if not matching_values:
        raise ValueError(
            f"Missing Kafka header: {name}"
        )

    if len(matching_values) > 1:
        raise ValueError(
            f"Duplicate Kafka header: {name}"
        )

    value = matching_values[0]

    if isinstance(value, bytes):

        try:
            return value.decode("utf-8")

        except UnicodeDecodeError as exc:
            raise ValueError(
                f"Invalid UTF-8 in Kafka header: {name}"
            ) from exc

    if isinstance(value, str):
        return value

    raise ValueError(
        f"Unsupported Kafka header type: {name}"
    )


def read_message_key(message: Any) -> str:
    """Decode the Kafka message key."""

    key = message.key()

    if isinstance(key, bytes):

        try:
            return key.decode("utf-8")

        except UnicodeDecodeError as exc:
            raise ValueError(
                "Invalid UTF-8 in Kafka message key"
            ) from exc

    if isinstance(key, str):
        return key

    raise ValueError(
        "Kafka message must contain a valid camera ID key"
    )


# ============================================================
# 3. PERSON DETECTOR
# ============================================================

class PersonDetector:
    """
    Consume camera frames and produce synthetic person detections.

    Dependencies:
        settings:
            Validated application configuration.

        cameras:
            Camera configurations loaded at application startup.

        reference_data:
            Optional preloaded employee and authorization data.

        consumer:
            Optional injected Kafka consumer for testing.
    """

    def __init__(
        self,
        settings: ApplicationSettings,
        cameras: tuple[CameraConfig, ...] | list[CameraConfig],
        *,
        reference_data: PersonReferenceData | None = None,
        consumer: Any | None = None,
        output_path: str | Path | None = None,
        metrics_collector: MetricsCollector | None = None,
    ) -> None:

        # ----------------------------------------------------
        # Validate settings
        # ----------------------------------------------------

        if not isinstance(settings, ApplicationSettings):
            raise TypeError(
                "settings must be an ApplicationSettings instance"
            )

        self.settings = settings
        self.metrics_collector = metrics_collector

        # ----------------------------------------------------
        # Validate camera configurations
        # ----------------------------------------------------

        self.cameras = tuple(cameras)

        if not self.cameras:
            raise ValueError(
                "No camera configurations supplied"
            )

        if not all(
            isinstance(camera, CameraConfig)
            for camera in self.cameras
        ):
            raise TypeError(
                "Every camera must be a CameraConfig instance"
            )

        camera_ids = [
            camera.camera_id
            for camera in self.cameras
        ]

        if len(camera_ids) != len(set(camera_ids)):
            raise ValueError(
                "Duplicate camera IDs detected"
            )

        self.camera_by_id = {
            camera.camera_id: camera
            for camera in self.cameras
        }

        # ----------------------------------------------------
        # Load reference data once
        # ----------------------------------------------------

        if reference_data is None:

            reference_data = load_reference_data(
                employees_csv=settings.paths.employees_csv,
                restricted_access_csv=(
                    settings.paths.restricted_access_csv
                ),
            )

        self.reference_data = reference_data

        # ----------------------------------------------------
        # Initialize simulation engine
        # ----------------------------------------------------

        self.simulator = PersonMovementSimulator(
            settings=settings.simulation,
            reference_data=reference_data,
            cameras=self.cameras,
        )

        # ----------------------------------------------------
        # Output path
        # ----------------------------------------------------

        self.output_path = Path(
            output_path
            if output_path is not None
            else settings.paths.person_detections_csv(
                settings.person_detector_worker_id
            )
        )

        # ----------------------------------------------------
        # Kafka consumer
        # ----------------------------------------------------

        if consumer is None:

            consumer_config = {
                "bootstrap.servers": (
                    settings.kafka.bootstrap_servers
                ),

                "broker.address.family": (
                    settings.kafka.address_family
                ),

                "group.id": (
                    settings.kafka.person_detector_group_id
                ),

                "client.id": (
                    f"person-detector-"
                    f"{settings.person_detector_worker_id}"
                ),

                "auto.offset.reset": "latest",

                "enable.auto.commit": False,
            }

            self._consumer = Consumer(
                consumer_config
            )

        else:
            self._consumer = consumer

        self._closed = False

        logger.info(
            "Person detector initialized | "
            "Employees: %d | Cameras: %d | Output: %s",
            len(reference_data.employees),
            len(self.simulator.person_camera_ids),
            self.output_path,
        )

    # ========================================================
    # 4. MESSAGE VALIDATION AND PROCESSING
    # ========================================================

    def process_message(
        self,
        message: Any,
    ) -> dict[str, str]:
        """
        Validate one Kafka message and generate a detection event.

        This method does not write files or commit Kafka offsets.
        """

        if message.error() is not None:
            raise RuntimeError(
                f"Kafka message error: {message.error()}"
            )

        expected_topic = (
            self.settings.kafka.person_frames_topic
        )

        if message.topic() != expected_topic:
            raise ValueError(
                f"Unexpected Kafka topic: {message.topic()}"
            )

        # ----------------------------------------------------
        # Extract metadata
        # ----------------------------------------------------

        camera_id = read_header(
            message,
            "camera_id",
        )

        zone = read_header(
            message,
            "zone_id",
        )

        timestamp = read_header(
            message,
            "timestamp_utc",
        )

        # ----------------------------------------------------
        # Validate message key
        # ----------------------------------------------------

        message_key = read_message_key(message)

        if message_key != camera_id:
            raise ValueError(
                "Kafka message key does not match camera ID"
            )

        # ----------------------------------------------------
        # Validate image payload
        # ----------------------------------------------------

        payload = message.value()

        if not isinstance(payload, bytes) or not payload:
            raise ValueError(
                f"Invalid image payload from camera {camera_id}"
            )

        # ----------------------------------------------------
        # Validate camera configuration
        # ----------------------------------------------------

        camera = self.camera_by_id.get(camera_id)

        if camera is None:
            raise ValueError(
                f"Unknown camera ID: {camera_id}"
            )

        if not camera.enabled:
            raise ValueError(
                f"Camera {camera_id} is disabled"
            )

        if camera.detector_type != "person":
            raise ValueError(
                f"Camera {camera_id} is not a person camera"
            )

        if camera.zone_id != zone:
            raise ValueError(
                f"Zone mismatch for camera {camera_id}"
            )

        if camera.kafka_partition != message.partition():
            raise ValueError(
                f"Kafka partition mismatch for camera {camera_id}"
            )

        # ----------------------------------------------------
        # Simulate detected employees
        # ----------------------------------------------------

        detected_ids = self.simulator.simulate(
            camera_id=camera_id,
            timestamp=timestamp,
        )
        time.sleep(DETECTOR_PROCESSING_DELAY_SECONDS)
        authorization = [
            is_authorized(
                employee_id=employee_id,
                zone=zone,
                restricted_access=(
                    self.reference_data.restricted_access
                ),
            )
            for employee_id in detected_ids
        ]

        # ----------------------------------------------------
        # Generate unique event identifier
        # ----------------------------------------------------

        event_id = (
            f"{message.topic()}:"
            f"{message.partition()}:"
            f"{message.offset()}"
        )

        return {
            "Time_stamp": timestamp,
            "Zone": zone,

            "detected_persons_id": json.dumps(
                detected_ids
            ),

            "authorized": json.dumps(
                authorization
            ),

            "camera_id": camera_id,
            "event_id": event_id,
            "detector_type": "synthetic",
        }

    # ========================================================
    # 5. OUTPUT FILE VALIDATION
    # ========================================================

    def _prepare_output(self) -> bool:
        """
        Prepare the output directory and validate existing CSV schema.

        Returns:
            True if a CSV header needs to be written.
        """

        self.output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        needs_header = (
            not self.output_path.exists()
            or self.output_path.stat().st_size == 0
        )

        if not needs_header:

            with self.output_path.open(
                newline="",
                encoding="utf-8",
            ) as file:

                existing_header = next(
                    csv.reader(file),
                    [],
                )

            if existing_header != list(CSV_FIELDS):
                raise ValueError(
                    f"Existing CSV has an incompatible schema: "
                    f"{self.output_path}"
                )

        return needs_header

    # ========================================================
    # 6. CONSUMER LIFECYCLE
    # ========================================================

    def run_forever(
        self,
        poll_timeout: float = 1.0,
    ) -> None:
        """
        Consume messages continuously.

        Kafka offsets are committed only after the corresponding
        detection event has been written and flushed to CSV.
        """

        if self._closed:
            raise RuntimeError(
                "Cannot run a closed person detector"
            )

        if (
            isinstance(poll_timeout, bool)
            or not isinstance(poll_timeout, (int, float))
            or poll_timeout <= 0
        ):
            raise ValueError(
                "Poll timeout must be a positive number"
            )

        topic = self.settings.kafka.person_frames_topic

        needs_header = self._prepare_output()

        logger.info(
            "Starting person detector | Topic: %s",
            topic,
        )

        self._consumer.subscribe([topic])

        try:

            with self.output_path.open(
                mode="a",
                newline="",
                encoding="utf-8",
            ) as file:

                writer = csv.DictWriter(
                    file,
                    fieldnames=CSV_FIELDS,
                )

                if needs_header:
                    writer.writeheader()
                    file.flush()

                while True:

                    message = self._consumer.poll(
                        float(poll_timeout)
                    )

                    if message is None:
                        if self.metrics_collector is not None:
                            self.metrics_collector.write_snapshot()
                        continue

                    if self.metrics_collector is not None:
                        self.metrics_collector.increment_counter(
                            "messages_received"
                        )
                        payload = message.value()
                        if isinstance(payload, bytes):
                            self.metrics_collector.increment_counter(
                                "payload_bytes",
                                len(payload),
                            )

                    if message.error() is not None:
                        if self.metrics_collector is not None:
                            self.metrics_collector.increment_counter(
                                "messages_failed"
                            )
                        raise RuntimeError(
                            f"Kafka consumer error: {message.error()}"
                        )

                    try:
                        # 1. Validate and process the message.
                        processing_started = time.perf_counter()
                        event = self.process_message(message)

                        if self.metrics_collector is not None:
                            self.metrics_collector.observe_latency_ms(
                                "message_processing",
                                (
                                    time.perf_counter() - processing_started
                                ) * 1000,
                            )

                        # 2. Persist the event.
                        write_started = time.perf_counter()
                        writer.writerow(event)

                        if self.metrics_collector is not None:
                            self.metrics_collector.observe_latency_ms(
                                "csv_write",
                                (
                                    time.perf_counter() - write_started
                                ) * 1000,
                            )

                        # 3. Ensure the data is flushed.
                        flush_started = time.perf_counter()
                        file.flush()

                        if self.metrics_collector is not None:
                            self.metrics_collector.observe_latency_ms(
                                "csv_flush",
                                (
                                    time.perf_counter() - flush_started
                                ) * 1000,
                            )
                            self.metrics_collector.increment_counter(
                                "messages_processed"
                            )

                        # 4. Commit the Kafka offset.
                        commit_started = time.perf_counter()
                        self._consumer.commit(
                            message=message,
                            asynchronous=False,
                        )

                        if self.metrics_collector is not None:
                            self.metrics_collector.observe_latency_ms(
                                "kafka_commit",
                                (
                                    time.perf_counter() - commit_started
                                ) * 1000,
                            )
                            self.metrics_collector.increment_counter(
                                "messages_committed"
                            )
                            self.metrics_collector.write_snapshot()

                    except Exception:
                        if self.metrics_collector is not None:
                            self.metrics_collector.increment_counter(
                                "messages_failed"
                            )
                        raise

                    logger.info(
                        "Person detection processed | "
                        "Camera: %s | Zone: %s | Persons: %s",
                        event["camera_id"],
                        event["Zone"],
                        event["detected_persons_id"],
                    )

        except KeyboardInterrupt:

            logger.info(
                "Person detector interrupted by user"
            )

        finally:
            self.close()

    # ========================================================
    # 7. CLEAN SHUTDOWN
    # ========================================================

    def close(self) -> None:
        """Close the Kafka consumer exactly once."""

        if self._closed:
            return

        self._closed = True

        try:
            self._consumer.close()
        finally:
            if self.metrics_collector is not None:
                self.metrics_collector.close()

        logger.info(
            "Person detector closed"
        )


# ============================================================
# 8. APPLICATION ENTRY POINT
# ============================================================

def main() -> None:
    """Initialize dependencies and start the person detector."""

    parser = argparse.ArgumentParser(
        description="Run the Kafka person detector."
    )
    parser.add_argument(
        "--run-id",
        default="run_001",
        help="Experiment run ID (default: run_001).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    # Load application configuration once.
    settings = load_settings()

    # Load camera configurations once.
    cameras = load_cameras(
        settings.paths.cameras_csv
    )

    # Create worker-specific metrics output.
    worker_id = settings.person_detector_worker_id
    metrics_collector = MetricsCollector(
        output_path=(
            settings.paths.experiment_run_dir(args.run_id)
            / f"person_{worker_id}_metrics.jsonl"
        ),
        run_id=args.run_id,
        component=f"person_{worker_id}",
    )

    # Initialize detector.
    detector = PersonDetector(
        settings=settings,
        cameras=cameras,
        metrics_collector=metrics_collector,
    )

    detector.run_forever()


if __name__ == "__main__":
    main()

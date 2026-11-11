"""
Kafka consumer for synthetic license-plate detection.

Responsibilities:
- Consume parking-camera frames from Kafka.
- Validate Kafka message metadata and payloads.
- Run the vehicle simulation.
- Persist plate detection events to CSV.
- Commit Kafka offsets after successful persistence.

Application settings, camera configurations and vehicle
reference data are injected during initialization.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
import math

from collections.abc import Iterable
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
from advanced.performance_monitoring.pipeline_config import (
    cameras_from_config,
    load_pipeline_config,
    load_run_settings,
)

from advanced.Consumers.plate_simulator import (
    PlateSimulator,
    VehicleReferenceData,
    load_registered_vehicles,
)


# ============================================================
# 1. LOGGING AND CONSTANTS
# ============================================================

logger = logging.getLogger(__name__)

CSV_FIELDS = (
    "Time_stamp",
    "Zone",
    "detected_plates",
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
    """Read and decode exactly one required Kafka header."""

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
    """Read and decode the Kafka message key."""

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
# 3. PLATE DETECTOR
# ============================================================

class PlateDetector:
    """
    Consume parking-camera frames and produce plate detections.

    Dependencies:
        settings:
            Validated ApplicationSettings.

        cameras:
            Validated camera configurations.

        reference_data:
            Registered vehicle information.

        consumer:
            Optional injected Kafka consumer for testing.

        output_path:
            Optional CSV output path for testing or custom setups.
    """

    def __init__(
        self,
        settings: ApplicationSettings,
        cameras: Iterable[CameraConfig],
        *,
        reference_data: VehicleReferenceData | None = None,
        consumer: Any | None = None,
        output_path: str | Path | None = None,
        metrics_collector: MetricsCollector | None = None,
        worker_id: str = "worker1",
    ) -> None:

        # ----------------------------------------------------
        # Validate application settings
        # ----------------------------------------------------

        if not isinstance(settings, ApplicationSettings):
            raise TypeError(
                "settings must be an ApplicationSettings instance"
            )

        self.settings = settings
        self.metrics_collector = metrics_collector
        self.worker_id = worker_id
        settings.paths.plate_worker_detections_csv(worker_id)

        # ----------------------------------------------------
        # Validate camera configurations
        # ----------------------------------------------------

        if isinstance(cameras, (str, bytes)):
            raise TypeError(
                "cameras must be an iterable of CameraConfig objects"
            )

        try:
            camera_list = tuple(cameras)

        except TypeError as exc:
            raise TypeError(
                "cameras must be an iterable of CameraConfig objects"
            ) from exc

        if not camera_list:
            raise ValueError(
                "No camera configurations supplied"
            )

        if not all(
            isinstance(camera, CameraConfig)
            for camera in camera_list
        ):
            raise TypeError(
                "Every camera must be a CameraConfig instance"
            )

        camera_ids = [
            camera.camera_id
            for camera in camera_list
        ]

        if len(camera_ids) != len(set(camera_ids)):
            raise ValueError(
                "Duplicate camera IDs detected"
            )

        self.cameras = camera_list

        self.camera_by_id = {
            camera.camera_id: camera
            for camera in self.cameras
        }

        # ----------------------------------------------------
        # Identify active parking cameras
        # ----------------------------------------------------

        self.parking_camera_ids = tuple(
            sorted(
                camera.camera_id
                for camera in self.cameras
                if (
                    camera.enabled
                    and camera.zone_id == "E"
                    and camera.detector_type == "plate"
                )
            )
        )

        if not self.parking_camera_ids:
            raise ValueError(
                "No active parking cameras available"
            )

        # ----------------------------------------------------
        # Load vehicle reference data once
        # ----------------------------------------------------

        if reference_data is None:

            reference_data = load_registered_vehicles(
                settings.paths.employees_csv
            )

        if not isinstance(
            reference_data,
            VehicleReferenceData,
        ):
            raise TypeError(
                "reference_data must be a VehicleReferenceData instance"
            )

        self.reference_data = reference_data

        # ----------------------------------------------------
        # Initialize simulation engine
        # ----------------------------------------------------

        self.simulator = PlateSimulator(
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
            else settings.paths.plate_detections_csv
        )

        # ----------------------------------------------------
        # Initialize Kafka consumer
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
                    settings.kafka.plate_detector_group_id
                ),

                "client.id": (
                    f"ai-monitoring-plate-detector-{worker_id}"
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
            "Plate detector initialized | "
            "Parking cameras: %d | Output: %s",
            len(self.parking_camera_ids),
            self.output_path,
        )

    # ========================================================
    # 4. KAFKA PARTITION ASSIGNMENT
    # ========================================================

    @staticmethod
    def _on_assign(
        consumer: Any,
        partitions: list[Any],
    ) -> None:
        """Log Kafka partition assignments."""

        logger.info(
            "Plate detector assigned Kafka partitions: %s",
            partitions,
        )

    # ========================================================
    # 5. MESSAGE VALIDATION AND PROCESSING
    # ========================================================

    def process_message(
        self,
        message: Any,
    ) -> dict[str, str]:
        """
        Validate a Kafka message and generate a plate event.

        This method does not write to CSV or commit offsets.
        """

        # ----------------------------------------------------
        # Validate Kafka message
        # ----------------------------------------------------

        if message.error() is not None:
            raise RuntimeError(
                f"Kafka message error: {message.error()}"
            )

        expected_topic = (
            self.settings.kafka.plate_frames_topic
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

        if camera.zone_id != "E":
            raise ValueError(
                f"Camera {camera_id} is not in parking Zone E"
            )

        if camera.detector_type != "plate":
            raise ValueError(
                f"Camera {camera_id} is not a plate camera"
            )

        if zone != camera.zone_id:
            raise ValueError(
                f"Zone mismatch for camera {camera_id}"
            )

        if camera_id not in self.parking_camera_ids:
            raise ValueError(
                f"Camera {camera_id} is not an active parking camera"
            )

        if camera.kafka_partition != message.partition():
            raise ValueError(
                f"Kafka partition mismatch for camera {camera_id}"
            )

        # ----------------------------------------------------
        # Run plate simulation
        # ----------------------------------------------------

        detected_plates = self.simulator.simulate(
            camera_id=camera_id,
            timestamp=timestamp,
        )
        time.sleep(DETECTOR_PROCESSING_DELAY_SECONDS)
        # ----------------------------------------------------
        # Generate event ID
        # ----------------------------------------------------

        event_id = (
            f"{message.topic()}:"
            f"{message.partition()}:"
            f"{message.offset()}"
        )

        # ----------------------------------------------------
        # Build detection event
        # ----------------------------------------------------

        return {
            "Time_stamp": timestamp,
            "Zone": zone,

            "detected_plates": json.dumps(
                detected_plates
            ),

            "camera_id": camera_id,
            "event_id": event_id,
            "detector_type": "synthetic",
        }

    # ========================================================
    # 6. OUTPUT FILE VALIDATION
    # ========================================================

    def _prepare_output(self) -> bool:
        """
        Prepare the output directory and validate CSV schema.

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
                    "Existing plate CSV has an incompatible schema: "
                    f"{self.output_path}"
                )

        return needs_header

    # ========================================================
    # 7. CONSUMER LIFECYCLE
    # ========================================================

    def run_forever(
        self,
        poll_timeout: float = 1.0,
    ) -> None:
        """
        Consume Kafka messages continuously.

        Offsets are committed only after the corresponding
        detection event has been persisted to CSV.
        """

        if self._closed:
            raise RuntimeError(
                "Cannot run a closed plate detector"
            )

        if (
            isinstance(poll_timeout, bool)
            or not isinstance(poll_timeout, (int, float))
            or not math.isfinite(poll_timeout)
            or poll_timeout <= 0
        ):
            raise ValueError(
                "Poll timeout must be a positive finite number"
            )

        topic = self.settings.kafka.plate_frames_topic

        try:

            needs_header = self._prepare_output()

            logger.info(
                "Starting plate detector | Topic: %s",
                topic,
            )

            self._consumer.subscribe(
                [topic],
                on_assign=self._on_assign,
            )

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

                        # 3. Flush data to the output file.
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

                        # 4. Commit the corresponding Kafka offset.
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
                        "Plate detection processed | "
                        "Camera: %s | Zone: %s | Plates: %s",
                        event["camera_id"],
                        event["Zone"],
                        event["detected_plates"],
                    )

        except KeyboardInterrupt:

            logger.info(
                "Plate detector interrupted by user"
            )

        finally:
            self.close()

    # ========================================================
    # 8. CLEAN SHUTDOWN
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
            "Plate detector closed"
        )


# ============================================================
# 9. APPLICATION ENTRY POINT
# ============================================================

def main() -> None:
    """Initialize dependencies and start one plate detector worker."""

    parser = argparse.ArgumentParser(
        description="Run a Kafka plate detector worker."
    )
    parser.add_argument(
        "--run-id",
        default="run_001",
        help="Experiment run ID for standalone execution.",
    )
    parser.add_argument(
        "--worker-id",
        default="worker1",
        help="Worker identity (worker1, worker2, etc.).",
    )
    parser.add_argument(
        "--config-path",
        help="Shared pipeline configuration created by the scaling dashboard.",
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

    if args.config_path:
        pipeline_config = load_pipeline_config(args.config_path)
        settings = load_run_settings(pipeline_config)
        cameras = cameras_from_config(pipeline_config)
        run_id = pipeline_config["run_id"]
    else:
        settings = load_settings()
        cameras = load_cameras(settings.paths.cameras_csv)
        run_id = args.run_id

    worker_id = args.worker_id
    reference_data = load_registered_vehicles(
        settings.paths.employees_csv
    )

    metrics_collector = MetricsCollector(
        output_path=(
            settings.paths.experiment_run_dir(run_id)
            / f"plate_{worker_id}_metrics.jsonl"
        ),
        run_id=run_id,
        component=f"plate_{worker_id}",
    )

    detector = PlateDetector(
        settings=settings,
        cameras=cameras,
        reference_data=reference_data,
        output_path=settings.paths.plate_worker_detections_csv(worker_id),
        metrics_collector=metrics_collector,
        worker_id=worker_id,
    )

    detector.run_forever()


if __name__ == "__main__":
    main()
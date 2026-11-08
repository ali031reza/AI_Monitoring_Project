
"""
Kafka producer for simulated camera frames.

Responsibilities:
- Manage the Kafka producer lifecycle.
- Generate frames for enabled cameras.
- Route frames to the appropriate Kafka topic.
- Attach camera metadata to Kafka messages.
- Handle asynchronous delivery callbacks.
- Manage producer shutdown and pending messages.

Application settings and camera configurations are injected
at initialization rather than loaded inside the streaming loop.
"""

from __future__ import annotations

import logging
import time

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

from confluent_kafka import Producer

from advanced.config import (
    ApplicationSettings,
    CameraConfig,
    load_cameras,
    load_settings,
)

from advanced.producers.mock_frame import generate_mock_jpeg


# ============================================================
# 1. LOGGING AND CONSTANTS
# ============================================================

logger = logging.getLogger(__name__)

QUEUE_FULL_RETRY_SECONDS = 10.0
DEFAULT_FLUSH_TIMEOUT_SECONDS = 20.0



# ============================================================
# 2. DELIVERY STATISTICS
# ============================================================

@dataclass(frozen=True, slots=True)
class DeliveryStats:
    """Immutable snapshot of Kafka delivery statistics."""

    queued: int = 0
    delivered: int = 0
    failed: int = 0


# ============================================================
# 3. CAMERA PRODUCER
# ============================================================

class CameraProducer:
    """
    Manage camera frame generation and Kafka delivery.

    Dependencies:
        settings:
            Validated application configuration.

        cameras:
            Validated camera configurations.

        producer:
            Optional Kafka producer instance.
            Primarily useful for testing.
    """

    def __init__(
        self,
        settings: ApplicationSettings,
        cameras: Iterable[CameraConfig],
        *,
        producer: Producer | None = None,
    ) -> None:

        # ----------------------------------------------------
        # Validate application settings
        # ----------------------------------------------------

        if not isinstance(settings, ApplicationSettings):
            raise TypeError(
                "settings must be an ApplicationSettings instance"
            )

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

        if not all(
            isinstance(camera, CameraConfig)
            for camera in camera_list
        ):
            raise TypeError(
                "Every camera must be a CameraConfig instance"
            )

        # Ensure camera IDs are unique.
        camera_ids = [
            camera.camera_id
            for camera in camera_list
        ]

        if len(camera_ids) != len(set(camera_ids)):
            raise ValueError(
                "Camera configurations contain duplicate IDs"
            )

        # Keep only enabled cameras.
        self.cameras = tuple(
            camera
            for camera in camera_list
            if camera.enabled
        )
        self._camera_by_id = {
        camera.camera_id: camera
        for camera in self.cameras
        }

        if not self.cameras:
            raise ValueError(
                "No enabled cameras available for streaming"
            )

        self.settings = settings

        # ----------------------------------------------------
        # Kafka topic mapping
        # ----------------------------------------------------

        self._topic_by_detector = {
            "person": settings.kafka.person_frames_topic,
            "plate": settings.kafka.plate_frames_topic,
        }

        # ----------------------------------------------------
        # Delivery counters
        # ----------------------------------------------------

        self._queued_count = 0
        self._delivered_count = 0
        self._failed_count = 0

        # ----------------------------------------------------
        # Initialize Kafka producer
        # ----------------------------------------------------

        if producer is None:

            producer_config = {
                "bootstrap.servers": (
                    settings.kafka.bootstrap_servers
                ),

                "broker.address.family": (
                    settings.kafka.address_family
                ),

                "client.id": "ai-monitoring-camera-producer",
            }

            self._producer = Producer(producer_config)

        else:
            # Dependency injection for tests or custom setups.
            self._producer = producer

        logger.info(
            "Camera producer initialized with %d enabled cameras",
            len(self.cameras),
        )

    # ========================================================
    # 4. DELIVERY STATISTICS
    # ========================================================

    @property
    def delivery_stats(self) -> DeliveryStats:
        """Return an immutable snapshot of delivery statistics."""

        return DeliveryStats(
            queued=self._queued_count,
            delivered=self._delivered_count,
            failed=self._failed_count,
        )

    # ========================================================
    # 5. DELIVERY CALLBACK
    # ========================================================

    def _delivery_callback(
        self,
        camera_id: str,
        frame_number: int,
    ):
        """
        Create a delivery callback for a specific camera frame.

        Each callback retains the camera ID and frame number
        associated with its Kafka message.
        """

        def on_delivery(error, message):

            if error is not None:

                self._failed_count += 1

                logger.error(
                    "Kafka delivery failed | Camera: %s | "
                    "Frame: %s | Error: %s",
                    camera_id,
                    frame_number,
                    error,
                )

                return

            self._delivered_count += 1

            logger.debug(
                "Kafka delivery successful | Camera: %s | "
                "Frame: %s | Topic: %s | Partition: %s | Offset: %s",
                camera_id,
                frame_number,
                message.topic(),
                message.partition(),
                message.offset(),
            )

        return on_delivery

    # ========================================================
    # 6. PRODUCER BACKPRESSURE HANDLING
    # ========================================================

    def _enqueue_message(
        self,
        *,
        topic: str,
        partition: int,
        camera_id: str,
        frame_number: int,
        jpeg_bytes: bytes,
        timestamp: str,
    ) -> None:
        """
        Queue a Kafka message.

        If the local producer queue is full, poll for delivery
        events and retry for a bounded period.
        """

        headers = [
            (
                "camera_id",
                camera_id.encode("utf-8"),
            ),
            (
                "zone_id",
                self._camera_by_id[camera_id].zone_id.encode("utf-8"),
            ),
            (
                "timestamp_utc",
                timestamp.encode("utf-8"),
            ),
        ]

        deadline = (
            time.monotonic() + QUEUE_FULL_RETRY_SECONDS
        )

        while True:

            try:

                self._producer.produce(
                    topic=topic,
                    partition=partition,
                    key=camera_id,
                    value=jpeg_bytes,
                    headers=headers,
                    callback=self._delivery_callback(
                        camera_id,
                        frame_number,
                    ),
                )

                self._queued_count += 1

                return

            except BufferError:

                remaining = deadline - time.monotonic()

                if remaining <= 0:

                    raise RuntimeError(
                        "Kafka producer queue remained full "
                        f"for {QUEUE_FULL_RETRY_SECONDS} seconds"
                    ) from None

                logger.warning(
                    "Kafka producer queue is full. "
                    "Waiting for available capacity."
                )

                self._producer.poll(
                    min(0.1, remaining)
                )

    # ========================================================
    # 7. SEND ONE STREAMING ROUND
    # ========================================================

    def send_round(
        self,
        frame_number: int,
    ) -> int:
        """
        Generate and queue one frame for every enabled camera.

        Returns:
            Number of successfully queued messages.
        """

        if (
            isinstance(frame_number, bool)
            or not isinstance(frame_number, int)
        ):
            raise TypeError(
                "frame_number must be an integer"
            )

        if frame_number < 0:
            raise ValueError(
                "frame_number cannot be negative"
            )

        timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        queued_this_round = 0

        for camera in self.cameras:

            # Determine destination Kafka topic.
            topic = self._topic_by_detector[
                camera.detector_type
            ]

            # Generate the synthetic camera frame.
            jpeg_bytes = generate_mock_jpeg(
                camera=camera,
                camera_settings=self.settings.camera,
                frame_number=frame_number,
            )

            # Queue the frame.
            self._enqueue_message(
                topic=topic,
                partition=camera.kafka_partition,
                camera_id=camera.camera_id,
                frame_number=frame_number,
                jpeg_bytes=jpeg_bytes,
                timestamp=timestamp,
            )

            queued_this_round += 1

            logger.debug(
                "Queued camera frame | Camera: %s | "
                "Topic: %s | Frame: %s",
                camera.camera_id,
                topic,
                frame_number,
            )

            # Process delivery callbacks and release queue capacity.
            self._producer.poll(0)

        logger.info(
            "Frame round %d completed | Queued: %d",
            frame_number,
            queued_this_round,
        )

        return queued_this_round

    # ========================================================
    # 8. FLUSH PENDING MESSAGES
    # ========================================================

    def flush(
        self,
        timeout: float = DEFAULT_FLUSH_TIMEOUT_SECONDS,
    ) -> int:
        """
        Wait for pending Kafka messages to be delivered.

        Returns:
            Number of messages still pending after timeout.
        """

        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout < 0
        ):
            raise ValueError(
                "Flush timeout must be a non-negative number"
            )

        remaining = self._producer.flush(
            timeout=float(timeout)
        )

        if remaining:

            logger.warning(
                "%d Kafka messages remain pending after flush",
                remaining,
            )

        return remaining

    # ========================================================
    # 9. CONTINUOUS CAMERA STREAMING
    # ========================================================

    def run_forever(self) -> None:
        """
        Continuously generate and publish camera frames.

        The frame interval is measured between the start of
        consecutive rounds, avoiding unnecessary additional
        delay after frame generation and queueing.
        """

        frame_number = 0

        frame_interval = (
            self.settings.camera.frame_interval_seconds
        )

        logger.info("Camera streaming started")

        logger.info(
            "Frame interval: %.3f seconds",
            frame_interval,
        )

        logger.info(
            "Enabled cameras: %d",
            len(self.cameras),
        )

        logger.info("Press Ctrl+C to stop")

        try:

            while True:

                round_started = time.monotonic()

                frame_number += 1

                self.send_round(frame_number)

                # Service any available delivery callbacks.
                self._producer.poll(0)

                elapsed = (
                    time.monotonic() - round_started
                )

                remaining_interval = (
                    frame_interval - elapsed
                )

                if remaining_interval > 0:
                    time.sleep(remaining_interval)

        except KeyboardInterrupt:

            logger.info(
                "Camera streaming stopped by user"
            )

        finally:

            logger.info(
                "Flushing pending Kafka messages..."
            )

            remaining = self.flush()

            stats = self.delivery_stats

            logger.info(
                "Producer shutdown | Queued: %d | "
                "Delivered: %d | Failed: %d | Pending: %d",
                stats.queued,
                stats.delivered,
                stats.failed,
                remaining,
            )


# ============================================================
# 10. APPLICATION ENTRY POINT
# ============================================================

def main() -> None:
    """
    Initialize the application dependencies once and start streaming.
    """

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    # Load application settings once.
    settings = load_settings()

    # Load camera configurations once.
    cameras = load_cameras(
        settings.paths.cameras_csv
    )

    # Inject dependencies into the producer.
    camera_producer = CameraProducer(
        settings=settings,
        cameras=cameras,
    )

    camera_producer.run_forever()


if __name__ == "__main__":
    main()

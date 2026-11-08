from datetime import datetime, timezone
from confluent_kafka import Producer
from advanced.producers.mock_frame import generate_mock_jpeg
from advanced.config.camera_loader import load_cameras
from advanced.config.settings import (
    PERSON_FRAMES_TOPIC,
    PLATE_FRAMES_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    FRAME_INTERVAL_SECONDS,
)
import time

TOPIC_BY_DETECTOR = {
    "person": PERSON_FRAMES_TOPIC,
    "plate": PLATE_FRAMES_TOPIC,
}


def send_one_round(producer, frame_number):
    cameras = load_cameras()

    active_cameras = [
        camera for camera in cameras
        if camera["enabled"]
    ]

    

    delivery = {"successful": 0, "failed": 0}

    def delivery_report(err, message):
        if err is not None:
            delivery["failed"] += 1
            print(f"Delivery failed: {err}")
        else:
            delivery["successful"] += 1
            print(
                f"Delivered to {message.topic()} | "
                f"Partition: {message.partition()} | "
                f"Offset: {message.offset()}"
            )

    for camera in active_cameras:
        camera_id = camera["camera_id"]
        zone_id = camera["zone_id"]
        partition = camera["kafka_partition"]
        topic = TOPIC_BY_DETECTOR[camera["detector_type"]]

        jpeg_bytes = generate_mock_jpeg(
        camera_id,
        zone_id,
        frame_number=frame_number,
        )

        producer.produce(
            topic=topic,
            partition=partition,
            key=camera_id,
            value=jpeg_bytes,
            headers={
                "camera_id": camera_id,
                "zone_id": zone_id,
                "timestamp_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
            },
            callback=delivery_report,
        )

        print(f"Queued camera {camera_id} -> {topic}")

    remaining = producer.flush(timeout=20)

    if (
        remaining != 0
        or delivery["failed"] > 0
        or delivery["successful"] != len(active_cameras)
    ):
        raise RuntimeError("Not all camera frames were delivered")

    print(
        f"\nSuccess! {delivery['successful']} frames "
        "delivered to Kafka."
    )



def stream_camera_frames():
    if FRAME_INTERVAL_SECONDS <= 0:
        raise ValueError("Frame interval must be positive")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
    })

    frame_number = 0

    print("Camera streaming started.")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            frame_number += 1

            print(f"\n--- Round {frame_number} ---")

            send_one_round(producer, frame_number)

            time.sleep(FRAME_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")

    finally:
        remaining = producer.flush(timeout=20)

        if remaining:
            print(f"Warning: {remaining} messages still pending")

        print("Producer shutdown complete.")


if __name__ == "__main__":
    stream_camera_frames()

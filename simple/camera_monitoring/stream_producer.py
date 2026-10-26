
import time
from datetime import datetime, timezone

import cv2
from confluent_kafka import Producer

from camera_monitoring.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    CAMERA_ID,
    ZONE_ID,
    FRAME_INTERVAL_SECONDS,
)
from camera_monitoring.frame_codec import encode_frame
from camera_monitoring.frame_producer import FRAME_TOPIC


def stream_camera(source=0):
    camera = cv2.VideoCapture(source)

    if not camera.isOpened():
        raise RuntimeError("Cannot open camera")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
    })

    frame_number = 0

    print("Streaming camera frames to Kafka...")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            # 1. Capture a new frame.
            success, frame = camera.read()

            if not success:
                print("Could not read frame. Stopping.")
                break

            # 2. Compress the frame.
            jpeg_bytes = encode_frame(frame)

            # 3. Track whether Kafka accepted it.
            delivery = {"successful": False}

            def delivery_report(err, message):
                if err is not None:
                    print(f"Delivery failed: {err}")
                else:
                    delivery["successful"] = True

            # 4. Send the frame to Kafka.
            producer.produce(
                topic=FRAME_TOPIC,
                key=CAMERA_ID,
                value=jpeg_bytes,
                headers={
                    "camera_id": CAMERA_ID,
                    "zone_id": ZONE_ID,
                    "timestamp_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                },
                callback=delivery_report,
            )

            # Wait for this frame's delivery result.
            remaining = producer.flush(timeout=10)

            if remaining or not delivery["successful"]:
                raise RuntimeError(
                    "Frame was not delivered to Kafka"
                )

            frame_number += 1

            print(
                f"Frame #{frame_number} sent "
                f"({len(jpeg_bytes) / 1024:.1f} KB)"
            )

            # 5. Wait before capturing the next frame.
            time.sleep(FRAME_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")

    finally:
        camera.release()
        producer.flush(timeout=10)
        print("Camera released.")


if __name__ == "__main__":
    stream_camera()


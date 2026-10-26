
from datetime import datetime, timezone
from pathlib import Path

import cv2
from confluent_kafka import Producer

from camera_monitoring.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    CAMERA_ID,
    ZONE_ID,
)
from camera_monitoring.frame_codec import encode_frame


FRAME_TOPIC = "camera.frames.jpeg.dev"


def send_camera_frame():
    # Load the image we captured earlier.
    image_path = Path("captured_frames/test_frame.jpg")
    frame = cv2.imread(str(image_path))

    if frame is None:
        raise RuntimeError(f"Cannot read image: {image_path}")

    # Compress the image into JPEG bytes.
    jpeg_bytes = encode_frame(frame)

    print(f"Frame size: {len(jpeg_bytes) / 1024:.1f} KB")

    # Configure the Kafka producer.
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
    })

    delivery = {"successful": False}

    def delivery_report(err, message):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            delivery["successful"] = True
            print(
                f"Frame delivered! "
                f"Partition: {message.partition()}, "
                f"Offset: {message.offset()}"
            )

    # Publish the image and its metadata.
    producer.produce(
        topic=FRAME_TOPIC,
        key=CAMERA_ID,
        value=jpeg_bytes,
        headers={
            "camera_id": CAMERA_ID,
            "zone_id": ZONE_ID,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
        callback=delivery_report,
    )

    remaining = producer.flush(timeout=20)

    if remaining or not delivery["successful"]:
        raise RuntimeError("Camera frame was not delivered to Kafka.")


if __name__ == "__main__":
    send_camera_frame()

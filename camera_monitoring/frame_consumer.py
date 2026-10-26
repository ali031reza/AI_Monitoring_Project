from pathlib import Path
from time import monotonic

import cv2
from confluent_kafka import Consumer

from camera_monitoring.config import KAFKA_BOOTSTRAP_SERVERS
from camera_monitoring.frame_codec import decode_frame


FRAME_TOPIC = "camera.frames.jpeg.dev"


def receive_camera_frame():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "group.id": "camera-frame-verification-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([FRAME_TOPIC])

    print(f"Waiting for a frame on {FRAME_TOPIC}...")

    try:
        deadline = monotonic() + 20

        while monotonic() < deadline:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            # Retrieve the JPEG bytes from Kafka.
            jpeg_bytes = message.value()

            # Reconstruct the OpenCV image.
            frame = decode_frame(jpeg_bytes)

            # Save the reconstructed frame.
            output_path = Path("captured_frames/received_frame.jpg")
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if not cv2.imwrite(str(output_path), frame):
                raise RuntimeError("Failed to save received frame")

            height, width = frame.shape[:2]

            print(f"Received {len(jpeg_bytes) / 1024:.1f} KB")
            print(f"Image dimensions: {width} x {height}")
            print(f"Kafka offset: {message.offset()}")
            print(f"Saved to: {output_path.resolve()}")

            return

        raise TimeoutError("No frame received within 20 seconds")

    finally:
        consumer.close()


if __name__ == "__main__":
    receive_camera_frame()


from pathlib import Path

import cv2
from confluent_kafka import Consumer

from camera_monitoring.config import KAFKA_BOOTSTRAP_SERVERS
from camera_monitoring.frame_codec import decode_frame
from camera_monitoring.frame_producer import FRAME_TOPIC


def on_assign(consumer, partitions):
    print(f"Consumer ready! Assigned partitions: {partitions}")


def receive_live_frames():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "group.id": "camera-live-view-dev",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([FRAME_TOPIC], on_assign=on_assign)

    frame_count = 0
    output_path = Path("captured_frames/live_received.jpg")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("Waiting for live camera frames...")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            # 1. Get the JPEG bytes from Kafka.
            jpeg_bytes = message.value()

            # 2. Decode the JPEG into an OpenCV image.
            frame = decode_frame(jpeg_bytes)

            frame_count += 1
            height, width = frame.shape[:2]

            # 3. Read the camera ID from Kafka headers.
            headers = dict(message.headers() or [])
            camera_id = headers.get("camera_id", b"unknown")

            if isinstance(camera_id, bytes):
                camera_id = camera_id.decode("utf-8")

            print(
                f"Received frame #{frame_count} "
                f"| Camera: {camera_id} "
                f"| Size: {width}x{height} "
                f"| Kafka offset: {message.offset()}"
            )

            # 4. Save the first frame and every fifth frame.
            # Reuse one file instead of filling the disk.
            if frame_count == 1 or frame_count % 5 == 0:
                if not cv2.imwrite(str(output_path), frame):
                    raise RuntimeError("Could not save received frame")

                print(f"Snapshot updated: {output_path}")

    except KeyboardInterrupt:
        print("\nStopping live consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    receive_live_frames()

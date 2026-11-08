
import csv
import random
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer

from camera_monitoring.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ZONE_ID,
)
from camera_monitoring.frame_producer import FRAME_TOPIC


# Ten fictional employees.
EMPLOYEE_IDS = [f"00xx{i:02d}" for i in range(10)]

# Always write to the simple application's data directory.
PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_FILE = PROJECT_DIR / "data" / "detections.csv"

CSV_FIELDS = [
    "event_id",
    "timestamp_utc",
    "camera_id",
    "zone_id",
    "detected",
    "employee_id",
    "detector_type",
]


def fake_detect():
    """Generate one synthetic detection result."""

    if random.random() < 0.75:
        return {
            "detected": 0,
            "employee_id": "",
        }

    return {
        "detected": 1,
        "employee_id": random.choice(EMPLOYEE_IDS),
    }


def read_header(message, name, default):
    """Retrieve a Kafka message header as a string."""

    headers = dict(message.headers() or [])
    value = headers.get(name)

    if value is None:
        return default

    if isinstance(value, bytes):
        return value.decode("utf-8")

    return str(value)


def on_assign(consumer, partitions):
    print(f"Assigned partitions: {partitions}")


def run_fake_detector():
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    needs_header = (
        not OUTPUT_FILE.exists()
        or OUTPUT_FILE.stat().st_size == 0
    )

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "group.id": "fake-detector-zone-a-v1",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    consumer.subscribe(
        [FRAME_TOPIC],
        on_assign=on_assign,
    )

    print("Fake detector started.")
    print(f"CSV output: {OUTPUT_FILE}")
    print("Press Ctrl+C to stop.")

    try:
        with OUTPUT_FILE.open(
            "a",
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
                message = consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    print(f"Kafka error: {message.error()}")
                    continue

                # One synthetic result per received frame.
                detection = fake_detect()

                timestamp = read_header(
                    message,
                    "timestamp_utc",
                    datetime.now(timezone.utc).isoformat(),
                )

                camera_id = read_header(
                    message,
                    "camera_id",
                    "unknown",
                )

                zone_id = read_header(
                    message,
                    "zone_id",
                    ZONE_ID,
                )

                # Topic + partition + offset identifies
                # this source Kafka message.
                event_id = (
                    f"{message.topic()}:"
                    f"{message.partition()}:"
                    f"{message.offset()}"
                )

                event = {
                    "event_id": event_id,
                    "timestamp_utc": timestamp,
                    "camera_id": camera_id,
                    "zone_id": zone_id,
                    **detection,
                    "detector_type": "synthetic",
                }

                # Write the observation to CSV.
                writer.writerow(event)
                file.flush()

                # Commit only after the CSV write.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                if detection["detected"]:
                    print(
                        f"{detection['employee_id']} "
                        f"detected in Zone {zone_id}"
                    )
                else:
                    print("No employee detected")

    except KeyboardInterrupt:
        print("\nStopping fake detector...")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    run_fake_detector()


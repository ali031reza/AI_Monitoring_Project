
import json
from datetime import datetime, timezone

from confluent_kafka import Producer

from camera_monitoring.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CAMERA_ID,
    ZONE_ID,
)


def delivery_report(err, message):
    """Report whether Kafka received our message."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message delivered to {message.topic()}, "
            f"partition {message.partition()}, "
            f"offset {message.offset()}"
        )


def send_test_message():
    # Connect our Python producer to Kafka.
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
	"broker.address.family": "v4",
    })

    # Create a small JSON event to simulate camera metadata.
    event = {
        "event_type": "test",
        "camera_id": CAMERA_ID,
        "zone_id": ZONE_ID,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "message": "Hello from Python!"
    }

    # Send the event to our Kafka topic.
    producer.produce(
        topic=KAFKA_TOPIC,
        key=CAMERA_ID,
        value=json.dumps(event),
        callback=delivery_report,
    )

    # Wait up to 10 seconds for delivery to finish.
    remaining = producer.flush(timeout=10)

    if remaining:
        raise TimeoutError(
            f"{remaining} message(s) were not delivered within 10 seconds."
        )

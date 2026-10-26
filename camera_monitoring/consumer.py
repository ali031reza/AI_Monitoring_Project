
import json

from confluent_kafka import Consumer

from camera_monitoring.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


def run_consumer():
    # Configure how our Python application connects to Kafka.
	consumer = Consumer({
    		"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    		"broker.address.family": "v4",
    		"group.id": "camera-monitoring-dev",
    		"auto.offset.reset": "earliest",
})

    # Tell Kafka which topic we want to read.
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening for events on: {KAFKA_TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            # Wait up to one second for a message.
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            # Convert the message bytes into a Python dictionary.
            event = json.loads(message.value().decode("utf-8"))

            print("Received camera event:")
            print(json.dumps(event, indent=2))
            print("-" * 40)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        # Close the consumer cleanly.
        consumer.close()


if __name__ == "__main__":
    run_consumer()

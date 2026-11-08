
import csv
import hashlib
import json
import random

from datetime import datetime, timezone

from confluent_kafka import Consumer

from advanced.config.camera_loader import load_cameras
from advanced.config.settings import (
    EMPLOYEES_CSV,
    PLATE_DETECTIONS_CSV,
    PLATE_FRAMES_TOPIC,
    PLATE_DETECTOR_GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_ADDRESS_FAMILY,
    SIMULATION_SEED,
    SHIFT_START_HOURS,
    SHIFT_LENGTH_SECONDS,
    ARRIVAL_SPREAD_SECONDS,
    PARKING_TRANSIT_SECONDS,
    PARKING_DETECTION_WINDOW_SECONDS,
)


CSV_FIELDS = [
    "Time_stamp",
    "Zone",
    "detected_plates",
    "camera_id",
    "event_id",
    "detector_type",
]


def seeded_random(*values):
    text = "|".join(
        str(value)
        for value in (SIMULATION_SEED, *values)
    )

    digest = hashlib.sha256(
        text.encode("utf-8")
    ).digest()

    seed = int.from_bytes(digest[:8], "big")

    return random.Random(seed)


def load_registered_vehicles():
    vehicles = {}

    with EMPLOYEES_CSV.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            employee_id = row["employee_id"].strip()
            plate = row["plate_number"].strip()

            if plate and plate.upper() != "NA":
                if employee_id in vehicles:
                    raise ValueError(
                        f"Duplicate employee ID: {employee_id}"
                    )

                vehicles[employee_id] = plate

    if len(vehicles) != 75:
        raise ValueError(
            f"Expected 75 registered cars, found {len(vehicles)}"
        )

    if len(set(vehicles.values())) != len(vehicles):
        raise ValueError("Duplicate plate numbers found")

    return vehicles


def simulate_plates(
    camera_id,
    timestamp,
    vehicles,
    parking_cameras,
):
    event_time = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )

    if event_time.tzinfo is None:
        raise ValueError("Timestamp must have a timezone")

    event_time = event_time.astimezone(timezone.utc)

    epoch_seconds = event_time.timestamp()
    utc_day = int(epoch_seconds // 86400)
    seconds_today = epoch_seconds - utc_day * 86400

    detected_plates = []

    for employee_id, plate in sorted(vehicles.items()):

        # Same seed and daily shift calculation
        # as the person detector.
        rng = seeded_random(
            "employee-route",
            employee_id,
            utc_day,
        )

        shift_hour = rng.choice(SHIFT_START_HOURS)

        arrival_delay = rng.randint(
            0,
            ARRIVAL_SPREAD_SECONDS,
        )

        shift_start = shift_hour * 3600 + arrival_delay
        shift_end = shift_start + SHIFT_LENGTH_SECONDS

        # Vehicle arrives before the shift and
        # leaves after the shift.
        arrival_time = (
            shift_start - PARKING_TRANSIT_SECONDS
        )

        departure_time = (
            shift_end + PARKING_TRANSIT_SECONDS
        )

        for event_type, event_second in (
            ("arrival", arrival_time),
            ("departure", departure_time),
        ):
            # Only observe vehicles during the
            # configured arrival/departure window.
            if not (
                event_second
                <= seconds_today
                < event_second + PARKING_DETECTION_WINDOW_SECONDS
            ):
                continue

            # Each vehicle appears on only one of
            # the parking cameras for this event.
            camera_rng = seeded_random(
                "parking-camera",
                employee_id,
                utc_day,
                event_type,
            )

            assigned_camera = camera_rng.choice(
                parking_cameras
            )

            if assigned_camera == camera_id:
                detected_plates.append(plate)

    return sorted(detected_plates)


def read_header(message, name):
    headers = dict(message.headers() or [])
    value = headers.get(name)

    if value is None:
        raise ValueError(
            f"Missing Kafka header: {name}"
        )

    if isinstance(value, bytes):
        return value.decode("utf-8")

    return str(value)


def on_assign(consumer, partitions):
    print(f"Assigned Kafka partitions: {partitions}")


def run_plate_detector():
    if PARKING_TRANSIT_SECONDS < 0:
        raise ValueError("Invalid parking transit time")

    if PARKING_DETECTION_WINDOW_SECONDS <= 0:
        raise ValueError("Invalid parking detection window")

    vehicles = load_registered_vehicles()
    cameras = load_cameras()

    parking_cameras = sorted(
        camera["camera_id"]
        for camera in cameras
        if camera["enabled"]
        and camera["zone_id"] == "E"
        and camera["detector_type"] == "plate"
    )

    if not parking_cameras:
        raise ValueError("No active parking cameras")

    camera_by_id = {
        camera["camera_id"]: camera
        for camera in cameras
    }

    PLATE_DETECTIONS_CSV.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    needs_header = (
        not PLATE_DETECTIONS_CSV.exists()
        or PLATE_DETECTIONS_CSV.stat().st_size == 0
    )

    if not needs_header:
        with PLATE_DETECTIONS_CSV.open(
            newline="",
            encoding="utf-8",
        ) as file:
            existing_header = next(
                csv.reader(file),
                [],
            )

        if existing_header != CSV_FIELDS:
            raise ValueError(
                "Existing plate CSV has different columns"
            )

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": KAFKA_ADDRESS_FAMILY,
        "group.id": PLATE_DETECTOR_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    consumer.subscribe(
        [PLATE_FRAMES_TOPIC],
        on_assign=on_assign,
    )

    print("Fake plate detector started.")
    print(f"Registered vehicles: {len(vehicles)}")
    print(f"Parking cameras: {parking_cameras}")
    print(f"Output: {PLATE_DETECTIONS_CSV}")
    print("Press Ctrl+C to stop.")

    try:
        with PLATE_DETECTIONS_CSV.open(
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
                    raise RuntimeError(message.error())

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

                camera = camera_by_id.get(camera_id)

                if (
                    camera is None
                    or not camera["enabled"]
                    or camera["zone_id"] != "E"
                    or camera["detector_type"] != "plate"
                    or camera["kafka_partition"]
                    != message.partition()
                    or zone != "E"
                ):
                    raise ValueError(
                        f"Invalid parking camera: {camera_id}"
                    )

                detected_plates = simulate_plates(
                    camera_id,
                    timestamp,
                    vehicles,
                    parking_cameras,
                )

                event_id = (
                    f"{message.topic()}:"
                    f"{message.partition()}:"
                    f"{message.offset()}"
                )

                event = {
                    "Time_stamp": timestamp,
                    "Zone": zone,
                    "detected_plates": json.dumps(
                        detected_plates
                    ),
                    "camera_id": camera_id,
                    "event_id": event_id,
                    "detector_type": "synthetic",
                }

                writer.writerow(event)
                file.flush()

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print(
                    f"Camera {camera_id} | "
                    f"Zone E | "
                    f"Plates: {detected_plates}"
                )

    except KeyboardInterrupt:
        print("\nStopping plate detector...")

    finally:
        consumer.close()
        print("Plate detector closed.")


if __name__ == "__main__":
    run_plate_detector()

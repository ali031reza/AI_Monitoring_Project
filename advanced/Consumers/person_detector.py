
import csv
import hashlib
import json
import random

from datetime import datetime, timezone

from confluent_kafka import Consumer

from advanced.config.camera_loader import load_cameras

from advanced.config.settings import (
    EMPLOYEES_CSV,
    RESTRICTED_ACCESS_CSV,
    PERSON_DETECTIONS_CSV,
    PERSON_FRAMES_TOPIC,
    PERSON_DETECTOR_GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_ADDRESS_FAMILY,
    SIMULATION_SEED,
    PERSON_PRESENCE_PROBABILITY,
    PERSON_MOVEMENT_WINDOW_SECONDS,
    MAX_PERSONS_PER_FRAME,
    MIN_ZONE_DWELL_SECONDS,
    MAX_ZONE_DWELL_SECONDS,
    MIN_ZONE_TRAVEL_SECONDS,
    MAX_ZONE_TRAVEL_SECONDS,
    SHIFT_START_HOURS,
    SHIFT_LENGTH_SECONDS,
    ARRIVAL_SPREAD_SECONDS,
    ZONE_CONNECTIONS,
)


# --------------------------------------------------
# 1. CSV output schema
# --------------------------------------------------

CSV_FIELDS = [
    "Time_stamp",
    "Zone",
    "detected_persons_id",
    "authorized",
    "camera_id",
    "event_id",
    "detector_type",
]


# --------------------------------------------------
# 2. Load employee and authorization reference data
# --------------------------------------------------

def load_reference_data():

    employees = set()

    with EMPLOYEES_CSV.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            employee_id = row["employee_id"].strip()

            if not employee_id:
                raise ValueError("Empty employee ID")

            if employee_id in employees:
                raise ValueError(
                    f"Duplicate employee ID: {employee_id}"
                )

            employees.add(employee_id)

    if len(employees) != 100:
        raise ValueError(
            f"Expected 100 employees, found {len(employees)}"
        )

    restricted_access = {
        "C": set(),
        "D": set(),
    }

    with RESTRICTED_ACCESS_CSV.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            employee_id = row["employee_id"].strip()
            zone = row["zone_id"].strip().upper()

            if employee_id not in employees:
                raise ValueError(
                    f"Unknown employee: {employee_id}"
                )

            if zone not in restricted_access:
                raise ValueError(
                    f"Invalid restricted zone: {zone}"
                )

            restricted_access[zone].add(employee_id)

    if len(restricted_access["C"]) != 10:
        raise ValueError(
            "Exactly 10 employees must be authorized "
            "for restricted zones"
        )

    if restricted_access["C"] != restricted_access["D"]:
        raise ValueError(
            "Zones C and D must have identical "
            "authorized employee lists"
        )

    return employees, restricted_access


# --------------------------------------------------
# 3. Authorization policy
# --------------------------------------------------

def is_authorized(employee_id, zone, restricted_access):

    if zone in {"A", "B"}:
        return True

    if zone in {"C", "D"}:
        return employee_id in restricted_access[zone]

    raise ValueError(
        f"Unsupported person detection zone: {zone}"
    )


# --------------------------------------------------
# 4. Deterministic random number generator
# --------------------------------------------------

def seeded_random(*values):

    seed_text = "|".join(
        str(value)
        for value in (SIMULATION_SEED, *values)
    )

    digest = hashlib.sha256(
        seed_text.encode("utf-8")
    ).digest()

    seed = int.from_bytes(digest[:8], "big")

    return random.Random(seed)


# --------------------------------------------------
# 5. Validate simulation configuration
# --------------------------------------------------

def validate_simulation_settings():

    if not 0 <= PERSON_PRESENCE_PROBABILITY <= 1:
        raise ValueError(
            "Person presence probability must be "
            "between 0 and 1"
        )

    if PERSON_MOVEMENT_WINDOW_SECONDS <= 0:
        raise ValueError(
            "Movement window must be positive"
        )

    if MAX_PERSONS_PER_FRAME <= 0:
        raise ValueError(
            "Maximum persons per frame must be positive"
        )

    if not 0 < MIN_ZONE_DWELL_SECONDS <= MAX_ZONE_DWELL_SECONDS:
        raise ValueError("Invalid zone dwell time settings")

    if not 0 < MIN_ZONE_TRAVEL_SECONDS <= MAX_ZONE_TRAVEL_SECONDS:
        raise ValueError("Invalid zone travel time settings")

    if not SHIFT_START_HOURS:
        raise ValueError("At least one shift is required")

    if not all(0 <= hour <= 23 for hour in SHIFT_START_HOURS):
        raise ValueError("Invalid shift start hour")

    if SHIFT_LENGTH_SECONDS <= 0 or ARRIVAL_SPREAD_SECONDS < 0:
        raise ValueError("Invalid shift settings")

    # Our first prototype keeps each shift within one UTC day.
    if any(
        hour * 3600
        + ARRIVAL_SPREAD_SECONDS
        + SHIFT_LENGTH_SECONDS > 86400
        for hour in SHIFT_START_HOURS
    ):
        raise ValueError(
            "Configured shifts must finish within "
            "the same UTC day"
        )

    required_zones = {"A", "B", "C", "D", "E"}

    if set(ZONE_CONNECTIONS) != required_zones:
        raise ValueError("Incomplete zone connectivity")

    for zone, neighbors in ZONE_CONNECTIONS.items():
        for neighbor in neighbors:

            if neighbor not in required_zones:
                raise ValueError(
                    f"Unknown connected zone: {neighbor}"
                )

            if zone not in ZONE_CONNECTIONS[neighbor]:
                raise ValueError(
                    f"Zone connection {zone}-{neighbor} "
                    "must be bidirectional"
                )


# --------------------------------------------------
# 6. Build camera lists for each person zone
# --------------------------------------------------

def build_person_camera_map(cameras):

    cameras_by_zone = {
        "A": [],
        "B": [],
        "C": [],
        "D": [],
    }

    for camera in cameras:

        zone = camera["zone_id"]

        if (
            camera["enabled"]
            and camera["detector_type"] == "person"
            and zone in cameras_by_zone
        ):
            cameras_by_zone[zone].append(
                camera["camera_id"]
            )

    for zone, camera_ids in cameras_by_zone.items():

        if not camera_ids:
            raise ValueError(
                f"No enabled person cameras for Zone {zone}"
            )

        camera_ids.sort()

    return cameras_by_zone


# --------------------------------------------------
# 7. Generate allowed next-zone choices
# --------------------------------------------------

def get_next_zones(
    current_zone,
    may_enter_restricted,
):

    possible_zones = []

    for neighbor in ZONE_CONNECTIONS[current_zone]:

        # Zone E is handled by the plate detector.
        if neighbor == "E":
            continue

        # Restrict entry to Zones C and D.
        if (
            neighbor in {"C", "D"}
            and not may_enter_restricted
        ):
            continue

        possible_zones.append(neighbor)

    return possible_zones


# --------------------------------------------------
# 8. Simulate employee movements and detections
# --------------------------------------------------

def simulate_people(
    camera_id,
    timestamp,
    employees,
    access,
    cameras,
    unauthorized_employee,
):

    event_time = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )

    if event_time.tzinfo is None:
        raise ValueError(
            "Timestamp must include timezone information"
        )

    event_time = event_time.astimezone(timezone.utc)

    epoch_seconds = event_time.timestamp()

    # Divide the timeline into UTC days.
    utc_day = int(epoch_seconds // 86400)

    seconds_today = epoch_seconds - utc_day * 86400

    cameras_by_zone = build_person_camera_map(cameras)

    detected = []

    # Calculate the movement of every employee.
    for employee_id in sorted(employees):

        # The same employee on the same day always
        # receives the same movement schedule.
        rng = seeded_random(
            "employee-route",
            employee_id,
            utc_day,
        )

        # Choose the employee's daily shift.
        shift_hour = rng.choice(
            SHIFT_START_HOURS
        )

        arrival_delay = rng.randint(
            0,
            ARRIVAL_SPREAD_SECONDS,
        )

        shift_start = (
            shift_hour * 3600 + arrival_delay
        )

        shift_end = (
            shift_start + SHIFT_LENGTH_SECONDS
        )

        # Employee is not at work.
        if not (
            shift_start <= seconds_today < shift_end
        ):
            continue

        # Access eligibility for C and D.
        may_enter_restricted = (
            employee_id in access["C"]
            or employee_id == unauthorized_employee
        )

        # All person routes begin in Zone A.
        current_zone = "A"

        assigned_camera = rng.choice(
            cameras_by_zone[current_zone]
        )

        position = shift_start

        # Start returning toward Zone A before shift ends.
        exit_margin = 2 * (
            MAX_ZONE_DWELL_SECONDS
            + MAX_ZONE_TRAVEL_SECONDS
        )

        # Reconstruct the employee's route up to
        # the timestamp of the Kafka frame.
        while position < shift_end:

            dwell_seconds = rng.randint(
                MIN_ZONE_DWELL_SECONDS,
                MAX_ZONE_DWELL_SECONDS,
            )

            # Once back in A near shift end,
            # remain there until departure.
            if (
                current_zone == "A"
                and shift_end - position <= exit_margin
            ):
                dwell_seconds = shift_end - position

            visit_end = min(
                position + dwell_seconds,
                shift_end,
            )

            # Employee is currently staying in this zone.
            if seconds_today < visit_end:

                if assigned_camera == camera_id:

                    # Visibility is stable within an
                    # observation time window.
                    observation_window = int(
                        epoch_seconds
                        // PERSON_MOVEMENT_WINDOW_SECONDS
                    )

                    visibility_rng = seeded_random(
                        "person-visibility",
                        employee_id,
                        observation_window,
                    )

                    if (
                        visibility_rng.random()
                        < PERSON_PRESENCE_PROBABILITY
                    ):
                        detected.append(employee_id)

                break

            position = visit_end

            if position >= shift_end:
                break

            # Moving between zones takes time.
            travel_seconds = rng.randint(
                MIN_ZONE_TRAVEL_SECONDS,
                MAX_ZONE_TRAVEL_SECONDS,
            )

            travel_end = position + travel_seconds

            # The person is temporarily not visible
            # to any zone camera during travel.
            if seconds_today < travel_end:
                break

            position = travel_end

            if position >= shift_end:
                break

            # Choose the next zone based on connectivity.
            if position >= shift_end - exit_margin:

                # Every person zone is connected to A.
                next_zone = "A"

            else:

                possible_zones = get_next_zones(
                    current_zone,
                    may_enter_restricted,
                )

                if not possible_zones:
                    raise ValueError(
                        f"No valid exit from Zone {current_zone}"
                    )

                next_zone = rng.choice(
                    possible_zones
                )

            current_zone = next_zone

            # Choose a camera in the destination zone.
            assigned_camera = rng.choice(
                cameras_by_zone[current_zone]
            )

    # One camera frame can only show a limited
    # number of people.
    if len(detected) > MAX_PERSONS_PER_FRAME:

        observation_window = int(
            epoch_seconds
            // PERSON_MOVEMENT_WINDOW_SECONDS
        )

        selection_rng = seeded_random(
            "camera-capacity",
            camera_id,
            observation_window,
        )

        detected = selection_rng.sample(
            detected,
            MAX_PERSONS_PER_FRAME,
        )

    return sorted(detected)


# --------------------------------------------------
# 9. Read Kafka message headers
# --------------------------------------------------

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


# --------------------------------------------------
# 10. Kafka partition assignment callback
# --------------------------------------------------

def on_assign(consumer, partitions):

    print(
        f"Assigned Kafka partitions: {partitions}"
    )


# --------------------------------------------------
# 11. Start the fake person detector
# --------------------------------------------------

def run_person_detector():

    validate_simulation_settings()

    employees, restricted_access = load_reference_data()

    cameras = load_cameras()

    camera_by_id = {
        camera["camera_id"]: camera
        for camera in cameras
    }

    # 1% of 100 employees = exactly 1 employee.
    # Select from those without restricted access.
    unauthorized_candidates = sorted(
        employees - restricted_access["C"]
    )

    unauthorized_employee = seeded_random(
        "unauthorized-employee"
    ).choice(unauthorized_candidates)

    # Prepare output directory.
    PERSON_DETECTIONS_CSV.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Check whether the CSV already contains data.
    needs_header = (
        not PERSON_DETECTIONS_CSV.exists()
        or PERSON_DETECTIONS_CSV.stat().st_size == 0
    )

    # Protect against appending to an old CSV schema.
    if not needs_header:

        with PERSON_DETECTIONS_CSV.open(
            newline="",
            encoding="utf-8",
        ) as existing_file:

            existing_header = next(
                csv.reader(existing_file),
                [],
            )

        if existing_header != CSV_FIELDS:
            raise ValueError(
                "Existing persons_detected.csv has "
                "different columns"
            )

    # Create Kafka consumer.
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "broker.address.family": KAFKA_ADDRESS_FAMILY,
        "group.id": PERSON_DETECTOR_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    consumer.subscribe(
        [PERSON_FRAMES_TOPIC],
        on_assign=on_assign,
    )

    print("Fake person detector started.")
    print(f"Output: {PERSON_DETECTIONS_CSV}")
    print("Press Ctrl+C to stop.")

    try:

        with PERSON_DETECTIONS_CSV.open(
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
                    raise RuntimeError(
                        f"Kafka error: {message.error()}"
                    )

                # Extract frame metadata.
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

                # Make sure Kafka metadata agrees
                # with our camera configuration.
                if (
                    camera is None
                    or not camera["enabled"]
                    or camera["detector_type"] != "person"
                    or camera["zone_id"] != zone
                    or camera["kafka_partition"]
                    != message.partition()
                ):
                    raise ValueError(
                        f"Invalid camera metadata: {camera_id}"
                    )

                # Simulate the people visible in this frame.
                detected_ids = simulate_people(
                    camera_id,
                    timestamp,
                    employees,
                    restricted_access,
                    cameras,
                    unauthorized_employee,
                )

                # Authorization values have the same
                # order as the detected employee IDs.
                authorization = [
                    is_authorized(
                        employee_id,
                        zone,
                        restricted_access,
                    )
                    for employee_id in detected_ids
                ]

                # Unique Kafka-based event identifier.
                event_id = (
                    f"{message.topic()}:"
                    f"{message.partition()}:"
                    f"{message.offset()}"
                )

                event = {
                    "Time_stamp": timestamp,
                    "Zone": zone,
                    "detected_persons_id": json.dumps(
                        detected_ids
                    ),
                    "authorized": json.dumps(
                        authorization
                    ),
                    "camera_id": camera_id,
                    "event_id": event_id,
                    "detector_type": "synthetic",
                }

                # Persist the observation.
                writer.writerow(event)
                file.flush()

                # Commit the offset after writing.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print(
                    f"Camera {camera_id} | "
                    f"Zone {zone} | "
                    f"Persons: {detected_ids} | "
                    f"Authorized: {authorization}"
                )

    except KeyboardInterrupt:

        print("\nStopping person detector...")

    finally:

        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    run_person_detector()

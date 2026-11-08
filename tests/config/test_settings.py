
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from advanced.config import (
    ApplicationSettings,
    CameraSettings,
    EmployeeMovementSettings,
    EmployeeShiftSettings,
    KafkaSettings,
    ParkingSettings,
    PersonDetectionSettings,
    ProjectPaths,
    SimulationSettings,
    load_settings,
)


# ============================================================
# 1. DEFAULT CONFIGURATION
# ============================================================

def test_load_settings_returns_application_settings():

    settings = load_settings(environ={})

    assert isinstance(settings, ApplicationSettings)


def test_default_kafka_settings():

    settings = load_settings(environ={})

    assert settings.kafka.bootstrap_servers == "127.0.0.1:9092"
    assert settings.kafka.address_family == "v4"

    assert settings.kafka.person_frames_topic == (
        "camera.person.frames.dev"
    )

    assert settings.kafka.plate_frames_topic == (
        "camera.plate.frames.dev"
    )

    assert settings.kafka.person_detector_group_id == (
        "person-detectors-dev"
    )

    assert settings.kafka.plate_detector_group_id == (
        "plate-detectors-dev"
    )


def test_default_camera_settings():

    settings = load_settings(environ={})

    assert settings.camera.frame_interval_seconds == 1.0
    assert settings.camera.frame_width == 640
    assert settings.camera.frame_height == 360
    assert settings.camera.jpeg_quality == 80


def test_default_person_detection_settings():

    settings = load_settings(environ={})

    person = settings.simulation.person_detection

    assert person.presence_probability == 0.25
    assert person.movement_window_seconds == 30
    assert person.max_persons_per_frame == 4


def test_default_employee_movement_settings():

    settings = load_settings(environ={})

    movement = settings.simulation.employee_movement

    assert movement.min_zone_dwell_seconds == 120
    assert movement.max_zone_dwell_seconds == 300

    assert movement.min_zone_travel_seconds == 30
    assert movement.max_zone_travel_seconds == 90

    assert movement.zone_connections == {
        "A": ("B", "C", "D", "E"),
        "B": ("A", "C", "D"),
        "C": ("A", "B"),
        "D": ("A", "B"),
        "E": ("A",),
    }


def test_default_employee_shift_settings():

    settings = load_settings(environ={})

    shifts = settings.simulation.employee_shifts

    assert shifts.start_hours == (0, 8, 16)
    assert shifts.length_seconds == 25200
    assert shifts.arrival_spread_seconds == 1800


def test_default_parking_settings():

    settings = load_settings(environ={})

    parking = settings.simulation.parking

    assert parking.transit_seconds == 180
    assert parking.detection_window_seconds == 30


def test_default_application_settings():

    settings = load_settings(environ={})

    assert settings.simulation.random_seed == 20261027
    assert settings.person_detector_worker_id == "worker1"

    assert settings.paths == ProjectPaths.discover()


# ============================================================
# 2. ENVIRONMENT VARIABLE OVERRIDES
# ============================================================

def test_environment_variables_override_defaults():

    environ = {
        # Kafka
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_ADDRESS_FAMILY": "v6",
        "KAFKA_PERSON_FRAMES_TOPIC": "person.frames.test",
        "KAFKA_PLATE_FRAMES_TOPIC": "plate.frames.test",
        "PERSON_DETECTOR_GROUP_ID": "person-test",
        "PLATE_DETECTOR_GROUP_ID": "plate-test",

        # Camera
        "FRAME_INTERVAL_SECONDS": "0.5",
        "FRAME_WIDTH": "1280",
        "FRAME_HEIGHT": "720",
        "JPEG_QUALITY": "90",

        # Simulation
        "SIMULATION_SEED": "42",
        "PERSON_PRESENCE_PROBABILITY": "0.75",
        "PERSON_MOVEMENT_WINDOW_SECONDS": "60",
        "MAX_PERSONS_PER_FRAME": "8",

        # Employee movement
        "MIN_ZONE_DWELL_SECONDS": "100",
        "MAX_ZONE_DWELL_SECONDS": "400",
        "MIN_ZONE_TRAVEL_SECONDS": "20",
        "MAX_ZONE_TRAVEL_SECONDS": "120",

        # Employee shifts
        "SHIFT_START_HOURS": "6,14,22",
        "SHIFT_LENGTH_SECONDS": "28800",
        "ARRIVAL_SPREAD_SECONDS": "900",

        # Parking
        "PARKING_TRANSIT_SECONDS": "240",
        "PARKING_DETECTION_WINDOW_SECONDS": "45",

        # Worker
        "PERSON_DETECTOR_WORKER_ID": "worker2",
    }

    settings = load_settings(environ=environ)

    # Kafka
    assert settings.kafka.bootstrap_servers == "kafka:9092"
    assert settings.kafka.address_family == "v6"
    assert settings.kafka.person_frames_topic == "person.frames.test"
    assert settings.kafka.plate_frames_topic == "plate.frames.test"
    assert settings.kafka.person_detector_group_id == "person-test"
    assert settings.kafka.plate_detector_group_id == "plate-test"

    # Camera
    assert settings.camera.frame_interval_seconds == 0.5
    assert settings.camera.frame_width == 1280
    assert settings.camera.frame_height == 720
    assert settings.camera.jpeg_quality == 90

    # Simulation
    assert settings.simulation.random_seed == 42

    person = settings.simulation.person_detection

    assert person.presence_probability == 0.75
    assert person.movement_window_seconds == 60
    assert person.max_persons_per_frame == 8

    movement = settings.simulation.employee_movement

    assert movement.min_zone_dwell_seconds == 100
    assert movement.max_zone_dwell_seconds == 400
    assert movement.min_zone_travel_seconds == 20
    assert movement.max_zone_travel_seconds == 120

    shifts = settings.simulation.employee_shifts

    assert shifts.start_hours == (6, 14, 22)
    assert shifts.length_seconds == 28800
    assert shifts.arrival_spread_seconds == 900

    parking = settings.simulation.parking

    assert parking.transit_seconds == 240
    assert parking.detection_window_seconds == 45

    # Worker
    assert settings.person_detector_worker_id == "worker2"


def test_environment_values_are_normalized():

    settings = load_settings(
        environ={
            "KAFKA_BOOTSTRAP_SERVERS": "  kafka:9092  ",
            "KAFKA_ADDRESS_FAMILY": " V6 ",
            "SHIFT_START_HOURS": " 0, 8, 16 ",
        }
    )

    assert settings.kafka.bootstrap_servers == "kafka:9092"
    assert settings.kafka.address_family == "v6"

    assert settings.simulation.employee_shifts.start_hours == (
        0, 8, 16
    )


def test_explicit_environment_mapping_is_isolated(monkeypatch):

    monkeypatch.setenv("FRAME_WIDTH", "1920")

    settings = load_settings(environ={})

    assert settings.camera.frame_width == 640


def test_custom_project_paths(tmp_path):

    custom_paths = ProjectPaths(
        advanced_dir=tmp_path / "custom_advanced"
    )

    settings = load_settings(
        environ={},
        paths=custom_paths,
    )

    assert settings.paths is custom_paths


def test_environment_mapping_is_not_modified():

    environ = {
        "FRAME_WIDTH": "1280",
        "FRAME_HEIGHT": "720",
    }

    original = environ.copy()

    load_settings(environ=environ)

    assert environ == original


def test_environment_must_be_a_mapping():

    with pytest.raises(TypeError):
        load_settings(environ=[])


# ============================================================
# 3. ENVIRONMENT TYPE CONVERSION
# ============================================================

@pytest.mark.parametrize(
    ("environ", "expected_message"),
    [
        (
            {"FRAME_WIDTH": "wide"},
            "FRAME_WIDTH must be an integer",
        ),
        (
            {"FRAME_INTERVAL_SECONDS": "fast"},
            "FRAME_INTERVAL_SECONDS must be a number",
        ),
        (
            {"SHIFT_START_HOURS": "0,eight"},
            "SHIFT_START_HOURS must contain comma-separated integers",
        ),
    ],
)
def test_invalid_environment_types(
    environ,
    expected_message,
):

    with pytest.raises(ValueError, match=expected_message):
        load_settings(environ=environ)


@pytest.mark.parametrize(
    "environ",
    [
        {"KAFKA_BOOTSTRAP_SERVERS": " "},
        {"PERSON_DETECTOR_WORKER_ID": " "},
        {"KAFKA_PERSON_FRAMES_TOPIC": ""},
    ],
)
def test_blank_environment_values_are_rejected(environ):

    with pytest.raises(ValueError):
        load_settings(environ=environ)


@pytest.mark.parametrize(
    "value",
    ["nan", "inf", "-inf"],
)
def test_non_finite_frame_interval_from_environment(value):

    with pytest.raises(ValueError):
        load_settings(
            environ={"FRAME_INTERVAL_SECONDS": value}
        )


# ============================================================
# 4. KAFKA VALIDATION
# ============================================================

def test_invalid_kafka_address_family():

    with pytest.raises(
        ValueError,
        match="Invalid Kafka address family",
    ):
        KafkaSettings(address_family="ipv4")


def test_kafka_address_family_is_normalized():

    settings = KafkaSettings(address_family=" V6 ")

    assert settings.address_family == "v6"


def test_empty_kafka_bootstrap_servers():

    with pytest.raises(ValueError):
        KafkaSettings(bootstrap_servers=" ")


def test_empty_kafka_topic():

    with pytest.raises(ValueError):
        KafkaSettings(person_frames_topic=" ")


def test_empty_consumer_group():

    with pytest.raises(ValueError):
        KafkaSettings(person_detector_group_id="")


# ============================================================
# 5. CAMERA VALIDATION
# ============================================================

def test_zero_frame_interval():

    with pytest.raises(
        ValueError,
        match="Frame interval must be greater than zero",
    ):
        CameraSettings(frame_interval_seconds=0)


def test_negative_frame_interval():

    with pytest.raises(ValueError):
        CameraSettings(frame_interval_seconds=-1)


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_non_finite_frame_interval(value):

    with pytest.raises(ValueError):
        CameraSettings(frame_interval_seconds=value)


@pytest.mark.parametrize(
    "width",
    [0, -1, True, 640.0],
)
def test_invalid_frame_width(width):

    with pytest.raises(ValueError):
        CameraSettings(frame_width=width)


def test_invalid_frame_height():

    with pytest.raises(ValueError):
        CameraSettings(frame_height=-1)


@pytest.mark.parametrize(
    "quality",
    [0, 101],
)
def test_invalid_jpeg_quality(quality):

    with pytest.raises(
        ValueError,
        match="JPEG quality must be between 1 and 100",
    ):
        CameraSettings(jpeg_quality=quality)


# ============================================================
# 6. PERSON DETECTION VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "probability",
    [-0.1, 1.1],
)
def test_invalid_person_presence_probability(probability):

    with pytest.raises(ValueError):
        PersonDetectionSettings(
            presence_probability=probability
        )


@pytest.mark.parametrize(
    "probability",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        True,
    ],
)
def test_non_finite_or_invalid_person_probability(probability):

    with pytest.raises(ValueError):
        PersonDetectionSettings(
            presence_probability=probability
        )


def test_invalid_person_movement_window():

    with pytest.raises(ValueError):
        PersonDetectionSettings(
            movement_window_seconds=0
        )


def test_invalid_max_persons_per_frame():

    with pytest.raises(ValueError):
        PersonDetectionSettings(
            max_persons_per_frame=0
        )


# ============================================================
# 7. EMPLOYEE MOVEMENT VALIDATION
# ============================================================

def test_minimum_dwell_cannot_exceed_maximum():

    with pytest.raises(
        ValueError,
        match="Minimum zone dwell time cannot exceed",
    ):
        EmployeeMovementSettings(
            min_zone_dwell_seconds=400,
            max_zone_dwell_seconds=300,
        )


def test_minimum_travel_cannot_exceed_maximum():

    with pytest.raises(
        ValueError,
        match="Minimum zone travel time cannot exceed",
    ):
        EmployeeMovementSettings(
            min_zone_travel_seconds=100,
            max_zone_travel_seconds=90,
        )


def _default_zone_graph():

    return {
        zone: tuple(neighbors)
        for zone, neighbors
        in EmployeeMovementSettings().zone_connections.items()
    }


def test_missing_zone_is_rejected():

    graph = _default_zone_graph()

    graph.pop("E")

    with pytest.raises(
        ValueError,
        match="must define exactly zones",
    ):
        EmployeeMovementSettings(zone_connections=graph)


def test_unknown_zone_connection_is_rejected():

    graph = _default_zone_graph()

    graph["A"] = ("B", "C", "D", "E", "Z")

    with pytest.raises(
        ValueError,
        match="Unknown zone connection",
    ):
        EmployeeMovementSettings(zone_connections=graph)


def test_zone_cannot_connect_to_itself():

    graph = _default_zone_graph()

    graph["A"] = ("A", "B", "C", "D", "E")

    with pytest.raises(
        ValueError,
        match="cannot connect to itself",
    ):
        EmployeeMovementSettings(zone_connections=graph)


def test_zone_connections_must_be_bidirectional():

    graph = _default_zone_graph()

    graph["A"] = ("B", "C", "D")

    with pytest.raises(
        ValueError,
        match="must be bidirectional",
    ):
        EmployeeMovementSettings(zone_connections=graph)


def test_duplicate_zone_connections_are_rejected():

    graph = _default_zone_graph()

    graph["A"] = ("B", "B", "C", "D", "E")

    with pytest.raises(
        ValueError,
        match="Duplicate connections",
    ):
        EmployeeMovementSettings(zone_connections=graph)


def test_zone_connections_are_immutable():

    settings = EmployeeMovementSettings()

    with pytest.raises(TypeError):
        settings.zone_connections["A"] = ("B",)


def test_zone_connection_lists_are_normalized_to_tuples():

    graph = _default_zone_graph()

    graph["A"] = ["B", "C", "D", "E"]

    settings = EmployeeMovementSettings(
        zone_connections=graph
    )

    assert settings.zone_connections["A"] == (
        "B", "C", "D", "E"
    )


# ============================================================
# 8. EMPLOYEE SHIFT VALIDATION
# ============================================================

def test_empty_shift_start_hours():

    with pytest.raises(
        ValueError,
        match="At least one shift start hour",
    ):
        EmployeeShiftSettings(start_hours=())


def test_invalid_shift_start_hour():

    with pytest.raises(
        ValueError,
        match="Invalid shift start hour",
    ):
        EmployeeShiftSettings(start_hours=(0, 25))


def test_duplicate_shift_start_hours():

    with pytest.raises(
        ValueError,
        match="cannot contain duplicates",
    ):
        EmployeeShiftSettings(start_hours=(0, 8, 8))


def test_boolean_shift_hour_is_rejected():

    with pytest.raises(ValueError):
        EmployeeShiftSettings(start_hours=(0, True))


def test_shift_start_hours_are_normalized_to_tuple():

    settings = EmployeeShiftSettings(
        start_hours=[0, 8, 16]
    )

    assert settings.start_hours == (0, 8, 16)
    assert isinstance(settings.start_hours, tuple)


def test_invalid_shift_length():

    with pytest.raises(ValueError):
        EmployeeShiftSettings(length_seconds=0)


def test_negative_arrival_spread():

    with pytest.raises(ValueError):
        EmployeeShiftSettings(arrival_spread_seconds=-1)


# ============================================================
# 9. PARKING VALIDATION
# ============================================================

def test_negative_parking_transit_time():

    with pytest.raises(ValueError):
        ParkingSettings(transit_seconds=-1)


def test_zero_parking_detection_window():

    with pytest.raises(ValueError):
        ParkingSettings(detection_window_seconds=0)


def test_zero_parking_transit_time_is_allowed():

    settings = ParkingSettings(transit_seconds=0)

    assert settings.transit_seconds == 0


# ============================================================
# 10. SIMULATION VALIDATION
# ============================================================

def test_simulation_rejects_invalid_random_seed():

    with pytest.raises(ValueError):
        SimulationSettings(random_seed=True)


def test_simulation_rejects_invalid_person_detection_type():

    with pytest.raises(
        ValueError,
        match="Person detection must be",
    ):
        SimulationSettings(person_detection="invalid")


def test_simulation_rejects_invalid_movement_type():

    with pytest.raises(
        ValueError,
        match="Employee movement must be",
    ):
        SimulationSettings(employee_movement="invalid")


def test_simulation_rejects_invalid_shift_type():

    with pytest.raises(
        ValueError,
        match="Employee shifts must be",
    ):
        SimulationSettings(employee_shifts="invalid")


def test_simulation_rejects_invalid_parking_type():

    with pytest.raises(
        ValueError,
        match="Parking must be",
    ):
        SimulationSettings(parking="invalid")


# ============================================================
# 11. APPLICATION VALIDATION
# ============================================================

def test_application_rejects_invalid_kafka_type():

    with pytest.raises(
        ValueError,
        match="Kafka must be a KafkaSettings instance",
    ):
        ApplicationSettings(kafka="invalid")


def test_application_rejects_invalid_camera_type():

    with pytest.raises(
        ValueError,
        match="Camera must be a CameraSettings instance",
    ):
        ApplicationSettings(camera="invalid")


def test_application_rejects_invalid_simulation_type():

    with pytest.raises(
        ValueError,
        match="Simulation must be a SimulationSettings instance",
    ):
        ApplicationSettings(simulation="invalid")


def test_application_rejects_invalid_paths_type():

    with pytest.raises(
        ValueError,
        match="Project paths must be a ProjectPaths instance",
    ):
        ApplicationSettings(paths="invalid")


@pytest.mark.parametrize(
    "worker_id",
    [
        "",
        ".",
        "..",
        "../worker",
        r"worker\1",
        "worker/1",
        "worker 1",
    ],
)
def test_invalid_worker_id_is_rejected(worker_id):

    with pytest.raises(ValueError):
        ApplicationSettings(
            person_detector_worker_id=worker_id
        )


# ============================================================
# 12. IMMUTABILITY
# ============================================================

def test_application_settings_are_immutable():

    settings = load_settings(environ={})

    with pytest.raises(FrozenInstanceError):
        settings.person_detector_worker_id = "worker2"


def test_kafka_settings_are_immutable():

    kafka = KafkaSettings()

    with pytest.raises(FrozenInstanceError):
        kafka.bootstrap_servers = "another-server"


def test_camera_settings_are_immutable():

    camera = CameraSettings()

    with pytest.raises(FrozenInstanceError):
        camera.frame_width = 1280


def test_simulation_settings_are_immutable():

    simulation = SimulationSettings()

    with pytest.raises(FrozenInstanceError):
        simulation.random_seed = 100


def test_nested_configuration_is_immutable():

    settings = load_settings(environ={})

    with pytest.raises(FrozenInstanceError):
        settings.camera.frame_width = 1280

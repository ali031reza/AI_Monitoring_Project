
"""
Centralized application configuration.

Configuration is represented by immutable dataclasses and loaded
once during application startup through load_settings().

Environment variables override the default values.
"""

from __future__ import annotations

import math
import os
import re

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from .paths import ProjectPaths


# ============================================================
# 1. VALIDATION HELPERS
# ============================================================

def _require_non_empty_string(value: str, name: str) -> str:
    """Validate and normalize a non-empty string."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")

    return value.strip()


def _require_integer(
    value: int,
    name: str,
    minimum: int | None = None,
) -> None:
    """Validate an integer, explicitly rejecting booleans."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")

    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


def _require_finite_number(value: float, name: str) -> None:
    """Validate a finite numeric value, rejecting booleans."""

    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{name} must be a finite number")


# ============================================================
# 2. KAFKA SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """Kafka connection, topic and consumer group settings."""

    bootstrap_servers: str = "127.0.0.1:9092"
    address_family: str = "v4"

    person_frames_topic: str = "camera.person.frames.dev"
    plate_frames_topic: str = "camera.plate.frames.dev"

    person_detector_group_id: str = "person-detectors-dev"
    plate_detector_group_id: str = "plate-detectors-dev"

    def __post_init__(self) -> None:

        string_fields = (
            "bootstrap_servers",
            "person_frames_topic",
            "plate_frames_topic",
            "person_detector_group_id",
            "plate_detector_group_id",
        )

        for name in string_fields:
            value = getattr(self, name)

            object.__setattr__(
                self,
                name,
                _require_non_empty_string(value, name),
            )

        address_family = _require_non_empty_string(
            self.address_family,
            "Kafka address family",
        ).lower()

        if address_family not in {"v4", "v6"}:
            raise ValueError(
                f"Invalid Kafka address family: {address_family!r}. "
                "Expected 'v4' or 'v6'."
            )

        object.__setattr__(
            self,
            "address_family",
            address_family,
        )


# ============================================================
# 3. CAMERA SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class CameraSettings:
    """Camera frame generation and image compression settings."""

    frame_interval_seconds: float = 1.0

    frame_width: int = 640
    frame_height: int = 360

    jpeg_quality: int = 80

    def __post_init__(self) -> None:

        _require_finite_number(
            self.frame_interval_seconds,
            "Frame interval",
        )

        if self.frame_interval_seconds <= 0:
            raise ValueError(
                "Frame interval must be greater than zero"
            )

        _require_integer(
            self.frame_width,
            "Frame width",
            minimum=1,
        )

        _require_integer(
            self.frame_height,
            "Frame height",
            minimum=1,
        )

        _require_integer(
            self.jpeg_quality,
            "JPEG quality",
        )

        if not 1 <= self.jpeg_quality <= 100:
            raise ValueError(
                "JPEG quality must be between 1 and 100"
            )


# ============================================================
# 4. PERSON DETECTION SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class PersonDetectionSettings:
    """Parameters controlling simulated person detection."""

    presence_probability: float = 0.25

    movement_window_seconds: int = 30

    max_persons_per_frame: int = 4

    def __post_init__(self) -> None:

        _require_finite_number(
            self.presence_probability,
            "Person presence probability",
        )

        if not 0.0 <= self.presence_probability <= 1.0:
            raise ValueError(
                "Person presence probability must be between 0 and 1"
            )

        _require_integer(
            self.movement_window_seconds,
            "Person movement window",
            minimum=1,
        )

        _require_integer(
            self.max_persons_per_frame,
            "Maximum persons per frame",
            minimum=1,
        )


# ============================================================
# 5. EMPLOYEE MOVEMENT SETTINGS
# ============================================================

def _default_zone_connections() -> dict[str, tuple[str, ...]]:
    """Return the default building zone connection graph."""

    return {
        "A": ("B", "C", "D", "E"),
        "B": ("A", "C", "D"),
        "C": ("A", "B"),
        "D": ("A", "B"),
        "E": ("A",),
    }


@dataclass(frozen=True, slots=True)
class EmployeeMovementSettings:
    """Employee movement and building zone configuration."""

    min_zone_dwell_seconds: int = 120
    max_zone_dwell_seconds: int = 300

    min_zone_travel_seconds: int = 30
    max_zone_travel_seconds: int = 90

    zone_connections: Mapping[str, tuple[str, ...]] = field(
        default_factory=_default_zone_connections
    )

    def __post_init__(self) -> None:

        # Validate time parameters.

        time_fields = (
            "min_zone_dwell_seconds",
            "max_zone_dwell_seconds",
            "min_zone_travel_seconds",
            "max_zone_travel_seconds",
        )

        for name in time_fields:
            _require_integer(
                getattr(self, name),
                name,
                minimum=1,
            )

        if self.min_zone_dwell_seconds > self.max_zone_dwell_seconds:
            raise ValueError(
                "Minimum zone dwell time cannot exceed maximum zone dwell time"
            )

        if self.min_zone_travel_seconds > self.max_zone_travel_seconds:
            raise ValueError(
                "Minimum zone travel time cannot exceed maximum zone travel time"
            )

        # Validate zone connections.

        if not isinstance(self.zone_connections, Mapping):
            raise ValueError("Zone connections must be a mapping")

        expected_zones = {"A", "B", "C", "D", "E"}

        connections = dict(self.zone_connections)

        if set(connections) != expected_zones:
            raise ValueError(
                "Zone connections must define exactly zones A, B, C, D, and E"
            )

        normalized_connections = {}

        for zone, neighbors in connections.items():

            if not isinstance(neighbors, (tuple, list)):
                raise ValueError(
                    f"Connections for zone {zone} must be a list or tuple"
                )

            neighbors = tuple(neighbors)

            if any(
                not isinstance(neighbor, str)
                for neighbor in neighbors
            ):
                raise ValueError(
                    f"Invalid zone connection for zone {zone}"
                )

            if len(neighbors) != len(set(neighbors)):
                raise ValueError(
                    f"Duplicate connections found for zone {zone}"
                )

            for neighbor in neighbors:

                if neighbor not in expected_zones:
                    raise ValueError(
                        f"Unknown zone connection: {zone} -> {neighbor}"
                    )

                if neighbor == zone:
                    raise ValueError(
                        f"Zone {zone} cannot connect to itself"
                    )

            normalized_connections[zone] = neighbors

        # Every connection must be bidirectional.

        for zone, neighbors in normalized_connections.items():

            for neighbor in neighbors:

                if zone not in normalized_connections[neighbor]:
                    raise ValueError(
                        f"Zone connection must be bidirectional: "
                        f"{zone} -> {neighbor}"
                    )

        # Make the nested mapping immutable too.

        object.__setattr__(
            self,
            "zone_connections",
            MappingProxyType(normalized_connections),
        )


# ============================================================
# 6. EMPLOYEE SHIFT SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class EmployeeShiftSettings:
    """Employee shift scheduling parameters."""

    start_hours: tuple[int, ...] = (0, 8, 16)

    length_seconds: int = 25200

    arrival_spread_seconds: int = 1800

    def __post_init__(self) -> None:

        if not isinstance(self.start_hours, (tuple, list)):
            raise ValueError(
                "Shift start hours must be a tuple or list"
            )

        start_hours = tuple(self.start_hours)

        if not start_hours:
            raise ValueError(
                "At least one shift start hour must be configured"
            )

        for hour in start_hours:

            if (
                isinstance(hour, bool)
                or not isinstance(hour, int)
                or not 0 <= hour <= 23
            ):
                raise ValueError(
                    f"Invalid shift start hour: {hour!r}"
                )

        if len(start_hours) != len(set(start_hours)):
            raise ValueError(
                "Shift start hours cannot contain duplicates"
            )

        object.__setattr__(
            self,
            "start_hours",
            start_hours,
        )

        _require_integer(
            self.length_seconds,
            "Shift length",
            minimum=1,
        )

        _require_integer(
            self.arrival_spread_seconds,
            "Arrival spread",
            minimum=0,
        )


# ============================================================
# 7. PARKING SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class ParkingSettings:
    """Parking simulation parameters."""

    transit_seconds: int = 180

    detection_window_seconds: int = 30

    def __post_init__(self) -> None:

        _require_integer(
            self.transit_seconds,
            "Parking transit time",
            minimum=0,
        )

        _require_integer(
            self.detection_window_seconds,
            "Parking detection window",
            minimum=1,
        )


# ============================================================
# 8. SIMULATION SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class SimulationSettings:
    """Collection of all simulation-related configuration."""

    random_seed: int = 20261027

    person_detection: PersonDetectionSettings = field(
        default_factory=PersonDetectionSettings
    )

    employee_movement: EmployeeMovementSettings = field(
        default_factory=EmployeeMovementSettings
    )

    employee_shifts: EmployeeShiftSettings = field(
        default_factory=EmployeeShiftSettings
    )

    parking: ParkingSettings = field(
        default_factory=ParkingSettings
    )

    def __post_init__(self) -> None:

        _require_integer(
            self.random_seed,
            "Simulation random seed",
        )

        nested_settings = (
            (
                self.person_detection,
                PersonDetectionSettings,
                "Person detection",
            ),
            (
                self.employee_movement,
                EmployeeMovementSettings,
                "Employee movement",
            ),
            (
                self.employee_shifts,
                EmployeeShiftSettings,
                "Employee shifts",
            ),
            (
                self.parking,
                ParkingSettings,
                "Parking",
            ),
        )

        for value, expected_type, name in nested_settings:

            if not isinstance(value, expected_type):
                raise ValueError(
                    f"{name} must be a {expected_type.__name__} instance"
                )


# ============================================================
# 9. APPLICATION SETTINGS
# ============================================================

@dataclass(frozen=True, slots=True)
class ApplicationSettings:
    """
    Root configuration object for the AI Monitoring application.

    All application components receive the settings they need
    through this centralized object.
    """

    kafka: KafkaSettings = field(
        default_factory=KafkaSettings
    )

    camera: CameraSettings = field(
        default_factory=CameraSettings
    )

    simulation: SimulationSettings = field(
        default_factory=SimulationSettings
    )

    paths: ProjectPaths = field(
        default_factory=ProjectPaths.discover
    )

    person_detector_worker_id: str = "worker1"

    def __post_init__(self) -> None:

        nested_settings = (
            (self.kafka, KafkaSettings, "Kafka"),
            (self.camera, CameraSettings, "Camera"),
            (self.simulation, SimulationSettings, "Simulation"),
            (self.paths, ProjectPaths, "Project paths"),
        )

        for value, expected_type, name in nested_settings:

            if not isinstance(value, expected_type):
                raise ValueError(
                    f"{name} must be a {expected_type.__name__} instance"
                )

        worker_id = _require_non_empty_string(
            self.person_detector_worker_id,
            "Person detector worker ID",
        )

        if not re.fullmatch(r"worker[1-9]\d*", worker_id):
            raise ValueError(
                f"Invalid person detector worker ID: {worker_id!r}. "
                "Expected worker1, worker2, worker3, etc."
            )

        object.__setattr__(
            self,
            "person_detector_worker_id",
            worker_id,
        )


# ============================================================
# 10. ENVIRONMENT VARIABLE READERS
# ============================================================

def _read_string(
    environ: Mapping[str, str],
    name: str,
    default: str,
) -> str:

    value = environ.get(name, default)

    return _require_non_empty_string(value, name)


def _read_integer(
    environ: Mapping[str, str],
    name: str,
    default: int,
) -> int:

    value = environ.get(name, str(default))

    try:
        return int(value.strip())

    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"{name} must be an integer"
        ) from exc


def _read_float(
    environ: Mapping[str, str],
    name: str,
    default: float,
) -> float:

    value = environ.get(name, str(default))

    try:
        return float(value.strip())

    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"{name} must be a number"
        ) from exc


def _read_integer_tuple(
    environ: Mapping[str, str],
    name: str,
    default: tuple[int, ...],
) -> tuple[int, ...]:

    default_value = ",".join(str(hour) for hour in default)

    value = environ.get(name, default_value)

    try:
        return tuple(
            int(hour.strip())
            for hour in value.split(",")
        )

    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"{name} must contain comma-separated integers"
        ) from exc


# ============================================================
# 11. CENTRALIZED CONFIGURATION LOADER
# ============================================================

def load_settings(
    *,
    environ: Mapping[str, str] | None = None,
    paths: ProjectPaths | None = None,
) -> ApplicationSettings:
    """
    Load and validate the complete application configuration.

    Args:
        environ:
            Optional environment variable mapping.
            If omitted, os.environ is used.

        paths:
            Optional custom project paths.
            If omitted, ProjectPaths.discover() is used.

    Returns:
        A fully validated ApplicationSettings instance.

    Note:
        This function does not cache configuration.
        The application should call it once during startup
        and pass the returned object to its components.
    """

    source = os.environ if environ is None else environ

    if not isinstance(source, Mapping):
        raise TypeError("Environment configuration must be a mapping")

    # --------------------------------------------------------
    # Kafka
    # --------------------------------------------------------

    kafka = KafkaSettings(
        bootstrap_servers=_read_string(
            source,
            "KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9092",
        ),

        address_family=_read_string(
            source,
            "KAFKA_ADDRESS_FAMILY",
            "v4",
        ),

        person_frames_topic=_read_string(
            source,
            "KAFKA_PERSON_FRAMES_TOPIC",
            "camera.person.frames.dev",
        ),

        plate_frames_topic=_read_string(
            source,
            "KAFKA_PLATE_FRAMES_TOPIC",
            "camera.plate.frames.dev",
        ),

        person_detector_group_id=_read_string(
            source,
            "PERSON_DETECTOR_GROUP_ID",
            "person-detectors-dev",
        ),

        plate_detector_group_id=_read_string(
            source,
            "PLATE_DETECTOR_GROUP_ID",
            "plate-detectors-dev",
        ),
    )

    # --------------------------------------------------------
    # Camera
    # --------------------------------------------------------

    camera = CameraSettings(
        frame_interval_seconds=_read_float(
            source,
            "FRAME_INTERVAL_SECONDS",
            1.0,
        ),

        frame_width=_read_integer(
            source,
            "FRAME_WIDTH",
            640,
        ),

        frame_height=_read_integer(
            source,
            "FRAME_HEIGHT",
            360,
        ),

        jpeg_quality=_read_integer(
            source,
            "JPEG_QUALITY",
            80,
        ),
    )

    # --------------------------------------------------------
    # Person detection
    # --------------------------------------------------------

    person_detection = PersonDetectionSettings(
        presence_probability=_read_float(
            source,
            "PERSON_PRESENCE_PROBABILITY",
            0.25,
        ),

        movement_window_seconds=_read_integer(
            source,
            "PERSON_MOVEMENT_WINDOW_SECONDS",
            30,
        ),

        max_persons_per_frame=_read_integer(
            source,
            "MAX_PERSONS_PER_FRAME",
            4,
        ),
    )

    # --------------------------------------------------------
    # Employee movement
    # --------------------------------------------------------

    employee_movement = EmployeeMovementSettings(
        min_zone_dwell_seconds=_read_integer(
            source,
            "MIN_ZONE_DWELL_SECONDS",
            120,
        ),

        max_zone_dwell_seconds=_read_integer(
            source,
            "MAX_ZONE_DWELL_SECONDS",
            300,
        ),

        min_zone_travel_seconds=_read_integer(
            source,
            "MIN_ZONE_TRAVEL_SECONDS",
            30,
        ),

        max_zone_travel_seconds=_read_integer(
            source,
            "MAX_ZONE_TRAVEL_SECONDS",
            90,
        ),
    )

    # --------------------------------------------------------
    # Employee shifts
    # --------------------------------------------------------

    employee_shifts = EmployeeShiftSettings(
        start_hours=_read_integer_tuple(
            source,
            "SHIFT_START_HOURS",
            (0, 8, 16),
        ),

        length_seconds=_read_integer(
            source,
            "SHIFT_LENGTH_SECONDS",
            25200,
        ),

        arrival_spread_seconds=_read_integer(
            source,
            "ARRIVAL_SPREAD_SECONDS",
            1800,
        ),
    )

    # --------------------------------------------------------
    # Parking
    # --------------------------------------------------------

    parking = ParkingSettings(
        transit_seconds=_read_integer(
            source,
            "PARKING_TRANSIT_SECONDS",
            180,
        ),

        detection_window_seconds=_read_integer(
            source,
            "PARKING_DETECTION_WINDOW_SECONDS",
            30,
        ),
    )

    # --------------------------------------------------------
    # Simulation
    # --------------------------------------------------------

    simulation = SimulationSettings(
        random_seed=_read_integer(
            source,
            "SIMULATION_SEED",
            20261027,
        ),

        person_detection=person_detection,

        employee_movement=employee_movement,

        employee_shifts=employee_shifts,

        parking=parking,
    )

    # --------------------------------------------------------
    # Application
    # --------------------------------------------------------

    return ApplicationSettings(
        kafka=kafka,
        camera=camera,
        simulation=simulation,
        paths=paths or ProjectPaths.discover(),

        person_detector_worker_id=_read_string(
            source,
            "PERSON_DETECTOR_WORKER_ID",
            "worker1",
        ),
    )

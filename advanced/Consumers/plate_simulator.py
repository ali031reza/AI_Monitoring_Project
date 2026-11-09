
"""
Vehicle movement and license-plate detection simulation.

Responsibilities:
- Load registered employee vehicles.
- Validate employee and plate registrations.
- Identify enabled parking cameras.
- Simulate deterministic vehicle arrivals and departures.
- Determine which parking camera observes each vehicle.

This module has no Kafka or filesystem-output dependencies.
"""

from __future__ import annotations

import csv
import hashlib
import random

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType


from advanced.config import (
    CameraConfig,
    SimulationSettings,
)


# ============================================================
# 1. CONSTANTS
# ============================================================

PARKING_ZONE = "E"
PLATE_DETECTOR_TYPE = "plate"

EXPECTED_REGISTERED_VEHICLES = 75

REQUIRED_EMPLOYEE_COLUMNS = frozenset({
    "employee_id",
    "plate_number",
})


# ============================================================
# 2. REFERENCE DATA MODEL
# ============================================================

@dataclass(frozen=True, slots=True)
class VehicleReferenceData:
    """
    Immutable mapping between employees and registered vehicles.

    Mapping:
        employee_id -> plate_number
    """

    plate_by_employee: Mapping[str, str]

    def __post_init__(self) -> None:

        if not isinstance(self.plate_by_employee, Mapping):
            raise TypeError(
                "plate_by_employee must be a mapping"
            )

        normalized: dict[str, str] = {}

        for employee_id, plate in self.plate_by_employee.items():

            if (
                not isinstance(employee_id, str)
                or not employee_id.strip()
            ):
                raise ValueError(
                    "Employee ID must be a non-empty string"
                )

            if (
                not isinstance(plate, str)
                or not plate.strip()
            ):
                raise ValueError(
                    f"Invalid plate number for {employee_id}"
                )

            employee_id = employee_id.strip()
            plate = plate.strip().upper()

            if employee_id in normalized:
                raise ValueError(
                    f"Duplicate employee ID: {employee_id}"
                )

            normalized[employee_id] = plate

        if not normalized:
            raise ValueError(
                "No registered vehicles provided"
            )

        if len(set(normalized.values())) != len(normalized):
            raise ValueError(
                "Duplicate plate numbers found"
            )

        object.__setattr__(
            self,
            "plate_by_employee",
            MappingProxyType(normalized),
        )


# ============================================================
# 3. REFERENCE DATA LOADER
# ============================================================

def load_registered_vehicles(
    employees_csv: str | Path,
    expected_count: int = EXPECTED_REGISTERED_VEHICLES,
) -> VehicleReferenceData:
    """
    Load registered vehicles from employees.csv.

    Employees with empty or NA plate numbers are excluded.

    Args:
        employees_csv:
            Path to the employee reference CSV.

        expected_count:
            Expected number of registered vehicles.
            Defaults to 75, matching the current project data.

    Returns:
        Immutable VehicleReferenceData.

    Raises:
        FileNotFoundError:
            If the CSV does not exist.

        ValueError:
            If the CSV schema or records are invalid.
    """

    if (
        isinstance(expected_count, bool)
        or not isinstance(expected_count, int)
        or expected_count < 0
    ):
        raise ValueError(
            "expected_count must be a non-negative integer"
        )

    csv_path = Path(employees_csv).expanduser()

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Employee CSV not found: {csv_path}"
        )

    if not csv_path.is_file():
        raise IsADirectoryError(
            f"Employee CSV path is not a file: {csv_path}"
        )

    vehicles: dict[str, str] = {}
    seen_employees: set[str] = set()

    try:

        with csv_path.open(
            mode="r",
            newline="",
            encoding="utf-8-sig",
        ) as file:

            reader = csv.DictReader(
                file,
                strict=True,
            )

            if not reader.fieldnames:
                raise ValueError(
                    "Employee CSV is empty"
                )

            headers = [
                header.strip()
                if header is not None
                else ""
                for header in reader.fieldnames
            ]

            if any(not header for header in headers):
                raise ValueError(
                    "Employee CSV contains an empty column name"
                )

            if len(headers) != len(set(headers)):
                raise ValueError(
                    "Employee CSV contains duplicate columns"
                )

            missing_columns = (
                REQUIRED_EMPLOYEE_COLUMNS - set(headers)
            )

            if missing_columns:
                raise ValueError(
                    "Missing required employee columns: "
                    + ", ".join(sorted(missing_columns))
                )

            reader.fieldnames = headers

            for line_number, row in enumerate(reader, start=2):

                if None in row:
                    raise ValueError(
                        f"Unexpected extra values at line {line_number}"
                    )

                if all(
                    value is None or not value.strip()
                    for value in row.values()
                ):
                    continue

                employee_id = (
                    row.get("employee_id") or ""
                ).strip()

                plate = (
                    row.get("plate_number") or ""
                ).strip()

                if not employee_id:
                    raise ValueError(
                        f"Missing employee ID at line {line_number}"
                    )

                if employee_id in seen_employees:
                    raise ValueError(
                        f"Duplicate employee ID: {employee_id}"
                    )

                seen_employees.add(employee_id)

                # Ignore employees without registered vehicles.
                if not plate or plate.upper() == "NA":
                    continue

                normalized_plate = plate.upper()

                if normalized_plate in vehicles.values():
                    raise ValueError(
                        f"Duplicate plate number: {normalized_plate}"
                    )

                vehicles[employee_id] = normalized_plate

    except csv.Error as exc:
        raise ValueError(
            f"Malformed employee CSV: {exc}"
        ) from exc

    if len(vehicles) != expected_count:
        raise ValueError(
            f"Expected {expected_count} registered vehicles, "
            f"found {len(vehicles)}"
        )

    return VehicleReferenceData(
        plate_by_employee=vehicles
    )


# ============================================================
# 4. DETERMINISTIC RANDOM GENERATOR
# ============================================================

def seeded_random(
    random_seed: int,
    *values: object,
) -> random.Random:
    """
    Create a deterministic random generator.

    Identical inputs always produce identical random sequences.

    The seed construction matches the person simulator.
    """

    seed_text = "|".join(
        str(value)
        for value in (random_seed, *values)
    )

    digest = hashlib.sha256(
        seed_text.encode("utf-8")
    ).digest()

    seed = int.from_bytes(
        digest[:8],
        "big",
    )

    return random.Random(seed)


# ============================================================
# 5. PARKING CAMERA DISCOVERY
# ============================================================

def build_parking_camera_ids(
    cameras: Iterable[CameraConfig],
) -> tuple[str, ...]:
    """
    Identify enabled plate-detection cameras in Zone E.

    Returns:
        Sorted tuple of camera IDs.
    """

    if isinstance(cameras, (str, bytes)):
        raise TypeError(
            "cameras must be an iterable of CameraConfig objects"
        )

    try:
        camera_list = tuple(cameras)

    except TypeError as exc:
        raise TypeError(
            "cameras must be iterable"
        ) from exc

    if not all(
        isinstance(camera, CameraConfig)
        for camera in camera_list
    ):
        raise TypeError(
            "Every camera must be a CameraConfig instance"
        )

    camera_ids = [
        camera.camera_id
        for camera in camera_list
    ]

    if len(camera_ids) != len(set(camera_ids)):
        raise ValueError(
            "Duplicate camera IDs detected"
        )

    parking_cameras = tuple(
        sorted(
            camera.camera_id
            for camera in camera_list
            if (
                camera.enabled
                and camera.zone_id == PARKING_ZONE
                and camera.detector_type == PLATE_DETECTOR_TYPE
            )
        )
    )

    if not parking_cameras:
        raise ValueError(
            "No enabled plate cameras found in Zone E"
        )

    return parking_cameras


# ============================================================
# 6. PLATE SIMULATOR
# ============================================================

class PlateSimulator:
    """
    Simulate vehicle arrivals and departures in the parking area.

    Dependencies:
        settings:
            Validated SimulationSettings.

        reference_data:
            Registered employee vehicles.

        cameras:
            Camera configurations loaded at application startup.
    """

    def __init__(
        self,
        settings: SimulationSettings,
        reference_data: VehicleReferenceData,
        cameras: Iterable[CameraConfig],
    ) -> None:

        if not isinstance(settings, SimulationSettings):
            raise TypeError(
                "settings must be a SimulationSettings instance"
            )

        if not isinstance(
            reference_data,
            VehicleReferenceData,
        ):
            raise TypeError(
                "reference_data must be VehicleReferenceData"
            )

        self.settings = settings

        self.shift_settings = settings.employee_shifts
        self.parking_settings = settings.parking

        self.vehicles = reference_data.plate_by_employee

        self.parking_camera_ids = build_parking_camera_ids(
            cameras
        )

        # ----------------------------------------------------
        # Validate shift boundaries
        # ----------------------------------------------------

        for hour in self.shift_settings.start_hours:

            shift_end = (
                hour * 3600
                + self.shift_settings.arrival_spread_seconds
                + self.shift_settings.length_seconds
            )

            if shift_end > 86400:
                raise ValueError(
                    "Configured shifts must finish within "
                    "the same UTC day"
                )

    # ========================================================
    # 7. VEHICLE EVENT SIMULATION
    # ========================================================

    def simulate(
        self,
        camera_id: str,
        timestamp: str,
    ) -> list[str]:
        """
        Simulate license plates visible to a parking camera.

        Each registered vehicle can generate two events per day:

        - Arrival: before the employee's shift.
        - Departure: after the employee's shift.

        A vehicle is assigned deterministically to one parking
        camera for each event.

        Args:
            camera_id:
                Parking camera identifier.

            timestamp:
                Timezone-aware ISO 8601 timestamp.

        Returns:
            Sorted list of detected plate numbers.
        """

        # ----------------------------------------------------
        # Validate camera
        # ----------------------------------------------------

        if not isinstance(camera_id, str):
            raise TypeError(
                "camera_id must be a string"
            )

        if camera_id not in self.parking_camera_ids:
            raise ValueError(
                f"Unknown parking camera: {camera_id}"
            )

        # ----------------------------------------------------
        # Validate timestamp
        # ----------------------------------------------------

        if not isinstance(timestamp, str) or not timestamp.strip():
            raise ValueError(
                "Timestamp must be a non-empty string"
            )

        normalized_timestamp = timestamp.strip()

        if normalized_timestamp.endswith("Z"):
            normalized_timestamp = (
                normalized_timestamp[:-1] + "+00:00"
            )

        try:

            event_time = datetime.fromisoformat(
                normalized_timestamp
            )

        except ValueError as exc:

            raise ValueError(
                f"Invalid timestamp: {timestamp!r}"
            ) from exc

        if event_time.tzinfo is None:
            raise ValueError(
                "Timestamp must include timezone information"
            )

        event_time = event_time.astimezone(
            timezone.utc
        )

        epoch_seconds = event_time.timestamp()

        utc_day = int(
            epoch_seconds // 86400
        )

        seconds_today = (
            epoch_seconds - utc_day * 86400
        )

        # ----------------------------------------------------
        # Load parking simulation parameters
        # ----------------------------------------------------

        transit_seconds = (
            self.parking_settings.transit_seconds
        )

        detection_window = (
            self.parking_settings.detection_window_seconds
        )

        detected_plates: list[str] = []

        # ----------------------------------------------------
        # Simulate every registered vehicle
        # ----------------------------------------------------

        for employee_id, plate in sorted(
            self.vehicles.items()
        ):

            # Same daily shift calculation as person simulation.
            rng = seeded_random(
                self.settings.random_seed,
                "employee-route",
                employee_id,
                utc_day,
            )

            shift_hour = rng.choice(
                self.shift_settings.start_hours
            )

            arrival_delay = rng.randint(
                0,
                self.shift_settings.arrival_spread_seconds,
            )

            shift_start = (
                shift_hour * 3600
                + arrival_delay
            )

            shift_end = (
                shift_start
                + self.shift_settings.length_seconds
            )

            # Vehicle arrival and departure.
            arrival_time = (
                shift_start - transit_seconds
            )

            departure_time = (
                shift_end + transit_seconds
            )

            events = (
                ("arrival", arrival_time),
                ("departure", departure_time),
            )

            # ------------------------------------------------
            # Evaluate each vehicle event
            # ------------------------------------------------

            for event_type, event_second in events:

                is_inside_detection_window = (
                    event_second
                    <= seconds_today
                    < event_second + detection_window
                )

                if not is_inside_detection_window:
                    continue

                # Assign the vehicle to one parking camera.
                camera_rng = seeded_random(
                    self.settings.random_seed,
                    "parking-camera",
                    employee_id,
                    utc_day,
                    event_type,
                )

                assigned_camera = camera_rng.choice(
                    self.parking_camera_ids
                )

                if assigned_camera == camera_id:
                    detected_plates.append(plate)

        return sorted(detected_plates)

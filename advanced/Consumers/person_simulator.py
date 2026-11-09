
"""
Person movement and detection simulation.

This module contains the business logic for synthetic person
detection. It has no Kafka or output-file dependencies.
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


PERSON_ZONES = frozenset({"A", "B", "C", "D"})
RESTRICTED_ZONES = frozenset({"C", "D"})

EXPECTED_EMPLOYEE_COUNT = 100
EXPECTED_RESTRICTED_EMPLOYEE_COUNT = 10


# ============================================================
# 1. REFERENCE DATA MODEL
# ============================================================

@dataclass(frozen=True, slots=True)
class PersonReferenceData:
    """Immutable employee and restricted-access reference data."""

    employees: frozenset[str]

    restricted_access: Mapping[str, frozenset[str]]


# ============================================================
# 2. REFERENCE DATA LOADER
# ============================================================

def load_reference_data(
    employees_csv: str | Path,
    restricted_access_csv: str | Path,
) -> PersonReferenceData:
    """
    Load and validate employee and restricted-access CSV files.
    """

    employees_path = Path(employees_csv)
    access_path = Path(restricted_access_csv)

    employees: set[str] = set()

    # --------------------------------------------------------
    # Employee records
    # --------------------------------------------------------

    with employees_path.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:

        reader = csv.DictReader(file)

        if (
            not reader.fieldnames
            or "employee_id" not in reader.fieldnames
        ):
            raise ValueError(
                "Employee CSV must contain an employee_id column"
            )

        for line_number, row in enumerate(reader, start=2):

            employee_id = (
                row.get("employee_id") or ""
            ).strip()

            if not employee_id:
                raise ValueError(
                    f"Empty employee ID at line {line_number}"
                )

            if employee_id in employees:
                raise ValueError(
                    f"Duplicate employee ID: {employee_id}"
                )

            employees.add(employee_id)

    if len(employees) != EXPECTED_EMPLOYEE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_EMPLOYEE_COUNT} employees, "
            f"found {len(employees)}"
        )

    # --------------------------------------------------------
    # Restricted-access records
    # --------------------------------------------------------

    restricted_access = {
        "C": set(),
        "D": set(),
    }

    seen_records: set[tuple[str, str]] = set()

    with access_path.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:

        reader = csv.DictReader(file)

        required_columns = {
            "employee_id",
            "zone_id",
        }

        if (
            not reader.fieldnames
            or not required_columns.issubset(reader.fieldnames)
        ):
            raise ValueError(
                "Restricted-access CSV must contain "
                "employee_id and zone_id columns"
            )

        for line_number, row in enumerate(reader, start=2):

            employee_id = (
                row.get("employee_id") or ""
            ).strip()

            zone = (
                row.get("zone_id") or ""
            ).strip().upper()

            if not employee_id or not zone:
                raise ValueError(
                    f"Missing access data at line {line_number}"
                )

            if employee_id not in employees:
                raise ValueError(
                    f"Unknown employee: {employee_id}"
                )

            if zone not in RESTRICTED_ZONES:
                raise ValueError(
                    f"Invalid restricted zone: {zone}"
                )

            record = (employee_id, zone)

            if record in seen_records:
                raise ValueError(
                    f"Duplicate access record: {record}"
                )

            seen_records.add(record)
            restricted_access[zone].add(employee_id)

    # --------------------------------------------------------
    # Business-rule validation
    # --------------------------------------------------------

    for zone, authorized_employees in restricted_access.items():

        if len(authorized_employees) != EXPECTED_RESTRICTED_EMPLOYEE_COUNT:
            raise ValueError(
                f"Expected exactly "
                f"{EXPECTED_RESTRICTED_EMPLOYEE_COUNT} employees "
                f"authorized for Zone {zone}, "
                f"found {len(authorized_employees)}"
            )

    if restricted_access["C"] != restricted_access["D"]:
        raise ValueError(
            "Zones C and D must have identical "
            "authorized employee lists"
        )

    immutable_access = MappingProxyType({
        zone: frozenset(employee_ids)
        for zone, employee_ids in restricted_access.items()
    })

    return PersonReferenceData(
        employees=frozenset(employees),
        restricted_access=immutable_access,
    )


# ============================================================
# 3. AUTHORIZATION POLICY
# ============================================================

def is_authorized(
    employee_id: str,
    zone: str,
    restricted_access: Mapping[str, frozenset[str]],
) -> bool:
    """Determine whether an employee may enter a zone."""

    if zone in {"A", "B"}:
        return True

    if zone in RESTRICTED_ZONES:
        return employee_id in restricted_access[zone]

    raise ValueError(
        f"Unsupported person detection zone: {zone}"
    )


# ============================================================
# 4. DETERMINISTIC RANDOM GENERATOR
# ============================================================

def seeded_random(
    random_seed: int,
    *values: object,
) -> random.Random:
    """
    Generate a deterministic random generator.

    Identical seed inputs produce identical random sequences.
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
# 5. CAMERA-ZONE MAPPING
# ============================================================

def build_person_camera_map(
    cameras: Iterable[CameraConfig],
) -> Mapping[str, tuple[str, ...]]:
    """
    Build an immutable mapping of person cameras by zone.

    This mapping is constructed once and reused during simulation.
    """

    cameras_by_zone = {
        zone: []
        for zone in PERSON_ZONES
    }

    for camera in cameras:

        if not isinstance(camera, CameraConfig):
            raise TypeError(
                "Every camera must be a CameraConfig instance"
            )

        if not camera.enabled:
            continue

        if camera.detector_type != "person":
            continue

        if camera.zone_id not in PERSON_ZONES:
            raise ValueError(
                f"Person camera {camera.camera_id} "
                f"has unsupported zone {camera.zone_id}"
            )

        cameras_by_zone[camera.zone_id].append(
            camera.camera_id
        )

    for zone, camera_ids in cameras_by_zone.items():

        if not camera_ids:
            raise ValueError(
                f"No enabled person cameras for Zone {zone}"
            )

        camera_ids.sort()

    return MappingProxyType({
        zone: tuple(camera_ids)
        for zone, camera_ids in cameras_by_zone.items()
    })


# ============================================================
# 6. PERSON MOVEMENT SIMULATOR
# ============================================================

class PersonMovementSimulator:
    """
    Simulate employee movements and camera visibility.

    This class has no Kafka or filesystem dependencies.
    """

    def __init__(
        self,
        settings: SimulationSettings,
        reference_data: PersonReferenceData,
        cameras: Iterable[CameraConfig],
    ) -> None:

        if not isinstance(settings, SimulationSettings):
            raise TypeError(
                "settings must be a SimulationSettings instance"
            )

        if not isinstance(reference_data, PersonReferenceData):
            raise TypeError(
                "reference_data must be a PersonReferenceData instance"
            )

        self.settings = settings

        self.person_settings = settings.person_detection
        self.movement_settings = settings.employee_movement
        self.shift_settings = settings.employee_shifts

        self.employees = reference_data.employees

        self.restricted_access = (
            reference_data.restricted_access
        )

        self.cameras_by_zone = build_person_camera_map(
            cameras
        )

        self.person_camera_ids = frozenset(
            camera_id
            for camera_ids in self.cameras_by_zone.values()
            for camera_id in camera_ids
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

        # ----------------------------------------------------
        # Select one unauthorized employee deterministically
        # ----------------------------------------------------

        unauthorized_candidates = sorted(
            self.employees
            - self.restricted_access["C"]
        )

        if not unauthorized_candidates:
            raise ValueError(
                "No employee available for unauthorized simulation"
            )

        self.unauthorized_employee = seeded_random(
            settings.random_seed,
            "unauthorized-employee",
        ).choice(unauthorized_candidates)

    # ========================================================
    # 7. NEXT-ZONE SELECTION
    # ========================================================

    def _get_next_zones(
        self,
        current_zone: str,
        may_enter_restricted: bool,
    ) -> list[str]:

        possible_zones = []

        for neighbor in self.movement_settings.zone_connections[
            current_zone
        ]:

            # Zone E belongs to the parking simulation.
            if neighbor == "E":
                continue

            if (
                neighbor in RESTRICTED_ZONES
                and not may_enter_restricted
            ):
                continue

            possible_zones.append(neighbor)

        return possible_zones

    # ========================================================
    # 8. PERSON SIMULATION
    # ========================================================

    def simulate(
        self,
        camera_id: str,
        timestamp: str,
    ) -> list[str]:
        """
        Return the employee IDs visible in a camera frame.
        """

        if camera_id not in self.person_camera_ids:
            raise ValueError(
                f"Unknown person camera: {camera_id}"
            )

        if not isinstance(timestamp, str):
            raise TypeError(
                "Timestamp must be a string"
            )

        try:

            event_time = datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            )

        except ValueError as exc:

            raise ValueError(
                f"Invalid timestamp: {timestamp!r}"
            ) from exc

        if event_time.tzinfo is None:
            raise ValueError(
                "Timestamp must include timezone information"
            )

        event_time = event_time.astimezone(timezone.utc)

        epoch_seconds = event_time.timestamp()

        utc_day = int(epoch_seconds // 86400)

        seconds_today = (
            epoch_seconds - utc_day * 86400
        )

        detected: list[str] = []

        # ----------------------------------------------------
        # Simulate each employee
        # ----------------------------------------------------

        for employee_id in sorted(self.employees):

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
                shift_hour * 3600 + arrival_delay
            )

            shift_end = (
                shift_start
                + self.shift_settings.length_seconds
            )

            if not (
                shift_start <= seconds_today < shift_end
            ):
                continue

            may_enter_restricted = (
                employee_id in self.restricted_access["C"]
                or employee_id == self.unauthorized_employee
            )

            current_zone = "A"

            assigned_camera = rng.choice(
                self.cameras_by_zone[current_zone]
            )

            position = shift_start

            exit_margin = 2 * (
                self.movement_settings.max_zone_dwell_seconds
                + self.movement_settings.max_zone_travel_seconds
            )

            # Reconstruct the employee's route.
            while position < shift_end:

                dwell_seconds = rng.randint(
                    self.movement_settings.min_zone_dwell_seconds,
                    self.movement_settings.max_zone_dwell_seconds,
                )

                if (
                    current_zone == "A"
                    and shift_end - position <= exit_margin
                ):
                    dwell_seconds = shift_end - position

                visit_end = min(
                    position + dwell_seconds,
                    shift_end,
                )

                # Employee is currently in this zone.
                if seconds_today < visit_end:

                    if assigned_camera == camera_id:

                        observation_window = int(
                            epoch_seconds
                            // self.person_settings.movement_window_seconds
                        )

                        visibility_rng = seeded_random(
                            self.settings.random_seed,
                            "person-visibility",
                            employee_id,
                            observation_window,
                        )

                        if (
                            visibility_rng.random()
                            < self.person_settings.presence_probability
                        ):
                            detected.append(employee_id)

                    break

                position = visit_end

                if position >= shift_end:
                    break

                # Employee travels to another zone.
                travel_seconds = rng.randint(
                    self.movement_settings.min_zone_travel_seconds,
                    self.movement_settings.max_zone_travel_seconds,
                )

                travel_end = position + travel_seconds

                if seconds_today < travel_end:
                    break

                position = travel_end

                if position >= shift_end:
                    break

                # Return to Zone A near the end of the shift.
                if position >= shift_end - exit_margin:

                    next_zone = "A"

                else:

                    possible_zones = self._get_next_zones(
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

                assigned_camera = rng.choice(
                    self.cameras_by_zone[current_zone]
                )

        # ----------------------------------------------------
        # Enforce camera detection capacity
        # ----------------------------------------------------

        if len(detected) > self.person_settings.max_persons_per_frame:

            observation_window = int(
                epoch_seconds
                // self.person_settings.movement_window_seconds
            )

            selection_rng = seeded_random(
                self.settings.random_seed,
                "camera-capacity",
                camera_id,
                observation_window,
            )

            detected = selection_rng.sample(
                detected,
                self.person_settings.max_persons_per_frame,
            )

        return sorted(detected)

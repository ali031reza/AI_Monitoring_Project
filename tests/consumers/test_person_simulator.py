
import csv

from types import MappingProxyType

import pytest

from advanced.config import (
    CameraConfig,
    load_settings,
)

from advanced.Consumers.person_simulator import (
    PersonReferenceData,
    PersonMovementSimulator,
    build_person_camera_map,
    is_authorized,
    load_reference_data,
    seeded_random,
)


@pytest.fixture
def settings():

    return load_settings(environ={}).simulation


@pytest.fixture
def employees():

    return frozenset(
        f"EMP{i:04d}"
        for i in range(1, 101)
    )


@pytest.fixture
def reference_data(employees):

    authorized = frozenset(
        sorted(employees)[:10]
    )

    return PersonReferenceData(
        employees=employees,
        restricted_access=MappingProxyType({
            "C": authorized,
            "D": authorized,
        }),
    )


@pytest.fixture
def cameras():

    return (
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("002", "B", "person", True, 0),
        CameraConfig("003", "C", "person", True, 1),
        CameraConfig("004", "D", "person", True, 1),
        CameraConfig("008", "E", "plate", True, 0),
    )


# ============================================================
# 1. REFERENCE DATA
# ============================================================

def test_load_reference_data(tmp_path):

    employees_path = tmp_path / "employees.csv"
    access_path = tmp_path / "restricted_access.csv"

    employees = [
        f"EMP{i:04d}"
        for i in range(1, 101)
    ]

    with employees_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow(["employee_id", "plate_number"])

        for employee in employees:
            writer.writerow([employee, "NA"])

    with access_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow(["employee_id", "zone_id"])

        for employee in employees[:10]:
            writer.writerow([employee, "C"])
            writer.writerow([employee, "D"])

    result = load_reference_data(
        employees_path,
        access_path,
    )

    assert len(result.employees) == 100
    assert len(result.restricted_access["C"]) == 10
    assert result.restricted_access["C"] == (
        result.restricted_access["D"]
    )


def test_duplicate_employee_ids_are_rejected(tmp_path):

    employees_path = tmp_path / "employees.csv"
    access_path = tmp_path / "access.csv"

    employees_path.write_text(
        "employee_id,plate_number\n"
        + "".join(
            f"EMP{i:04d},NA\n"
            for i in range(1, 100)
        )
        + "EMP0001,NA\n",
        encoding="utf-8",
    )

    access_path.write_text(
        "employee_id,zone_id\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Duplicate employee ID",
    ):
        load_reference_data(
            employees_path,
            access_path,
        )


def test_reference_data_is_immutable(reference_data):

    with pytest.raises(TypeError):
        reference_data.restricted_access["C"] = frozenset()


# ============================================================
# 2. CAMERA-ZONE MAPPING
# ============================================================

def test_build_person_camera_map(cameras):

    result = build_person_camera_map(cameras)

    assert result["A"] == ("001",)
    assert result["B"] == ("002",)
    assert result["C"] == ("003",)
    assert result["D"] == ("004",)

    assert "E" not in result


def test_camera_mapping_is_immutable(cameras):

    result = build_person_camera_map(cameras)

    with pytest.raises(TypeError):
        result["A"] = ("999",)


def test_disabled_camera_is_excluded():

    cameras = (
        CameraConfig("001", "A", "person", False, 0),
        CameraConfig("002", "A", "person", True, 0),
        CameraConfig("003", "B", "person", True, 0),
        CameraConfig("004", "C", "person", True, 1),
        CameraConfig("005", "D", "person", True, 1),
    )

    result = build_person_camera_map(cameras)

    assert result["A"] == ("002",)


def test_missing_zone_camera_is_rejected():

    cameras = (
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("002", "B", "person", True, 0),
        CameraConfig("003", "C", "person", True, 1),
    )

    with pytest.raises(
        ValueError,
        match="No enabled person cameras for Zone D",
    ):
        build_person_camera_map(cameras)


def test_person_camera_in_parking_zone_is_rejected():

    cameras = (
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("002", "B", "person", True, 0),
        CameraConfig("003", "C", "person", True, 1),
        CameraConfig("004", "D", "person", True, 1),
        CameraConfig("005", "E", "person", True, 0),
    )

    with pytest.raises(
        ValueError,
        match="unsupported zone",
    ):
        build_person_camera_map(cameras)


# ============================================================
# 3. AUTHORIZATION
# ============================================================

def test_employee_is_authorized_in_public_zone(reference_data):

    employee = "EMP0099"

    assert is_authorized(
        employee,
        "A",
        reference_data.restricted_access,
    )


def test_authorized_employee_can_enter_restricted_zone(
    reference_data,
):

    employee = "EMP0001"

    assert is_authorized(
        employee,
        "C",
        reference_data.restricted_access,
    )

    assert is_authorized(
        employee,
        "D",
        reference_data.restricted_access,
    )


def test_unauthorized_employee_cannot_enter_restricted_zone(
    reference_data,
):

    employee = "EMP0099"

    assert not is_authorized(
        employee,
        "C",
        reference_data.restricted_access,
    )


def test_unsupported_zone_is_rejected(reference_data):

    with pytest.raises(ValueError):
        is_authorized(
            "EMP0001",
            "E",
            reference_data.restricted_access,
        )


# ============================================================
# 4. DETERMINISTIC RANDOMNESS
# ============================================================

def test_seeded_random_is_deterministic():

    first = seeded_random(42, "employee", "EMP0001")
    second = seeded_random(42, "employee", "EMP0001")

    assert first.random() == second.random()


def test_different_seeds_produce_different_sequences():

    first = seeded_random(42, "employee")
    second = seeded_random(43, "employee")

    assert first.random() != second.random()


# ============================================================
# 5. SIMULATION
# ============================================================

def test_simulator_initialization(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    assert len(simulator.employees) == 100
    assert len(simulator.person_camera_ids) == 4


def test_simulation_is_deterministic(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    timestamp = "2026-11-08T10:00:00Z"

    first = simulator.simulate("001", timestamp)
    second = simulator.simulate("001", timestamp)

    assert first == second


def test_simulation_returns_sorted_employee_ids(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    detected = simulator.simulate(
        "001",
        "2026-11-08T10:00:00Z",
    )

    assert detected == sorted(detected)


def test_detection_capacity_is_respected(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    detected = simulator.simulate(
        "001",
        "2026-11-08T10:00:00Z",
    )

    assert len(detected) <= (
        settings.person_detection.max_persons_per_frame
    )


def test_unknown_camera_is_rejected(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    with pytest.raises(ValueError):

        simulator.simulate(
            "999",
            "2026-11-08T10:00:00Z",
        )


def test_naive_timestamp_is_rejected(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    with pytest.raises(
        ValueError,
        match="timezone information",
    ):

        simulator.simulate(
            "001",
            "2026-11-08T10:00:00",
        )


def test_invalid_timestamp_is_rejected(
    settings,
    reference_data,
    cameras,
):

    simulator = PersonMovementSimulator(
        settings,
        reference_data,
        cameras,
    )

    with pytest.raises(ValueError):

        simulator.simulate(
            "001",
            "invalid-timestamp",
        )

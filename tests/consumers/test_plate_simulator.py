
import csv

import pytest

from advanced.config import (
    CameraConfig,
    load_settings,
)

from advanced.Consumers.plate_simulator import (
    VehicleReferenceData,
    PlateSimulator,
    load_registered_vehicles,
    build_parking_camera_ids,
)


# ============================================================
# 1. FIXTURES
# ============================================================

@pytest.fixture
def simulation_settings():

    return load_settings(environ={}).simulation


@pytest.fixture
def fixed_schedule_settings():

    return load_settings(
        environ={
            "SIMULATION_SEED": "42",
            "SHIFT_START_HOURS": "8",
            "SHIFT_LENGTH_SECONDS": "3600",
            "ARRIVAL_SPREAD_SECONDS": "0",
            "PARKING_TRANSIT_SECONDS": "60",
            "PARKING_DETECTION_WINDOW_SECONDS": "30",
        }
    ).simulation


@pytest.fixture
def reference_data():

    return VehicleReferenceData({
        "EMP0001": "SYN-1234",
        "EMP0002": "SYN-5678",
        "EMP0003": "SYN-9012",
    })


@pytest.fixture
def single_vehicle():

    return VehicleReferenceData({
        "EMP0001": "SYN-1234",
    })


@pytest.fixture
def parking_cameras():

    return (
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("009", "E", "plate", True, 0),
        CameraConfig("010", "E", "plate", True, 0),
    )


@pytest.fixture
def single_parking_camera():

    return (
        CameraConfig("008", "E", "plate", True, 0),
    )


# ============================================================
# 2. CSV TEST HELPERS
# ============================================================

def write_employees_csv(
    tmp_path,
    rows,
    headers=("employee_id", "plate_number"),
):
    """Create an employee CSV for testing."""

    path = tmp_path / "employees.csv"

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow(headers)
        writer.writerows(rows)

    return path


def valid_employee_rows():

    return [
        (
            f"EMP{i:04d}",
            f"SYN-{i:04d}" if i <= 75 else "NA",
        )
        for i in range(1, 101)
    ]


# ============================================================
# 3. VEHICLE REFERENCE DATA
# ============================================================

def test_vehicle_reference_data_initialization():

    data = VehicleReferenceData({
        "EMP0001": "SYN-1234",
        "EMP0002": "SYN-5678",
    })

    assert len(data.plate_by_employee) == 2


def test_vehicle_reference_data_normalizes_values():

    data = VehicleReferenceData({
        " EMP0001 ": " syn-1234 ",
    })

    assert data.plate_by_employee == {
        "EMP0001": "SYN-1234",
    }


def test_vehicle_reference_data_is_immutable(
    reference_data,
):

    with pytest.raises(TypeError):

        reference_data.plate_by_employee[
            "EMP0004"
        ] = "SYN-1111"


def test_empty_vehicle_reference_data_is_rejected():

    with pytest.raises(
        ValueError,
        match="No registered vehicles",
    ):

        VehicleReferenceData({})


def test_duplicate_employee_ids_after_normalization():

    with pytest.raises(
        ValueError,
        match="Duplicate employee ID",
    ):

        VehicleReferenceData({
            "EMP0001": "SYN-1234",
            " EMP0001 ": "SYN-5678",
        })


def test_duplicate_plate_numbers_are_rejected():

    with pytest.raises(
        ValueError,
        match="Duplicate plate numbers",
    ):

        VehicleReferenceData({
            "EMP0001": "SYN-1234",
            "EMP0002": "SYN-1234",
        })


def test_empty_employee_id_is_rejected():

    with pytest.raises(
        ValueError,
        match="Employee ID",
    ):

        VehicleReferenceData({
            "": "SYN-1234",
        })


def test_empty_plate_number_is_rejected():

    with pytest.raises(
        ValueError,
        match="Invalid plate number",
    ):

        VehicleReferenceData({
            "EMP0001": "",
        })


# ============================================================
# 4. REGISTERED VEHICLE LOADER
# ============================================================

def test_load_registered_vehicles(tmp_path):

    path = write_employees_csv(
        tmp_path,
        valid_employee_rows(),
    )

    result = load_registered_vehicles(path)

    assert isinstance(result, VehicleReferenceData)

    assert len(result.plate_by_employee) == 75

    assert result.plate_by_employee["EMP0001"] == "SYN-0001"

    assert result.plate_by_employee["EMP0075"] == "SYN-0075"

    assert "EMP0076" not in result.plate_by_employee


def test_loader_skips_na_and_empty_plates(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("EMP0001", "NA"),
            ("EMP0002", ""),
            ("EMP0003", "SYN-1234"),
        ],
    )

    result = load_registered_vehicles(
        path,
        expected_count=1,
    )

    assert result.plate_by_employee == {
        "EMP0003": "SYN-1234",
    }


def test_loader_rejects_wrong_vehicle_count(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("EMP0001", "SYN-1234"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Expected 75 registered vehicles",
    ):

        load_registered_vehicles(path)


def test_loader_rejects_duplicate_employee_ids(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("EMP0001", "SYN-1234"),
            ("EMP0001", "NA"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate employee ID",
    ):

        load_registered_vehicles(
            path,
            expected_count=1,
        )


def test_loader_rejects_duplicate_plate_numbers(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("EMP0001", "SYN-1234"),
            ("EMP0002", "SYN-1234"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate plate number",
    ):

        load_registered_vehicles(
            path,
            expected_count=2,
        )


def test_loader_rejects_missing_employee_id(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("", "SYN-1234"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing employee ID",
    ):

        load_registered_vehicles(
            path,
            expected_count=1,
        )


def test_loader_rejects_missing_required_columns(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("EMP0001",),
        ],
        headers=("employee_id",),
    )

    with pytest.raises(
        ValueError,
        match="Missing required employee columns",
    ):

        load_registered_vehicles(path)


def test_loader_rejects_extra_csv_values(tmp_path):

    path = write_employees_csv(
        tmp_path,
        [
            ("EMP0001", "SYN-1234", "unexpected"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Unexpected extra values",
    ):

        load_registered_vehicles(
            path,
            expected_count=1,
        )


def test_loader_rejects_missing_file(tmp_path):

    path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):

        load_registered_vehicles(path)


def test_loader_rejects_directory(tmp_path):

    with pytest.raises(IsADirectoryError):

        load_registered_vehicles(tmp_path)


def test_loader_rejects_invalid_expected_count(tmp_path):

    path = write_employees_csv(
        tmp_path,
        valid_employee_rows(),
    )

    for invalid_count in (-1, True, 1.5):

        with pytest.raises(ValueError):

            load_registered_vehicles(
                path,
                expected_count=invalid_count,
            )


# ============================================================
# 5. PARKING CAMERA DISCOVERY
# ============================================================

def test_build_parking_camera_ids(parking_cameras):

    result = build_parking_camera_ids(
        parking_cameras
    )

    assert result == (
        "008",
        "009",
        "010",
    )


def test_camera_ids_are_sorted():

    cameras = (
        CameraConfig("010", "E", "plate", True, 0),
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("009", "E", "plate", True, 0),
    )

    result = build_parking_camera_ids(cameras)

    assert result == (
        "008",
        "009",
        "010",
    )


def test_disabled_parking_camera_is_excluded():

    cameras = (
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("009", "E", "plate", False, 0),
    )

    result = build_parking_camera_ids(cameras)

    assert result == ("008",)


def test_person_camera_is_excluded():

    cameras = (
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("008", "E", "plate", True, 0),
    )

    result = build_parking_camera_ids(cameras)

    assert result == ("008",)


def test_plate_camera_outside_zone_e_is_excluded():

    cameras = (
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("009", "D", "plate", True, 0),
    )

    result = build_parking_camera_ids(cameras)

    assert result == ("008",)


def test_no_active_parking_cameras_is_rejected():

    cameras = (
        CameraConfig("008", "E", "plate", False, 0),
        CameraConfig("001", "A", "person", True, 0),
    )

    with pytest.raises(
        ValueError,
        match="No enabled plate cameras",
    ):

        build_parking_camera_ids(cameras)


def test_duplicate_camera_ids_are_rejected():

    cameras = (
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("008", "E", "plate", True, 0),
    )

    with pytest.raises(
        ValueError,
        match="Duplicate camera IDs",
    ):

        build_parking_camera_ids(cameras)


def test_invalid_camera_type_is_rejected():

    with pytest.raises(TypeError):

        build_parking_camera_ids([
            "008",
        ])


# ============================================================
# 6. SIMULATOR INITIALIZATION
# ============================================================

def test_simulator_initialization(
    simulation_settings,
    reference_data,
    parking_cameras,
):

    simulator = PlateSimulator(
        settings=simulation_settings,
        reference_data=reference_data,
        cameras=parking_cameras,
    )

    assert len(simulator.vehicles) == 3

    assert simulator.parking_camera_ids == (
        "008",
        "009",
        "010",
    )


def test_simulator_uses_injected_reference_data(
    simulation_settings,
    reference_data,
    parking_cameras,
):

    simulator = PlateSimulator(
        simulation_settings,
        reference_data,
        parking_cameras,
    )

    assert simulator.vehicles is (
        reference_data.plate_by_employee
    )


def test_invalid_settings_are_rejected(
    reference_data,
    parking_cameras,
):

    with pytest.raises(TypeError):

        PlateSimulator(
            settings="invalid",
            reference_data=reference_data,
            cameras=parking_cameras,
        )


def test_invalid_reference_data_is_rejected(
    simulation_settings,
    parking_cameras,
):

    with pytest.raises(TypeError):

        PlateSimulator(
            settings=simulation_settings,
            reference_data={},
            cameras=parking_cameras,
        )


def test_simulator_rejects_missing_parking_cameras(
    simulation_settings,
    reference_data,
):

    cameras = (
        CameraConfig("001", "A", "person", True, 0),
    )

    with pytest.raises(
        ValueError,
        match="No enabled plate cameras",
    ):

        PlateSimulator(
            simulation_settings,
            reference_data,
            cameras,
        )


def test_shift_crossing_midnight_is_rejected(
    reference_data,
    single_parking_camera,
):

    settings = load_settings(
        environ={
            "SHIFT_START_HOURS": "20",
            "SHIFT_LENGTH_SECONDS": "14400",
        }
    ).simulation

    with pytest.raises(
        ValueError,
        match="same UTC day",
    ):

        PlateSimulator(
            settings,
            reference_data,
            single_parking_camera,
        )


# ============================================================
# 7. DETERMINISTIC SIMULATION
# ============================================================

def test_simulation_is_deterministic(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    timestamp = "2026-11-08T07:59:00Z"

    first = simulator.simulate(
        "008",
        timestamp,
    )

    second = simulator.simulate(
        "008",
        timestamp,
    )

    assert first == second


def test_simulation_returns_sorted_plates(
    fixed_schedule_settings,
    single_parking_camera,
):

    vehicles = VehicleReferenceData({
        "EMP0002": "SYN-2000",
        "EMP0001": "SYN-1000",
    })

    simulator = PlateSimulator(
        fixed_schedule_settings,
        vehicles,
        single_parking_camera,
    )

    detected = simulator.simulate(
        "008",
        "2026-11-08T07:59:00Z",
    )

    assert detected == sorted(detected)


def test_vehicle_is_assigned_to_only_one_camera(
    fixed_schedule_settings,
    single_vehicle,
):

    cameras = (
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("009", "E", "plate", True, 0),
    )

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        cameras,
    )

    timestamp = "2026-11-08T07:59:00Z"

    first = simulator.simulate(
        "008",
        timestamp,
    )

    second = simulator.simulate(
        "009",
        timestamp,
    )

    assert sorted(first + second) == [
        "SYN-1234",
    ]

    assert bool(first) != bool(second)


# ============================================================
# 8. ARRIVAL DETECTION WINDOW
# ============================================================

def test_arrival_window_start_is_inclusive(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    detected = simulator.simulate(
        "008",
        "2026-11-08T07:59:00Z",
    )

    assert detected == ["SYN-1234"]


def test_arrival_window_end_is_exclusive(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    detected = simulator.simulate(
        "008",
        "2026-11-08T07:59:30Z",
    )

    assert detected == []


# ============================================================
# 9. DEPARTURE DETECTION WINDOW
# ============================================================

def test_departure_window_start_is_inclusive(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    detected = simulator.simulate(
        "008",
        "2026-11-08T09:01:00Z",
    )

    assert detected == ["SYN-1234"]


def test_departure_window_end_is_exclusive(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    detected = simulator.simulate(
        "008",
        "2026-11-08T09:01:30Z",
    )

    assert detected == []


# ============================================================
# 10. TIMESTAMP VALIDATION
# ============================================================

def test_naive_timestamp_is_rejected(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    with pytest.raises(
        ValueError,
        match="timezone information",
    ):

        simulator.simulate(
            "008",
            "2026-11-08T07:59:00",
        )


def test_invalid_timestamp_is_rejected(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    with pytest.raises(
        ValueError,
        match="Invalid timestamp",
    ):

        simulator.simulate(
            "008",
            "not-a-timestamp",
        )


def test_empty_timestamp_is_rejected(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    with pytest.raises(
        ValueError,
        match="non-empty string",
    ):

        simulator.simulate(
            "008",
            "",
        )


def test_naive_timezone_conversion_is_consistent(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    utc_result = simulator.simulate(
        "008",
        "2026-11-08T07:59:00Z",
    )

    offset_result = simulator.simulate(
        "008",
        "2026-11-08T08:59:00+01:00",
    )

    assert utc_result == offset_result


# ============================================================
# 11. CAMERA VALIDATION
# ============================================================

def test_unknown_camera_is_rejected(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    with pytest.raises(
        ValueError,
        match="Unknown parking camera",
    ):

        simulator.simulate(
            "999",
            "2026-11-08T07:59:00Z",
        )


def test_non_string_camera_id_is_rejected(
    fixed_schedule_settings,
    single_vehicle,
    single_parking_camera,
):

    simulator = PlateSimulator(
        fixed_schedule_settings,
        single_vehicle,
        single_parking_camera,
    )

    with pytest.raises(TypeError):

        simulator.simulate(
            8,
            "2026-11-08T07:59:00Z",
        )

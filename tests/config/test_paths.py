
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from advanced.config import ProjectPaths


# ============================================================
# 1. PROJECT DIRECTORY DISCOVERY
# ============================================================

def test_discover_returns_correct_advanced_directory():

    paths = ProjectPaths.discover()

    expected = Path(__file__).resolve().parents[2] / "advanced"

    assert paths.advanced_dir == expected


def test_advanced_directory_is_absolute():

    paths = ProjectPaths.discover()

    assert paths.advanced_dir.is_absolute()


# ============================================================
# 2. REFERENCE DATA PATHS
# ============================================================

def test_starter_sources_directory():

    paths = ProjectPaths.discover()

    assert paths.starter_sources_dir == (
        paths.advanced_dir / "Starter_Sources"
    )


def test_cameras_csv_path():

    paths = ProjectPaths.discover()

    assert paths.cameras_csv == (
        paths.starter_sources_dir / "cameras.csv"
    )


def test_employees_csv_path():

    paths = ProjectPaths.discover()

    assert paths.employees_csv == (
        paths.starter_sources_dir / "employees.csv"
    )


def test_restricted_access_csv_path():

    paths = ProjectPaths.discover()

    assert paths.restricted_access_csv == (
        paths.starter_sources_dir / "restricted_access.csv"
    )


# ============================================================
# 3. GENERATED DATA PATHS
# ============================================================

def test_data_directory():

    paths = ProjectPaths.discover()

    assert paths.data_dir == paths.advanced_dir / "data"


def test_person_detections_csv_path():

    paths = ProjectPaths.discover()

    result = paths.person_detections_csv("worker1")

    expected = (
        paths.data_dir / "persons_detected_worker1.csv"
    )

    assert result == expected


def test_plate_detections_csv_path():

    paths = ProjectPaths.discover()

    assert paths.plate_detections_csv == (
        paths.data_dir / "plates_detected.csv"
    )


def test_preview_directory():

    paths = ProjectPaths.discover()

    assert paths.preview_dir == (
        paths.advanced_dir / "preview"
    )


# ============================================================
# 4. ETL AND SPARK PATHS
# ============================================================

def test_etl_directory():

    paths = ProjectPaths.discover()

    assert paths.etl_dir == paths.advanced_dir / "ETL"


def test_warehouse_directory():

    paths = ProjectPaths.discover()

    assert paths.warehouse_dir == (
        paths.etl_dir / "warehouse"
    )


def test_metastore_directory():

    paths = ProjectPaths.discover()

    assert paths.metastore_dir == (
        paths.etl_dir / "metastore_db"
    )


def test_spark_temp_directory():

    paths = ProjectPaths.discover()

    assert paths.spark_temp_dir == (
        paths.etl_dir / ".spark-temp"
    )


# ============================================================
# 5. WORKER ID VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "worker_id",
    [
        "worker1",
        "worker2",
        "worker_3",
        "worker-4",
        "Worker5",
    ],
)
def test_valid_worker_ids(worker_id):

    paths = ProjectPaths.discover()

    result = paths.person_detections_csv(worker_id)

    assert result.name == (
        f"persons_detected_{worker_id}.csv"
    )


@pytest.mark.parametrize(
    "worker_id",
    [
        "",
        ".",
        "..",
        "../worker",
        "worker/1",
        r"worker\1",
        "worker 1",
        "worker.1",
    ],
)
def test_invalid_worker_ids_are_rejected(worker_id):

    paths = ProjectPaths.discover()

    with pytest.raises(ValueError):
        paths.person_detections_csv(worker_id)


# ============================================================
# 6. CUSTOM PROJECT PATHS
# ============================================================

def test_custom_project_paths(tmp_path):

    custom_root = tmp_path / "custom_advanced"

    paths = ProjectPaths(advanced_dir=custom_root)

    assert paths.advanced_dir == custom_root

    assert paths.cameras_csv == (
        custom_root / "Starter_Sources" / "cameras.csv"
    )

    assert paths.warehouse_dir == (
        custom_root / "ETL" / "warehouse"
    )

def test_path_properties_do_not_create_directories(tmp_path):

    custom_root = tmp_path / "advanced"

    paths = ProjectPaths(advanced_dir=custom_root)

    # Accessing path properties must not create directories.
    _ = paths.data_dir
    _ = paths.warehouse_dir
    _ = paths.preview_dir

    assert not custom_root.exists()
# ============================================================
# 7. IMMUTABILITY
# ============================================================

def test_project_paths_are_immutable():

    paths = ProjectPaths.discover()

    with pytest.raises(FrozenInstanceError):
        paths.advanced_dir = Path("another_directory")

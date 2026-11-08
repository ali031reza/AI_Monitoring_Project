
import csv

from dataclasses import FrozenInstanceError

import pytest

from advanced.config import CameraConfig, load_cameras


HEADERS = [
    "camera_id",
    "zone_id",
    "detector_type",
    "enabled",
    "kafka_partition",
]


def valid_camera():

    return {
        "camera_id": "001",
        "zone_id": "A",
        "detector_type": "person",
        "enabled": "TRUE",
        "kafka_partition": "0",
    }


def write_csv(tmp_path, rows, headers=None):

    path = tmp_path / "cameras.csv"

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=headers or HEADERS,
            extrasaction="ignore",
        )


        writer.writeheader()
        writer.writerows(rows)

    return path


# ============================================================
# 1. VALID CAMERA LOADING
# ============================================================

def test_load_valid_camera(tmp_path):

    path = write_csv(tmp_path, [valid_camera()])

    cameras = load_cameras(path)

    assert len(cameras) == 1

    assert cameras[0] == CameraConfig(
        camera_id="001",
        zone_id="A",
        detector_type="person",
        enabled=True,
        kafka_partition=0,
    )


def test_leading_zero_in_camera_id_is_preserved(tmp_path):

    path = write_csv(tmp_path, [valid_camera()])

    cameras = load_cameras(path)

    assert cameras[0].camera_id == "001"


def test_multiple_cameras(tmp_path):

    camera_one = valid_camera()

    camera_two = {
        "camera_id": "008",
        "zone_id": "E",
        "detector_type": "plate",
        "enabled": "TRUE",
        "kafka_partition": "1",
    }

    path = write_csv(
        tmp_path,
        [camera_one, camera_two],
    )

    cameras = load_cameras(path)

    assert len(cameras) == 2
    assert cameras[1].camera_id == "008"
    assert cameras[1].detector_type == "plate"


def test_uppercase_values_are_normalized(tmp_path):

    camera = valid_camera()

    camera["zone_id"] = "a"
    camera["detector_type"] = "PERSON"

    path = write_csv(tmp_path, [camera])

    result = load_cameras(path)[0]

    assert result.zone_id == "A"
    assert result.detector_type == "person"


def test_disabled_camera(tmp_path):

    camera = valid_camera()
    camera["enabled"] = "FALSE"

    path = write_csv(tmp_path, [camera])

    result = load_cameras(path)[0]

    assert result.enabled is False


def test_blank_records_are_ignored(tmp_path):

    camera = valid_camera()

    blank_record = {
        column: ""
        for column in HEADERS
    }

    path = write_csv(
        tmp_path,
        [camera, blank_record],
    )

    assert len(load_cameras(path)) == 1


# ============================================================
# 2. FILE VALIDATION
# ============================================================

def test_missing_file(tmp_path):

    path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        load_cameras(path)


def test_directory_instead_of_file(tmp_path):

    with pytest.raises(IsADirectoryError):
        load_cameras(tmp_path)


def test_empty_file(tmp_path):

    path = tmp_path / "cameras.csv"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError):
        load_cameras(path)


def test_no_camera_records(tmp_path):

    path = write_csv(tmp_path, [])

    with pytest.raises(
        ValueError,
        match="No camera records found",
    ):
        load_cameras(path)


# ============================================================
# 3. CSV HEADER VALIDATION
# ============================================================

def test_missing_required_columns(tmp_path):

    path = write_csv(
        tmp_path,
        [valid_camera()],
        headers=["camera_id", "zone_id"],
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        load_cameras(path)


def test_duplicate_headers(tmp_path):

    path = tmp_path / "cameras.csv"

    path.write_text(
        "camera_id,camera_id,zone_id,detector_type,"
        "enabled,kafka_partition\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Duplicate CSV columns",
    ):
        load_cameras(path)


# ============================================================
# 4. CAMERA ID VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "camera_id",
    ["1", "12", "abcd", "12a", "１２３"],
)
def test_invalid_camera_id(tmp_path, camera_id):

    camera = valid_camera()
    camera["camera_id"] = camera_id

    path = write_csv(tmp_path, [camera])

    with pytest.raises(
        ValueError,
        match="Invalid camera ID",
    ):
        load_cameras(path)


def test_duplicate_camera_ids(tmp_path):

    path = write_csv(
        tmp_path,
        [valid_camera(), valid_camera()],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate camera ID",
    ):
        load_cameras(path)

def test_empty_camera_id(tmp_path):

    camera = valid_camera()
    camera["camera_id"] = ""

    path = write_csv(tmp_path, [camera])

    with pytest.raises(
        ValueError,
        match="Missing value for 'camera_id'",
    ):
        load_cameras(path)
# ============================================================
# 5. ZONE VALIDATION
# ============================================================

def test_invalid_zone(tmp_path):

    camera = valid_camera()
    camera["zone_id"] = "Z"

    path = write_csv(tmp_path, [camera])

    with pytest.raises(
        ValueError,
        match="Invalid camera zone",
    ):
        load_cameras(path)


# ============================================================
# 6. DETECTOR VALIDATION
# ============================================================

def test_invalid_detector(tmp_path):

    camera = valid_camera()
    camera["detector_type"] = "vehicle"

    path = write_csv(tmp_path, [camera])

    with pytest.raises(
        ValueError,
        match="Invalid detector type",
    ):
        load_cameras(path)


# ============================================================
# 7. ENABLED VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "value",
    ["yes", "no", "1", "0", "enabled", ""],
)
def test_invalid_enabled_value(tmp_path, value):

    camera = valid_camera()
    camera["enabled"] = value

    path = write_csv(tmp_path, [camera])

    with pytest.raises(
        ValueError,
        match="Missing value|Invalid enabled value",
    ):
        load_cameras(path)


# ============================================================
# 8. KAFKA PARTITION VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "partition",
    ["abc", "1.5", "-1", ""],
)
def test_invalid_partition(tmp_path, partition):

    camera = valid_camera()
    camera["kafka_partition"] = partition

    path = write_csv(tmp_path, [camera])

    with pytest.raises(ValueError):
        load_cameras(path)


# ============================================================
# 9. CAMERA CONFIGURATION IMMUTABILITY
# ============================================================

def test_camera_configuration_is_immutable():

    camera = CameraConfig(
        camera_id="001",
        zone_id="A",
        detector_type="person",
        enabled=True,
        kafka_partition=0,
    )

    with pytest.raises(FrozenInstanceError):
        camera.camera_id = "002"


def test_camera_collection_is_immutable(tmp_path):

    path = write_csv(tmp_path, [valid_camera()])

    cameras = load_cameras(path)

    assert isinstance(cameras, tuple)

    with pytest.raises(AttributeError):
        cameras.append(cameras[0])

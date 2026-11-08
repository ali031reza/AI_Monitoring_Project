
import cv2
import numpy as np
import pytest

from dataclasses import FrozenInstanceError

from advanced.config import CameraConfig, CameraSettings

from advanced.producers.mock_frame import (
    generate_mock_jpeg,
    save_preview,
)


@pytest.fixture
def camera():

    return CameraConfig(
        camera_id="001",
        zone_id="A",
        detector_type="person",
        enabled=True,
        kafka_partition=0,
    )


@pytest.fixture
def camera_settings():

    return CameraSettings(
        frame_width=640,
        frame_height=360,
        jpeg_quality=80,
    )


# ============================================================
# 1. JPEG GENERATION
# ============================================================

def test_generate_mock_jpeg_returns_bytes(
    camera,
    camera_settings,
):

    result = generate_mock_jpeg(
        camera,
        camera_settings,
        frame_number=1,
    )

    assert isinstance(result, bytes)
    assert len(result) > 0


def test_generated_jpeg_can_be_decoded(
    camera,
    camera_settings,
):

    jpeg_bytes = generate_mock_jpeg(
        camera,
        camera_settings,
        frame_number=1,
    )

    image = cv2.imdecode(
        np.frombuffer(jpeg_bytes, dtype=np.uint8),
        cv2.IMREAD_COLOR,
    )

    assert image is not None

    assert image.shape == (
        camera_settings.frame_height,
        camera_settings.frame_width,
        3,
    )


def test_generated_image_uses_configured_dimensions(camera):

    settings = CameraSettings(
        frame_width=1280,
        frame_height=720,
    )

    jpeg_bytes = generate_mock_jpeg(
        camera,
        settings,
        frame_number=1,
    )

    image = cv2.imdecode(
        np.frombuffer(jpeg_bytes, dtype=np.uint8),
        cv2.IMREAD_COLOR,
    )

    assert image.shape == (720, 1280, 3)


def test_different_frame_numbers_generate_different_images(
    camera,
    camera_settings,
):

    first = generate_mock_jpeg(
        camera,
        camera_settings,
        frame_number=1,
    )

    second = generate_mock_jpeg(
        camera,
        camera_settings,
        frame_number=2,
    )

    assert first != second


# ============================================================
# 2. INPUT VALIDATION
# ============================================================

def test_invalid_camera_type(camera_settings):

    with pytest.raises(TypeError):
        generate_mock_jpeg(
            camera="invalid",
            camera_settings=camera_settings,
            frame_number=1,
        )


def test_invalid_camera_settings(camera):

    with pytest.raises(TypeError):
        generate_mock_jpeg(
            camera=camera,
            camera_settings="invalid",
            frame_number=1,
        )


@pytest.mark.parametrize(
    "frame_number",
    [-1, -10],
)
def test_negative_frame_number_is_rejected(
    camera,
    camera_settings,
    frame_number,
):

    with pytest.raises(ValueError):
        generate_mock_jpeg(
            camera,
            camera_settings,
            frame_number,
        )


@pytest.mark.parametrize(
    "frame_number",
    [1.5, "1", True, None],
)
def test_invalid_frame_number_type(
    camera,
    camera_settings,
    frame_number,
):

    with pytest.raises(TypeError):
        generate_mock_jpeg(
            camera,
            camera_settings,
            frame_number,
        )


# ============================================================
# 3. JPEG ENCODING FAILURE
# ============================================================

def test_jpeg_encoding_failure(
    camera,
    camera_settings,
    monkeypatch,
):

    def mock_imencode(*args, **kwargs):
        return False, None

    monkeypatch.setattr(
        "advanced.producers.mock_frame.cv2.imencode",
        mock_imencode,
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to encode JPEG",
    ):
        generate_mock_jpeg(
            camera,
            camera_settings,
            frame_number=1,
        )


# ============================================================
# 4. PREVIEW SAVING
# ============================================================

def test_save_preview(tmp_path):

    output_path = tmp_path / "preview" / "camera_001.jpg"

    result = save_preview(
        b"test-image-data",
        output_path,
    )

    assert result == output_path
    assert output_path.exists()
    assert output_path.read_bytes() == b"test-image-data"


def test_save_preview_creates_parent_directories(tmp_path):

    output_path = (
        tmp_path
        / "nested"
        / "preview"
        / "camera_001.jpg"
    )

    save_preview(
        b"test-image-data",
        output_path,
    )

    assert output_path.exists()


def test_save_preview_rejects_empty_bytes(tmp_path):

    with pytest.raises(ValueError):
        save_preview(
            b"",
            tmp_path / "camera.jpg",
        )


def test_save_preview_rejects_invalid_type(tmp_path):

    with pytest.raises(TypeError):
        save_preview(
            "not-bytes",
            tmp_path / "camera.jpg",
        )

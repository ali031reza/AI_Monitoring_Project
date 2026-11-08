
"""
Mock camera frame generation.

Responsible for:
- Generating synthetic camera frames.
- Adding camera metadata to frames.
- Encoding frames into JPEG bytes.
- Saving optional preview images.

This module does not access global application configuration.
"""

from __future__ import annotations

import cv2
import numpy as np

from pathlib import Path

from advanced.config import (
    CameraConfig,
    CameraSettings,
    load_cameras,
    load_settings,
)


# ============================================================
# CONSTANTS
# ============================================================

BACKGROUND_COLOR = (35, 35, 35)
TEXT_COLOR = (255, 255, 255)

FONT = cv2.FONT_HERSHEY_SIMPLEX


# ============================================================
# FRAME GENERATION
# ============================================================

def generate_mock_jpeg(
    camera: CameraConfig,
    camera_settings: CameraSettings,
    frame_number: int,
) -> bytes:
    """
    Generate a synthetic camera frame and encode it as JPEG.

    Args:
        camera:
            Camera configuration containing camera ID and zone.

        camera_settings:
            Camera dimensions and JPEG compression settings.

        frame_number:
            Sequential frame number.

    Returns:
        JPEG-encoded image bytes.

    Raises:
        TypeError:
            If arguments have incorrect types.

        ValueError:
            If the frame number is negative.

        RuntimeError:
            If JPEG encoding fails.
    """

    if not isinstance(camera, CameraConfig):
        raise TypeError(
            "camera must be a CameraConfig instance"
        )

    if not isinstance(camera_settings, CameraSettings):
        raise TypeError(
            "camera_settings must be a CameraSettings instance"
        )

    if (
        isinstance(frame_number, bool)
        or not isinstance(frame_number, int)
    ):
        raise TypeError(
            "frame_number must be an integer"
        )

    if frame_number < 0:
        raise ValueError(
            "frame_number cannot be negative"
        )

    width = camera_settings.frame_width
    height = camera_settings.frame_height

    # --------------------------------------------------------
    # Create background frame
    # --------------------------------------------------------

    frame = np.full(
        (height, width, 3),
        BACKGROUND_COLOR,
        dtype=np.uint8,
    )

    # --------------------------------------------------------
    # Calculate responsive text dimensions
    # --------------------------------------------------------

    font_scale = max(
        0.35,
        min(1.0, min(width, height) / 360),
    )

    thickness = max(
        1,
        round(font_scale * 2),
    )

    margin_x = min(
        max(1, int(width * 0.05)),
        max(0, width - 1),
    )

    text_positions = (
        int(height * 0.25),
        int(height * 0.50),
        int(height * 0.75),
    )

    labels = (
        f"Camera: {camera.camera_id}",
        f"Zone: {camera.zone_id}",
        f"Frame: {frame_number}",
    )

    # --------------------------------------------------------
    # Render camera information
    # --------------------------------------------------------

    for label, position_y in zip(labels, text_positions):

        cv2.putText(
            frame,
            label,
            (margin_x, position_y),
            FONT,
            font_scale,
            TEXT_COLOR,
            thickness,
            cv2.LINE_AA,
        )

    # --------------------------------------------------------
    # Encode JPEG
    # --------------------------------------------------------

    success, buffer = cv2.imencode(
        ".jpg",
        frame,
        [
            cv2.IMWRITE_JPEG_QUALITY,
            camera_settings.jpeg_quality,
        ],
    )

    if not success:
        raise RuntimeError(
            f"Failed to encode JPEG for camera {camera.camera_id}"
        )

    return buffer.tobytes()


# ============================================================
# PREVIEW SAVING
# ============================================================

def save_preview(
    jpeg_bytes: bytes,
    output_path: str | Path,
) -> Path:
    """
    Save generated JPEG bytes as a preview image.

    Directory creation is performed only when required.
    """

    if not isinstance(jpeg_bytes, bytes):
        raise TypeError(
            "jpeg_bytes must be bytes"
        )

    if not jpeg_bytes:
        raise ValueError(
            "Cannot save an empty JPEG image"
        )

    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_bytes(jpeg_bytes)

    return output_path


# ============================================================
# DEVELOPMENT ENTRY POINT
# ============================================================

def main() -> None:
    """
    Generate a preview using the first enabled camera.

    This is a standalone development utility.
    The main application should load settings once at startup.
    """

    settings = load_settings()

    cameras = load_cameras(
        settings.paths.cameras_csv
    )

    camera = next(
        (
            camera
            for camera in cameras
            if camera.enabled
        ),
        None,
    )

    if camera is None:
        raise RuntimeError(
            "No enabled cameras found in camera configuration"
        )

    jpeg_bytes = generate_mock_jpeg(
        camera=camera,
        camera_settings=settings.camera,
        frame_number=1,
    )

    output_path = (
        settings.paths.preview_dir
        / f"camera_{camera.camera_id}_preview.jpg"
    )

    saved_path = save_preview(
        jpeg_bytes,
        output_path,
    )

    print(f"Preview saved: {saved_path}")

    print(
        f"JPEG size: {len(jpeg_bytes) / 1024:.1f} KB"
    )


if __name__ == "__main__":
    main()


import cv2
import numpy as np

from advanced.config.settings import (
    FRAME_WIDTH,
    FRAME_HEIGHT,
    JPEG_QUALITY,
    PREVIEW_DIR,
)


def generate_mock_jpeg(camera_id, zone_id, frame_number):
    # Create a dark image
    frame = np.full(
        (FRAME_HEIGHT, FRAME_WIDTH, 3),
        35,
        dtype=np.uint8,
    )

    # Add camera information to the image
    cv2.putText(
        frame,
        f"Camera: {camera_id}",
        (30, 80),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (255, 255, 255),
        2,
    )

    cv2.putText(
        frame,
        f"Zone: {zone_id}",
        (30, 140),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (255, 255, 255),
        2,
    )

    cv2.putText(
        frame,
        f"Frame: {frame_number}",
        (30, 200),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (255, 255, 255),
        2,
    )

    # Encode the image into JPEG bytes
    success, buffer = cv2.imencode(
        ".jpg",
        frame,
        [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY],
    )

    if not success:
        raise RuntimeError("Failed to encode mock camera frame")

    return buffer.tobytes()


if __name__ == "__main__":
    from advanced.config.camera_loader import load_cameras

    cameras = load_cameras()
    camera = next(c for c in cameras if c["enabled"])

    jpeg_bytes = generate_mock_jpeg(
        camera["camera_id"],
        camera["zone_id"],
        frame_number=1,
    )

    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)

    output_path = (
        PREVIEW_DIR / f"camera_{camera['camera_id']}_preview.jpg"
    )

    output_path.write_bytes(jpeg_bytes)

    print(f"Preview saved: {output_path}")
    print(f"JPEG size: {len(jpeg_bytes) / 1024:.1f} KB")

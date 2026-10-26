
from pathlib import Path

import cv2
import numpy as np


def encode_frame(frame, quality=80):
    """Compress an OpenCV frame into JPEG bytes."""
    success, buffer = cv2.imencode(
        ".jpg",
        frame,
        [cv2.IMWRITE_JPEG_QUALITY, quality],
    )

    if not success:
        raise RuntimeError("Failed to encode frame")

    return buffer.tobytes()


def decode_frame(jpeg_bytes):
    """Convert JPEG bytes back into an OpenCV frame."""
    buffer = np.frombuffer(jpeg_bytes, dtype=np.uint8)

    frame = cv2.imdecode(buffer, cv2.IMREAD_COLOR)

    if frame is None:
        raise RuntimeError("Failed to decode frame")

    return frame


if __name__ == "__main__":
    image_path = Path("captured_frames/test_frame.jpg")

    frame = cv2.imread(str(image_path))

    if frame is None:
        raise RuntimeError(f"Cannot read image: {image_path}")

    jpeg_bytes = encode_frame(frame)
    restored_frame = decode_frame(jpeg_bytes)

    height, width = restored_frame.shape[:2]

    print(f"Original frame: {frame.shape}")
    print(f"Encoded size: {len(jpeg_bytes):,} bytes")
    print(f"Encoded size: {len(jpeg_bytes) / 1024:.1f} KB")
    print(f"Decoded frame: {width} x {height}")
    print("Encoding and decoding successful!")

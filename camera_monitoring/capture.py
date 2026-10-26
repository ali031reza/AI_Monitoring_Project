
import sys
from pathlib import Path

import cv2


def capture_one_frame(source=0):
    # Open the webcam or video file.
    camera = cv2.VideoCapture(source)

    if not camera.isOpened():
        raise RuntimeError(f"Could not open video source: {source}")

    try:
        # Read a single frame.
        success, frame = camera.read()

        if not success:
            raise RuntimeError("Could not read a frame.")

    finally:
        # Release the camera or file handle.
        camera.release()

    # Create a directory for captured images.
    output_dir = Path("captured_frames")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / "test_frame.jpg"

    # Save the frame as a JPEG image.
    if not cv2.imwrite(str(output_path), frame):
        raise RuntimeError("Could not save the image.")

    height, width = frame.shape[:2]

    print(f"Frame captured: {width} x {height}")
    print(f"Saved to: {output_path.resolve()}")


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else 0
    capture_one_frame(source)

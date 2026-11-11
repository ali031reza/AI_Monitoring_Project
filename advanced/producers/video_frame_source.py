
"""
Video frame source for mock cameras.

Responsibilities:
- Open a recorded video file.
- Maintain independent playback state for each camera.
- Sample video frames according to the configured camera FPS.
- Loop playback indefinitely when the video reaches EOF.
- Resize frames to the configured camera resolution.
- Encode frames into JPEG bytes.
- Release video resources when a camera is stopped.

Each camera should have its own VideoFrameSource instance.
"""

from __future__ import annotations

import logging
import math

from pathlib import Path

import cv2

from advanced.config import ApplicationSettings


# ============================================================
# 1. LOGGING AND CONSTANTS
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# 2. VIDEO FRAME SOURCE
# ============================================================

class VideoFrameSource:
    """
    Read and encode frames from a continuously looping video.

    Dependencies:
        settings:
            Validated application configuration.

        camera_id:
            Unique camera identifier.

        video_path:
            Optional video path override.
            If omitted, the centralized project video path
            is used.
    """

    def __init__(
        self,
        camera_id: str,
        settings: ApplicationSettings,
        *,
        video_path: str | Path | None = None,
    ) -> None:

        # ----------------------------------------------------
        # Validate camera ID
        # ----------------------------------------------------

        if (
            not isinstance(camera_id, str)
            or not camera_id.strip()
        ):
            raise ValueError(
                "camera_id must be a non-empty string"
            )

        # ----------------------------------------------------
        # Validate application settings
        # ----------------------------------------------------

        if not isinstance(settings, ApplicationSettings):
            raise TypeError(
                "settings must be an ApplicationSettings instance"
            )

        self.camera_id = camera_id

        self.settings = settings

        self.camera_settings = settings.camera

        # ----------------------------------------------------
        # Resolve video path
        # ----------------------------------------------------

        if video_path is None:

            self.video_path = (
                settings.paths.camera_sample_video
            )

        else:

            self.video_path = Path(video_path)

        if not self.video_path.is_file():

            raise FileNotFoundError(
                f"Video file not found: {self.video_path}"
            )

        # ----------------------------------------------------
        # Initialize video capture
        # ----------------------------------------------------

        self._capture = cv2.VideoCapture(
            str(self.video_path)
        )

        if not self._capture.isOpened():

            self._capture.release()

            raise RuntimeError(
                f"Cannot open video for camera "
                f"{self.camera_id}: {self.video_path}"
            )

        self._released = False

        # ----------------------------------------------------
        # Read video metadata
        # ----------------------------------------------------

        self._source_fps = float(
            self._capture.get(
                cv2.CAP_PROP_FPS
            )
        )

        self._reported_frame_count = int(
            self._capture.get(
                cv2.CAP_PROP_FRAME_COUNT
            )
        )

        # ----------------------------------------------------
        # Calculate configured output FPS
        # ----------------------------------------------------

        self._output_fps = (
            1.0
            / self.camera_settings.frame_interval_seconds
        )

        self._use_timestamp_sampling = (
            math.isfinite(self._source_fps)
            and self._source_fps > 0
        )

        # ----------------------------------------------------
        # Initialize playback state
        # ----------------------------------------------------

        self._output_frame_number = 0

        self._next_source_frame_index = 0

        self._last_source_frame_index = None

        self._last_frame = None

        # Actual frame count is discovered when EOF is reached.
        self._frame_count = None

        logger.info(
            "Video source initialized | Camera: %s | "
            "Video: %s | Source FPS: %.2f | "
            "Output FPS: %.2f | Frames: %d",
            self.camera_id,
            self.video_path,
            self._source_fps,
            self._output_fps,
            self._reported_frame_count,
        )

        if not self._use_timestamp_sampling:

            logger.warning(
                "Video FPS unavailable for camera %s. "
                "Sequential frame reading will be used.",
                self.camera_id,
            )

    # ========================================================
    # 3. VIDEO PROPERTIES
    # ========================================================

    @property
    def is_open(self) -> bool:
        """Return whether the video source is open."""

        return (
            not self._released
            and self._capture.isOpened()
        )

    @property
    def source_fps(self) -> float:
        """Return the original video FPS."""

        return self._source_fps

    @property
    def output_fps(self) -> float:
        """Return the configured camera output FPS."""

        return self._output_fps

    @property
    def frames_read(self) -> int:
        """Return the number of output frames produced."""

        return self._output_frame_number

    # ========================================================
    # 4. REWIND VIDEO
    # ========================================================

    def _rewind(self) -> None:
        """Return the video capture to its first frame."""

        if self._released:

            raise RuntimeError(
                f"Video source for camera "
                f"{self.camera_id} has been released"
            )

        seek_success = self._capture.set(
            cv2.CAP_PROP_POS_FRAMES,
            0,
        )

        if not seek_success:

            self._capture.release()

            self._capture = cv2.VideoCapture(
                str(self.video_path)
            )

            if not self._capture.isOpened():

                self._capture.release()

                self._released = True

                raise RuntimeError(
                    f"Cannot reopen video for camera "
                    f"{self.camera_id}"
                )

        self._next_source_frame_index = 0

        self._last_source_frame_index = None

        self._last_frame = None

        logger.debug(
            "Video restarted | Camera: %s",
            self.camera_id,
        )

    # ========================================================
    # 5. READ FRAME AT SOURCE INDEX
    # ========================================================

    def _read_frame_at_index(self, target_index: int):
        """
        Read a specific source frame.

        Intermediate frames are discarded when the configured
        output FPS is lower than the original video FPS.

        The actual video length is detected at EOF, allowing
        playback to loop even if metadata is inaccurate.
        """

        while True:

            if self._frame_count is not None:

                target_index %= self._frame_count

            # Return cached frame when the requested source
            # position has not changed.
            if (
                self._last_source_frame_index == target_index
                and self._last_frame is not None
            ):
                return self._last_frame

            # Restart when the target is behind the current
            # playback position.
            if target_index < self._next_source_frame_index:

                self._rewind()

            while (
                self._next_source_frame_index <= target_index
            ):

                success, frame = self._capture.read()

                if not success:

                    actual_frame_count = (
                        self._next_source_frame_index
                    )

                    if actual_frame_count <= 0:

                        raise RuntimeError(
                            f"Video contains no readable frames: "
                            f"{self.video_path}"
                        )

                    # Store the actual video length.
                    self._frame_count = actual_frame_count

                    # Map the target frame to the beginning
                    # of the video when EOF is reached.
                    target_index %= actual_frame_count

                    self._rewind()

                    break

                current_index = (
                    self._next_source_frame_index
                )

                self._next_source_frame_index += 1

                if current_index == target_index:

                    self._last_source_frame_index = (
                        current_index
                    )

                    self._last_frame = frame

                    return frame

    # ========================================================
    # 6. READ NEXT VIDEO FRAME
    # ========================================================

    def _read_next_frame(self):
        """
        Read the next frame according to the configured FPS.

        Timestamp-based sampling prevents the recorded video
        from playing slower or faster than its original speed.
        """

        if self._use_timestamp_sampling:

            target_index = int(
                self._output_frame_number
                * self._source_fps
                / self._output_fps
            )

            return self._read_frame_at_index(
                target_index
            )

        # Fallback when the source FPS is unavailable.
        success, frame = self._capture.read()

        if not success:

            self._rewind()

            success, frame = self._capture.read()

        if not success:

            raise RuntimeError(
                f"Unable to read video frame for camera "
                f"{self.camera_id}"
            )

        return frame

    # ========================================================
    # 7. RESIZE FRAME
    # ========================================================

    def _resize_frame(self, frame):
        """Resize the source frame to configured dimensions."""

        width = self.camera_settings.frame_width

        height = self.camera_settings.frame_height

        current_height, current_width = frame.shape[:2]

        if (
            current_width == width
            and current_height == height
        ):
            return frame

        return cv2.resize(
            frame,
            (width, height),
            interpolation=cv2.INTER_AREA,
        )

    # ========================================================
    # 8. ENCODE JPEG
    # ========================================================

    def _encode_jpeg(self, frame) -> bytes:
        """Encode a frame into JPEG bytes."""

        success, buffer = cv2.imencode(
            ".jpg",
            frame,
            [
                cv2.IMWRITE_JPEG_QUALITY,
                self.camera_settings.jpeg_quality,
            ],
        )

        if not success:

            raise RuntimeError(
                f"JPEG encoding failed for camera "
                f"{self.camera_id}"
            )

        return buffer.tobytes()

    # ========================================================
    # 9. GET NEXT JPEG FRAME
    # ========================================================

    def read_jpeg(self) -> bytes:
        """
        Return the next JPEG-encoded camera frame.

        Video playback automatically restarts at EOF.

        Returns:
            JPEG-encoded image bytes.
        """

        if not self.is_open:

            raise RuntimeError(
                f"Video source for camera "
                f"{self.camera_id} is not open"
            )

        # Read the appropriate source frame.
        frame = self._read_next_frame()

        # Apply configured camera resolution.
        frame = self._resize_frame(frame)

        # Encode into JPEG.
        jpeg_bytes = self._encode_jpeg(frame)

        self._output_frame_number += 1

        return jpeg_bytes

    # ========================================================
    # 10. RELEASE VIDEO RESOURCE
    # ========================================================

    def release(self) -> None:
        """Release the video capture resource."""

        if self._released:
            return

        self._capture.release()

        self._released = True

        self._last_frame = None

        logger.info(
            "Video source released | Camera: %s",
            self.camera_id,
        )

    # ========================================================
    # 11. CONTEXT MANAGER SUPPORT
    # ========================================================

    def __enter__(self):

        if not self.is_open:

            raise RuntimeError(
                f"Video source for camera "
                f"{self.camera_id} is not open"
            )

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:

        self.release()

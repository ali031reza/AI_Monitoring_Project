
"""
Camera configuration loader.

Responsible for:
- Reading camera configuration from CSV.
- Validating the CSV schema and camera records.
- Returning immutable, typed camera configurations.

This module does not manage application settings or decide
where the camera configuration file is located.
"""

from __future__ import annotations

import csv

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


# ============================================================
# 1. CAMERA CONFIGURATION MODEL
# ============================================================

DetectorType = Literal["person", "plate"]

VALID_ZONES = frozenset({"A", "B", "C", "D", "E"})

VALID_DETECTORS = frozenset({"person", "plate"})

REQUIRED_COLUMNS = frozenset({
    "camera_id",
    "zone_id",
    "detector_type",
    "enabled",
    "kafka_partition",
})


@dataclass(frozen=True, slots=True)
class CameraConfig:
    """
    Immutable configuration for a single camera.
    """

    camera_id: str
    zone_id: str
    detector_type: DetectorType
    enabled: bool
    kafka_partition: int

    def __post_init__(self) -> None:

        # Camera ID
        if not isinstance(self.camera_id, str):
            raise ValueError("Camera ID must be a string")

        camera_id = self.camera_id.strip()

        if not (
            len(camera_id) == 3
            and camera_id.isascii()
            and camera_id.isdecimal()
        ):
            raise ValueError(
                f"Invalid camera ID: {camera_id!r}"
            )

        object.__setattr__(self, "camera_id", camera_id)

        # Zone
        if not isinstance(self.zone_id, str):
            raise ValueError("Zone ID must be a string")

        zone_id = self.zone_id.strip().upper()

        if zone_id not in VALID_ZONES:
            raise ValueError(
                f"Invalid camera zone: {zone_id!r}"
            )

        object.__setattr__(self, "zone_id", zone_id)

        # Detector
        if not isinstance(self.detector_type, str):
            raise ValueError("Detector type must be a string")

        detector_type = self.detector_type.strip().lower()

        if detector_type not in VALID_DETECTORS:
            raise ValueError(
                f"Invalid detector type: {detector_type!r}"
            )

        object.__setattr__(
            self,
            "detector_type",
            detector_type,
        )

        # Enabled status
        if not isinstance(self.enabled, bool):
            raise ValueError(
                "Camera enabled status must be a boolean"
            )

        # Kafka partition
        if (
            isinstance(self.kafka_partition, bool)
            or not isinstance(self.kafka_partition, int)
        ):
            raise ValueError(
                "Kafka partition must be an integer"
            )

        if self.kafka_partition < 0:
            raise ValueError(
                "Kafka partition cannot be negative"
            )


# ============================================================
# 2. CSV FIELD VALIDATION
# ============================================================

def _get_required_value(
    row: dict,
    column: str,
    line_number: int,
) -> str:
    """Retrieve a required, non-empty CSV field."""

    value = row.get(column)

    if value is None or not value.strip():
        raise ValueError(
            f"Missing value for '{column}' "
            f"at CSV line {line_number}"
        )

    return value.strip()


def _parse_camera(
    row: dict,
    line_number: int,
) -> CameraConfig:
    """Parse and validate one CSV record."""

    if None in row:
        raise ValueError(
            f"Unexpected extra values at CSV line {line_number}"
        )

    camera_id = _get_required_value(
        row,
        "camera_id",
        line_number,
    )

    zone_id = _get_required_value(
        row,
        "zone_id",
        line_number,
    )

    detector_type = _get_required_value(
        row,
        "detector_type",
        line_number,
    )

    enabled_text = _get_required_value(
        row,
        "enabled",
        line_number,
    ).lower()

    partition_text = _get_required_value(
        row,
        "kafka_partition",
        line_number,
    )

    # Enabled status
    if enabled_text not in {"true", "false"}:
        raise ValueError(
            f"Invalid enabled value at CSV line {line_number}: "
            f"{enabled_text!r}"
        )

    enabled = enabled_text == "true"

    # Kafka partition
    try:
        partition = int(partition_text)

    except ValueError as exc:
        raise ValueError(
            f"Invalid Kafka partition at CSV line {line_number}: "
            f"{partition_text!r}"
        ) from exc

    # Construct validated camera object.
    try:
        return CameraConfig(
            camera_id=camera_id,
            zone_id=zone_id,
            detector_type=detector_type,
            enabled=enabled,
            kafka_partition=partition,
        )

    except ValueError as exc:
        raise ValueError(
            f"Invalid camera configuration at CSV line "
            f"{line_number}: {exc}"
        ) from exc


# ============================================================
# 3. CSV HEADER VALIDATION
# ============================================================

def _validate_headers(
    headers: list[str] | None,
    file_path: Path,
) -> list[str]:
    """Validate and normalize CSV column names."""

    if not headers:
        raise ValueError(
            f"Camera configuration file is empty: {file_path}"
        )

    normalized_headers = []

    for header in headers:

        if header is None or not header.strip():
            raise ValueError(
                f"Empty column name in camera CSV: {file_path}"
            )

        normalized_headers.append(header.strip())

    # Detect duplicate columns.
    seen = set()
    duplicates = set()

    for header in normalized_headers:

        if header in seen:
            duplicates.add(header)

        seen.add(header)

    if duplicates:
        raise ValueError(
            "Duplicate CSV columns: "
            + ", ".join(sorted(duplicates))
        )

    missing_columns = REQUIRED_COLUMNS - set(normalized_headers)

    if missing_columns:
        raise ValueError(
            "Missing required columns in cameras CSV: "
            + ", ".join(sorted(missing_columns))
        )

    return normalized_headers


# ============================================================
# 4. CAMERA CONFIGURATION LOADER
# ============================================================

def load_cameras(
    cameras_csv: str | Path,
) -> tuple[CameraConfig, ...]:
    """
    Load and validate camera configurations.

    Args:
        cameras_csv:
            Explicit path to the camera configuration CSV.

    Returns:
        An immutable tuple of validated CameraConfig objects.

    Raises:
        FileNotFoundError:
            If the CSV file does not exist.

        IsADirectoryError:
            If the supplied path points to a directory.

        ValueError:
            If the CSV schema or camera records are invalid.

    Notes:
        The caller is responsible for providing the file path.
        This function does not access global application settings.
    """

    csv_path = Path(cameras_csv).expanduser()

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Camera configuration file not found: {csv_path}"
        )

    if not csv_path.is_file():
        raise IsADirectoryError(
            f"Camera configuration path is not a file: {csv_path}"
        )

    cameras: list[CameraConfig] = []
    seen_ids: set[str] = set()

    try:

        with csv_path.open(
            mode="r",
            newline="",
            encoding="utf-8-sig",
        ) as file:

            reader = csv.DictReader(
                file,
                strict=True,
            )

            headers = _validate_headers(
                reader.fieldnames,
                csv_path,
            )

            # Normalize column names, including surrounding whitespace.
            reader.fieldnames = headers

            for row in reader:

                line_number = reader.line_num

                # Ignore completely empty records.
                if all(
                    value is None or not value.strip()
                    for key, value in row.items()
                    if key is not None
                ):
                    continue

                camera = _parse_camera(
                    row,
                    line_number,
                )

                # Check uniqueness.
                if camera.camera_id in seen_ids:
                    raise ValueError(
                        f"Duplicate camera ID "
                        f"{camera.camera_id!r} "
                        f"at CSV line {line_number}"
                    )

                cameras.append(camera)
                seen_ids.add(camera.camera_id)

    except csv.Error as exc:
        raise ValueError(
            f"Malformed camera CSV file '{csv_path}': {exc}"
        ) from exc

    if not cameras:
        raise ValueError(
            f"No camera records found in: {csv_path}"
        )

    return tuple(cameras)

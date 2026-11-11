"""Shared configuration for an end-to-end Kafka scaling run."""

from __future__ import annotations

import json
import os
import re

from pathlib import Path
from typing import Any

from advanced.config import CameraConfig, load_settings


RUN_ID_PATTERN = re.compile(r"^run_[A-Za-z0-9][A-Za-z0-9_-]*$")
WORKER_ID_PATTERN = re.compile(r"^worker[1-9][0-9]*$")


def build_scaled_cameras(
    base_cameras: tuple[CameraConfig, ...],
    *,
    camera_count: int,
    person_partitions: int,
    plate_partitions: int,
) -> tuple[CameraConfig, ...]:
    """Scale the configured camera mix while preserving its detector ratio.

    Camera zones and detector types are repeated from the checked-in camera
    inventory. Camera IDs are generated deterministically from 001 upward.
    Each detector type is assigned round-robin to its Kafka partitions.
    """

    for name, value in (
        ("camera_count", camera_count),
        ("person_partitions", person_partitions),
        ("plate_partitions", plate_partitions),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")

    if not 2 <= camera_count <= 40:
        raise ValueError("camera_count must be between 2 and 40")

    enabled = tuple(camera for camera in base_cameras if camera.enabled)
    person_templates = tuple(
        sorted(
            (camera for camera in enabled if camera.detector_type == "person"),
            key=lambda camera: camera.camera_id,
        )
    )
    plate_templates = tuple(
        sorted(
            (camera for camera in enabled if camera.detector_type == "plate"),
            key=lambda camera: camera.camera_id,
        )
    )

    if not person_templates or not plate_templates:
        raise ValueError(
            "The base camera inventory must contain enabled person and plate cameras"
        )

    person_ratio = len(person_templates) / len(enabled)
    person_count = int(camera_count * person_ratio + 0.5)
    person_count = max(1, min(camera_count - 1, person_count))

    templates = {
        "person": person_templates,
        "plate": plate_templates,
    }
    partition_counts = {
        "person": person_partitions,
        "plate": plate_partitions,
    }
    detector_indexes = {"person": 0, "plate": 0}
    cameras: list[CameraConfig] = []

    for index in range(camera_count):
        detector_type = "person" if index < person_count else "plate"
        detector_index = detector_indexes[detector_type]
        template = templates[detector_type][
            detector_index % len(templates[detector_type])
        ]
        partition = detector_index % partition_counts[detector_type]

        cameras.append(
            CameraConfig(
                camera_id=f"{index + 1:03d}",
                zone_id=template.zone_id,
                detector_type=detector_type,
                enabled=True,
                kafka_partition=partition,
            )
        )
        detector_indexes[detector_type] += 1

    return tuple(cameras)


def create_pipeline_config(
    *,
    run_id: str,
    cameras: tuple[CameraConfig, ...],
    fps_per_camera: float,
    person_workers: int,
    plate_workers: int,
    person_partitions: int,
    plate_partitions: int,
    person_delay_ms: float = 30.0,
    plate_delay_ms: float = 30.0,
) -> dict[str, Any]:
    """Build the immutable run configuration shared by child processes."""

    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"Invalid run ID: {run_id!r}")

    if (
        isinstance(fps_per_camera, bool)
        or not isinstance(fps_per_camera, (int, float))
        or fps_per_camera <= 0
    ):
        raise ValueError("fps_per_camera must be a positive number")

    for name, value in (
        ("person_workers", person_workers),
        ("plate_workers", plate_workers),
        ("person_partitions", person_partitions),
        ("plate_partitions", plate_partitions),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")

    if person_partitions < person_workers:
        raise ValueError("Person partitions must be at least the person worker count")

    if plate_partitions < plate_workers:
        raise ValueError("Plate partitions must be at least the plate worker count")

    return {
        "schema_version": 1,
        "run_id": run_id,
        "camera_count": len(cameras),
        "fps_per_camera": float(fps_per_camera),
        "person_workers": person_workers,
        "plate_workers": plate_workers,
        "person_partitions": person_partitions,
        "plate_partitions": plate_partitions,
        "processing_delay_ms": {
            "person": float(person_delay_ms),
            "plate": float(plate_delay_ms),
        },
        "cameras": [
            {
                "camera_id": camera.camera_id,
                "zone_id": camera.zone_id,
                "detector_type": camera.detector_type,
                "enabled": camera.enabled,
                "kafka_partition": camera.kafka_partition,
            }
            for camera in cameras
        ],
    }


def save_pipeline_config(
    config: dict[str, Any],
    output_path: str | Path,
) -> Path:
    """Write a run configuration atomically."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(path.name + ".tmp")
    temporary_path.write_text(
        json.dumps(config, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    temporary_path.replace(path)
    return path


def load_pipeline_config(config_path: str | Path) -> dict[str, Any]:
    """Load and validate a configuration created by the scaling dashboard."""

    path = Path(config_path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"Pipeline configuration not found: {path}")

    config = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(config, dict) or config.get("schema_version") != 1:
        raise ValueError("Unsupported pipeline configuration")

    if not isinstance(config.get("run_id"), str) or not RUN_ID_PATTERN.fullmatch(
        config["run_id"]
    ):
        raise ValueError("Pipeline configuration contains an invalid run ID")

    camera_records = config.get("cameras")
    if not isinstance(camera_records, list) or not camera_records:
        raise ValueError("Pipeline configuration must contain cameras")

    cameras = tuple(CameraConfig(**record) for record in camera_records)
    if len(cameras) != config.get("camera_count"):
        raise ValueError("Configured camera count does not match camera records")

    fps = config.get("fps_per_camera")
    if (
        isinstance(fps, bool)
        or not isinstance(fps, (int, float))
        or fps <= 0
    ):
        raise ValueError("Pipeline configuration contains an invalid FPS")

    return config


def cameras_from_config(config: dict[str, Any]) -> tuple[CameraConfig, ...]:
    """Convert serialized camera records into validated camera objects."""

    return tuple(
        CameraConfig(**record)
        for record in config["cameras"]
    )


def load_run_settings(
    config: dict[str, Any],
    *,
    person_worker_id: str | None = None,
):
    """Load application settings using the run's FPS and worker identity."""

    environment = dict(os.environ)
    environment["FRAME_INTERVAL_SECONDS"] = str(
        1.0 / float(config["fps_per_camera"])
    )

    if person_worker_id is not None:
        if not WORKER_ID_PATTERN.fullmatch(person_worker_id):
            raise ValueError(f"Invalid person worker ID: {person_worker_id!r}")
        environment["PERSON_DETECTOR_WORKER_ID"] = person_worker_id

    return load_settings(environ=environment)

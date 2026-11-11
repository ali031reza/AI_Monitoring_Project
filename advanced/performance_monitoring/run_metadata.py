
"""
Experiment run metadata management.

Responsibilities:
- Capture application configuration.
- Record enabled camera configurations.
- Record Kafka topics and partition assignments.
- Record detector worker counts and processing delays.
- Record the current Git revision.
- Save metadata as a JSON file for each experiment.

Metadata describes the experimental configuration.
It does not collect runtime performance measurements.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import re
import subprocess
import sys

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from advanced.config import (
    ApplicationSettings,
    CameraConfig,
    load_cameras,
    load_settings,
)


# ============================================================
# 1. LOGGING AND CONSTANTS
# ============================================================

DETECTOR_TYPES = ("person", "plate")

YOLO_REFERENCE = {
    "model": "YOLO11n",
    "parameters_millions": 2.6,
    "flops_billions": 6.5,
    "input_resolution": {
        "width": 640,
        "height": 640,
    },
    "batch_size": 1,
    "cpu_benchmark": {
        "device": "CPU",
        "backend": "ONNX",
        "latency_ms_per_image": 56.1,
    },
    "gpu_benchmark": {
        "device": "NVIDIA T4",
        "backend": "TensorRT 10",
        "latency_ms_per_image": 1.5,
    },
    "weight_memory_estimate_mb": {
        "fp32_per_instance": 10.4,
        "fp16_per_instance": 5.2,
    },
    "source_url": "https://docs.ultralytics.com/models/yolo11/",
    "note": (
        "Published model-only benchmark and weight-size reference. "
        "YOLO inference is not executed by this project."
    ),
}


# ============================================================
# 2. VALIDATION HELPERS
# ============================================================

def _validate_worker_configuration(
    worker_counts: Mapping[str, int],
    processing_delays_seconds: Mapping[str, float],
) -> None:
    """Validate worker counts and artificial processing delays."""

    if set(worker_counts) != set(DETECTOR_TYPES):

        raise ValueError(
            "Worker counts must define person and plate detectors"
        )

    if set(processing_delays_seconds) != set(DETECTOR_TYPES):

        raise ValueError(
            "Processing delays must define person and plate detectors"
        )

    for detector_type in DETECTOR_TYPES:

        count = worker_counts[detector_type]

        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or not 1 <= count <= 20
        ):
            raise ValueError(
                f"Worker count for {detector_type} must be between 1 and 20"
            )

        delay = processing_delays_seconds[detector_type]

        if (
            isinstance(delay, bool)
            or not isinstance(delay, (int, float))
            or not math.isfinite(delay)
            or delay < 0
        ):
            raise ValueError(
                f"Invalid processing delay for {detector_type}"
            )


def _validate_cameras(
    cameras: Iterable[CameraConfig],
) -> tuple[CameraConfig, ...]:
    """Validate and normalize camera configurations."""

    if isinstance(cameras, (str, bytes)):
        raise TypeError(
            "cameras must be an iterable of CameraConfig objects"
        )

    camera_list = tuple(cameras)

    if not camera_list:
        raise ValueError(
            "No camera configurations supplied"
        )

    if not all(
        isinstance(camera, CameraConfig)
        for camera in camera_list
    ):
        raise TypeError(
            "Every camera must be a CameraConfig instance"
        )

    camera_ids = [
        camera.camera_id
        for camera in camera_list
    ]

    if len(camera_ids) != len(set(camera_ids)):
        raise ValueError(
            "Duplicate camera IDs detected"
        )

    return camera_list


# ============================================================
# 3. GIT METADATA
# ============================================================

def _git_command(
    project_root: Path,
    *arguments: str,
) -> str | None:
    """Execute a Git command and return its output."""

    try:

        result = subprocess.run(
            ["git", *arguments],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )

    except (OSError, subprocess.SubprocessError):

        return None

    if result.returncode != 0:
        return None

    return result.stdout.strip()


def _collect_git_metadata(
    project_root: Path,
) -> dict[str, Any]:
    """Collect the current Git revision and working-tree status."""

    commit = _git_command(
        project_root,
        "rev-parse",
        "HEAD",
    )

    branch = _git_command(
        project_root,
        "branch",
        "--show-current",
    )

    status = _git_command(
        project_root,
        "status",
        "--porcelain",
    )

    return {
        "commit": commit,
        "branch": branch or None,
        "working_tree_clean": (
            None if status is None else not bool(status)
        ),
    }


# ============================================================
# 4. CAMERA METADATA
# ============================================================

def _collect_camera_metadata(
    settings: ApplicationSettings,
    cameras: tuple[CameraConfig, ...],
) -> dict[str, Any]:
    """Collect camera configuration and expected input rates."""

    enabled_cameras = tuple(
        camera
        for camera in cameras
        if camera.enabled
    )

    disabled_cameras = tuple(
        camera
        for camera in cameras
        if not camera.enabled
    )

    frame_interval = (
        settings.camera.frame_interval_seconds
    )

    target_fps = 1.0 / frame_interval

    counts_by_detector = {
        detector: sum(
            camera.detector_type == detector
            for camera in enabled_cameras
        )
        for detector in DETECTOR_TYPES
    }

    partitions_by_detector = {}

    for detector in DETECTOR_TYPES:

        assignments = {}

        for camera in enabled_cameras:

            if camera.detector_type != detector:
                continue

            partition = str(camera.kafka_partition)

            assignments.setdefault(
                partition,
                [],
            ).append(camera.camera_id)

        partitions_by_detector[detector] = {
            partition: sorted(camera_ids)
            for partition, camera_ids in sorted(
                assignments.items()
            )
        }

    return {
        "configured_camera_count": len(cameras),

        "enabled_camera_count": len(enabled_cameras),

        "disabled_camera_count": len(disabled_cameras),

        "enabled_camera_ids": sorted(
            camera.camera_id
            for camera in enabled_cameras
        ),

        "disabled_camera_ids": sorted(
            camera.camera_id
            for camera in disabled_cameras
        ),

        "camera_counts_by_detector": counts_by_detector,

        "sampling": {
            "frame_interval_seconds": frame_interval,
            "target_fps_per_camera": target_fps,
            "nominal_input_fps_by_detector": {
                detector: (
                    counts_by_detector[detector] * target_fps
                )
                for detector in DETECTOR_TYPES
            },
        },

        "resolution": {
            "width": settings.camera.frame_width,
            "height": settings.camera.frame_height,
        },

        "jpeg_quality": settings.camera.jpeg_quality,

        "camera_partition_assignments": (
            partitions_by_detector
        ),
    }


# ============================================================
# 5. VIDEO SOURCE METADATA
# ============================================================

def _collect_video_metadata(
    settings: ApplicationSettings,
) -> dict[str, Any]:
    """Collect information about the configured video source."""

    video_path = settings.paths.camera_sample_video

    exists = video_path.is_file()

    file_size = (
        video_path.stat().st_size
        if exists
        else None
    )

    return {
        "source_type": "recorded_video",
        "video_path": str(video_path),
        "exists": exists,
        "file_size_bytes": file_size,
    }


# ============================================================
# 6. KAFKA METADATA
# ============================================================

def _collect_kafka_metadata(
    settings: ApplicationSettings,
) -> dict[str, Any]:
    """Collect Kafka topics and consumer group configuration."""

    return {
        "bootstrap_servers": (
            settings.kafka.bootstrap_servers
        ),

        "topics": {
            "person": settings.kafka.person_frames_topic,
            "plate": settings.kafka.plate_frames_topic,
        },

        "consumer_groups": {
            "person": (
                settings.kafka.person_detector_group_id
            ),
            "plate": (
                settings.kafka.plate_detector_group_id
            ),
        },
    }


# ============================================================
# 7. WORKER METADATA
# ============================================================

def _collect_worker_metadata(
    worker_counts: Mapping[str, int],
    processing_delays_seconds: Mapping[str, float],
) -> dict[str, Any]:
    """Collect declared detector worker configuration."""

    workers = {}

    for detector in DETECTOR_TYPES:

        workers[detector] = {
            "worker_count": worker_counts[detector],

            "processing_delay_seconds": (
                processing_delays_seconds[detector]
            ),

            "processing_delay_ms": (
                processing_delays_seconds[detector] * 1000
            ),
        }

    return {
        "workers": workers,

        "total_worker_count": sum(
            worker_counts.values()
        ),

        "configuration_source": (
            "Experiment launcher arguments; "
            "worker counts are declared, not discovered automatically."
        ),
    }


def build_yolo_reference_estimate(
    person_workers: int,
    plate_workers: int,
) -> dict[str, Any]:
    """
    Build a benchmark-based YOLO11n reference estimate.

    The memory estimate covers model weights only. It is not a
    measurement of complete process RAM or GPU memory usage.
    """

    worker_counts = {
        "person": person_workers,
        "plate": plate_workers,
    }
    for detector, count in worker_counts.items():
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or not 1 <= count <= 20
        ):
            raise ValueError(
                f"{detector} workers must be between 1 and 20"
            )

    total_workers = sum(worker_counts.values())
    cpu_latency = YOLO_REFERENCE["cpu_benchmark"][
        "latency_ms_per_image"
    ]
    gpu_latency = YOLO_REFERENCE["gpu_benchmark"][
        "latency_ms_per_image"
    ]
    fp32_per_worker = YOLO_REFERENCE["weight_memory_estimate_mb"][
        "fp32_per_instance"
    ]
    fp16_per_worker = YOLO_REFERENCE["weight_memory_estimate_mb"][
        "fp16_per_instance"
    ]

    return {
        "model": YOLO_REFERENCE["model"],
        "input_resolution": dict(YOLO_REFERENCE["input_resolution"]),
        "batch_size": YOLO_REFERENCE["batch_size"],
        "parameters_millions": YOLO_REFERENCE["parameters_millions"],
        "flops_billions": YOLO_REFERENCE["flops_billions"],
        "worker_instances": {
            **worker_counts,
            "total": total_workers,
        },
        "cpu_benchmark": {
            **YOLO_REFERENCE["cpu_benchmark"],
            "idealized_images_per_second_per_worker": round(
                1000.0 / cpu_latency,
                2,
            ),
        },
        "gpu_benchmark": {
            **YOLO_REFERENCE["gpu_benchmark"],
            "idealized_images_per_second_per_worker": round(
                1000.0 / gpu_latency,
                2,
            ),
        },
        "model_weight_memory_estimate_mb": {
            "fp32_per_worker": fp32_per_worker,
            "fp32_all_workers": round(
                fp32_per_worker * total_workers,
                2,
            ),
            "fp16_per_worker": fp16_per_worker,
            "fp16_all_workers": round(
                fp16_per_worker * total_workers,
                2,
            ),
        },
        "memory_scope": (
            "Model weights only. Excludes Python/runtime overhead, "
            "activations, input buffers, preprocessing and postprocessing."
        ),
        "source_url": YOLO_REFERENCE["source_url"],
        "measurement_status": "reference_estimate_not_measured",
        "note": YOLO_REFERENCE["note"],
    }


# ========================================================
# 8. BUILD RUN METADATA
# ========================================================

def build_run_metadata(
    settings: ApplicationSettings,
    cameras: Iterable[CameraConfig],
    *,
    run_id: str,
    worker_counts: Mapping[str, int],
    processing_delays_seconds: Mapping[str, float],
) -> dict[str, Any]:
    """
    Build a complete metadata record for one experiment.

    Configuration values are collected from the application
    settings and camera configuration.

    Worker counts and processing delays are explicitly supplied
    because they are not currently part of ApplicationSettings.
    """

    if not isinstance(settings, ApplicationSettings):
        raise TypeError(
            "settings must be an ApplicationSettings instance"
        )

    if (
        not isinstance(run_id, str)
        or not re.fullmatch(
            r"run_[A-Za-z0-9][A-Za-z0-9_-]*",
            run_id,
        )
    ):
        raise ValueError(
            f"Invalid experiment run ID: {run_id!r}"
        )

    if not isinstance(worker_counts, Mapping):
        raise TypeError(
            "worker_counts must be a mapping"
        )

    if not isinstance(processing_delays_seconds, Mapping):
        raise TypeError(
            "processing_delays_seconds must be a mapping"
        )

    _validate_worker_configuration(
        worker_counts,
        processing_delays_seconds,
    )

    camera_list = _validate_cameras(cameras)

    project_root = settings.paths.advanced_dir.parent

    return {
        "experiment": {
            "run_id": run_id,
            "created_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "git": _collect_git_metadata(
                project_root
            ),
        },

        "runtime": {
            "python_version": platform.python_version(),
        },

        "camera": _collect_camera_metadata(
            settings,
            camera_list,
        ),

        "video_source": _collect_video_metadata(
            settings
        ),

        "kafka": _collect_kafka_metadata(
            settings
        ),

        "detector_workers": _collect_worker_metadata(
            worker_counts,
            processing_delays_seconds,
        ),

        "yolo_reference": build_yolo_reference_estimate(
            worker_counts["person"],
            worker_counts["plate"],
        ),

        "measurement_policy": {
            "kafka_and_application_metrics": (
                "Collected from actual producer and consumer execution."
            ),
            "host_resource_metrics": (
                "CPU, RAM and GPU utilization are not collected by the "
                "current mock detector implementation."
            ),
            "inference_metrics": (
                "The detector workers use mock simulation; no model inference "
                "is executed."
            ),
        },
    }


# ============================================================
# 9. SAVE RUN METADATA
# ============================================================

def write_run_metadata(
    settings: ApplicationSettings,
    cameras: Iterable[CameraConfig],
    *,
    run_id: str,
    worker_counts: Mapping[str, int],
    processing_delays_seconds: Mapping[str, float],
    overwrite: bool = False,
) -> Path:
    """Build and save metadata for an experiment run."""

    metadata = build_run_metadata(
        settings=settings,
        cameras=cameras,
        run_id=run_id,
        worker_counts=worker_counts,
        processing_delays_seconds=processing_delays_seconds,
    )

    run_dir = settings.paths.experiment_run_dir(
        run_id
    )

    run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_path = run_dir / "run_metadata.json"

    # Preserve existing metadata unless explicitly requested.
    if metadata_path.exists() and not overwrite:

        existing_content = metadata_path.read_text(
            encoding="utf-8-sig"
        ).strip()

        if existing_content not in {"", "{}"}:

            raise FileExistsError(
                f"Metadata already exists: {metadata_path}. "
                "Use overwrite=True to replace it."
            )

    # Write to a temporary file before replacing the target.
    temporary_path = metadata_path.with_name(
        metadata_path.name + ".tmp"
    )

    temporary_path.write_text(
        json.dumps(
            metadata,
            indent=4,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    temporary_path.replace(metadata_path)

    return metadata_path


# ============================================================
# 10. COMMAND-LINE ENTRY POINT
# ============================================================

def main() -> None:
    """Generate metadata from the current application configuration."""

    parser = argparse.ArgumentParser(
        description="Generate experiment run metadata."
    )

    parser.add_argument(
        "--run-id",
        default="run_001",
    )

    parser.add_argument(
        "--person-workers",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--plate-workers",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--person-delay-ms",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--plate-delay-ms",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    args = parser.parse_args()

    # Load application configuration.
    settings = load_settings()

    # Load camera configurations.
    cameras = load_cameras(
        settings.paths.cameras_csv
    )

    # Generate experiment metadata.
    metadata_path = write_run_metadata(
        settings=settings,
        cameras=cameras,
        run_id=args.run_id,

        worker_counts={
            "person": args.person_workers,
            "plate": args.plate_workers,
        },

        processing_delays_seconds={
            "person": args.person_delay_ms / 1000,
            "plate": args.plate_delay_ms / 1000,
        },

        overwrite=args.overwrite,
    )

    
    print("=" * 55)
    print("EXPERIMENT METADATA CREATED")
    print("=" * 55)

    print(f"File: {metadata_path}")

    target_fps = (
        1.0 / settings.camera.frame_interval_seconds
    )

    enabled_camera_count = sum(
        camera.enabled
        for camera in cameras
    )

    total_worker_count = (
        args.person_workers + args.plate_workers
    )

    print(f"Target camera FPS: {target_fps:.2f}")
    print(f"Enabled cameras: {enabled_camera_count}")
    print(f"Total detector workers: {total_worker_count}")

    print("=" * 55)


if __name__ == "__main__":
    main()

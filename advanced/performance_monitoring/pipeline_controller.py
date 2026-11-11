"""Local process controller for an end-to-end Kafka scaling experiment."""

from __future__ import annotations

import atexit
import os
import signal
import subprocess
import sys
import threading
import time

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

from advanced.config import ProjectPaths, load_cameras, load_settings
from advanced.performance_monitoring.pipeline_config import (
    build_scaled_cameras,
    create_pipeline_config,
    save_pipeline_config,
)
from advanced.performance_monitoring.run_metadata import write_run_metadata


class PipelineController:
    """Start and stop the producer, detector workers and lag monitor."""

    def __init__(self) -> None:
        self.project_root = ProjectPaths.discover().advanced_dir.parent
        self.paths = ProjectPaths.discover()
        self._processes: dict[str, subprocess.Popen] = {}
        self._log_handles: dict[str, Any] = {}
        self._active_run_id: str | None = None
        self._lock = threading.RLock()
        atexit.register(self.stop)

    @property
    def active_run_id(self) -> str | None:
        return self._active_run_id

    @property
    def is_running(self) -> bool:
        return any(
            process.poll() is None
            for process in self._processes.values()
        )

    def process_status(self) -> list[dict[str, Any]]:
        rows = []
        for name, process in self._processes.items():
            return_code = process.poll()
            rows.append({
                "Process": name,
                "PID": process.pid,
                "Status": "running" if return_code is None else "stopped",
                "Exit code": return_code,
            })
        return rows

    @staticmethod
    def _ensure_topics(settings, person_workers: int, plate_workers: int):
        """Create missing topics or increase partitions to support worker counts.

        Kafka partition counts can be increased but not decreased. Existing
        partitions are therefore retained when a later run uses fewer workers.
        """

        admin = AdminClient({
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "broker.address.family": settings.kafka.address_family,
        })
        desired = {
            settings.kafka.person_frames_topic: person_workers,
            settings.kafka.plate_frames_topic: plate_workers,
        }

        metadata = admin.list_topics(timeout=10.0)
        futures = {}

        for topic, required_count in desired.items():
            topic_metadata = metadata.topics.get(topic)

            if topic_metadata is None or topic_metadata.error is not None:
                futures[topic] = admin.create_topics([
                    NewTopic(
                        topic,
                        num_partitions=required_count,
                        replication_factor=1,
                    )
                ])[topic]
                continue

            current_count = len(topic_metadata.partitions)
            if current_count < required_count:
                futures[topic] = admin.create_partitions([
                    NewPartitions(topic, required_count)
                ])[topic]

        for topic, future in futures.items():
            try:
                future.result(timeout=20.0)
            except Exception as exc:
                raise RuntimeError(
                    f"Unable to provision Kafka topic {topic}: {exc}"
                ) from exc

        refreshed = admin.list_topics(timeout=10.0)
        actual = {}

        for detector, topic in (
            ("person", settings.kafka.person_frames_topic),
            ("plate", settings.kafka.plate_frames_topic),
        ):
            topic_metadata = refreshed.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Kafka topic is unavailable: {topic}")

            actual[detector] = len(topic_metadata.partitions)

        if actual["person"] < person_workers or actual["plate"] < plate_workers:
            raise RuntimeError(
                "Kafka topic partition counts are lower than the requested worker counts"
            )

        return actual

    def _spawn(
        self,
        name: str,
        module: str,
        arguments: list[str],
        log_path: Path,
    ) -> None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("a", encoding="utf-8")
        environment = dict(os.environ)
        environment["PYTHONUNBUFFERED"] = "1"

        process_options: dict[str, Any] = {}
        if os.name == "nt":
            process_options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            process_options["start_new_session"] = True

        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    "-m",
                    module,
                    *arguments,
                ],
                cwd=str(self.project_root),
                env=environment,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                **process_options,
            )
        except Exception:
            log_handle.close()
            raise

        self._processes[name] = process
        self._log_handles[name] = log_handle

    def start(
        self,
        *,
        camera_count: int,
        fps_per_camera: float,
        person_workers: int,
        plate_workers: int,
    ) -> str:
        """Provision Kafka and launch a complete run."""

        with self._lock:
            if self.is_running:
                raise RuntimeError("A Kafka pipeline run is already active")

            if (
                isinstance(camera_count, bool)
                or not isinstance(camera_count, int)
                or not 5 <= camera_count <= 40
            ):
                raise ValueError("camera_count must be between 5 and 40")

            if (
                isinstance(fps_per_camera, bool)
                or not isinstance(fps_per_camera, (int, float))
                or not 1 <= fps_per_camera <= 20
            ):
                raise ValueError("fps_per_camera must be between 1 and 20")

            for name, value, maximum in (
                ("person_workers", person_workers, 20),
                ("plate_workers", plate_workers, 20),
            ):
                if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or not 1 <= value <= maximum
                ):
                    raise ValueError(
                        f"{name} must be between 1 and {maximum}"
                    )

            for handle in self._log_handles.values():
                try:
                    handle.close()
                except Exception:
                    pass
            self._log_handles.clear()
            self._processes.clear()

            environment = dict(os.environ)
            environment["FRAME_INTERVAL_SECONDS"] = str(
                1.0 / float(fps_per_camera)
            )
            settings = load_settings(environ=environment)
            if not settings.paths.camera_sample_video.is_file():
                raise FileNotFoundError(
                    f"Camera sample video not found: {settings.paths.camera_sample_video}"
                )
            base_cameras = load_cameras(settings.paths.cameras_csv)

            actual_partitions = self._ensure_topics(
                settings,
                person_workers,
                plate_workers,
            )

            cameras = build_scaled_cameras(
                base_cameras,
                camera_count=camera_count,
                person_partitions=actual_partitions["person"],
                plate_partitions=actual_partitions["plate"],
            )

            run_id = datetime.now(timezone.utc).strftime(
                "run_%Y%m%d_%H%M%S_%f"
            )
            environment["PERSON_DETECTOR_GROUP_ID"] = (
                f"person-detectors-{run_id}"
            )
            environment["PLATE_DETECTOR_GROUP_ID"] = (
                f"plate-detectors-{run_id}"
            )
            settings = load_settings(environ=environment)
            run_dir = settings.paths.experiment_run_dir(run_id)
            run_dir.mkdir(parents=True, exist_ok=False)

            config = create_pipeline_config(
                run_id=run_id,
                cameras=cameras,
                fps_per_camera=fps_per_camera,
                person_workers=person_workers,
                plate_workers=plate_workers,
                person_partitions=actual_partitions["person"],
                plate_partitions=actual_partitions["plate"],
            )
            config_path = save_pipeline_config(
                config,
                run_dir / "pipeline_config.json",
            )

            self._active_run_id = run_id

            write_run_metadata(
                settings=settings,
                cameras=cameras,
                run_id=run_id,
                worker_counts={
                    "person": person_workers,
                    "plate": plate_workers,
                },
                processing_delays_seconds={
                    "person": 0.030,
                    "plate": 0.030,
                },
            )

            config_argument = str(config_path)

            try:
                # Consumers start before the producer so the first frames
                # are not published before the groups have joined.
                for worker_number in range(1, person_workers + 1):
                    worker_id = f"worker{worker_number}"
                    self._spawn(
                        f"person_{worker_id}",
                        "advanced.Consumers.person_detector",
                        [
                            "--config-path", config_argument,
                            "--worker-id", worker_id,
                        ],
                        run_dir / f"person_{worker_id}.log",
                    )

                for worker_number in range(1, plate_workers + 1):
                    worker_id = f"worker{worker_number}"
                    self._spawn(
                        f"plate_{worker_id}",
                        "advanced.Consumers.plate_detector",
                        [
                            "--config-path", config_argument,
                            "--worker-id", worker_id,
                        ],
                        run_dir / f"plate_{worker_id}.log",
                    )

                self._spawn(
                    "consumer_lag_monitor",
                    "advanced.performance_monitoring.consumer_lag_monitor",
                    ["--config-path", config_argument],
                    run_dir / "consumer_lag_monitor.log",
                )

                time.sleep(1.0)
                failed = [
                    name
                    for name, process in self._processes.items()
                    if process.poll() is not None
                ]
                if failed:
                    raise RuntimeError(
                        "Pipeline workers failed during startup: "
                        + ", ".join(failed)
                    )

                self._spawn(
                    "producer",
                    "advanced.producers.camera_producer",
                    ["--config-path", config_argument],
                    run_dir / "producer.log",
                )

                self._active_run_id = run_id
                return run_id

            except Exception:
                self.stop()
                raise

    @staticmethod
    def _interrupt_process(process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return

        try:
            if os.name == "nt":
                process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(process.pid, signal.SIGINT)
            process.wait(timeout=8.0)
            return
        except Exception:
            pass

        process.terminate()
        try:
            process.wait(timeout=4.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2.0)

    def stop(self) -> None:
        """Stop production first, then workers, then the lag monitor."""

        with self._lock:
            ordered_names = []
            if "producer" in self._processes:
                ordered_names.append("producer")
            ordered_names.extend(
                name
                for name in self._processes
                if name not in {"producer", "consumer_lag_monitor"}
            )
            if "consumer_lag_monitor" in self._processes:
                ordered_names.append("consumer_lag_monitor")

            for name in ordered_names:
                was_running = self._processes[name].poll() is None
                self._interrupt_process(self._processes[name])
                if name == "producer" and was_running:
                    # Give consumers a short window to commit the final
                    # records already accepted by Kafka.
                    time.sleep(2.0)

            for handle in self._log_handles.values():
                try:
                    handle.close()
                except Exception:
                    pass

            self._log_handles.clear()

    def run_directory(self) -> Path | None:
        if self._active_run_id is None:
            return None
        return self.paths.experiment_run_dir(self._active_run_id)

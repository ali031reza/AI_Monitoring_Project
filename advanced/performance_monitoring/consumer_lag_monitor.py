"""Collect committed Kafka consumer-group lag for the scaling dashboard."""

from __future__ import annotations

import argparse
import json
import logging
import time

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from advanced.performance_monitoring.pipeline_config import (
    load_pipeline_config,
    load_run_settings,
)


logger = logging.getLogger(__name__)


class ConsumerLagMonitor:
    """Periodically record broker watermarks and committed offsets."""

    def __init__(
        self,
        settings,
        *,
        run_id: str,
        output_path: str | Path,
    ) -> None:
        self.settings = settings
        self.run_id = run_id
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self._consumers = {
            "person": self._create_consumer(
                settings.kafka.person_detector_group_id,
                "person",
            ),
            "plate": self._create_consumer(
                settings.kafka.plate_detector_group_id,
                "plate",
            ),
        }

    def _create_consumer(self, group_id: str, name: str) -> Consumer:
        return Consumer({
            "bootstrap.servers": self.settings.kafka.bootstrap_servers,
            "broker.address.family": self.settings.kafka.address_family,
            "group.id": group_id,
            "client.id": f"ai-monitoring-lag-monitor-{name}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        })

    def _read_topic(
        self,
        consumer: Consumer,
        topic: str,
        group_id: str,
    ) -> dict[str, Any]:
        try:
            metadata = consumer.list_topics(topic, timeout=5.0)
            topic_metadata = metadata.topics.get(topic)

            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Topic metadata unavailable for {topic}")

            requested = [
                TopicPartition(topic, partition_id)
                for partition_id in sorted(topic_metadata.partitions)
            ]
            committed = consumer.committed(requested, timeout=5.0)

            partitions = []
            for item in committed:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, item.partition),
                    timeout=5.0,
                    cached=False,
                )
                committed_offset = item.offset

                if committed_offset < 0:
                    committed_lag = None
                    retained_lag = max(0, high - low)
                    expired_gap = 0
                    status = "missing_commit"
                elif committed_offset < low:
                    committed_lag = max(0, high - low)
                    retained_lag = committed_lag
                    expired_gap = low - committed_offset
                    status = "expired_offsets"
                else:
                    committed_lag = max(0, high - committed_offset)
                    retained_lag = committed_lag
                    expired_gap = 0
                    status = "ok"

                partitions.append({
                    "partition": item.partition,
                    "low_watermark": low,
                    "high_watermark": high,
                    "committed_offset": (
                        committed_offset if committed_offset >= 0 else None
                    ),
                    "committed_lag_messages": committed_lag,
                    "retained_lag_messages": retained_lag,
                    "expired_offset_gap": expired_gap,
                    "status": status,
                })

            known_lag = sum(
                item["committed_lag_messages"] or 0
                for item in partitions
            )
            missing_commits = sum(
                item["status"] == "missing_commit"
                for item in partitions
            )
            errors = sum(
                item["status"] == "error"
                for item in partitions
            )

            return {
                "topic": topic,
                "group_id": group_id,
                "partition_count": len(partitions),
                "partitions": partitions,
                "summary": {
                    "known_committed_lag_messages": known_lag,
                    "total_committed_lag_messages": (
                        None if missing_commits else known_lag
                    ),
                    "partitions_with_committed_offsets": (
                        len(partitions) - missing_commits
                    ),
                    "partitions_without_committed_offsets": missing_commits,
                    "partitions_with_errors": errors,
                },
                "error": None,
            }

        except Exception as exc:
            logger.exception("Unable to read lag for topic %s", topic)
            return {
                "topic": topic,
                "group_id": group_id,
                "partition_count": 0,
                "partitions": [],
                "summary": {
                    "known_committed_lag_messages": 0,
                    "total_committed_lag_messages": None,
                    "partitions_with_committed_offsets": 0,
                    "partitions_without_committed_offsets": 0,
                    "partitions_with_errors": 1,
                },
                "error": str(exc),
            }

    def snapshot(self) -> dict[str, Any]:
        """Read lag for both detector groups."""

        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "component": "consumer_lag_monitor",
            "measurement_type": "committed_offset_lag",
            "topics": {
                "person": self._read_topic(
                    self._consumers["person"],
                    self.settings.kafka.person_frames_topic,
                    self.settings.kafka.person_detector_group_id,
                ),
                "plate": self._read_topic(
                    self._consumers["plate"],
                    self.settings.kafka.plate_frames_topic,
                    self.settings.kafka.plate_detector_group_id,
                ),
            },
        }

    def write_snapshot(self) -> None:
        record = self.snapshot()
        with self.output_path.open(
            "a",
            encoding="utf-8",
            newline="",
        ) as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")

    def run_forever(self, interval_seconds: float = 1.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")

        logger.info("Consumer lag monitoring started for %s", self.run_id)
        try:
            while True:
                started = time.monotonic()
                self.write_snapshot()
                remaining = interval_seconds - (time.monotonic() - started)
                if remaining > 0:
                    time.sleep(remaining)
        except KeyboardInterrupt:
            logger.info("Consumer lag monitoring stopped")
        finally:
            self.close()

    def close(self) -> None:
        for consumer in self._consumers.values():
            consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor Kafka committed consumer-group lag."
    )
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    config = load_pipeline_config(args.config_path)
    settings = load_run_settings(config)
    run_dir = settings.paths.experiment_run_dir(config["run_id"])

    monitor = ConsumerLagMonitor(
        settings,
        run_id=config["run_id"],
        output_path=run_dir / "consumer_lag_metrics.jsonl",
    )
    monitor.run_forever(args.interval)


if __name__ == "__main__":
    main()

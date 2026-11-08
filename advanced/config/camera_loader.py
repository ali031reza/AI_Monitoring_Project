
import csv

from advanced.config.settings import CAMERAS_CSV


def load_cameras():
    cameras = []
    seen_ids = set()

    required_columns = {
        "camera_id",
        "zone_id",
        "detector_type",
        "enabled",
        "kafka_partition",
    }

    with CAMERAS_CSV.open(
        "r", newline="", encoding="utf-8-sig"
    ) as file:

        reader = csv.DictReader(file)

        if not required_columns.issubset(reader.fieldnames or []):
            raise ValueError("Missing required columns in cameras.csv")

        for row in reader:
            camera_id = row["camera_id"].strip()
            zone_id = row["zone_id"].strip().upper()
            detector_type = row["detector_type"].strip().lower()
            enabled_text = row["enabled"].strip().lower()

            if len(camera_id) != 3 or not camera_id.isdigit():
                raise ValueError(f"Invalid camera ID: {camera_id}")

            if camera_id in seen_ids:
                raise ValueError(f"Duplicate camera ID: {camera_id}")

            if zone_id not in {"A", "B", "C", "D", "E"}:
                raise ValueError(f"Invalid zone: {zone_id}")

            if detector_type not in {"person", "plate"}:
                raise ValueError(f"Invalid detector: {detector_type}")

            if enabled_text not in {"true", "false"}:
                raise ValueError(f"Invalid enabled value: {enabled_text}")

            enabled = enabled_text == "true"
            partition = int(row["kafka_partition"])

            if partition < 0:
                raise ValueError("Kafka partition cannot be negative")

            cameras.append({
                "camera_id": camera_id,
                "zone_id": zone_id,
                "detector_type": detector_type,
                "enabled": enabled,
                "kafka_partition": partition,
            })

            seen_ids.add(camera_id)

    return cameras

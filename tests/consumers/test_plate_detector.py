
import csv
import json

import pytest

import advanced.Consumers.plate_detector as detector_module

from advanced.config import (
    ApplicationSettings,
    CameraConfig,
    load_settings,
)

from advanced.Consumers.plate_simulator import (
    VehicleReferenceData,
)

from advanced.Consumers.plate_detector import (
    PlateDetector,
    CSV_FIELDS,
    read_header,
    read_message_key,
)


# ============================================================
# 1. FAKE KAFKA MESSAGE
# ============================================================

_UNSET = object()


class FakeKafkaMessage:

    def __init__(
        self,
        topic="camera.plate.frames.dev",
        camera_id="008",
        zone="E",
        timestamp="2026-11-08T08:00:00Z",
        partition=0,
        offset=42,
        payload=b"fake-jpeg-data",
        key=_UNSET,
        headers=_UNSET,
        error=None,
    ):

        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._payload = payload
        self._error = error

        self._key = (
            camera_id.encode("utf-8")
            if key is _UNSET
            else key
        )

        self._headers = (
            [
                ("camera_id", camera_id.encode()),
                ("zone_id", zone.encode()),
                ("timestamp_utc", timestamp.encode()),
            ]
            if headers is _UNSET
            else headers
        )

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._payload

    def headers(self):
        return self._headers


# ============================================================
# 2. FAKE KAFKA CONSUMER
# ============================================================

class FakeKafkaConsumer:

    def __init__(
        self,
        messages=None,
        output_path=None,
    ):

        self.messages = list(messages or [])

        self.output_path = output_path

        self.subscribed_topics = []
        self.assignment_callback = None

        self.commit_count = 0
        self.close_count = 0

        self.persisted_row_counts = []

    def subscribe(
        self,
        topics,
        on_assign=None,
    ):

        self.subscribed_topics = topics
        self.assignment_callback = on_assign

    def poll(self, timeout):

        if self.messages:
            return self.messages.pop(0)

        # Stop the test loop after all messages are processed.
        raise KeyboardInterrupt

    def commit(
        self,
        message=None,
        asynchronous=False,
    ):

        self.commit_count += 1

        # Verify CSV persistence before committing.
        if self.output_path is not None:

            with self.output_path.open(
                newline="",
                encoding="utf-8",
            ) as file:

                rows = list(csv.DictReader(file))

            self.persisted_row_counts.append(
                len(rows)
            )

            assert len(rows) == self.commit_count

        return []

    def close(self):

        self.close_count += 1


# ============================================================
# 3. FAKE PLATE SIMULATOR
# ============================================================

class FakePlateSimulator:
    """
    Isolate detector tests from simulation logic.

    The actual PlateSimulator is tested separately in
    test_plate_simulator.py.
    """

    def __init__(
        self,
        settings,
        reference_data,
        cameras,
    ):

        self.settings = settings
        self.reference_data = reference_data
        self.cameras = tuple(cameras)

        self.calls = []

    def simulate(
        self,
        camera_id,
        timestamp,
    ):

        self.calls.append({
            "camera_id": camera_id,
            "timestamp": timestamp,
        })

        return ["SYN-1234"]


# ============================================================
# 4. FIXTURES
# ============================================================

@pytest.fixture
def settings():

    return load_settings(environ={})


@pytest.fixture
def cameras():

    return (
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("009", "E", "plate", False, 0),
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("002", "E", "person", True, 0),
    )


@pytest.fixture
def reference_data():

    return VehicleReferenceData({
        "EMP0001": "SYN-1234",
        "EMP0002": "SYN-5678",
    })


@pytest.fixture
def patch_simulator(monkeypatch):

    monkeypatch.setattr(
        detector_module,
        "PlateSimulator",
        FakePlateSimulator,
    )


@pytest.fixture
def detector(
    settings,
    cameras,
    reference_data,
    tmp_path,
    patch_simulator,
):

    return PlateDetector(
        settings=settings,
        cameras=cameras,
        reference_data=reference_data,
        consumer=FakeKafkaConsumer(),
        output_path=tmp_path / "plates_detected.csv",
    )


# ============================================================
# 5. INITIALIZATION
# ============================================================

def test_detector_initialization(detector):

    assert isinstance(
        detector.settings,
        ApplicationSettings,
    )

    assert len(detector.cameras) == 4

    assert detector.parking_camera_ids == (
        "008",
    )

    assert detector.output_path.name == (
        "plates_detected.csv"
    )


def test_detector_uses_injected_consumer(
    settings,
    cameras,
    reference_data,
    patch_simulator,
):

    fake_consumer = FakeKafkaConsumer()

    detector = PlateDetector(
        settings,
        cameras,
        reference_data=reference_data,
        consumer=fake_consumer,
    )

    assert detector._consumer is fake_consumer


def test_detector_uses_injected_reference_data(
    detector,
    reference_data,
):

    assert detector.reference_data is reference_data


def test_detector_initializes_simulator(
    detector,
    reference_data,
):

    assert isinstance(
        detector.simulator,
        FakePlateSimulator,
    )

    assert detector.simulator.reference_data is reference_data


def test_invalid_settings_are_rejected(
    cameras,
    patch_simulator,
):

    with pytest.raises(TypeError):

        PlateDetector(
            settings="invalid",
            cameras=cameras,
        )


def test_empty_camera_list_is_rejected(
    settings,
    reference_data,
    patch_simulator,
):

    with pytest.raises(
        ValueError,
        match="No camera configurations",
    ):

        PlateDetector(
            settings,
            [],
            reference_data=reference_data,
            consumer=FakeKafkaConsumer(),
        )


def test_invalid_camera_type_is_rejected(
    settings,
    reference_data,
    patch_simulator,
):

    with pytest.raises(TypeError):

        PlateDetector(
            settings,
            ["008"],
            reference_data=reference_data,
            consumer=FakeKafkaConsumer(),
        )


def test_duplicate_camera_ids_are_rejected(
    settings,
    reference_data,
    patch_simulator,
):

    cameras = [
        CameraConfig("008", "E", "plate", True, 0),
        CameraConfig("008", "E", "plate", True, 0),
    ]

    with pytest.raises(
        ValueError,
        match="Duplicate camera IDs",
    ):

        PlateDetector(
            settings,
            cameras,
            reference_data=reference_data,
            consumer=FakeKafkaConsumer(),
        )


def test_no_active_parking_cameras_is_rejected(
    settings,
    reference_data,
    patch_simulator,
):

    cameras = (
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("009", "E", "plate", False, 0),
    )

    with pytest.raises(
        ValueError,
        match="No active parking cameras",
    ):

        PlateDetector(
            settings,
            cameras,
            reference_data=reference_data,
            consumer=FakeKafkaConsumer(),
        )


def test_invalid_reference_data_is_rejected(
    settings,
    cameras,
    patch_simulator,
):

    with pytest.raises(TypeError):

        PlateDetector(
            settings,
            cameras,
            reference_data={},
            consumer=FakeKafkaConsumer(),
        )


# ============================================================
# 6. REFERENCE DATA LOADING
# ============================================================

def test_reference_data_is_loaded_when_not_injected(
    settings,
    cameras,
    reference_data,
    monkeypatch,
    patch_simulator,
):

    calls = []

    def fake_loader(path):

        calls.append(path)

        return reference_data

    monkeypatch.setattr(
        detector_module,
        "load_registered_vehicles",
        fake_loader,
    )

    detector = PlateDetector(
        settings,
        cameras,
        consumer=FakeKafkaConsumer(),
    )

    assert detector.reference_data is reference_data

    assert calls == [
        settings.paths.employees_csv,
    ]


def test_injected_reference_data_skips_csv_loading(
    settings,
    cameras,
    reference_data,
    monkeypatch,
    patch_simulator,
):

    def unexpected_loader(path):

        raise AssertionError(
            "Vehicle loader should not be called"
        )

    monkeypatch.setattr(
        detector_module,
        "load_registered_vehicles",
        unexpected_loader,
    )

    PlateDetector(
        settings,
        cameras,
        reference_data=reference_data,
        consumer=FakeKafkaConsumer(),
    )


# ============================================================
# 7. KAFKA CONSUMER CONFIGURATION
# ============================================================

def test_kafka_consumer_configuration(
    settings,
    cameras,
    reference_data,
    monkeypatch,
    patch_simulator,
):

    captured_config = {}

    fake_consumer = FakeKafkaConsumer()

    def fake_consumer_factory(config):

        captured_config.update(config)

        return fake_consumer

    monkeypatch.setattr(
        detector_module,
        "Consumer",
        fake_consumer_factory,
    )

    detector = PlateDetector(
        settings,
        cameras,
        reference_data=reference_data,
    )

    assert detector._consumer is fake_consumer

    assert captured_config["bootstrap.servers"] == (
        settings.kafka.bootstrap_servers
    )

    assert captured_config["broker.address.family"] == (
        settings.kafka.address_family
    )

    assert captured_config["group.id"] == (
        settings.kafka.plate_detector_group_id
    )

    assert captured_config["auto.offset.reset"] == "latest"

    assert captured_config["enable.auto.commit"] is False


# ============================================================
# 8. HEADER AND KEY HELPERS
# ============================================================

def test_read_header():

    message = FakeKafkaMessage()

    assert read_header(
        message,
        "camera_id",
    ) == "008"


def test_read_header_supports_string_values():

    message = FakeKafkaMessage(
        headers=[
            ("camera_id", "008"),
        ]
    )

    assert read_header(
        message,
        "camera_id",
    ) == "008"


def test_missing_header():

    message = FakeKafkaMessage(
        headers=[],
    )

    with pytest.raises(
        ValueError,
        match="Missing Kafka header",
    ):

        read_header(
            message,
            "camera_id",
        )


def test_duplicate_header():

    message = FakeKafkaMessage(
        headers=[
            ("camera_id", b"008"),
            ("camera_id", b"009"),
        ]
    )

    with pytest.raises(
        ValueError,
        match="Duplicate Kafka header",
    ):

        read_header(
            message,
            "camera_id",
        )


def test_invalid_header_encoding():

    message = FakeKafkaMessage(
        headers=[
            ("camera_id", b"\xff"),
        ]
    )

    with pytest.raises(
        ValueError,
        match="Invalid UTF-8",
    ):

        read_header(
            message,
            "camera_id",
        )


def test_unsupported_header_type():

    message = FakeKafkaMessage(
        headers=[
            ("camera_id", 123),
        ]
    )

    with pytest.raises(
        ValueError,
        match="Unsupported Kafka header type",
    ):

        read_header(
            message,
            "camera_id",
        )


def test_read_message_key():

    message = FakeKafkaMessage()

    assert read_message_key(message) == "008"


def test_invalid_message_key():

    message = FakeKafkaMessage(
        key=None,
    )

    with pytest.raises(
        ValueError,
        match="valid camera ID key",
    ):

        read_message_key(message)


def test_invalid_message_key_encoding():

    message = FakeKafkaMessage(
        key=b"\xff",
    )

    with pytest.raises(
        ValueError,
        match="Invalid UTF-8",
    ):

        read_message_key(message)


# ============================================================
# 9. VALID MESSAGE PROCESSING
# ============================================================

def test_process_valid_message(detector, settings):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
    )

    event = detector.process_message(message)

    assert event["camera_id"] == "008"

    assert event["Zone"] == "E"

    assert event["Time_stamp"] == (
        "2026-11-08T08:00:00Z"
    )

    assert event["event_id"] == (
        "camera.plate.frames.dev:0:42"
    )

    assert event["detector_type"] == "synthetic"

    assert json.loads(
        event["detected_plates"]
    ) == ["SYN-1234"]


def test_process_message_uses_simulator(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="008",
    )

    detector.process_message(message)

    assert detector.simulator.calls == [
        {
            "camera_id": "008",
            "timestamp": "2026-11-08T08:00:00Z",
        }
    ]


def test_process_message_preserves_empty_detection_list(
    detector,
    settings,
    monkeypatch,
):

    monkeypatch.setattr(
        detector.simulator,
        "simulate",
        lambda **kwargs: [],
    )

    event = detector.process_message(
        FakeKafkaMessage(
            topic=settings.kafka.plate_frames_topic,
        )
    )

    assert json.loads(
        event["detected_plates"]
    ) == []


def test_event_contains_exact_csv_fields(
    detector,
    settings,
):

    event = detector.process_message(
        FakeKafkaMessage(
            topic=settings.kafka.plate_frames_topic,
        )
    )

    assert tuple(event.keys()) == CSV_FIELDS


# ============================================================
# 10. MESSAGE VALIDATION
# ============================================================

def test_unexpected_topic_is_rejected(detector):

    message = FakeKafkaMessage(
        topic="unexpected.topic",
    )

    with pytest.raises(
        ValueError,
        match="Unexpected Kafka topic",
    ):

        detector.process_message(message)


def test_kafka_message_error_is_rejected(detector):

    message = FakeKafkaMessage(
        error="Kafka transport error",
    )

    with pytest.raises(
        RuntimeError,
        match="Kafka message error",
    ):

        detector.process_message(message)


def test_unknown_camera_is_rejected(detector, settings):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="999",
        zone="E",
    )

    with pytest.raises(
        ValueError,
        match="Unknown camera ID",
    ):

        detector.process_message(message)


def test_disabled_camera_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="009",
        zone="E",
    )

    with pytest.raises(
        ValueError,
        match="disabled",
    ):

        detector.process_message(message)


def test_person_camera_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="002",
        zone="E",
    )

    with pytest.raises(
        ValueError,
        match="not a plate camera",
    ):

        detector.process_message(message)


def test_camera_outside_parking_zone_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="001",
        zone="A",
    )

    with pytest.raises(
        ValueError,
        match="not in parking Zone E",
    ):

        detector.process_message(message)


def test_zone_mismatch_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="008",
        zone="D",
    )

    with pytest.raises(
        ValueError,
        match="Zone mismatch",
    ):

        detector.process_message(message)


def test_partition_mismatch_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="008",
        partition=1,
    )

    with pytest.raises(
        ValueError,
        match="Kafka partition mismatch",
    ):

        detector.process_message(message)


def test_message_key_mismatch_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        camera_id="008",
        key=b"009",
    )

    with pytest.raises(
        ValueError,
        match="message key does not match",
    ):

        detector.process_message(message)


def test_empty_payload_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        payload=b"",
    )

    with pytest.raises(
        ValueError,
        match="Invalid image payload",
    ):

        detector.process_message(message)


def test_missing_timestamp_header_is_rejected(
    detector,
    settings,
):

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        headers=[
            ("camera_id", b"008"),
            ("zone_id", b"E"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing Kafka header",
    ):

        detector.process_message(message)


def test_invalid_timestamp_is_rejected_by_simulator(
    detector,
    settings,
    monkeypatch,
):

    def reject_timestamp(**kwargs):

        raise ValueError(
            "Invalid timestamp"
        )

    monkeypatch.setattr(
        detector.simulator,
        "simulate",
        reject_timestamp,
    )

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
        timestamp="invalid",
    )

    with pytest.raises(
        ValueError,
        match="Invalid timestamp",
    ):

        detector.process_message(message)


# ============================================================
# 11. CSV OUTPUT
# ============================================================

def test_prepare_output_creates_directory(
    detector,
    tmp_path,
):

    output_path = (
        tmp_path
        / "nested"
        / "data"
        / "plates_detected.csv"
    )

    detector.output_path = output_path

    needs_header = detector._prepare_output()

    assert needs_header is True

    assert output_path.parent.exists()


def test_prepare_output_returns_false_for_existing_file(
    detector,
):

    detector.output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    detector.output_path.write_text(
        ",".join(CSV_FIELDS) + "\n",
        encoding="utf-8",
    )

    assert detector._prepare_output() is False


def test_existing_output_schema_is_validated(
    detector,
):

    detector.output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    detector.output_path.write_text(
        "wrong,column,names\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="incompatible schema",
    ):

        detector._prepare_output()


def test_empty_existing_output_needs_header(
    detector,
):

    detector.output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    detector.output_path.write_text(
        "",
        encoding="utf-8",
    )

    assert detector._prepare_output() is True


# ============================================================
# 12. CONSUMER LIFECYCLE
# ============================================================

def test_run_forever_persists_before_commit(
    settings,
    cameras,
    reference_data,
    tmp_path,
    patch_simulator,
):

    output_path = tmp_path / "plates_detected.csv"

    message = FakeKafkaMessage(
        topic=settings.kafka.plate_frames_topic,
    )

    fake_consumer = FakeKafkaConsumer(
        messages=[message],
        output_path=output_path,
    )

    detector = PlateDetector(
        settings,
        cameras,
        reference_data=reference_data,
        consumer=fake_consumer,
        output_path=output_path,
    )

    detector.run_forever()

    assert fake_consumer.subscribed_topics == [
        settings.kafka.plate_frames_topic,
    ]

    assert callable(
        fake_consumer.assignment_callback
    )

    assert fake_consumer.commit_count == 1

    assert fake_consumer.close_count == 1

    assert fake_consumer.persisted_row_counts == [1]

    with output_path.open(
        newline="",
        encoding="utf-8",
    ) as file:

        rows = list(csv.DictReader(file))

    assert len(rows) == 1

    assert rows[0]["camera_id"] == "008"

    assert rows[0]["Zone"] == "E"

    assert json.loads(
        rows[0]["detected_plates"]
    ) == ["SYN-1234"]


def test_failed_message_is_not_committed(
    settings,
    cameras,
    reference_data,
    tmp_path,
    patch_simulator,
):

    output_path = tmp_path / "plates_detected.csv"

    invalid_message = FakeKafkaMessage(
        topic="unexpected.topic",
    )

    fake_consumer = FakeKafkaConsumer(
        messages=[invalid_message],
        output_path=output_path,
    )

    detector = PlateDetector(
        settings,
        cameras,
        reference_data=reference_data,
        consumer=fake_consumer,
        output_path=output_path,
    )

    with pytest.raises(
        ValueError,
        match="Unexpected Kafka topic",
    ):

        detector.run_forever()

    assert fake_consumer.commit_count == 0

    assert fake_consumer.close_count == 1


def test_close_is_idempotent(detector):

    detector.close()
    detector.close()

    assert detector._consumer.close_count == 1


def test_closed_detector_cannot_restart(detector):

    detector.close()

    with pytest.raises(
        RuntimeError,
        match="closed plate detector",
    ):

        detector.run_forever()


# ============================================================
# 13. POLL TIMEOUT VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "timeout",
    [
        0,
        -1,
        True,
        float("nan"),
        float("inf"),
        -float("inf"),
        "1.0",
    ],
)
def test_invalid_poll_timeout_is_rejected(
    detector,
    timeout,
):

    with pytest.raises(ValueError):

        detector.run_forever(
            poll_timeout=timeout
        )

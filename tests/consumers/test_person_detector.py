
import csv
import json

from types import MappingProxyType

import pytest

from advanced.config import (
    CameraConfig,
    load_settings,
)

from advanced.Consumers.person_simulator import (
    PersonReferenceData,
)

from advanced.Consumers.person_detector import (
    PersonDetector,
    read_header,
    read_message_key,
)


# ============================================================
# 1. FAKE KAFKA MESSAGE
# ============================================================

class FakeKafkaMessage:

    def __init__(
        self,
        topic="camera.person.frames.dev",
        camera_id="001",
        zone="A",
        timestamp="2026-11-08T10:00:00Z",
        partition=0,
        offset=42,
        payload=b"fake-jpeg-data",
        key=None,
        headers=None,
    ):

        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._payload = payload

        self._key = (
            camera_id.encode("utf-8")
            if key is None
            else key
        )

        self._headers = (
            [
                ("camera_id", camera_id.encode()),
                ("zone_id", zone.encode()),
                ("timestamp_utc", timestamp.encode()),
            ]
            if headers is None
            else headers
        )

    def error(self):
        return None

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

    def __init__(self, messages=None, output_path=None):

        self.messages = list(messages or [])

        self.output_path = output_path

        self.subscribed_topics = []

        self.commit_count = 0
        self.close_count = 0

    def subscribe(self, topics):

        self.subscribed_topics = topics

    def poll(self, timeout):

        if self.messages:
            return self.messages.pop(0)

        # Stop the test loop after all messages are processed.
        raise KeyboardInterrupt

    def commit(self, message=None, asynchronous=False):

        self.commit_count += 1

        # Verify the output was persisted before committing.
        if self.output_path is not None:

            with self.output_path.open(
                newline="",
                encoding="utf-8",
            ) as file:

                rows = list(csv.DictReader(file))

            assert len(rows) == self.commit_count

        return []

    def close(self):

        self.close_count += 1


# ============================================================
# 3. FIXTURES
# ============================================================

@pytest.fixture
def settings():

    return load_settings(environ={})


@pytest.fixture
def cameras():

    return (
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("002", "B", "person", True, 0),
        CameraConfig("003", "C", "person", True, 1),
        CameraConfig("004", "D", "person", True, 1),
        CameraConfig("008", "E", "plate", True, 0),
    )


@pytest.fixture
def reference_data():

    employees = frozenset(
        f"EMP{i:04d}"
        for i in range(1, 101)
    )

    authorized = frozenset(
        sorted(employees)[:10]
    )

    return PersonReferenceData(
        employees=employees,
        restricted_access=MappingProxyType({
            "C": authorized,
            "D": authorized,
        }),
    )


@pytest.fixture
def detector(
    settings,
    cameras,
    reference_data,
    tmp_path,
):

    return PersonDetector(
        settings=settings,
        cameras=cameras,
        reference_data=reference_data,
        consumer=FakeKafkaConsumer(),
        output_path=tmp_path / "persons_detected.csv",
    )


# ============================================================
# 4. INITIALIZATION
# ============================================================

def test_detector_initialization(detector):

    assert detector.settings is not None
    assert len(detector.cameras) == 5

    assert detector.output_path.name == (
        "persons_detected.csv"
    )


def test_detector_uses_injected_consumer(
    settings,
    cameras,
    reference_data,
):

    fake_consumer = FakeKafkaConsumer()

    detector = PersonDetector(
        settings,
        cameras,
        reference_data=reference_data,
        consumer=fake_consumer,
    )

    assert detector._consumer is fake_consumer


def test_invalid_settings_are_rejected(
    cameras,
):

    with pytest.raises(TypeError):

        PersonDetector(
            settings="invalid",
            cameras=cameras,
        )


def test_duplicate_camera_ids_are_rejected(
    settings,
    reference_data,
):

    cameras = [
        CameraConfig("001", "A", "person", True, 0),
        CameraConfig("001", "B", "person", True, 0),
    ]

    with pytest.raises(
        ValueError,
        match="Duplicate camera IDs",
    ):

        PersonDetector(
            settings,
            cameras,
            reference_data=reference_data,
        )


# ============================================================
# 5. HEADER AND KEY HELPERS
# ============================================================

def test_read_header():

    message = FakeKafkaMessage()

    assert read_header(
        message,
        "camera_id",
    ) == "001"


def test_missing_header():

    message = FakeKafkaMessage(headers=[])

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
            ("camera_id", b"001"),
            ("camera_id", b"002"),
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

    with pytest.raises(ValueError):
        read_header(message, "camera_id")


def test_read_message_key():

    message = FakeKafkaMessage()

    assert read_message_key(message) == "001"


def test_invalid_message_key():

    message = FakeKafkaMessage(key=None)

    message._key = None

    with pytest.raises(ValueError):
        read_message_key(message)


# ============================================================
# 6. MESSAGE PROCESSING
# ============================================================

def test_process_valid_message(
    detector,
):

    message = FakeKafkaMessage()

    event = detector.process_message(message)

    assert event["camera_id"] == "001"
    assert event["Zone"] == "A"

    assert event["event_id"] == (
        "camera.person.frames.dev:0:42"
    )

    assert event["detector_type"] == "synthetic"

    assert isinstance(
        json.loads(event["detected_persons_id"]),
        list,
    )

    assert isinstance(
        json.loads(event["authorized"]),
        list,
    )


def test_detected_ids_and_authorization_lengths_match(
    detector,
):

    event = detector.process_message(
        FakeKafkaMessage()
    )

    detected = json.loads(
        event["detected_persons_id"]
    )

    authorized = json.loads(
        event["authorized"]
    )

    assert len(detected) == len(authorized)


def test_unexpected_topic_is_rejected(detector):

    message = FakeKafkaMessage(
        topic="unexpected.topic"
    )

    with pytest.raises(
        ValueError,
        match="Unexpected Kafka topic",
    ):

        detector.process_message(message)


def test_unknown_camera_is_rejected(detector):

    message = FakeKafkaMessage(
        camera_id="999",
        zone="A",
    )

    with pytest.raises(
        ValueError,
        match="Unknown camera ID",
    ):

        detector.process_message(message)


def test_zone_mismatch_is_rejected(detector):

    message = FakeKafkaMessage(
        camera_id="001",
        zone="B",
    )

    with pytest.raises(
        ValueError,
        match="Zone mismatch",
    ):

        detector.process_message(message)


def test_partition_mismatch_is_rejected(detector):

    message = FakeKafkaMessage(
        camera_id="001",
        partition=1,
    )

    with pytest.raises(
        ValueError,
        match="Kafka partition mismatch",
    ):

        detector.process_message(message)


def test_message_key_mismatch_is_rejected(detector):

    message = FakeKafkaMessage(
        camera_id="001",
        key=b"002",
    )

    with pytest.raises(
        ValueError,
        match="message key does not match",
    ):

        detector.process_message(message)


def test_empty_payload_is_rejected(detector):

    message = FakeKafkaMessage(
        payload=b"",
    )

    with pytest.raises(
        ValueError,
        match="Invalid image payload",
    ):

        detector.process_message(message)


def test_missing_timestamp_header_is_rejected(detector):

    message = FakeKafkaMessage(
        headers=[
            ("camera_id", b"001"),
            ("zone_id", b"A"),
        ]
    )

    with pytest.raises(
        ValueError,
        match="Missing Kafka header",
    ):

        detector.process_message(message)


# ============================================================
# 7. CSV OUTPUT
# ============================================================

def test_prepare_output_creates_directory(
    detector,
    tmp_path,
):

    output_path = (
        tmp_path
        / "nested"
        / "data"
        / "persons_detected.csv"
    )

    detector.output_path = output_path

    needs_header = detector._prepare_output()

    assert needs_header is True
    assert output_path.parent.exists()


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


def test_existing_valid_schema_does_not_need_header(
    detector,
):

    detector.output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    detector.output_path.write_text(
        ",".join([
            "Time_stamp",
            "Zone",
            "detected_persons_id",
            "authorized",
            "camera_id",
            "event_id",
            "detector_type",
        ]) + "\n",
        encoding="utf-8",
    )

    assert detector._prepare_output() is False


# ============================================================
# 8. CONSUMER LIFECYCLE
# ============================================================

def test_run_forever_persists_before_commit(
    settings,
    cameras,
    reference_data,
    tmp_path,
):

    output_path = tmp_path / "persons_detected.csv"

    message = FakeKafkaMessage()

    fake_consumer = FakeKafkaConsumer(
        messages=[message],
        output_path=output_path,
    )

    detector = PersonDetector(
        settings,
        cameras,
        reference_data=reference_data,
        consumer=fake_consumer,
        output_path=output_path,
    )

    detector.run_forever()

    assert fake_consumer.subscribed_topics == [
        settings.kafka.person_frames_topic
    ]

    assert fake_consumer.commit_count == 1
    assert fake_consumer.close_count == 1

    with output_path.open(
        newline="",
        encoding="utf-8",
    ) as file:

        rows = list(csv.DictReader(file))

    assert len(rows) == 1
    assert rows[0]["camera_id"] == "001"


def test_close_is_idempotent(detector):

    detector.close()
    detector.close()

    assert detector._consumer.close_count == 1


def test_closed_detector_cannot_restart(detector):

    detector.close()

    with pytest.raises(
        RuntimeError,
        match="closed person detector",
    ):

        detector.run_forever()


def test_invalid_poll_timeout_is_rejected(detector):

    with pytest.raises(ValueError):
        detector.run_forever(poll_timeout=0)

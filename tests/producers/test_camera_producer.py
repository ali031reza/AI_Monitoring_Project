
import pytest

from advanced.config import (
    CameraConfig,
    load_settings,
)

from advanced.producers.camera_producer import CameraProducer


# ============================================================
# 1. FAKE KAFKA OBJECTS
# ============================================================

class FakeKafkaMessage:

    def __init__(self, topic, partition):

        self._topic = topic
        self._partition = partition

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return 100


class FakeKafkaProducer:
    """
    Simulate a Kafka producer without requiring a Kafka broker.
    """

    def __init__(
        self,
        delivery_error=None,
        flush_remaining=0,
    ):

        self.messages = []
        self.pending = []

        self.delivery_error = delivery_error
        self.flush_remaining = flush_remaining

    def produce(self, **kwargs):

        self.messages.append(kwargs)
        self.pending.append(kwargs)

    def poll(self, timeout):

        if not self.pending:
            return 0

        message = self.pending.pop(0)

        callback = message.get("callback")

        if callback:

            callback(
                self.delivery_error,
                FakeKafkaMessage(
                    message["topic"],
                    message["partition"],
                ),
            )

        return 1

    def flush(self, timeout):

        while self.pending:
            self.poll(0)

        return self.flush_remaining


# ============================================================
# 2. FIXTURES
# ============================================================

@pytest.fixture
def settings():

    return load_settings(environ={})


@pytest.fixture
def cameras():

    return (
        CameraConfig(
            camera_id="001",
            zone_id="A",
            detector_type="person",
            enabled=True,
            kafka_partition=0,
        ),

        CameraConfig(
            camera_id="002",
            zone_id="E",
            detector_type="plate",
            enabled=True,
            kafka_partition=1,
        ),

        CameraConfig(
            camera_id="003",
            zone_id="B",
            detector_type="person",
            enabled=False,
            kafka_partition=0,
        ),
    )


@pytest.fixture
def fake_producer():

    return FakeKafkaProducer()


@pytest.fixture
def camera_producer(
    settings,
    cameras,
    fake_producer,
):

    return CameraProducer(
        settings=settings,
        cameras=cameras,
        producer=fake_producer,
    )


# ============================================================
# 3. INITIALIZATION
# ============================================================

def test_camera_producer_initialization(
    camera_producer,
    cameras,
):

    assert len(camera_producer.cameras) == 2

    assert all(
        camera.enabled
        for camera in camera_producer.cameras
    )


def test_disabled_cameras_are_excluded(
    camera_producer,
):

    camera_ids = {
        camera.camera_id
        for camera in camera_producer.cameras
    }

    assert camera_ids == {"001", "002"}


def test_no_enabled_cameras_is_rejected(
    settings,
):

    disabled_camera = CameraConfig(
        camera_id="003",
        zone_id="B",
        detector_type="person",
        enabled=False,
        kafka_partition=0,
    )

    with pytest.raises(
        ValueError,
        match="No enabled cameras",
    ):

        CameraProducer(
            settings,
            [disabled_camera],
            producer=FakeKafkaProducer(),
        )


def test_invalid_settings_type():

    with pytest.raises(TypeError):

        CameraProducer(
            settings="invalid",
            cameras=[],
            producer=FakeKafkaProducer(),
        )


def test_invalid_camera_type(
    settings,
):

    with pytest.raises(
        TypeError,
        match="Every camera",
    ):

        CameraProducer(
            settings,
            [{"camera_id": "001"}],
            producer=FakeKafkaProducer(),
        )


def test_duplicate_camera_ids_are_rejected(
    settings,
):

    cameras = [
        CameraConfig(
            camera_id="001",
            zone_id="A",
            detector_type="person",
            enabled=True,
            kafka_partition=0,
        ),

        CameraConfig(
            camera_id="001",
            zone_id="E",
            detector_type="plate",
            enabled=True,
            kafka_partition=1,
        ),
    ]

    with pytest.raises(
        ValueError,
        match="duplicate IDs",
    ):

        CameraProducer(
            settings,
            cameras,
            producer=FakeKafkaProducer(),
        )


# ============================================================
# 4. KAFKA CONFIGURATION
# ============================================================

def test_kafka_configuration_uses_application_settings(
    cameras,
    monkeypatch,
):

    import advanced.producers.camera_producer as module

    captured_config = {}

    def fake_kafka_factory(config):

        captured_config.update(config)

        return FakeKafkaProducer()

    monkeypatch.setattr(
        module,
        "Producer",
        fake_kafka_factory,
    )

    custom_settings = load_settings(
        environ={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka-test:9092",
            "KAFKA_ADDRESS_FAMILY": "v6",
        }
    )

    CameraProducer(
        custom_settings,
        cameras,
    )

    assert captured_config["bootstrap.servers"] == (
        "kafka-test:9092"
    )

    assert captured_config["broker.address.family"] == "v6"

    assert captured_config["client.id"] == (
        "ai-monitoring-camera-producer"
    )


# ============================================================
# 5. FRAME GENERATION AND MESSAGE ROUTING
# ============================================================

def test_send_round_queues_all_enabled_cameras(
    camera_producer,
    fake_producer,
    monkeypatch,
):

    calls = []

    def fake_generate(
        camera,
        camera_settings,
        frame_number,
    ):

        calls.append(
            (camera, camera_settings, frame_number)
        )

        return f"jpeg-{camera.camera_id}".encode()

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        fake_generate,
    )

    queued = camera_producer.send_round(1)

    assert queued == 2

    assert len(fake_producer.messages) == 2

    assert len(calls) == 2

    assert all(
        call[1] is camera_producer.settings.camera
        for call in calls
    )


def test_person_camera_uses_person_topic(
    camera_producer,
    fake_producer,
    monkeypatch,
):

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-frame",
    )

    camera_producer.send_round(1)

    message = next(
        item
        for item in fake_producer.messages
        if item["key"] == "001"
    )

    assert message["topic"] == (
        camera_producer.settings.kafka.person_frames_topic
    )


def test_plate_camera_uses_plate_topic(
    camera_producer,
    fake_producer,
    monkeypatch,
):

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-frame",
    )

    camera_producer.send_round(1)

    message = next(
        item
        for item in fake_producer.messages
        if item["key"] == "002"
    )

    assert message["topic"] == (
        camera_producer.settings.kafka.plate_frames_topic
    )


def test_correct_kafka_partition_is_used(
    camera_producer,
    fake_producer,
    monkeypatch,
):

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-frame",
    )

    camera_producer.send_round(1)

    person_message = fake_producer.messages[0]
    plate_message = fake_producer.messages[1]

    assert person_message["partition"] == 0
    assert plate_message["partition"] == 1


def test_camera_metadata_is_encoded_as_bytes(
    camera_producer,
    fake_producer,
    monkeypatch,
):

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-frame",
    )

    camera_producer.send_round(1)

    message = fake_producer.messages[0]

    headers = dict(message["headers"])

    assert headers["camera_id"] == b"001"
    assert headers["zone_id"] == b"A"

    assert isinstance(
        headers["timestamp_utc"],
        bytes,
    )


def test_camera_key_and_frame_payload(
    camera_producer,
    fake_producer,
    monkeypatch,
):

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-jpeg",
    )

    camera_producer.send_round(1)

    message = fake_producer.messages[0]

    assert message["key"] == "001"
    assert message["value"] == b"test-jpeg"


# ============================================================
# 6. FRAME NUMBER VALIDATION
# ============================================================

@pytest.mark.parametrize(
    "frame_number",
    [-1, -10],
)
def test_negative_frame_number_is_rejected(
    camera_producer,
    frame_number,
):

    with pytest.raises(ValueError):

        camera_producer.send_round(frame_number)


@pytest.mark.parametrize(
    "frame_number",
    [1.5, "1", True, None],
)
def test_invalid_frame_number_type(
    camera_producer,
    frame_number,
):

    with pytest.raises(TypeError):

        camera_producer.send_round(frame_number)


# ============================================================
# 7. DELIVERY CALLBACKS
# ============================================================

def test_successful_delivery_is_counted(
    camera_producer,
    monkeypatch,
):

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-frame",
    )

    camera_producer.send_round(1)

    stats = camera_producer.delivery_stats

    assert stats.queued == 2
    assert stats.delivered == 2
    assert stats.failed == 0


def test_failed_delivery_is_counted(
    settings,
    cameras,
    monkeypatch,
):

    fake_producer = FakeKafkaProducer(
        delivery_error="Broker unavailable"
    )

    producer = CameraProducer(
        settings,
        cameras,
        producer=fake_producer,
    )

    monkeypatch.setattr(
        "advanced.producers.camera_producer.generate_mock_jpeg",
        lambda **kwargs: b"test-frame",
    )

    producer.send_round(1)

    stats = producer.delivery_stats

    assert stats.queued == 2
    assert stats.delivered == 0
    assert stats.failed == 2


# ============================================================
# 8. FLUSH
# ============================================================

def test_flush_returns_pending_message_count(
    settings,
    cameras,
    monkeypatch,
):

    fake_producer = FakeKafkaProducer(
        flush_remaining=3
    )

    producer = CameraProducer(
        settings,
        cameras,
        producer=fake_producer,
    )

    assert producer.flush() == 3


def test_negative_flush_timeout_is_rejected(
    camera_producer,
):

    with pytest.raises(ValueError):

        camera_producer.flush(timeout=-1)

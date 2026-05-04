import pickle
from unittest.mock import Mock

import pytest
from aiokafka.structs import ConsumerRecord
from kafka.admin import KafkaAdminClient, NewTopic
from taskiq import BrokerMessage

from taskiq_aio_kafka.broker import AioKafkaBroker
from taskiq_aio_kafka.constants import TASK_STREAM_LABEL
from taskiq_aio_kafka.subscriber import StreamMessage
from taskiq_aio_kafka.topic import Topic


class _ProducerMock:
    """Kafka producer mock."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        """Store produced message."""
        self.messages.append((topic, value))


class _ProducerStartStopMock:
    """Kafka producer lifecycle mock."""

    async def start(self) -> None:
        """Start producer."""

    async def stop(self) -> None:
        """Stop producer."""


class _ConsumerStartStopMock:
    """Kafka consumer lifecycle mock."""

    async def start(self) -> None:
        """Start consumer."""

    async def stop(self) -> None:
        """Stop consumer."""


class _ConsumerMock:
    """Kafka consumer mock."""

    def __init__(self, messages: list[ConsumerRecord[None, bytes]]) -> None:
        self._messages = iter(messages)

    def __aiter__(self) -> "_ConsumerMock":
        return self

    async def __anext__(self) -> ConsumerRecord[None, bytes]:
        """Return next message."""
        try:
            return next(self._messages)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def get_admin_client_mock() -> KafkaAdminClient:
    """Get kafka admin client mock."""
    admin_client = Mock(spec=KafkaAdminClient)
    admin_client.list_topics.return_value = []
    return admin_client


def build_consumer_record(topic: str, value: bytes) -> ConsumerRecord[None, bytes]:
    """Build Kafka consumer record."""
    return ConsumerRecord(
        topic=topic,
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=value,
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(value),
        headers=[],
    )


async def get_first_task(broker: AioKafkaBroker) -> bytes:  # type: ignore[return]
    """Get first message from the topic."""
    async for message in broker.listen():
        return message


async def test_task_topic_is_used_for_kick() -> None:
    """Test that task is sent to its declared topic."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_topics=[Topic("extra-topic")],
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True

    @broker.task_with_topic("extra-topic")
    async def test_task() -> None:
        return None

    await test_task.kiq()

    assert producer.messages[0][0] == "extra-topic"


async def test_task_topic_object_is_used_for_kick() -> None:
    """Test that task can be bound to Topic object."""
    extra_topic = Topic("extra-topic")
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_topics=[extra_topic],
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True

    @broker.task_with_topic(extra_topic)
    async def test_task() -> None:
        return None

    await test_task.kiq()

    assert producer.messages[0][0] == extra_topic.name


async def test_kicker_topic_overrides_task_default_topic() -> None:
    """Test that kicker can override task default topic."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_topics=["extra-topic", "override-topic"],
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True

    @broker.task_with_topic("extra-topic")
    async def test_task() -> None:
        return None

    await test_task.kicker().with_topic("override-topic").kiq()
    await test_task.kiq()

    assert producer.messages[0][0] == "override-topic"
    assert producer.messages[1][0] == "extra-topic"


async def test_kicker_topic_object_overrides_task_default_topic() -> None:
    """Test that kicker can override task default topic with Topic object."""
    override_topic = Topic("override-topic")
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_topics=["extra-topic", override_topic],
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True

    @broker.task_with_topic("extra-topic")
    async def test_task() -> None:
        return None

    await test_task.kicker().with_topic(override_topic).kiq()

    assert producer.messages[0][0] == override_topic.name


async def test_task_topic_label_keeps_default_broker_topic() -> None:
    """Test that regular task topic label doesn't override kafka topic."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True

    @broker.task(topic="regular-label")
    async def test_task() -> None:
        return None

    await test_task.kiq()

    assert test_task.labels["topic"] == "regular-label"
    assert producer.messages[0][0] == "default-topic"


async def test_kicker_topic_is_used_without_task_default_topic() -> None:
    """Test that kicker can set topic for task without declared topic."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_topics=["override-topic"],
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True

    @broker.task
    async def test_task() -> None:
        return None

    await test_task.kicker().with_topic("override-topic").kiq()

    assert producer.messages[0][0] == "override-topic"


async def test_kick_uses_default_topic_without_task_topic() -> None:
    """Test that default topic is used when message has no topic label."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )
    producer = _ProducerMock()
    broker._aiokafka_producer = producer
    broker._is_producer_started = True
    message = BrokerMessage(
        task_id="task-id",
        task_name="task-name",
        message=pickle.dumps("message"),
        labels={},
    )

    await broker.kick(message)

    assert producer.messages == [("default-topic", message.message)]


def test_broker_collects_topic_names() -> None:
    """Test that broker listens to default and task topics."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic=NewTopic(
            name="default-topic",
            num_partitions=1,
            replication_factor=1,
        ),
        kafka_topics=[Topic("extra-topic")],
        kafka_admin_client=get_admin_client_mock(),
    )

    assert set(broker._kafka_topics) == {"default-topic", "extra-topic"}


async def test_startup_subscribes_consumer_to_all_topics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that worker consumer subscribes to all broker topics."""
    consumer_topics: tuple[str, ...] = ()

    def create_producer(**_kwargs: object) -> _ProducerStartStopMock:
        return _ProducerStartStopMock()

    def create_consumer(
        *topics: str,
        **_kwargs: object,
    ) -> _ConsumerStartStopMock:
        nonlocal consumer_topics
        consumer_topics = topics
        return _ConsumerStartStopMock()

    monkeypatch.setattr(
        "taskiq_aio_kafka.broker.AIOKafkaProducer",
        create_producer,
    )
    monkeypatch.setattr(
        "taskiq_aio_kafka.broker.AIOKafkaConsumer",
        create_consumer,
    )
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_topics=[Topic("extra-topic")],
        kafka_admin_client=get_admin_client_mock(),
    )
    broker.is_worker_process = True

    await broker.startup()
    await broker.shutdown()

    assert consumer_topics == ("default-topic", "extra-topic")


async def test_subscribe_wraps_raw_topic_message() -> None:
    """Test that raw Kafka messages are wrapped as taskiq messages."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )

    @broker.task
    async def test_task(message: str) -> None:
        assert message

    broker.subscribe(
        "stream-topic",
        test_task,
        decoder=lambda message: message.decode(),
    )
    broker._aiokafka_consumer = _ConsumerMock(
        [build_consumer_record("stream-topic", b"raw-message")],
    )
    broker._is_consumer_started = True

    received_message = await get_first_task(broker)
    taskiq_message = broker.formatter.loads(received_message)

    assert taskiq_message.task_name == test_task.task_name
    assert taskiq_message.args == ["raw-message"]
    assert taskiq_message.kwargs == {}
    assert taskiq_message.labels[TASK_STREAM_LABEL] == "stream-topic"


async def test_subscribe_accepts_topic_object_and_custom_labels() -> None:
    """Test that stream subscriptions can use Topic objects."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )
    stream_topic = Topic("stream-topic")

    @broker.task
    async def test_task(message: bytes) -> None:
        assert message

    broker.subscribe(stream_topic, test_task, source="external")
    broker._aiokafka_consumer = _ConsumerMock(
        [build_consumer_record(stream_topic.name, b"raw-message")],
    )
    broker._is_consumer_started = True

    received_message = await get_first_task(broker)
    taskiq_message = broker.formatter.loads(received_message)

    assert taskiq_message.task_name == test_task.task_name
    assert taskiq_message.args == ["raw-message"]
    assert taskiq_message.labels == {
        TASK_STREAM_LABEL: stream_topic.name,
        "source": "external",
    }


async def test_subscribe_decoder_can_return_task_args_and_kwargs() -> None:
    """Test that stream messages can be mapped to task args and kwargs."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )

    @broker.task
    async def test_task(user_id: int, email: str, active: bool) -> None:
        assert user_id
        assert email
        assert active

    def decode_message(message: bytes) -> StreamMessage:
        user_id, email, active = message.decode().split(":")
        return StreamMessage(
            args=(int(user_id),),
            kwargs={
                "email": email,
                "active": active == "true",
            },
        )

    broker.subscribe("stream-topic", test_task, decoder=decode_message)
    broker._aiokafka_consumer = _ConsumerMock(
        [build_consumer_record("stream-topic", b"1:user@example.com:true")],
    )
    broker._is_consumer_started = True

    received_message = await get_first_task(broker)
    taskiq_message = broker.formatter.loads(received_message)

    assert taskiq_message.task_name == test_task.task_name
    assert taskiq_message.args == [1]
    assert taskiq_message.kwargs == {
        "email": "user@example.com",
        "active": True,
    }


def test_subscribe_adds_topic_to_broker_topics() -> None:
    """Test that subscribed topic is included in listened topics."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )

    @broker.task
    async def test_task(message: bytes) -> None:
        assert message

    broker.subscribe("stream-topic", test_task)

    assert set(broker._kafka_topics) == {"default-topic", "stream-topic"}


def test_subscribe_rejects_duplicate_topics() -> None:
    """Test that stream topic cannot be subscribed twice."""
    broker = AioKafkaBroker(
        bootstrap_servers="localhost",
        kafka_topic="default-topic",
        kafka_admin_client=get_admin_client_mock(),
    )

    @broker.task
    async def test_task(message: bytes) -> None:
        assert message

    broker.subscribe("stream-topic", test_task)

    with pytest.raises(ValueError, match="already subscribed"):
        broker.subscribe("stream-topic", test_task)

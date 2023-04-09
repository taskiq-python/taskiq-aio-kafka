import os
from typing import AsyncGenerator
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.admin import NewTopic

from taskiq_aio_kafka.broker import AioKafkaBroker


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Backend for anyio pytest plugin.

    :return: backend name.
    """
    return "asyncio"


@pytest.fixture()
def kafka_url() -> str:
    """Get custom kafka url.

    This function tries to get custom kafka URL,
    or returns default otherwise.

    :return: kafka url.
    """
    return os.environ.get("TEST_KAFKA_URL", "localhost")


@pytest.fixture()
def base_topic_name() -> str:
    """Generate topic name.

    :returns: random topic name.
    """
    return uuid4().hex


@pytest.fixture()
def base_topic(base_topic_name: str) -> NewTopic:
    """Generate base kafka topic.

    :param base_topic_name: name of the kafka topic.

    :return: base kafka topic.
    """
    return NewTopic(
        name=base_topic_name,
        num_partitions=1,
        replication_factor=1,
    )


@pytest.fixture()
async def test_kafka_producer(kafka_url: str) -> AIOKafkaProducer:
    """Create kafka producer.

    :param kafka_url: url to kafka.

    :returns: kafka producer.
    """
    return AIOKafkaProducer(
        bootstrap_servers=kafka_url,
    )


@pytest.fixture()
async def test_kafka_consumer(
    kafka_url: str,
    base_topic: NewTopic,
) -> AIOKafkaConsumer:
    """Create kafka consumer.

    :param kafka_url: url to kafka.
    :param base_topic: base topic in kafka.

    :returns: kafka consumer.
    """
    return AIOKafkaConsumer(
        base_topic.name,
        bootstrap_servers=kafka_url,
    )


@pytest.fixture()
async def broker_without_arguments(
    kafka_url: str,
    base_topic_name: str,
) -> AsyncGenerator[AioKafkaBroker, None]:
    """Return AioKafkaBroker default realization.

    In this fixture we don't pass custom topic, AIOKafkaProducer
    and AIOKafkaConsumer.

    :param kafka_url: url to kafka.
    :param base_topic_name: name of the topic.

    :yields: AioKafkaBroker.
    """
    broker = AioKafkaBroker(
        bootstrap_servers=kafka_url,
    )
    broker._default_kafka_topic = base_topic_name  # noqa: WPS437
    broker.is_worker_process = True

    await broker.startup()

    yield broker

    await broker.shutdown()


@pytest.fixture()
async def broker(
    kafka_url: str,
    base_topic: NewTopic,
    test_kafka_producer: AIOKafkaProducer,
    test_kafka_consumer: AIOKafkaConsumer,
    base_topic_name: str,
) -> AsyncGenerator[AioKafkaBroker, None]:
    """Yield new broker instance.

    This function is used to
    create broker, run startup,
    and shutdown after test.

    :param kafka_url: url to kafka.
    :param base_topic: custom topic.
    :param test_kafka_producer: custom AIOKafkaProducer.
    :param test_kafka_consumer: custom AIOKafkaConsumer.
    :param base_topic_name: name of the topic.

    :yields: broker.
    """
    broker = AioKafkaBroker(
        bootstrap_servers=kafka_url,
        kafka_topic=base_topic,
        aiokafka_producer=test_kafka_producer,
        aiokafka_consumer=test_kafka_consumer,
    )
    broker.is_worker_process = True

    await broker.startup()

    yield broker

    await broker.shutdown()

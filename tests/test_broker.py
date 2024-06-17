import asyncio
import pickle
from typing import Dict, List
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaProducer
from taskiq import BrokerMessage

from taskiq_aio_kafka.broker import AioKafkaBroker


async def get_first_task(broker: AioKafkaBroker) -> bytes:  # type: ignore
    """Get first message from the topic.

    :param broker: async message broker.

    :returns: first message from listen method
    """
    async for message in broker.listen():
        return message


@pytest.mark.anyio
async def test_kick_success(broker: AioKafkaBroker) -> None:
    """Test that message is published in the topic and read correctly.

    We kick the message into the topic and then listen to the topic
    to check that message in it.

    :param broker: current broker.
    """
    task_id = uuid4().hex
    task_name = uuid4().hex

    message_to_send = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=pickle.dumps("my_msg"),
        labels={
            "label1": "val1",
        },
    )

    await broker.kick(message_to_send)

    received_message_bytes: bytes = await asyncio.wait_for(
        get_first_task(broker),
        timeout=1,
    )
    assert received_message_bytes == message_to_send.message


@pytest.mark.anyio
async def test_startup(
    broker_without_arguments: AioKafkaBroker,
    base_topic_name: str,
) -> None:
    """Test startup event.

    In this test we check that startup event creates
    new KafkaAdminClient, base topic, high_priority_topic,
    AIOKafkaProducer and AIOKafkaConsumer.

    :param broker_without_arguments: broker.
    :param base_topic_name: base topic name.
    """
    assert broker_without_arguments._aiokafka_consumer
    assert broker_without_arguments._aiokafka_producer
    assert broker_without_arguments._kafka_admin_client

    all_kafka_topics: List[str] = (
        broker_without_arguments._kafka_admin_client.list_topics()
    )

    assert broker_without_arguments._kafka_topic.name in all_kafka_topics


@pytest.mark.anyio
async def test_listen(
    broker: AioKafkaBroker,
    test_kafka_producer: AIOKafkaProducer,
) -> None:
    """Test that message are read correctly.

    Tests that broker listens to the queue
    correctly and listen can be iterated.

    :param broker: current broker.
    :param test_kafka_producer: AIOKafkaProducer.
    """
    await test_kafka_producer.start()
    task_id: str = uuid4().hex
    task_name: str = uuid4().hex
    message: bytes = pickle.dumps(uuid4().hex)
    labels: Dict[str, str] = {"test_label": "123"}

    message_to_send: BrokerMessage = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=message,
        labels=labels,
    )

    await test_kafka_producer.send(
        topic=broker._kafka_topic.name,
        value=message_to_send.message,
    )

    received_message_bytes: bytes = await asyncio.wait_for(
        get_first_task(broker),
        timeout=1,
    )

    assert received_message_bytes == message_to_send.message

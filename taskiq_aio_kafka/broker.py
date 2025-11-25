import asyncio
from collections.abc import AsyncGenerator, Callable
from logging import getLogger
from typing import Any, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner.default import DefaultPartitioner
from taskiq import AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker

from taskiq_aio_kafka.exceptions import WrongAioKafkaBrokerParametersError
from taskiq_aio_kafka.models import KafkaConsumerParameters, KafkaProducerParameters

_T = TypeVar("_T")


logger = getLogger("taskiq.kafka_broker")


class AioKafkaBroker(AsyncBroker):
    """Broker that works with Kafka."""

    def __init__(  # noqa: PLR0913
        self,
        bootstrap_servers: str | list[str] | None,
        kafka_topic: NewTopic | None = None,
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        kafka_admin_client: KafkaAdminClient | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        delete_topic_on_shutdown: bool = False,
    ) -> None:
        """Construct a new broker.

        :param bootstrap_servers: string with url to kafka or list with urls.
        :param kafka_topic: kafka topic.
        :param result_backend: custom result backend.
        :param task_id_generator: custom task_id generator.
        :param kafka_admin_client: configured KafkaAdminClient.
        :param loop: specific even loop.
        :param delete_topic_on_shutdown: delete or don't delete topic on shutdown.

        :raises WrongAioKafkaBrokerParametersError: if aiokafka_producer and/or
            aiokafka_consumer were specified but bootstrap_servers wasn't specified.
        """
        super().__init__(result_backend, task_id_generator)

        if kafka_admin_client and not bootstrap_servers:
            raise WrongAioKafkaBrokerParametersError

        self._bootstrap_servers: str | list[str] | None = bootstrap_servers

        self._loop: asyncio.AbstractEventLoop | None = loop

        self._kafka_topic: NewTopic = kafka_topic or NewTopic(
            name="taskiq_topic",
            num_partitions=1,
            replication_factor=1,
        )

        self._aiokafka_producer_params: KafkaProducerParameters = (
            KafkaProducerParameters()
        )

        self._aiokafka_consumer_params: KafkaConsumerParameters = (
            KafkaConsumerParameters()
        )

        self._kafka_admin_client: KafkaAdminClient = (
            kafka_admin_client
            or KafkaAdminClient(
                bootstrap_servers=self._bootstrap_servers,
                client_id="kafka-python-taskiq",
            )
        )

        self._delete_topic_on_shutdown: bool = delete_topic_on_shutdown

        self._delay_kick_tasks: set[asyncio.Task[None]] = set()

        self._is_producer_started = False
        self._is_consumer_started = False

    def configure_producer(self, **producer_parameters: Any) -> None:
        """Configure kafka producer.

        You can pass here any configuration parameters
        accepted by the kafka producer.

        :param producer_parameters: producer parameters kwargs.
        """
        self._aiokafka_producer_params = KafkaProducerParameters(
            **producer_parameters,
        )

    def configure_consumer(self, **consumer_parameters: Any) -> None:
        """Configure kafka consumer.

        You can pass here any configuration parameters
        accepted by the kafka consumer.

        :param consumer_parameters: consumer parameters kwargs.
        """
        self._aiokafka_consumer_params = KafkaConsumerParameters(
            **consumer_parameters,
        )

    async def startup(self) -> None:
        """Setup AIOKafkaProducer, AIOKafkaConsumer and kafka topics.

        We will have 2 topics for default and high priority.

        Also we need to create AIOKafkaProducer and AIOKafkaConsumer
        if there are no producer and consumer passed.
        """
        await super().startup()
        available_condition: bool = (
            self._kafka_topic.name not in self._kafka_admin_client.list_topics()
        )
        if available_condition:
            self._kafka_admin_client.create_topics(
                new_topics=[self._kafka_topic],
                validate_only=False,
            )

        partitioner = self._aiokafka_producer_params.partitioner or DefaultPartitioner()
        producer_kwargs = self._aiokafka_producer_params.model_dump()
        producer_kwargs["partitioner"] = partitioner
        self._aiokafka_producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            loop=self._loop,
            **producer_kwargs,
        )
        await self._aiokafka_producer.start()

        if self.is_worker_process:
            partition_assignment_strategy = (
                self._aiokafka_consumer_params.partition_assignment_strategy
                or (RoundRobinPartitionAssignor,)
            )
            consumer_kwargs = self._aiokafka_consumer_params.model_dump()
            consumer_kwargs["partition_assignment_strategy"] = (
                partition_assignment_strategy
            )
            self._aiokafka_consumer = AIOKafkaConsumer(
                self._kafka_topic.name,
                bootstrap_servers=self._bootstrap_servers,
                loop=self._loop,
                **consumer_kwargs,
            )

            await self._aiokafka_consumer.start()
            self._is_consumer_started = True

        self._is_producer_started = True

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()

        if self._is_producer_started:
            await self._aiokafka_producer.stop()

        if self._is_consumer_started:
            await self._aiokafka_consumer.stop()

        topic_delete_condition: bool = all(
            (
                self._delete_topic_on_shutdown,
                self._kafka_topic.name in self._kafka_admin_client.list_topics(),
            ),
        )

        if self._kafka_admin_client:
            if topic_delete_condition:
                self._kafka_admin_client.delete_topics(
                    [self._kafka_topic.name],
                )
            self._kafka_admin_client.close()

    async def kick(self, message: BrokerMessage) -> None:
        """Send message to the topic.

        This function constructs message for kafka and sends it.

        The message has task_id and task_name and labels
        in headers.

        :raises ValueError: if startup wasn't called.
        :param message: message to send.
        """
        if not self._is_producer_started:
            raise ValueError("Please run startup before kicking.")

        topic_name: str = self._kafka_topic.name

        await self._aiokafka_producer.send(
            topic=topic_name,
            value=message.message,
        )

    async def listen(
        self,
    ) -> AsyncGenerator[bytes, None]:
        """Listen to topic.

        This function starts listen to topic and
        yields every new message.

        :yields: parsed broker message.
        :raises ValueError: if no aiokafka_consumer or startup wasn't called.
        """
        if not self._is_consumer_started:
            raise ValueError("Please run startup before listening.")

        async for raw_kafka_message in self._aiokafka_consumer:
            yield raw_kafka_message.value

__all__ = ("AioKafkaBroker",)

import asyncio
from collections.abc import AsyncGenerator, Callable, Iterable
from logging import getLogger
from typing import Any, TypeAlias, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner.default import DefaultPartitioner
from taskiq import AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker
from typing_extensions import ParamSpec

from taskiq_aio_kafka.exceptions import WrongAioKafkaBrokerParametersError
from taskiq_aio_kafka.models import KafkaConsumerParameters, KafkaProducerParameters
from taskiq_aio_kafka.topic import Topic

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")
TopicType: TypeAlias = str | NewTopic | Topic
TASK_TOPIC_LABEL = "taskiq_aio_kafka_topic"


logger = getLogger("taskiq.kafka_broker")


def _get_topic_name(topic: TopicType) -> str:
    if isinstance(topic, str):
        return topic
    return topic.name


class AioKafkaKicker(AsyncKicker[_FuncParams, _ReturnType]):
    """Kicker that can override kafka topic for a task call."""

    def with_topic(
        self,
        topic: TopicType,
    ) -> "AioKafkaKicker[_FuncParams, _ReturnType]":
        """Set kafka topic for current kick."""
        self.labels = {
            **self.labels,
            TASK_TOPIC_LABEL: _get_topic_name(topic),
        }
        return self


class AioKafkaDecoratedTask(AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]):
    """Taskiq decorated task with kafka-specific kicker."""

    def kicker(self) -> AioKafkaKicker[_FuncParams, _ReturnType]:
        """Return kafka-aware kicker."""
        return AioKafkaKicker(
            task_name=self.task_name,
            broker=self.broker,
            labels=self.labels,
            return_type=self.return_type,
        )


class AioKafkaBroker(AsyncBroker):
    """Broker that works with Kafka."""

    task_topic_label = TASK_TOPIC_LABEL

    def __init__(  # noqa: PLR0913
        self,
        bootstrap_servers: str | list[str] | None,
        kafka_topic: TopicType | None = None,
        kafka_topics: Iterable[TopicType] | None = None,
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        kafka_admin_client: KafkaAdminClient | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        delete_topic_on_shutdown: bool = False,
    ) -> None:
        """Construct a new broker.

        :param bootstrap_servers: string with url to kafka or list with urls.
        :param kafka_topic: default kafka topic.
        :param kafka_topics: all kafka topics to listen.
        :param result_backend: custom result backend.
        :param task_id_generator: custom task_id generator.
        :param kafka_admin_client: configured KafkaAdminClient.
        :param loop: specific even loop.
        :param delete_topic_on_shutdown: delete or don't delete topic on shutdown.

        :raises WrongAioKafkaBrokerParametersError: if aiokafka_producer and/or
            aiokafka_consumer were specified but bootstrap_servers wasn't specified.
        """
        super().__init__(result_backend, task_id_generator)
        self.decorator_class = AioKafkaDecoratedTask

        if kafka_admin_client and not bootstrap_servers:
            raise WrongAioKafkaBrokerParametersError

        self._bootstrap_servers: str | list[str] | None = bootstrap_servers

        self._loop: asyncio.AbstractEventLoop | None = loop

        self._kafka_topic: NewTopic = self._normalize_default_topic(kafka_topic)
        self._kafka_topics: dict[str, TopicType] = {
            self._kafka_topic.name: self._kafka_topic,
        }
        if kafka_topics is not None:
            for topic in kafka_topics:
                self._kafka_topics.setdefault(
                    self._get_topic_name(topic),
                    topic,
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

    @staticmethod
    def _get_topic_name(topic: TopicType) -> str:
        return _get_topic_name(topic)

    @classmethod
    def _normalize_default_topic(
        cls,
        kafka_topic: TopicType | None,
    ) -> NewTopic:
        if kafka_topic is None:
            return NewTopic(
                name="taskiq_topic",
                num_partitions=1,
                replication_factor=1,
            )
        if isinstance(kafka_topic, str):
            return NewTopic(
                name=kafka_topic,
                num_partitions=1,
                replication_factor=1,
            )
        if isinstance(kafka_topic, Topic):
            if kafka_topic.topic_config.declare:
                return kafka_topic.new_topic()
            return NewTopic(
                name=kafka_topic.name,
                num_partitions=1,
                replication_factor=1,
            )
        return kafka_topic

    @classmethod
    def _get_declaration_topic(
        cls,
        topic: TopicType,
    ) -> NewTopic | None:
        if isinstance(topic, NewTopic):
            return topic
        if isinstance(topic, Topic) and topic.topic_config.declare:
            return topic.new_topic()
        return None

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

    def task(  # type: ignore[override]
        self,
        task_name: str | Callable[..., Any] | None = None,
        *,
        topic: TopicType | None = None,
        **labels: Any,
    ) -> Any:
        """Decorate function and bind it to a kafka topic by default."""
        if topic is not None:
            topic_name = self._get_topic_name(topic)
            self._kafka_topics.setdefault(topic_name, topic)
            labels[self.task_topic_label] = topic_name

        return super().task(
            task_name=task_name,  # type: ignore[arg-type]
            **labels,
        )

    async def startup(self) -> None:
        """Setup AIOKafkaProducer, AIOKafkaConsumer and kafka topics.

        Also we need to create AIOKafkaProducer and AIOKafkaConsumer
        if there are no producer and consumer passed.
        """
        await super().startup()
        existed_topic_names = set(self._kafka_admin_client.list_topics())
        new_topics = [
            new_topic
            for topic in self._kafka_topics.values()
            if (new_topic := self._get_declaration_topic(topic)) is not None
            and new_topic.name not in existed_topic_names
        ]
        if new_topics:
            self._kafka_admin_client.create_topics(
                new_topics=new_topics,
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
                *self._kafka_topics,
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

        if self._kafka_admin_client:
            if self._delete_topic_on_shutdown:
                existed_topic_names = set(self._kafka_admin_client.list_topics())
                topic_names = [
                    topic_name
                    for topic_name in self._kafka_topics
                    if topic_name in existed_topic_names
                ]
                if topic_names:
                    self._kafka_admin_client.delete_topics(topic_names)
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

        topic_name: str = message.labels.get(
            self.task_topic_label,
            self._kafka_topic.name,
        )

        await self._aiokafka_producer.send_and_wait(
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

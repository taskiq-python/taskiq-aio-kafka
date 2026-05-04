__all__ = ("AioKafkaBroker",)

import asyncio
from collections.abc import AsyncGenerator, Callable, Iterable
from logging import getLogger
from typing import Any, TypeVar, overload

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner.default import DefaultPartitioner
from taskiq import AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.message import TaskiqMessage
from typing_extensions import ParamSpec

from .constants import TASK_STREAM_LABEL, TASK_TOPIC_LABEL
from .decorated_task import AioKafkaDecoratedTask
from .exceptions import WrongAioKafkaBrokerParametersError
from .models import KafkaConsumerParameters, KafkaProducerParameters
from .subscriber import StreamDecoder, StreamMessage, StreamSubscriber
from .topic import Topic
from .types import TopicType
from .utils import get_topic_name

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


logger = getLogger("taskiq.kafka_broker")


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
                    get_topic_name(topic),
                    topic,
                )
        self._stream_subscribers: dict[str, StreamSubscriber] = {}

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

    @staticmethod
    def _default_stream_decoder(message: bytes) -> StreamMessage:
        return StreamMessage(args=(message,))

    def subscribe(
        self,
        topic: TopicType,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        decoder: StreamDecoder | None = None,
        **labels: Any,
    ) -> None:
        """Subscribe task to raw Kafka topic messages."""
        topic_name = get_topic_name(topic)
        if topic_name in self._stream_subscribers:
            error_message = f"Topic {topic_name!r} is already subscribed."
            raise ValueError(error_message)

        self._kafka_topics.setdefault(topic_name, topic)
        self._stream_subscribers[topic_name] = StreamSubscriber(
            task_name=task.task_name,
            decoder=decoder or self._default_stream_decoder,
            labels=labels,
        )

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        **labels: Any,
    ) -> AioKafkaDecoratedTask[_FuncParams, _ReturnType]: ...

    @overload
    def task(
        self,
        task_name: str | None = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AioKafkaDecoratedTask[_FuncParams, _ReturnType],
    ]: ...

    def task(
        self,
        task_name: str | Callable[..., Any] | None = None,
        **labels: Any,
    ) -> Any:
        """Decorate function."""
        if callable(task_name):
            return super().task(task_name, **labels)

        return super().task(
            task_name=task_name,
            **labels,
        )

    @overload
    def task_with_topic(
        self,
        topic: TopicType,
        task_name: Callable[_FuncParams, _ReturnType],
        **labels: Any,
    ) -> AioKafkaDecoratedTask[_FuncParams, _ReturnType]: ...

    @overload
    def task_with_topic(
        self,
        topic: TopicType,
        task_name: str | None = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AioKafkaDecoratedTask[_FuncParams, _ReturnType],
    ]: ...

    def task_with_topic(
        self,
        topic: TopicType,
        task_name: str | Callable[..., Any] | None = None,
        **labels: Any,
    ) -> Any:
        """Decorate function and bind it to a kafka topic by default."""
        topic_name = get_topic_name(topic)
        self._kafka_topics.setdefault(topic_name, topic)
        labels[self.task_topic_label] = topic_name

        if callable(task_name):
            return super().task(task_name, **labels)

        return super().task(
            task_name=task_name,
            **labels,
        )

    async def startup(self) -> None:
        """Setup AIOKafkaProducer, AIOKafkaConsumer and kafka topics.

        Also we need to create AIOKafkaProducer and AIOKafkaConsumer
        if there are no producer and consumer passed.
        """
        await super().startup()
        existed_topic_names = set(self._kafka_admin_client.list_topics())

        new_topics = []
        for topic in self._kafka_topics.values():
            new_topic = self._get_declaration_topic(topic)
            if new_topic is not None and new_topic.name not in existed_topic_names:
                new_topics.append(new_topic)

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
            subscriber = self._stream_subscribers.get(raw_kafka_message.topic)
            if subscriber is None:
                yield raw_kafka_message.value
                continue

            yield self._build_stream_message(raw_kafka_message, subscriber)

    def _build_stream_message(
        self,
        raw_kafka_message: ConsumerRecord[Any, bytes],
        subscriber: StreamSubscriber,
    ) -> bytes:
        raw_value = raw_kafka_message.value
        decoded_value = subscriber.decoder(raw_value)
        stream_message = self._normalize_stream_message(decoded_value)
        labels = {
            **subscriber.labels,
            TASK_STREAM_LABEL: raw_kafka_message.topic,
        }
        message = TaskiqMessage(
            task_id=self.id_generator(),
            task_name=subscriber.task_name,
            labels=labels,
            labels_types={},
            args=list(stream_message.args),
            kwargs=stream_message.kwargs,
        )
        return self.formatter.dumps(message).message

    @staticmethod
    def _normalize_stream_message(message: Any | StreamMessage) -> StreamMessage:
        if isinstance(message, StreamMessage):
            return message
        return StreamMessage(args=(message,))

import asyncio
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, List, Optional, Set, TypeVar, Union

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner.default import DefaultPartitioner
from taskiq import AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.compat import model_dump

from taskiq_aio_kafka.exceptions import WrongAioKafkaBrokerParametersError
from taskiq_aio_kafka.models import KafkaConsumerParameters, KafkaProducerParameters

_T = TypeVar("_T")  # noqa: WPS111


logger = getLogger("taskiq.kafka_broker")


def parse_val(
    parse_func: Callable[[str], _T],
    target: Optional[str] = None,
) -> Optional[_T]:
    """
    Parse string to some value.

    :param parse_func: function to use if value is present.
    :param target: value to parse, defaults to None
    :return: Optional value.
    """
    if target is None:
        return None

    try:
        return parse_func(target)
    except ValueError:
        return None


class AioKafkaBroker(AsyncBroker):
    """Broker that works with Kafka."""

    def __init__(  # noqa: WPS211
        self,
        bootstrap_servers: Optional[Union[str, List[str]]],
        kafka_topic: Optional[NewTopic] = None,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        kafka_admin_client: Optional[KafkaAdminClient] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
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
            raise WrongAioKafkaBrokerParametersError(
                (
                    "If you specify `kafka_admin_client`, "
                    "you must specify `bootstrap_servers`."
                ),
            )

        self._bootstrap_servers: Optional[Union[str, List[str]]] = bootstrap_servers

        self._loop: Optional[asyncio.AbstractEventLoop] = loop

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

        self._delay_kick_tasks: Set[asyncio.Task[None]] = set()

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
        producer_kwargs = model_dump(self._aiokafka_producer_params)
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
            consumer_kwargs = model_dump(self._aiokafka_consumer_params)
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
                self._kafka_topic.name  # type: ignore
                in self._kafka_admin_client.list_topics(),  # type: ignore
            ),
        )

        if self._kafka_admin_client:
            if topic_delete_condition:
                self._kafka_admin_client.delete_topics(
                    [self._kafka_topic.name],  # type: ignore
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

        await self._aiokafka_producer.send(  # type: ignore
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

        async for raw_kafka_message in self._aiokafka_consumer:  # type: ignore
            yield raw_kafka_message.value

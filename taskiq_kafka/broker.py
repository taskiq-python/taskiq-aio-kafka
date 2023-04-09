import asyncio
import pickle  # noqa: S403
from logging import getLogger
from typing import AsyncGenerator, Callable, List, Optional, Set, TypeVar, Union

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from taskiq import AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker

from taskiq_kafka.exceptions import WrongAioKafkaBrokerParametersError

_T = TypeVar("_T")  # noqa: WPS111


logger = getLogger("taskiq.aio_pika_broker")


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
        aiokafka_producer: Optional[AIOKafkaProducer] = None,
        aiokafka_consumer: Optional[AIOKafkaConsumer] = None,
        kafka_admin_client: Optional[KafkaAdminClient] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        delete_topic_on_shutdown: bool = False,
    ) -> None:
        """Construct a new broker.

        :param bootstrap_servers: string with url to kafka or list with urls.
        :param kafka_topic: kafka topic.
        :param result_backend: custom result backend.
        :param task_id_generator: custom task_id generator.
        :param aiokafka_producer: configured AIOKafkaProducer.
        :param aiokafka_consumer: configured AIOKafkaConsumer.
        :param kafka_admin_client: configured KafkaAdminClient.
        :param loop: specific even loop.
        :param delete_topic_on_shutdown: delete or don't delete topic on shutdown.

        :raises WrongAioKafkaBrokerParametersError: if aiokafka_producer and/or
            aiokafka_consumer were specified but bootstrap_servers wasn't specified.
        """
        super().__init__(result_backend, task_id_generator)

        self._bootstrap_servers: Optional[Union[str, List[str]]] = bootstrap_servers
        self._kafka_topic: Optional[NewTopic] = kafka_topic
        self._aiokafka_producer: Optional[AIOKafkaProducer] = aiokafka_producer
        self._aiokafka_consumer: Optional[AIOKafkaConsumer] = aiokafka_consumer
        self._kafka_admin_client: Optional[KafkaAdminClient] = kafka_admin_client
        self._loop: Optional[asyncio.AbstractEventLoop] = loop

        self._default_kafka_topic: str = "taskiq_topic"
        self._delete_topic_on_shutdown: bool = delete_topic_on_shutdown

        self._delay_kick_tasks: Set[asyncio.Task[None]] = set()

        if (aiokafka_producer or aiokafka_consumer) and not bootstrap_servers:
            raise WrongAioKafkaBrokerParametersError(
                (
                    "If you specify `aiokafka_producer` and/or `aiokafka_consumer`, "
                    "you must specify `bootstrap_servers`."
                ),
            )

    async def startup(self) -> None:  # noqa: C901
        """Setup AIOKafkaProducer, AIOKafkaConsumer and kafka topics.

        We will have 2 topics for default and high priority.

        Also we need to create AIOKafkaProducer and AIOKafkaConsumer
        if there are no producer and consumer passed.
        """
        await super().startup()

        if not self._aiokafka_producer:
            self._aiokafka_producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                loop=self._loop,
            )

        if not self._kafka_topic:
            self._kafka_topic: NewTopic = NewTopic(  # type: ignore
                name=self._default_kafka_topic,
                num_partitions=1,
                replication_factor=1,
            )

        if not self._kafka_admin_client:
            self._kafka_admin_client: KafkaAdminClient = (  # type: ignore
                KafkaAdminClient(
                    bootstrap_servers=self._bootstrap_servers,
                    client_id="kafka-python-taskiq",
                )
            )

        if self._kafka_topic.name not in self._kafka_admin_client.list_topics():
            self._kafka_admin_client.create_topics(
                new_topics=[self._kafka_topic],
                validate_only=False,
            )

        if not self._aiokafka_consumer:
            self._aiokafka_consumer = AIOKafkaConsumer(
                self._kafka_topic.name,
                bootstrap_servers=self._bootstrap_servers,
                loop=self._loop,
            )

        await self._aiokafka_producer.start()
        await self._aiokafka_consumer.start()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()

        if self._aiokafka_producer:
            await self._aiokafka_producer.stop()

        if self._aiokafka_consumer:
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
        if not self._aiokafka_producer:
            raise ValueError("Specify aiokafka_producer or run startup before kicking.")

        kafka_message: bytes = pickle.dumps(message)
        topic_name: str = self._kafka_topic.name  # type: ignore

        delay = parse_val(int, message.labels.get("delay"))
        if delay is None:
            await self._aiokafka_producer.send(  # type: ignore
                topic=topic_name,
                value=kafka_message,
            )
        else:
            delay_kick_task = asyncio.create_task(
                self._delay_kick(
                    kafka_message=kafka_message,
                    topic_name=topic_name,
                    delay=delay,
                ),
            )
            self._delay_kick_tasks.add(delay_kick_task)
            delay_kick_task.add_done_callback(
                self._delay_kick_tasks.discard,
            )

    async def listen(
        self,
    ) -> AsyncGenerator[BrokerMessage, None]:
        """Listen to topic.

        This function starts listen to topic and
        yields every new message.

        :yields: parsed broker message.
        :raises ValueError: if no aiokafka_consumer or startup wasn't called.
        """
        if not self._aiokafka_consumer:
            raise ValueError("Specify aiokafka_consumer or run startup before kicking.")
        async for raw_kafka_message in self._aiokafka_consumer:
            try:
                broker_message: BrokerMessage = pickle.loads(  # noqa: S301
                    raw_kafka_message.value,
                )
            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Cannot parse message from Kafka %s",
                    exc,
                    exc_info=True,
                )
                continue
            yield broker_message

    async def _delay_kick(
        self,
        kafka_message: bytes,
        topic_name: str,
        delay: float,
    ) -> None:
        """Send message to the topic after delay time.

        :param kafka_message: message to send.
        :param topic_name: name of the topic.
        :param delay: delay before kicking in seconds.
        """
        await asyncio.sleep(delay=delay)
        await self._aiokafka_producer.send(  # type: ignore
            topic=topic_name,
            value=kafka_message,
        )

# AioKafka broker for taskiq

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq-aio-kafka?style=for-the-badge)](https://pypi.org/project/taskiq-aio-kafka/)
[![PyPI](https://img.shields.io/pypi/v/taskiq-aio-kafka?style=for-the-badge)](https://pypi.org/project/taskiq-aio-kafka/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/taskiq-aio-kafka?style=for-the-badge)](https://pypistats.org/packages/taskiq-aio-kafka)

This library provides you with aiokafka broker for taskiq.

Usage:
```python
from taskiq_aio_kafka import AioKafkaBroker

broker = AioKafkaBroker(bootstrap_servers="localhost")

@broker.task
async def test() -> None:
    print("The best task ever!")
```

## Non-obvious things

You can configure kafka producer and consumer with special methods `configure_producer` and `configure_consumer`.
Example:
```python
from taskiq_aio_kafka import AioKafkaBroker

broker = AioKafkaBroker(bootstrap_servers="localhost")

# configure producer, you can set any parameter from
# base AIOKafkaProducer, except `loop` and `bootstrap_servers`
broker.configure_producer(request_timeout_ms=100000)

# configure consumer, you can set any parameter from
# base AIOKafkaConsumer, except `loop` and `bootstrap_servers`
broker.configure_consumer(group_id="the best group ever.")
```

## Multiple topics

By default `AioKafkaBroker` sends all tasks to `kafka_topic`.
You can also configure the broker to listen to multiple topics and bind
different tasks to different default topics.

```python
from taskiq_aio_kafka import AioKafkaBroker
from taskiq_aio_kafka.topic import Topic

broker = AioKafkaBroker(
    bootstrap_servers="localhost",
    kafka_topic="default-topic",
    kafka_topics=[
        Topic("emails"),
        Topic("reports"),
    ],
)


@broker.task_with_topic("emails")
async def send_email(user_id: int) -> None:
    print(f"Send email to {user_id}")


@broker.task_with_topic("reports")
async def build_report(report_id: int) -> None:
    print(f"Build report {report_id}")
```

In this example the worker listens to `default-topic`, `emails`, and `reports`.
When you call `send_email.kiq(...)`, the task is sent to `emails` by default.
When you call `build_report.kiq(...)`, the task is sent to `reports` by default.

You can override a task topic for a single kick with `kicker().with_topic(...)`:

```python
await send_email.kicker().with_topic("reports").kiq(user_id=1)
```

Tasks without a custom topic keep the old behavior and are sent to `kafka_topic`.
The regular `@broker.task` decorator keeps the standard taskiq labels behavior.

```python
@broker.task
async def regular_task() -> None:
    print("This task goes to default-topic.")


await regular_task.kiq()
```

## Stream topics

You can subscribe an existing task to a Kafka topic with raw messages.
Messages from subscribed stream topics are wrapped into regular taskiq
messages before execution, so result backends and worker middlewares keep
working as usual.

```python
import json

from taskiq_aio_kafka import AioKafkaBroker

broker = AioKafkaBroker(
    bootstrap_servers="localhost",
    kafka_topic="taskiq-topic",
)


@broker.task
async def process_user_created(event: dict[str, object]) -> None:
    print(event)


broker.subscribe(
    "users.created",
    process_user_created,
    decoder=json.loads,
)
```

The decoded value is passed as the first task argument. Stream messages also
receive the `taskiq-stream` label with the source topic name.

## Configuration

AioKafkaBroker parameters:
* `bootstrap_servers` - url to kafka nodes. Can be either string or list of strings.
* `kafka_topic` - default topic in kafka.
* `kafka_topics` - additional topics that worker should listen to.
* `result_backend` - custom result backend.
* `task_id_generator` - custom task_id genertaor.
* `kafka_admin_client` - custom `kafka` admin client.
* `delete_topic_on_shutdown` - flag to delete topics on broker shutdown.

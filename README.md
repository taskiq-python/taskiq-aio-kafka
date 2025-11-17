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

## Configuration

AioKafkaBroker parameters:
* `bootstrap_servers` - url to kafka nodes. Can be either string or list of strings.
* `kafka_topic` - custom topic in kafka.
* `result_backend` - custom result backend.
* `task_id_generator` - custom task_id genertaor.
* `kafka_admin_client` - custom `kafka` admin client.
* `delete_topic_on_shutdown` - flag to delete topic on broker shutdown.

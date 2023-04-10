# AioKafka broker for taskiq

This lirary provides you with aiokafka broker for taskiq.

Usage:
```python
from taskiq_aio_kafka import AioKafkaBroker

broker = AioKafkaBroker(bootstrap_servers="localhost")

@broker.task
async def test() -> None:
    print("The best task ever!")
```

## Configuration

AioKafkaBroker parameters:
* `bootstrap_servers` - url to kafka nodes. Can be either string or list of strings.
* `kafka_topic` - custom topic in kafka.
* `result_backend` - custom result backend.
* `task_id_generator` - custom task_id genertaor.
* `aiokafka_producer` - custom `aiokafka` producer.
* `aiokafka_consumer` - custom `aiokafka` consumer.
* `kafka_admin_client` - custom `kafka` admin client.
* `delete_topic_on_shutdown` - flag to delete topic on broker shutdown.

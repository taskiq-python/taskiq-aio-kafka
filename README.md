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

## Non-obvious things

You can send delayed messages using labels.

## Delays

To send delayed message, you have to specify
delay label. You can do it with `task` decorator,
or by using kicker. For example:

```python
broker = AioKafkaBroker(bootstrap_servers="localhost")

@broker.task(delay=3)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers
    # After 3 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overriden the `delay` label using kicker.
    await delayed_task.kicker().with_labels(delay=4).kiq()

    # This message is going to be send immediately. Since we deleted the label.
    await delayed_task.kicker().with_labels(delay=None).kiq()

    # Of course the delay is managed by rabbitmq, so you don't
    # have to wait delay period before message is going to be sent.
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

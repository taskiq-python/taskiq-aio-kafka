class BaseTaskiqKafkaError(Exception):
    """Base class for all TaskiqKafka errors."""


class WrongAioKafkaBrokerParametersError(BaseTaskiqKafkaError):
    """Error if Producer or Consumer is specified but no bootstrap_servers."""

from taskiq.exceptions import BrokerError


class WrongAioKafkaBrokerParametersError(BrokerError):
    """Error if Producer or Consumer is specified but no bootstrap_servers."""

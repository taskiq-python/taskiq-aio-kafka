from taskiq.exceptions import BrokerError


class WrongAioKafkaBrokerParametersError(BrokerError):
    """Error if Producer or Consumer is specified but no bootstrap_servers."""

    __template__ = (
        "If you specify `kafka_admin_client`, you must specify `bootstrap_servers`."
    )

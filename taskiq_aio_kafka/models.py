from collections.abc import Callable
from typing import Any

from aiokafka import __version__
from pydantic import BaseModel


class KafkaProducerParameters(BaseModel):
    """Parameters to kafka producer."""

    client_id: str | None = None
    metadata_max_age_ms: int = 300000
    request_timeout_ms: int = 40000
    api_version: str = "auto"
    acks: str | int | None = 1
    key_serializer: Callable[..., bytes] | None = None
    value_serializer: Callable[..., bytes] | None = None
    compression_type: str | None = None
    max_batch_size: int = 16384
    partitioner: Callable[..., Any] | None = None
    max_request_size: int = 1048576
    linger_ms: int = 0
    retry_backoff_ms: int = 100
    security_protocol: str = "PLAINTEXT"
    ssl_context: Any | None = None
    connections_max_idle_ms: int = 540000
    enable_idempotence: bool = False
    transactional_id: Any | None = None
    transaction_timeout_ms: int = 60000
    sasl_mechanism: str = "PLAIN"
    sasl_plain_password: str | None = None
    sasl_plain_username: str | None = None
    sasl_kerberos_service_name: Any = "kafka"
    sasl_kerberos_domain_name: Any = None
    sasl_oauth_token_provider: Any = None


class KafkaConsumerParameters(BaseModel):
    """Parameters to kafka consumer."""

    client_id: str = f"aiokafka-{__version__}"
    group_id: str | None = None
    key_deserializer: Callable[..., Any] | None = None
    value_deserializer: Callable[..., Any] | None = None
    fetch_max_wait_ms: int = 500
    fetch_max_bytes: int = 52428800
    fetch_min_bytes: int = 1
    max_partition_fetch_bytes: int = 1024 * 1024
    request_timeout_ms: int = 40000
    retry_backoff_ms: int = 100
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    check_crcs: bool = True
    metadata_max_age_ms: int = 5 * 60 * 1000
    partition_assignment_strategy: Any = None
    max_poll_interval_ms: int = 300000
    rebalance_timeout_ms: int | None = None
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    consumer_timeout_ms: int = 200
    max_poll_records: int | None = None
    ssl_context: Any | None = None
    security_protocol: str = "PLAINTEXT"
    api_version: str = "auto"
    exclude_internal_topics: bool = True
    connections_max_idle_ms: int = 540000
    isolation_level: str = "read_uncommitted"
    sasl_mechanism: str = "PLAIN"
    sasl_plain_password: str | None = None
    sasl_plain_username: str | None = None
    sasl_kerberos_service_name: str | None = "kafka"
    sasl_kerberos_domain_name: str | None = None
    sasl_oauth_token_provider: str | None = None

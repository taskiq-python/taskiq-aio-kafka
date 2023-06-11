from typing import Any, Callable, Optional, Union

from aiokafka import __version__
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner.default import DefaultPartitioner
from pydantic import BaseModel


class KafkaProducerParameters(BaseModel):
    """Parameters to kafka producer."""

    client_id: Optional[str] = None
    metadata_max_age_ms: int = 300000
    request_timeout_ms: int = 40000
    api_version: str = "auto"
    acks: Optional[Union[str, int]] = 1
    key_serializer: Optional[Callable[..., bytes]] = None
    value_serializer: Optional[Callable[..., bytes]] = None
    compression_type: Optional[str] = None
    max_batch_size: int = 16384
    partitioner: Callable[..., Any] = DefaultPartitioner()
    max_request_size: int = 1048576
    linger_ms: int = 0
    send_backoff_ms: int = 100
    retry_backoff_ms: int = 100
    security_protocol: str = "PLAINTEXT"
    ssl_context: Optional[Any] = None
    connections_max_idle_ms: int = 540000
    enable_idempotence: bool = False
    transactional_id: Optional[Any] = None
    transaction_timeout_ms: int = 60000
    sasl_mechanism: str = "PLAIN"
    sasl_plain_password: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_kerberos_service_name: Any = "kafka"
    sasl_kerberos_domain_name: Any = None
    sasl_oauth_token_provider: Any = None


class KafkaConsumerParameters(BaseModel):
    """Parameters to kafka consumer."""

    client_id: str = f"aiokafka-{__version__}"
    group_id: Optional[str] = None
    key_deserializer: Optional[Callable[..., Any]] = None
    value_deserializer: Optional[Callable[..., Any]] = None
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
    partition_assignment_strategy: Any = (RoundRobinPartitionAssignor,)
    max_poll_interval_ms: int = 300000
    rebalance_timeout_ms: Optional[int] = None
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    consumer_timeout_ms: int = 200
    max_poll_records: Optional[int] = None
    ssl_context: Optional[Any] = None
    security_protocol: str = "PLAINTEXT"
    api_version: str = "auto"
    exclude_internal_topics: bool = True
    connections_max_idle_ms: int = 540000
    isolation_level: str = "read_uncommitted"
    sasl_mechanism: str = "PLAIN"
    sasl_plain_password: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_kerberos_service_name: Optional[str] = "kafka"
    sasl_kerberos_domain_name: Optional[str] = None
    sasl_oauth_token_provider: Optional[str] = None

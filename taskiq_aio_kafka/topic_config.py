__all__ = ("TopicConfig",)

import dataclasses


@dataclasses.dataclass
class TopicConfig:
    """Kafka topic declaration settings."""

    declare: bool = False
    num_partitions: int = 1
    replication_factor: int = 1
    replica_assignments: dict[int, list[int]] = dataclasses.field(default_factory=dict)
    topic_configs: dict[str, str] = dataclasses.field(default_factory=dict)

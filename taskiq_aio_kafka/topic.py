__all__ = (
    "Topic",
    "TopicConfig",
)

import dataclasses

from kafka.admin import NewTopic


@dataclasses.dataclass
class TopicConfig:
    """Kafka topic declaration settings."""

    declare: bool = False
    num_partitions: int = 1
    replication_factor: int = 1
    replica_assignments: dict[int, list[int]] = dataclasses.field(default_factory=dict)
    topic_configs: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Topic:
    """Taskiq kafka topic."""

    name: str
    topic_config: TopicConfig = dataclasses.field(default_factory=TopicConfig)

    def new_topic(self) -> NewTopic:
        """Create kafka-python NewTopic instance."""
        if not self.topic_config.declare:
            raise ValueError("Topic declaration is disabled for this topic.")
        return NewTopic(
            name=self.name,
            num_partitions=self.topic_config.num_partitions,
            replication_factor=self.topic_config.replication_factor,
            replica_assignments=self.topic_config.replica_assignments,
            topic_configs=self.topic_config.topic_configs,
        )

__all__ = ("get_topic_name",)

from .types import TopicType


def get_topic_name(topic: TopicType) -> str:
    """Get kafka topic name."""
    if isinstance(topic, str):
        return topic
    return topic.name

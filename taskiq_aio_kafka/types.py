__all__ = ("TopicType",)

from typing import TypeAlias

from kafka.admin import NewTopic

from .topic import Topic

TopicType: TypeAlias = str | NewTopic | Topic

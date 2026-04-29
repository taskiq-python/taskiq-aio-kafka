__all__ = ("AioKafkaKicker",)

from typing import TypeVar

from taskiq.kicker import AsyncKicker
from typing_extensions import ParamSpec

from .constants import TASK_TOPIC_LABEL
from .types import TopicType
from .utils import get_topic_name

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class AioKafkaKicker(AsyncKicker[_FuncParams, _ReturnType]):
    """Kicker that can override kafka topic for a task call."""

    def with_topic(
        self,
        topic: TopicType,
    ) -> "AioKafkaKicker[_FuncParams, _ReturnType]":
        """Set kafka topic for current kick."""
        self.labels = {
            **self.labels,
            TASK_TOPIC_LABEL: get_topic_name(topic),
        }
        return self

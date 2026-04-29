__all__ = ("AioKafkaDecoratedTask",)

from typing import TypeVar

from taskiq.decor import AsyncTaskiqDecoratedTask
from typing_extensions import ParamSpec

from .kicker import AioKafkaKicker

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class AioKafkaDecoratedTask(AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]):
    """Taskiq decorated task with kafka-specific kicker."""

    def kicker(self) -> AioKafkaKicker[_FuncParams, _ReturnType]:
        """Return kafka-aware kicker."""
        return AioKafkaKicker(
            task_name=self.task_name,
            broker=self.broker,
            labels=self.labels,
            return_type=self.return_type,
        )

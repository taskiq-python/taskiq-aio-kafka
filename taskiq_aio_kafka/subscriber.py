__all__ = ("StreamDecoder", "StreamMessage", "StreamSubscriber")

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StreamMessage:
    """Decoded stream message arguments for a taskiq task."""

    args: Sequence[Any] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)


StreamDecoder = Callable[[bytes], Any | StreamMessage]


@dataclass(frozen=True)
class StreamSubscriber:
    """Kafka stream subscriber bound to a taskiq task."""

    task_name: str
    decoder: StreamDecoder
    labels: dict[str, Any] = field(default_factory=dict)

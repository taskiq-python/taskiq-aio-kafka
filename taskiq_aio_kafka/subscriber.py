__all__ = ("StreamDecoder", "StreamSubscriber")

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

StreamDecoder = Callable[[bytes], Any]


@dataclass(frozen=True)
class StreamSubscriber:
    """Kafka stream subscriber bound to a taskiq task."""

    task_name: str
    decoder: StreamDecoder
    labels: dict[str, Any] = field(default_factory=dict)

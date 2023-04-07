from typing import Optional

from pydantic import BaseModel
from taskiq import BrokerMessage


class KafkaMessage(BaseModel):
    """Message model for Kafka."""

    broker_message: BrokerMessage
    priority: Optional[int] = None

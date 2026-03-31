"""Port - EventPublisher interface."""
from typing import Mapping, Optional


class EventPublisher:
    """Interface for publishing events to Kafka."""

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Publish a message to a topic."""
        raise NotImplementedError

    def poll(self) -> None:
        """Poll for delivery callbacks."""
        raise NotImplementedError

    def flush(self, timeout: int = 10) -> None:
        """Flush pending messages."""
        raise NotImplementedError

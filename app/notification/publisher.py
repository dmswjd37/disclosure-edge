import logging
from dataclasses import dataclass
from typing import Protocol


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NotificationEvent:
    service: str
    event_type: str
    title: str
    body: str
    url: str | None = None


class NotificationPublisher(Protocol):
    async def publish(self, event: NotificationEvent) -> bool:
        """Publish an operational notification."""


class LoggingNotificationPublisher:
    async def publish(self, event: NotificationEvent) -> bool:
        logger.info(
            "Notification skipped | service=%s | event_type=%s | title=%s",
            event.service,
            event.event_type,
            event.title,
        )
        return False

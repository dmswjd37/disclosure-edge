import logging
from datetime import datetime, timedelta

from app.notification.publisher import NotificationEvent, NotificationPublisher


logger = logging.getLogger(__name__)


class AlertManager:
    def __init__(
        self,
        publisher: NotificationPublisher,
        cooldown_seconds: int = 300,
    ):
        self.publisher = publisher
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.last_sent_at: dict[str, datetime] = {}

    async def publish_failure(
        self,
        key: str,
        title: str,
        body: str,
    ) -> bool:
        now = datetime.now()
        last_sent_at = self.last_sent_at.get(key)

        if last_sent_at and now - last_sent_at < self.cooldown:
            logger.info("Failure alert suppressed | key=%s", key)
            return False

        self.last_sent_at[key] = now
        return await self.publisher.publish(
            NotificationEvent(
                service="ops",
                event_type="failure.detected",
                title=title,
                body=body,
            )
        )

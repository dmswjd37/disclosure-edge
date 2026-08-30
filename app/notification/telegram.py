import asyncio
import json
import logging
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv

from app.notification.publisher import NotificationEvent


load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE_URL = "https://api.telegram.org"


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


class TelegramNotificationPublisher:
    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        enabled: bool | None = None,
    ):
        self.bot_token = (
            os.getenv("TELEGRAM_BOT_TOKEN")
            if bot_token is None
            else bot_token
        )
        self.chat_id = (
            os.getenv("TELEGRAM_CHAT_ID")
            if chat_id is None
            else chat_id
        )
        self.enabled = (
            _env_flag("TELEGRAM_ALERT_ENABLED", default=True)
            if enabled is None
            else enabled
        )

    @property
    def configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def publish(self, event: NotificationEvent) -> bool:
        if not self.enabled:
            logger.info(
                "Telegram notification disabled | service=%s | event_type=%s",
                event.service,
                event.event_type,
            )
            return False

        if not self.configured:
            logger.warning(
                "Telegram notification is not configured. "
                "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
            )
            return False

        text = self._format_message(event)

        return await asyncio.to_thread(self._send_message, text)

    def _format_message(self, event: NotificationEvent) -> str:
        parts = [
            event.title,
            "",
            event.body,
        ]

        if event.url:
            parts.extend(["", event.url])

        return "\n".join(parts)

    def _send_message(self, text: str) -> bool:
        url = f"{TELEGRAM_API_BASE_URL}/bot{self.bot_token}/sendMessage"
        payload = urlencode(
            {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": "false",
            }
        ).encode("utf-8")

        request = Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        try:
            with urlopen(request, timeout=10) as response:
                body = response.read().decode("utf-8")
        except Exception:
            logger.exception("Telegram notification failed")
            return False

        try:
            result = json.loads(body)
        except json.JSONDecodeError:
            logger.error("Telegram returned non-JSON response: %s", body)
            return False

        ok = bool(result.get("ok"))

        if not ok:
            logger.error("Telegram notification rejected: %s", result)

        return ok

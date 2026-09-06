import asyncio
import json
import logging
import os
from typing import Awaitable, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv

from app.notification.publisher import NotificationEvent, NotificationPublisher
from app.trading import TradingService


load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE_URL = "https://api.telegram.org"


class TelegramCommandMonitor:
    def __init__(
        self,
        trading_service: TradingService,
        notification_publisher: NotificationPublisher,
        on_failure: Callable[[str, str, str], Awaitable[None]] | None = None,
    ):
        self.trading_service = trading_service
        self.notification_publisher = notification_publisher
        self.on_failure = on_failure
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.allowed_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.last_update_id = 0
        self.running = False

    @property
    def configured(self) -> bool:
        return bool(self.bot_token and self.allowed_chat_id)

    async def start(self) -> None:
        if not self.configured:
            logger.warning(
                "Telegram command monitor is not configured. "
                "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
            )
            return

        self.running = True
        logger.info("Telegram command monitor started")

        while self.running:
            try:
                updates = await asyncio.to_thread(self._get_updates_sync)

                for update in updates:
                    await self._handle_update(update)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Telegram command polling failed")

                if self.on_failure:
                    await self.on_failure(
                        "telegram.commands",
                        "[장애] Telegram 명령어 수신 실패",
                        f"{type(exc).__name__}: {exc}",
                    )

                await asyncio.sleep(3)

    async def stop(self) -> None:
        self.running = False

    def _get_updates_sync(self) -> list[dict]:
        url = f"{TELEGRAM_API_BASE_URL}/bot{self.bot_token}/getUpdates"
        query = urlencode(
            {
                "offset": self.last_update_id + 1,
                "timeout": 10,
            }
        )
        request = Request(f"{url}?{query}", method="GET")

        with urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8")

        data = json.loads(body)

        if not data.get("ok"):
            raise RuntimeError(f"Telegram getUpdates rejected: {data}")

        return data.get("result") or []

    async def _handle_update(self, update: dict) -> None:
        update_id = update.get("update_id")

        if isinstance(update_id, int):
            self.last_update_id = max(self.last_update_id, update_id)

        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id") or "")

        if chat_id != str(self.allowed_chat_id):
            logger.warning("Telegram command ignored from unauthorized chat_id=%s", chat_id)
            return

        text = str(message.get("text") or "").strip().lower()

        if not text:
            return

        if text in {"stop", "/stop"}:
            await self.trading_service.set_auto_buy_enabled(False)
            await self.notification_publisher.publish(
                NotificationEvent(
                    service="ops",
                    event_type="auto_buy.disabled",
                    title="[운영] 자동매수 중지",
                    body="Telegram stop 명령으로 자동매수 동작을 중지했습니다. DART polling은 계속 실행됩니다.",
                )
            )
            return

        if text in {"start", "/start"}:
            await self.trading_service.set_auto_buy_enabled(True)
            await self.notification_publisher.publish(
                NotificationEvent(
                    service="ops",
                    event_type="auto_buy.enabled",
                    title="[운영] 자동매수 시작",
                    body="Telegram start 명령으로 자동매수 동작을 시작했습니다.",
                )
            )
            return

        if text in {"status", "/status"}:
            status = self.trading_service.status()
            await self.notification_publisher.publish(
                NotificationEvent(
                    service="ops",
                    event_type="service.status_requested",
                    title="[운영] 서비스 상태",
                    body=(
                        f"자동매수: {'켜짐' if status['auto_buy_enabled'] else '꺼짐'}\n"
                        f"dry-run: {'켜짐' if status['dry_run'] else '꺼짐'}\n"
                        f"계좌구분: {status['account_type']}\n"
                        f"키움설정: {'완료' if status['kiwoom_configured'] else '미완료'}\n"
                        f"체결수신: {'실행중' if status['fill_stream_running'] else '중지'}"
                    ),
                )
            )
            return

        logger.info("Telegram command ignored: %s", text)

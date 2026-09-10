import asyncio
import logging
import os
from typing import Awaitable, Callable

import httpx
from dotenv import load_dotenv

from app.notification.publisher import NotificationEvent, NotificationPublisher
from app.trading import TradingService


load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE_URL = "https://api.telegram.org"
POLL_TIMEOUT_SECONDS = 10
FAILURE_ALERT_THRESHOLD = 3


class TelegramPollingError(Exception):
    """API error containing no request URL, token, or raw response body."""

    def __init__(self, code: int, retry_after: float = 0):
        self.code = code
        self.retry_after = retry_after
        hint = {
            401: "check bot credentials",
            409: "check duplicate getUpdates consumers or an active webhook",
            429: "rate limited",
        }.get(code, "getUpdates rejected")
        super().__init__(f"Telegram API {code}: {hint}")


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
        self._stop_event = asyncio.Event()

    @property
    def configured(self) -> bool:
        return bool(self.bot_token and self.allowed_chat_id)

    async def start(self) -> None:
        if self.running:
            return
        if not self.configured:
            logger.warning(
                "Telegram command monitor is not configured. "
                "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
            )
            return

        self.running = True
        self._stop_event.clear()
        logger.info("Telegram command monitor started")
        try:
            # Reuse connections; the read timeout must exceed long polling.
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=10, read=20, write=10, pool=5),
            ) as client:
                await self._poll(client)
        finally:
            self.running = False
            logger.info("Telegram command monitor stopped")

    async def _poll(self, client: httpx.AsyncClient) -> None:
        failures = 0
        while self.running:
            try:
                updates = await self._get_updates(client)
            except (httpx.RequestError, TelegramPollingError, ValueError) as exc:
                failures += 1
                delay = min(3 * 2 ** min(failures - 1, 4), 30)
                transient = isinstance(exc, httpx.RequestError) or (
                    isinstance(exc, TelegramPollingError)
                    and (exc.code >= 500 or exc.code == 429)
                )
                # HTTPX exception strings can contain the bot-token URL.
                detail = (
                    str(exc) if isinstance(exc, TelegramPollingError)
                    else type(exc).__name__
                )
                if isinstance(exc, TelegramPollingError):
                    delay = max(delay, exc.retry_after)
                log = logger.warning if transient else logger.error
                log(
                    "Telegram command polling failed | error=%s | consecutive=%s | retry_in=%ss",
                    detail, failures, delay,
                )
                if not transient or failures >= FAILURE_ALERT_THRESHOLD:
                    await self._report_failure(f"{detail} | consecutive={failures}")
                await self._wait_before_retry(delay)
                continue

            if failures:
                logger.info("Telegram command polling recovered | previous_failures=%s", failures)
                failures = 0

            for update in updates:
                if not self.running:
                    break
                try:
                    await self._handle_update(update)
                except Exception as exc:
                    # An acknowledgement failure must not stop command reception.
                    logger.error("Telegram command handling failed | error=%s", type(exc).__name__)
                    await self._report_failure(f"Command handling: {type(exc).__name__}")

    async def _report_failure(self, detail: str) -> None:
        if self.on_failure:
            try:
                await self.on_failure(
                    "telegram.commands", "[장애] Telegram 명령어 수신 실패", detail,
                )
            except Exception as exc:
                logger.warning("Telegram failure alert delivery failed | error=%s", type(exc).__name__)

    async def _wait_before_retry(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def stop(self) -> None:
        self.running = False
        self._stop_event.set()

    async def _get_updates(self, client: httpx.AsyncClient) -> list[dict]:
        url = f"{TELEGRAM_API_BASE_URL}/bot{self.bot_token}/getUpdates"
        response = await client.get(
            url, params={"offset": self.last_update_id + 1, "timeout": POLL_TIMEOUT_SECONDS},
        )
        if response.status_code == 429:
            try:
                retry_after = float(response.json().get("parameters", {}).get("retry_after", 0))
            except (ValueError, TypeError, AttributeError):
                retry_after = 0
            raise TelegramPollingError(429, retry_after)
        if response.is_error:
            raise TelegramPollingError(response.status_code)
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Invalid Telegram response")
        if not data.get("ok"):
            code = data.get("error_code", response.status_code)
            if not isinstance(code, int):
                raise ValueError("Invalid Telegram error code")
            raise TelegramPollingError(code)
        updates = data.get("result")
        if not isinstance(updates, list) or any(not isinstance(update, dict) for update in updates):
            raise ValueError("Invalid Telegram updates")
        return updates

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
                        f"나무설정: {'완료' if status['namu_configured'] else '미완료'}\n"
                        f"체결수신: {'실행중' if status['fill_stream_running'] else '중지'}"
                    ),
                )
            )
            return

        logger.info("Telegram command ignored: %s", text)

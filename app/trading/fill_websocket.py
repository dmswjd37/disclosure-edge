import asyncio
import json
import logging
import os
import ssl
import re
from datetime import datetime, time, timedelta
from dataclasses import dataclass
from typing import Awaitable, Callable
from urllib.parse import urlsplit


logger = logging.getLogger(__name__)

TokenProvider = Callable[[], Awaitable[str]]
FillHandler = Callable[["FillEvent"], Awaitable[None]]
FailureHandler = Callable[[str, str, str], Awaitable[None]]
SessionResetter = Callable[[], Awaitable[None]]

WS_ACK_OK = "00000"
WS_PATH = "/websocket"
PORT_DOMESTIC = "7070"
PORT_MOCK = "17070"
NOTICE_TR_CD = "d2"
DART_POLLING_START_ENV = "DART_POLLING_START"
DART_POLLING_END_ENV = "DART_POLLING_END"
DART_POLLING_WEEKDAYS_ONLY_ENV = "DART_POLLING_WEEKDAYS_ONLY"

def _parse_time(value: str, env_name: str) -> time:
        match = re.fullmatch(r"(\d{2}):(\d{2})", value.strip())

        if not match:
            raise RuntimeError(f"{env_name} must be HH:MM")

        hour = int(match.group(1))
        minute = int(match.group(2))

        if hour > 23 or minute > 59:
            raise RuntimeError(f"{env_name} must be HH:MM")

        return time(hour=hour, minute=minute)

@dataclass(frozen=True)
class FillEvent:
    order_no: str
    stock_code: str
    stock_name: str
    filled_quantity: int
    filled_price: int
    filled_amount: int
    raw: dict

class NamuSubscriptionError(RuntimeError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(
            f"Namu websocket subscribe failed: {code} {message}"
        )

class NamuFillWebSocketClient:
    def __init__(
        self,
        uri: str | None,
        token_provider: TokenProvider,
        on_fill: FillHandler,
        on_failure: FailureHandler | None = None,
        session_resetter: SessionResetter | None = None,
    ):
        self.uri = uri
        self.token_provider = token_provider
        self.on_fill = on_fill
        self.on_failure = on_failure
        self.websocket = None
        self.keep_running = False
        self.task: asyncio.Task | None = None
        self.session_resetter = session_resetter
        self.subscribed = False

        self._retry_delay = 3.0
        self._next_reset_at = 0.0
        self._reset_cooldown = 300.0

        self.polling_start = _parse_time(
            os.getenv(DART_POLLING_START_ENV, "09:00"),
            DART_POLLING_START_ENV,
        )
        self.polling_end = _parse_time(
            os.getenv(DART_POLLING_END_ENV, "15:30"),
            DART_POLLING_END_ENV,
        )
        self.polling_weekdays_only = os.getenv(
            DART_POLLING_WEEKDAYS_ONLY_ENV,
            "true",
        ).strip().lower() in {"1", "true", "yes", "y", "on"}

    def start(self) -> None:
        if self.task and not self.task.done():
            logger.warning("Namu fill websocket is already running")
            return

        self.keep_running = True
        self.task = asyncio.create_task(self.run(), name="namu-fill-websocket")

    async def stop(self) -> None:
        self.keep_running = False
        task = self.task

        if task is None:
            return

        try:
            if self.websocket is not None and self.subscribed:
                try:
                    async with asyncio.timeout(3):
                        await self._unregister()
                except Exception:
                    logger.warning(
                        "Namu websocket unregister failed during stop"
                    )
        finally:
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                self.task = None
                self.websocket = None
                self.subscribed = False

    def _is_connection_time(self, now: datetime) -> bool:
        if self.polling_weekdays_only and now.weekday() >= 5:
            return False

        return self.polling_start <= now.time() < self.polling_end

    def _next_connection_start(self, now: datetime) -> datetime:
        next_start = datetime.combine(
            now.date(),
            self.polling_start,
        )

        if next_start <= now:
            next_start += timedelta(days=1)

        while self.polling_weekdays_only and next_start.weekday() >= 5:
            next_start += timedelta(days=1)

        return next_start

    async def run(self) -> None:
        waiting_logged = False

        while self.keep_running:
            now = datetime.now()

            # 접속 시간 밖에서는 연결하지 않고 대기
            if not self._is_connection_time(now):
                next_start = self._next_connection_start(now)

                if not waiting_logged:
                    logger.info(
                        "Namu websocket waiting | next_start=%s",
                        next_start.isoformat(timespec="seconds"),
                    )
                    waiting_logged = True

                wait_seconds = (next_start - now).total_seconds()

                await asyncio.sleep(
                    min(60.0, max(0.1, wait_seconds))
                )
                continue

            waiting_logged = False
            self._retry_delay = 3.0

            end_at = datetime.combine(
                now.date(),
                self.polling_end,
            )
            remaining = (end_at - now).total_seconds()

            logger.info(
                "Namu websocket operating window started | end=%s",
                end_at.isoformat(timespec="seconds"),
            )

            # 종료 시간이 되면 수신·재접속 작업을 취소
            window_timeout = asyncio.timeout(remaining)

            try:
                async with window_timeout:
                    await self._run_during_window()

            except TimeoutError:
                # 다른 원인의 TimeoutError를 시간 종료로 숨기지 않음
                if not window_timeout.expired():
                    raise

                logger.info(
                    "Namu websocket operating window ended"
                )

    async def _run_during_window(self) -> None:
        while self.keep_running:
            try:
                await self._run_once()

            except asyncio.CancelledError:
                raise

            except NamuSubscriptionError as exc:
                logger.warning("%s", exc)

                if exc.code == "WSS10015" and self.keep_running:
                    await self._reset_session_if_allowed()

                await self._notify_failure(exc)

            except Exception as exc:
                logger.exception("Namu fill websocket failed")
                await self._notify_failure(exc)

            if not self.keep_running:
                break

            delay = self._retry_delay
            # “3초 뒤에 재접속을 시도하겠다”는 안내 로그
            logger.info(
                "Namu websocket reconnect scheduled | delay=%s",
                delay,
            )
            await asyncio.sleep(delay)
            self._retry_delay = min(delay * 2, 30.0)

    async def _reset_session_if_allowed(self) -> None:
        if not self.keep_running:
            logger.info("Namu session reset skipped | stopping")
            return

        if self.session_resetter is None:
            logger.warning(
                "Namu session reset skipped | "
                "session_resetter is None; "
                "check NAMU_WS_SESSION_RESET_ENABLED"
            )
            return

        loop = asyncio.get_running_loop()
        now = loop.time()

        if now < self._next_reset_at:
            logger.warning(
                "Namu session reset deferred | remaining=%.1fs",
                self._next_reset_at - now,
            )
            return

        # 실패하더라도 곧바로 다시 초기화하지 않도록 먼저 기록
        self._next_reset_at = now + self._reset_cooldown

        try:
            logger.warning("Namu websocket session reset requested")

            async with asyncio.timeout(20):
                await self.session_resetter()

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Namu websocket session reset failed")

    # 실패 알림
    async def _notify_failure(self, exc: Exception) -> None:
        if not self.keep_running or self.on_failure is None:
            return

        try:
            async with asyncio.timeout(10):
                await self.on_failure(
                    "namu.fill_websocket",
                    "[failure] Namu fill WebSocket failed",
                    f"{type(exc).__name__}: {exc}",
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Namu websocket failure alert failed")

    async def _run_once(self) -> None:
        import websockets
        from websockets.exceptions import ConnectionClosedOK

        token = await self.token_provider()
        url = self.uri or _default_websocket_url()

        try:
            async with websockets.connect(
                url,
                ssl=_ssl_context(),
                ping_interval=60,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=10,
            ) as websocket:
                self.websocket = websocket
                self.subscribed = False

                logger.info(
                    "Namu fill websocket connected | url=%s",
                    url,
                )

                await self._register(token)

                loop = asyncio.get_running_loop()
                ack_deadline = loop.time() + 10.0

                while self.keep_running:
                    if self.subscribed:
                        # 구독 이후에는 체결이 없다고 연결을 끊지 않음
                        message = await websocket.recv()
                    else:
                        # 최초 구독 응답에만 제한 시간 적용
                        remaining = ack_deadline - loop.time()

                        if remaining <= 0:
                            raise TimeoutError(
                                "Namu subscription ACK timed out"
                            )

                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=remaining,
                        )

                    response = _parse_message(message)

                    if not response:
                        continue

                    if _is_ack(response):
                        _raise_for_ack(response)

                        header = response.get("header") or {}
                        body = response.get("body") or {}

                        if (
                            str(header.get("tr_type")) == "1"
                            and body.get("tr_cd", NOTICE_TR_CD)
                            == NOTICE_TR_CD
                        ):
                            self.subscribed = True
                            self._retry_delay = 3.0

                            logger.info(
                                "Namu fill websocket "
                                "subscription acknowledged"
                            )

                        continue

                    for event in extract_fill_events(response):
                        await self.on_fill(event)

        except ConnectionClosedOK:
            logger.info("Namu fill websocket closed normally")

        finally:
            self.websocket = None
            self.subscribed = False

    async def _register(self, token: str) -> None:
        await self._send(
            {
                "header": {
                    "token": token,
                    "tr_type": "1",
                },
                "body": {
                    "tr_cd": NOTICE_TR_CD,
                    "tr_key": "",
                },
            }
        )

    async def _unregister(self) -> None:
        await self._send(
            {
                "header": {
                    "token": await self.token_provider(),
                    "tr_type": "2",
                },
                "body": {
                    "tr_cd": NOTICE_TR_CD,
                    "tr_key": "",
                },
            }
        )

    async def _send(self, message: dict) -> None:
        if not self.websocket:
            return

        await self.websocket.send(json.dumps(message))


def extract_fill_events(response: dict) -> list[FillEvent]:
    events = []

    for row in _iter_payload_rows(response):
        side = _first_text(row, "slbygb", "907", "매도매수구분")

        if side and side != "2":
            continue

        order_no = _first_text(row, "orderno", "9203", "ord_no", "ordno", "주문번호")
        filled_quantity = _first_int(row, "concgty", "911", "915", "cntr_qty", "체결수량")
        filled_price = _first_int(row, "concprc", "910", "914", "cntr_pric", "cntr_uv", "체결가")

        if not order_no or filled_quantity <= 0 or filled_price <= 0:
            continue

        stock_code = _first_text(row, "issuecd", "9001", "stk_cd", "종목코드").replace("A", "")
        stock_name = _first_text(row, "issue_nm", "302", "stk_nm", "종목명")
        filled_amount = filled_quantity * filled_price

        events.append(
            FillEvent(
                order_no=order_no,
                stock_code=stock_code,
                stock_name=stock_name,
                filled_quantity=filled_quantity,
                filled_price=filled_price,
                filled_amount=filled_amount,
                raw=row,
            )
        )

    return events


def _default_websocket_url() -> str:
    try:
        from nhplug import get_base_url
    except ImportError as exc:
        raise RuntimeError('Install the official SDK first: pip install "nhplug[tls]"') from exc

    explicit = os.getenv("NHPLUG_WS_URL")

    if explicit:
        url = explicit.strip().rstrip("/")
        return url if urlsplit(url).path else f"{url}{WS_PATH}"

    host = get_base_url().split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
    port = PORT_MOCK if host.startswith("moapi") else PORT_DOMESTIC
    return f"wss://{host}:{port}{WS_PATH}"


def _ssl_context() -> ssl.SSLContext | None:
    try:
        import truststore
    except ImportError:
        return None

    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


def _parse_message(message) -> dict:
    if isinstance(message, bytes):
        message = message.decode("utf-8")

    try:
        data = json.loads(message)
    except (TypeError, json.JSONDecodeError):
        return {}

    return data if isinstance(data, dict) else {}


def _is_ack(message: dict) -> bool:
    header = message.get("header")
    return isinstance(header, dict) and ("tr_type" in header or "rsp_cd" in header)


def _raise_for_ack(message: dict) -> None:
    header = message.get("header") or {}
    code = str(header.get("rsp_cd") or "")

    if code != WS_ACK_OK:
        raise NamuSubscriptionError(
            code=code or "MISSING_ACK_CODE",
            message=str(header.get("rsp_msg") or ""),
        )

def _iter_payload_rows(value) -> list[dict]:
    if isinstance(value, dict):
        rows = []

        if "body" in value:
            rows.extend(_iter_payload_rows(value["body"]))

        if "data" in value:
            rows.extend(_iter_payload_rows(value["data"]))

        if "values" in value:
            rows.extend(_iter_payload_rows(value["values"]))

        if any(key in value for key in ("orderno", "9203", "ord_no", "ordno", "주문번호")):
            rows.append(value)

        return rows

    if isinstance(value, list):
        rows = []

        for item in value:
            rows.extend(_iter_payload_rows(item))

        return rows

    return []


def _first_text(row: dict, *keys: str) -> str:
    for key in keys:
        value = row.get(key)

        if value is not None and str(value).strip():
            return str(value).strip()

    return ""


def _first_int(row: dict, *keys: str) -> int:
    for key in keys:
        value = row.get(key)
        number = _to_int(value)

        if number > 0:
            return number

    return 0


def _to_int(value) -> int:
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        return abs(int(value))

    text = str(value).replace(",", "").strip()

    if not text:
        return 0

    try:
        return abs(int(float(text)))
    except ValueError:
        return 0

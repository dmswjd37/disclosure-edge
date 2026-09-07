import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable


logger = logging.getLogger(__name__)

AuthProvider = Callable[[], Awaitable[None]]
FillHandler = Callable[["FillEvent"], Awaitable[None]]
FailureHandler = Callable[[str, str, str], Awaitable[None]]


@dataclass(frozen=True)
class FillEvent:
    order_no: str
    stock_code: str
    stock_name: str
    filled_quantity: int
    filled_price: int
    filled_amount: int
    raw: dict


class NamuFillWebSocketClient:
    def __init__(
        self,
        uri: str | None,
        auth_provider: AuthProvider,
        on_fill: FillHandler,
        on_failure: FailureHandler | None = None,
        timeout_seconds: int = 30,
    ):
        self.uri = uri
        self.auth_provider = auth_provider
        self.on_fill = on_fill
        self.on_failure = on_failure
        self.timeout_seconds = timeout_seconds
        self.keep_running = False
        self.task: asyncio.Task | None = None

    def start(self) -> None:
        if self.task and not self.task.done():
            return

        self.keep_running = True
        self.task = asyncio.create_task(self.run(), name="namu-fill-websocket")

    async def stop(self) -> None:
        self.keep_running = False

        if self.task:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

            self.task = None

    async def run(self) -> None:
        while self.keep_running:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Namu fill websocket failed")

                if self.on_failure:
                    await self.on_failure(
                        "namu.fill_websocket",
                        "[장애] 나무 체결 WebSocket 실패",
                        f"{type(exc).__name__}: {exc}",
                    )

                await asyncio.sleep(3)

    async def _run_once(self) -> None:
        try:
            from nhplug.realtime import subscribe
        except ImportError as exc:
            raise RuntimeError('Install the official SDK first: pip install "nhplug[tls]"') from exc

        await self.auth_provider()
        loop = asyncio.get_running_loop()
        pending: set[asyncio.Future] = set()

        def on_message(message: dict) -> None:
            logger.info("Namu fill websocket message: %s", message)

            for fill_event in extract_fill_events(message):
                future = asyncio.run_coroutine_threadsafe(self.on_fill(fill_event), loop)
                pending.add(future)
                future.add_done_callback(pending.discard)

        await asyncio.to_thread(
            subscribe,
            [],
            on_message,
            tr_cd="d2",
            timeout=self.timeout_seconds,
            url=self.uri,
        )

        if pending:
            await asyncio.gather(*[asyncio.wrap_future(future) for future in pending])

        logger.info("Namu order fill realtime subscription ended")


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

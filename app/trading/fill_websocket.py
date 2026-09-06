import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable


logger = logging.getLogger(__name__)

TokenProvider = Callable[[], Awaitable[str]]
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


class KiwoomFillWebSocketClient:
    def __init__(
        self,
        uri: str,
        token_provider: TokenProvider,
        on_fill: FillHandler,
        on_failure: FailureHandler | None = None,
    ):
        self.uri = uri
        self.token_provider = token_provider
        self.on_fill = on_fill
        self.on_failure = on_failure
        self.websocket = None
        self.keep_running = False
        self.task: asyncio.Task | None = None

    def start(self) -> None:
        if self.task and not self.task.done():
            return

        self.keep_running = True
        self.task = asyncio.create_task(self.run(), name="kiwoom-fill-websocket")

    async def stop(self) -> None:
        self.keep_running = False

        if self.websocket:
            await self.websocket.close()

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
                logger.exception("Kiwoom fill websocket failed")
                if self.on_failure:
                    await self.on_failure(
                        "kiwoom.fill_websocket",
                        "[장애] 키움 체결 WebSocket 실패",
                        f"{type(exc).__name__}: {exc}",
                    )
                await asyncio.sleep(3)

    async def _run_once(self) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("Install the websockets package to use fill streaming") from exc

        token = await self.token_provider()

        async with websockets.connect(self.uri) as websocket:
            self.websocket = websocket
            logger.info("Kiwoom fill websocket connected")

            await self._send(
                {
                    "trnm": "LOGIN",
                    "token": token,
                }
            )

            async for message in websocket:
                response = json.loads(message)
                trnm = response.get("trnm")

                if trnm == "LOGIN":
                    if response.get("return_code") != 0:
                        raise RuntimeError(
                            f"Kiwoom websocket login failed: {response.get('return_msg')}"
                        )

                    logger.info("Kiwoom fill websocket login succeeded")
                    await self._register_order_fill()
                    continue

                if trnm == "PING":
                    await self._send(response)
                    continue

                logger.info("Kiwoom fill websocket message: %s", response)

                for fill_event in extract_fill_events(response):
                    await self.on_fill(fill_event)

    async def _send(self, message: dict) -> None:
        await self.websocket.send(json.dumps(message))

    async def _register_order_fill(self) -> None:
        await self._send(
            {
                "trnm": "REG",
                "grp_no": "1",
                "refresh": "1",
                "data": [
                    {
                        "item": [""],
                        "type": ["00"],
                    }
                ],
            }
        )
        logger.info("Kiwoom order fill realtime registered")


def extract_fill_events(response: dict) -> list[FillEvent]:
    events = []

    for row in _iter_payload_rows(response):
        order_status = _first_text(row, "913", "주문상태")
        side = _first_text(row, "907", "매도수구분")
        order_type = _first_text(row, "905", "주문구분")

        if "체결" not in order_status:
            continue

        if side and side != "2":
            continue

        if order_type and "매수" not in order_type:
            continue

        order_no = _first_text(row, "9203", "ord_no", "ordno", "주문번호")
        filled_quantity = _first_int(row, "911", "915", "cntr_qty", "체결량", "체결수량")
        filled_price = _first_int(row, "910", "914", "cntr_pric", "cntr_uv", "체결가", "체결단가")

        if not order_no or filled_quantity <= 0 or filled_price <= 0:
            continue

        stock_code = _first_text(row, "9001", "stk_cd", "종목코드").replace("A", "")
        stock_name = _first_text(row, "302", "stk_nm", "종목명")
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

        if "data" in value:
            rows.extend(_iter_payload_rows(value["data"]))

        if "values" in value:
            rows.extend(_iter_payload_rows(value["values"]))

        if any(key in value for key in ("9203", "ord_no", "ordno", "주문번호")):
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

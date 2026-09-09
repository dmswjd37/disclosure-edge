import logging
from dataclasses import asdict, dataclass
from datetime import date
from typing import Awaitable, Callable

from app.trading.config import TradingSettings, get_trading_settings
from app.trading.fill_websocket import FillEvent, NamuFillWebSocketClient
from app.trading.namu_client import NamuClient
from app.trading.order_repository import OrderRepository, new_order_record


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TradingResult:
    attempted: bool
    ordered: bool
    status: str
    reason: str
    stock_code: str
    stock_name: str
    rcp_no: str
    order_no: str = ""
    order_amount: int = 0
    quantity: int = 0
    price: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


class TradingService:
    def __init__(
        self,
        settings: TradingSettings | None = None,
        client: NamuClient | None = None,
        repository: OrderRepository | None = None,
        fill_handler: Callable[[FillEvent], Awaitable[None]] | None = None,
        failure_handler: Callable[[str, str, str], Awaitable[None]] | None = None,
    ):
        self.settings = settings or get_trading_settings()
        self.client = client or NamuClient(self.settings.namu)
        self.repository = repository or OrderRepository(self.settings.risk.state_path)
        self.fill_handler = fill_handler
        self.failure_handler = failure_handler
        self.fill_stream: NamuFillWebSocketClient | None = None
        self._auto_buy_enabled = self.settings.namu.auto_buy_enabled

    @property
    def auto_buy_enabled(self) -> bool:
        return self._auto_buy_enabled

    @property
    def dry_run(self) -> bool:
        return self.settings.namu.dry_run

    async def set_auto_buy_enabled(self, enabled: bool) -> None:
        self._auto_buy_enabled = enabled

        if enabled:
            self.start_fill_stream()
            logger.info("Auto buy enabled")
            return

        await self.stop_fill_stream()
        logger.info("Auto buy disabled")

    def start_fill_stream(self) -> None:
        if self.dry_run or not self.auto_buy_enabled:
            return

        socket_url = self.settings.namu.socket_url

        if self.fill_stream is None:
            self.fill_stream = NamuFillWebSocketClient(
                uri=socket_url,
                token_provider=self.client.get_websocket_token,
                on_fill=self.handle_fill_event,
                on_failure=self.failure_handler,
            )

        self.fill_stream.start()

    async def stop_fill_stream(self) -> None:
        if self.fill_stream:
            await self.fill_stream.stop()

    async def try_auto_buy(self, disclosure: dict) -> TradingResult:
        stock_code = (disclosure.get("stock_code") or "").strip()
        stock_name = disclosure.get("stock_name") or ""
        rcp_no = disclosure.get("rcp_no") or ""

        if not self.auto_buy_enabled:
            return self._skip(disclosure, "auto buy is disabled")

        if not stock_code:
            return self._skip(disclosure, "stock code is empty")

        order_date = date.today().strftime("%Y%m%d")

        if self.repository.has_receipt(rcp_no):
            return self._skip(disclosure, "receipt already ordered")

        if self.repository.has_stock_on_date(stock_code, order_date):
            return self._skip(disclosure, "stock already ordered today")

        daily_bought_amount = self.repository.bought_amount_on_date(order_date)
        daily_remaining = self.settings.risk.daily_max_buy_amount - daily_bought_amount

        if daily_remaining <= 0:
            return self._skip(disclosure, "daily buy limit reached")

        if not self.settings.namu.configured:
            return self._skip(disclosure, "namu credentials are not configured")

        await self.client.ensure_authenticated()
        # 보유종목과 잔고 확인
        account = await self.client.get_account_snapshot()
        holdings = account.holdings

        if _has_holding(holdings, stock_code):
            return self._skip(disclosure, "stock is already held")

        cash_balance = account.cash_balance

        if cash_balance < self.settings.risk.min_cash_balance:
            return self._skip(disclosure, "cash balance is below minimum")

        # 최우선 매도호가
        price = await self.client.get_best_ask_price(stock_code)

        if price <= 0:
            return self._skip(disclosure, "best ask price is invalid")

        order_amount = min(
            self.settings.risk.order_budget,
            self.settings.risk.daily_max_buy_amount,
            daily_remaining,
            cash_balance,
        )
        quantity = order_amount // price

        if quantity <= 0:
            return self._skip(disclosure, "calculated quantity is zero")

        actual_order_amount = quantity * price

        if self.dry_run:
            self.repository.append(
                new_order_record(
                    disclosure=disclosure,
                    order_no="",
                    order_date=order_date,
                    order_amount=actual_order_amount,
                    quantity=quantity,
                    price=price,
                    status="dry_run",
                )
            )
            logger.info(
                "Auto buy dry-run completed | stock_code=%s | quantity=%s | price=%s",
                stock_code,
                quantity,
                price,
            )
            return TradingResult(
                attempted=True,
                ordered=False,
                status="dry_run",
                reason="dry run",
                stock_code=stock_code,
                stock_name=stock_name,
                rcp_no=rcp_no,
                order_amount=actual_order_amount,
                quantity=quantity,
                price=price,
            )

        # 매수 신청
        submission = await self.client.buy_stock(stock_code, quantity, price)

        if submission.return_code != 0:
            logger.warning(
                "Auto buy rejected | stock_code=%s | return_code=%s | message=%s",
                stock_code,
                submission.return_code,
                submission.return_message,
            )
            return TradingResult(
                attempted=True,
                ordered=False,
                status="rejected",
                reason=f"namu return_code={submission.return_code}",
                stock_code=stock_code,
                stock_name=stock_name,
                rcp_no=rcp_no,
                order_no=submission.order_no,
                order_amount=actual_order_amount,
                quantity=quantity,
                price=price,
            )

        self.repository.append(
            new_order_record(
                disclosure=disclosure,
                order_no=submission.order_no,
                order_date=order_date,
                order_amount=actual_order_amount,
                quantity=quantity,
                price=price,
                status="ordered",
            )
        )
        logger.info(
            "Auto buy submitted | stock_code=%s | order_no=%s | quantity=%s | price=%s",
            stock_code,
            submission.order_no,
            quantity,
            price,
        )
        return TradingResult(
            attempted=True,
            ordered=True,
            status="ordered",
            reason="order submitted",
            stock_code=stock_code,
            stock_name=stock_name,
            rcp_no=rcp_no,
            order_no=submission.order_no,
            order_amount=actual_order_amount,
            quantity=quantity,
            price=price,
        )

    async def handle_fill_event(self, event: FillEvent) -> None:
        self.repository.mark_filled(
            order_no=event.order_no,
            filled_quantity=event.filled_quantity,
            filled_price=event.filled_price,
            filled_amount=event.filled_amount,
        )
        logger.info(
            "Auto buy filled | stock_code=%s | order_no=%s | filled_quantity=%s | filled_price=%s",
            event.stock_code,
            event.order_no,
            event.filled_quantity,
            event.filled_price,
        )

        if self.fill_handler:
            await self.fill_handler(event)

    def status(self) -> dict:
        return {
            "auto_buy_enabled": self.auto_buy_enabled,
            "dry_run": self.dry_run,
            "account_type": self.settings.namu.account_type,
            "namu_configured": self.settings.namu.configured,
            "fill_stream_running": bool(
                self.fill_stream
                and self.fill_stream.task
                and not self.fill_stream.task.done()
            ),
            "order_budget": self.settings.risk.order_budget,
            "min_cash_balance": self.settings.risk.min_cash_balance,
            "daily_max_buy_amount": self.settings.risk.daily_max_buy_amount,
            "recent_orders": [
                asdict(record)
                for record in self.repository.recent_records()
            ],
        }

    def _skip(self, disclosure: dict, reason: str) -> TradingResult:
        logger.info(
            "Auto buy skipped | rcp_no=%s | stock_code=%s | reason=%s",
            disclosure.get("rcp_no"),
            disclosure.get("stock_code"),
            reason,
        )
        return TradingResult(
            attempted=False,
            ordered=False,
            status="skipped",
            reason=reason,
            stock_code=disclosure.get("stock_code") or "",
            stock_name=disclosure.get("stock_name") or "",
            rcp_no=disclosure.get("rcp_no") or "",
        )


def _has_holding(holdings: list[dict], stock_code: str) -> bool:
    for holding in holdings:
        holding_code = str(holding.get("stk_cd") or "").replace("A", "")

        if holding_code == stock_code:
            return True

    return False

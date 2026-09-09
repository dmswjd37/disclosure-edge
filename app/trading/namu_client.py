import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderSubmission:
    return_code: int | None
    return_message: str
    order_no: str


@dataclass(frozen=True)
class AccountSnapshot:
    holdings: list[dict]
    cash_balance: int


class NamuClient:
    """NH투자증권 Namuh PLUG SDK adapter for the trading service."""

    def __init__(self, settings: Any | None = None):
        self.settings = settings
        self._account_no: str | None = _setting_value(settings, "account_no")

    async def ensure_authenticated(self) -> None:
        await asyncio.to_thread(self._ensure_authenticated_sync)

    async def get_websocket_token(self) -> str:
        return await asyncio.to_thread(self._get_websocket_token_sync)

    # 보유 종목만 필요할 때 사용합니다. 현금도 필요하면 get_account_snapshot을 우선 사용하세요.
    async def get_holdings(self) -> list[dict]:
        return await asyncio.to_thread(self._get_holdings_sync)

    # 주문 가능 현금만 필요할 때 사용합니다. 보유 종목도 필요하면 get_account_snapshot을 우선 사용하세요.
    async def get_cash_balance(self) -> int:
        return await asyncio.to_thread(self._get_cash_balance_sync)

    # 잔고를 한 번만 조회해 자동매수 판단에 필요한 보유 종목과 현금을 함께 추출합니다.
    async def get_account_snapshot(self) -> AccountSnapshot:
        return await asyncio.to_thread(self._get_account_snapshot_sync)

    async def get_best_ask_price(self, stock_code: str) -> int:
        return await asyncio.to_thread(self._get_best_ask_price_sync, stock_code)

    async def buy_stock(
        self,
        stock_code: str,
        quantity: int,
        price: int,
    ) -> OrderSubmission:
        return await asyncio.to_thread(self._buy_stock_sync, stock_code, quantity, price)

    def _ensure_authenticated_sync(self) -> None:
        self._call("/n2/acctinfo", {})

    def _get_websocket_token_sync(self) -> str:
        try:
            from nhplug import get_token
        except ImportError as exc:
            raise RuntimeError('Install the official SDK first: pip install "nhplug[tls]"') from exc

        return get_token()

    def _get_holdings_sync(self) -> list[dict]:
        data = self._balance()
        return self._extract_holdings(data)

    def _get_cash_balance_sync(self) -> int:
        data = self._balance()
        return self._extract_cash_balance(data)

    def _get_account_snapshot_sync(self) -> AccountSnapshot:
        data = self._balance()
        return AccountSnapshot(
            holdings=self._extract_holdings(data),
            cash_balance=self._extract_cash_balance(data),
        )

    def _extract_holdings(self, data: dict) -> list[dict]:
        holdings = data.get("Output_1") or []

        for holding in holdings:
            if "stk_cd" not in holding and holding.get("iem_cd"):
                holding["stk_cd"] = _normalize_stock_code(holding["iem_cd"])

        return holdings

    def _extract_cash_balance(self, data: dict) -> int:
        summary = data.get("Output_0") or {}

        return _first_int(
            summary,
            "orr_pbl_amt",
            "csh_wtm",
            "sba_amt",
            "dca",
            "nas_amt",
            "tot_aet_amt",
        )

    def _get_best_ask_price_sync(self, stock_code: str) -> int:
        data = self._call(
            "/krstock/quote/v1/currentPrice",
            {
                "iem_cd": _normalize_stock_code(stock_code),
                "market_cd": self._market_cd,
            },
        )
        quote = data.get("Output_0") or {}

        return _first_int(quote, "askp1", "askp", "stck_prpr", "bidp1", "bidp")

    def _buy_stock_sync(
        self,
        stock_code: str,
        quantity: int,
        price: int,
    ) -> OrderSubmission:
        data = self._call(
            "/krstock/order/v1/cashBuy",
            {
                "act_no": self._get_account_no(),
                "iem_cd": _normalize_stock_code(stock_code),
                "orr_qty": quantity,
                "nmn_pr_tp_cd": "01",
                "orr_cnd_dit_cd": "00",
                "ssl_nmn_pr_dit_cd": "00",
                "rmt_mkt_cd": self._market_cd,
                "sor_mkt_sli_yn": "N",
                "orr_pr": price,
            },
        )
        output = data.get("Output_0") or {}
        message = data.get("message") or {}

        return OrderSubmission(
            return_code=0,
            return_message=str(
                data.get("rsp_msg")
                or message.get("rsp_msg")
                or message.get("message")
                or ""
            ),
            order_no=str(
                output.get("mkt_orr_no")
                or output.get("anw_cld_mkt_orr_no1")
                or output.get("anw_cld_mkt_orr_no2")
                or ""
            ),
        )

    def _balance(self) -> dict:
        return self._call(
            "/krstock/inquiry/v1/balance",
            {
                "act_no": self._get_account_no(),
                "bnc_bse_cd": "5",
                "ltg_aot_dit_cd": "9",
                "aet_bse": "2",
                "qut_dit_cd": self._market_cd,
            },
        )

    def _get_account_no(self) -> str:
        if self._account_no:
            return self._account_no

        account_no = os.getenv("NHPLUG_DEFAULT_ACCOUNT")
        if account_no:
            self._account_no = account_no.strip()
            return self._account_no

        accounts = self._call("/n2/acctinfo", {}).get("Output_0") or []
        usable_accounts = [
            account
            for account in accounts
            if _account_matches_base_url(account, self._base_url)
        ]
        selected = (usable_accounts or accounts or [{}])[0].get("acct_no")

        if not selected:
            raise RuntimeError("NHPLUG_DEFAULT_ACCOUNT is not set and no account was returned")

        self._account_no = str(selected).strip()
        return self._account_no

    def _call(self, path: str, payload: dict) -> dict:
        try:
            from nhplug import call
        except ImportError as exc:
            raise RuntimeError('Install the official SDK first: pip install "nhplug[tls]"') from exc

        data = call(path, payload)
        logger.info("Namu API response | path=%s", path)
        return data

    @property
    def _market_cd(self) -> str:
        return str(
            _setting_value(self.settings, "market_cd")
            or os.getenv("NHPLUG_MARKET_CD")
            or "UNT"
        ).strip()

    @property
    def _base_url(self) -> str:
        try:
            from nhplug import get_base_url
        except ImportError:
            return os.getenv("NHPLUG_BASE_URL", "")

        return get_base_url()


def _account_matches_base_url(account: dict, base_url: str) -> bool:
    acct_type = str(account.get("acct_type") or "").strip()
    host = base_url.split("//")[-1].split("/")[0].lower()

    if host.startswith("moapi."):
        return acct_type == "03"

    if host.startswith("api."):
        return acct_type in {"01", "02"}

    return True


def _normalize_stock_code(stock_code: str) -> str:
    return str(stock_code).strip().upper().removeprefix("A")


def _setting_value(settings: Any | None, name: str) -> Any | None:
    if settings is None:
        return None

    return getattr(settings, name, None)


def _first_int(row: dict, *keys: str) -> int:
    for key in keys:
        number = _to_int(row.get(key))

        if number > 0:
            return number

    return 0


def _to_int(value: Any) -> int:
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

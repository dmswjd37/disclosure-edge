import asyncio
import json
import logging
from dataclasses import dataclass
from urllib.request import Request, urlopen

from app.trading.config import KiwoomSettings


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderSubmission:
    return_code: int | None
    return_message: str
    order_no: str


class KiwoomClient:
    def __init__(self, settings: KiwoomSettings):
        self.settings = settings

    async def get_access_token(self) -> str:
        return await asyncio.to_thread(self._get_access_token_sync)

    async def get_holdings(self, token: str) -> list[dict]:
        return await asyncio.to_thread(self._get_holdings_sync, token)

    async def get_cash_balance(self, token: str) -> int:
        return await asyncio.to_thread(self._get_cash_balance_sync, token)

    async def get_best_ask_price(self, stock_code: str, token: str) -> int:
        return await asyncio.to_thread(self._get_best_ask_price_sync, stock_code, token)

    async def buy_stock(
        self,
        stock_code: str,
        quantity: int,
        price: int,
        token: str,
    ) -> OrderSubmission:
        return await asyncio.to_thread(self._buy_stock_sync, stock_code, quantity, price, token)

    def _get_access_token_sync(self) -> str:
        if not self.settings.configured:
            raise RuntimeError("Kiwoom credentials are not configured")

        data = self._post_json(
            endpoint="/oauth2/token",
            headers={
                "Content-Type": "application/json;charset=UTF-8",
            },
            payload={
                "grant_type": "client_credentials",
                "appkey": self.settings.app_key,
                "secretkey": self.settings.app_secret,
            },
        )
        token = data.get("token")

        if not token:
            raise RuntimeError("Kiwoom access token was not returned")

        return str(token)

    def _get_holdings_sync(self, token: str) -> list[dict]:
        data = self._post_json(
            endpoint="/api/dostk/acnt",
            headers=self._auth_headers(token, api_id="kt00004"),
            payload={
                "qry_tp": "0",
                "dmst_stex_tp": "KRX",
            },
        )

        return data.get("stk_acnt_evlt_prst") or []

    def _get_cash_balance_sync(self, token: str) -> int:
        data = self._post_json(
            endpoint="/api/dostk/acnt",
            headers=self._auth_headers(token, api_id="kt00001"),
            payload={
                "qry_tp": "3",
            },
        )

        return _to_int(data.get("entr"))

    def _get_best_ask_price_sync(self, stock_code: str, token: str) -> int:
        data = self._post_json(
            endpoint="/api/dostk/mrkcond",
            headers=self._auth_headers(token, api_id="ka10004"),
            payload={
                "stk_cd": stock_code,
            },
        )

        return abs(_to_int(data.get("sel_fpr_bid")))

    # 실제 주문
    def _buy_stock_sync(
        self,
        stock_code: str,
        quantity: int,
        price: int,
        token: str,
    ) -> OrderSubmission:
        data = self._post_json(
            endpoint="/api/dostk/ordr",
            headers=self._auth_headers(token, api_id="kt10000"),
            payload={
                "dmst_stex_tp": "KRX",
                "stk_cd": stock_code,
                "ord_qty": str(quantity),
                "ord_uv": str(price),
                "trde_tp": "0",
                "cond_uv": "",
            },
        )

        return OrderSubmission(
            return_code=_to_optional_int(data.get("return_code")),
            return_message=str(data.get("return_msg") or ""),
            order_no=str(data.get("ord_no") or ""),
        )

    def _auth_headers(self, token: str, api_id: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}",
            "cont-yn": "N",
            "next-key": "",
            "api-id": api_id,
        }

    def _post_json(self, endpoint: str, headers: dict[str, str], payload: dict) -> dict:
        url = f"{self.settings.host_url}{endpoint}"
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers=headers,
        )

        with urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
            status = response.status

        data = json.loads(body)
        logger.info("Kiwoom API response | endpoint=%s | status=%s", endpoint, status)
        return data


def _to_int(value) -> int:
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).replace(",", "").strip()

    if not text:
        return 0

    return int(float(text))


def _to_optional_int(value) -> int | None:
    if value is None:
        return None

    return _to_int(value)

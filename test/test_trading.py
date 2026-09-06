import asyncio
import tempfile
import unittest
from pathlib import Path

from app.trading.config import KiwoomSettings, TradingRiskSettings, TradingSettings
from app.trading.fill_websocket import extract_fill_events
from app.trading.order_repository import OrderRepository
from app.trading.service import TradingService


class FakeKiwoomClient:
    async def get_access_token(self) -> str:
        return "token"

    async def get_holdings(self, token: str) -> list[dict]:
        return []

    async def get_cash_balance(self, token: str) -> int:
        return 10_000_000

    async def get_best_ask_price(self, stock_code: str, token: str) -> int:
        return 10_000

    async def buy_stock(self, stock_code: str, quantity: int, price: int, token: str) -> int:
        return 0


def make_settings(path: Path, auto_buy_enabled: bool = True) -> TradingSettings:
    return TradingSettings(
        kiwoom=KiwoomSettings(
            auto_buy_enabled=auto_buy_enabled,
            dry_run=True,
            is_paper_trading=True,
            real_app_key=None,
            real_app_secret=None,
            real_host_url=None,
            real_socket_url=None,
            paper_app_key="key",
            paper_app_secret="secret",
            paper_host_url="https://example.test",
            paper_socket_url="wss://example.test",
        ),
        risk=TradingRiskSettings(
            order_budget=4_000_000,
            min_cash_balance=5_000_000,
            daily_max_buy_amount=4_000_000,
            state_path=path,
        ),
    )


class TradingServiceTest(unittest.TestCase):
    def test_dry_run_records_order_and_blocks_duplicate_receipt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = OrderRepository(Path(temp_dir) / "orders.json")
            service = TradingService(
                settings=make_settings(repository.path),
                client=FakeKiwoomClient(),
                repository=repository,
            )
            disclosure = {
                "rcp_no": "20260904000047",
                "stock_code": "049430",
                "stock_name": "sample",
                "report_name": "report",
            }

            first = asyncio.run(service.try_auto_buy(disclosure))
            second = asyncio.run(service.try_auto_buy(disclosure))

            self.assertEqual(first.status, "dry_run")
            self.assertEqual(first.quantity, 400)
            self.assertEqual(second.status, "skipped")
            self.assertEqual(second.reason, "receipt already ordered")

    def test_disabled_auto_buy_skips_before_kiwoom_call(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = OrderRepository(Path(temp_dir) / "orders.json")
            service = TradingService(
                settings=make_settings(repository.path, auto_buy_enabled=False),
                client=FakeKiwoomClient(),
                repository=repository,
            )

            result = asyncio.run(
                service.try_auto_buy(
                    {
                        "rcp_no": "1",
                        "stock_code": "000000",
                        "stock_name": "sample",
                        "report_name": "report",
                    }
                )
            )

            self.assertEqual(result.status, "skipped")
            self.assertEqual(result.reason, "auto buy is disabled")

    def test_extract_fill_events_from_realtime_message(self):
        events = extract_fill_events(
            {
                "trnm": "REAL",
                "data": [
                    {
                        "type": "00",
                        "name": "주문체결",
                        "item": "",
                        "values": [
                            {
                                "9203": "12345",
                                "9001": "A049430",
                                "913": "체결",
                                "302": "sample",
                                "905": "+매수",
                                "907": "2",
                                "910": "12000",
                                "911": "10",
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].order_no, "12345")
        self.assertEqual(events[0].stock_code, "049430")
        self.assertEqual(events[0].filled_quantity, 10)
        self.assertEqual(events[0].filled_price, 12000)
        self.assertEqual(events[0].filled_amount, 120000)


if __name__ == "__main__":
    unittest.main()

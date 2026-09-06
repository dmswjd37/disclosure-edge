import asyncio
import unittest

from app.notification.publisher import NotificationEvent
from app.ops.telegram_commands import TelegramCommandMonitor


class FakeTradingService:
    def __init__(self):
        self.auto_buy_enabled = True
        self.dry_run = True

    async def set_auto_buy_enabled(self, enabled: bool) -> None:
        self.auto_buy_enabled = enabled

    def status(self) -> dict:
        return {
            "auto_buy_enabled": self.auto_buy_enabled,
            "dry_run": self.dry_run,
            "account_type": "paper",
            "kiwoom_configured": True,
            "fill_stream_running": False,
        }


class FakePublisher:
    def __init__(self):
        self.events: list[NotificationEvent] = []

    async def publish(self, event: NotificationEvent) -> bool:
        self.events.append(event)
        return True


class TelegramCommandMonitorTest(unittest.TestCase):
    def test_stop_command_disables_auto_buy(self):
        trading_service = FakeTradingService()
        publisher = FakePublisher()
        monitor = TelegramCommandMonitor(
            trading_service=trading_service,
            notification_publisher=publisher,
        )
        monitor.allowed_chat_id = "123"

        asyncio.run(
            monitor._handle_update(
                {
                    "update_id": 1,
                    "message": {
                        "chat": {"id": 123},
                        "text": "stop",
                    },
                }
            )
        )

        self.assertFalse(trading_service.auto_buy_enabled)
        self.assertEqual(publisher.events[-1].event_type, "auto_buy.disabled")

    def test_start_command_enables_stopped_auto_buy(self):
        trading_service = FakeTradingService()
        publisher = FakePublisher()
        monitor = TelegramCommandMonitor(
            trading_service=trading_service,
            notification_publisher=publisher,
        )
        monitor.allowed_chat_id = "123"

        asyncio.run(
            monitor._handle_update(
                {
                    "update_id": 1,
                    "message": {
                        "chat": {"id": 123},
                        "text": "stop",
                    },
                }
            )
        )
        asyncio.run(
            monitor._handle_update(
                {
                    "update_id": 2,
                    "message": {
                        "chat": {"id": 123},
                        "text": "start",
                    },
                }
            )
        )

        self.assertTrue(trading_service.auto_buy_enabled)
        self.assertEqual(publisher.events[-1].event_type, "auto_buy.enabled")


if __name__ == "__main__":
    unittest.main()

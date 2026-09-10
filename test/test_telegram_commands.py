import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.notification.publisher import NotificationEvent
from app.ops.telegram_commands import TelegramCommandMonitor, TelegramPollingError


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
            "namu_configured": True,
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


class TelegramPollingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.monitor = TelegramCommandMonitor(FakeTradingService(), FakePublisher(), AsyncMock())
        self.monitor.bot_token = "test-secret-token"
        self.monitor.allowed_chat_id = "123"

    async def run_responses(self, responses):
        responses = iter(responses)

        def respond(request):
            response = next(responses, None)
            if response is None:
                self.monitor.running = False
                return httpx.Response(200, json={"ok": True, "result": []})
            if isinstance(response, Exception):
                raise response
            return response

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("app.ops.telegram_commands.httpx.AsyncClient", return_value=client) as factory:
            await self.monitor.start()
            factory.assert_called_once()
        self.assertTrue(client.is_closed)
        self.assertFalse(self.monitor.running)

    async def test_transient_failures_backoff_alert_and_recover(self):
        self.monitor._wait_before_retry = AsyncMock()
        with self.assertLogs("app.ops.telegram_commands", level="INFO") as logs:
            await self.run_responses([
                httpx.ConnectTimeout("test-secret-token"),
                httpx.ConnectError("Network is unreachable"),
                httpx.ReadTimeout("test-secret-token"),
                httpx.Response(200, json={"ok": True, "result": []}),
                httpx.ConnectTimeout("test-secret-token"),
            ])
        self.assertEqual([c.args[0] for c in self.monitor._wait_before_retry.await_args_list], [3, 6, 12, 3])
        self.monitor.on_failure.assert_awaited_once()
        self.assertIn("polling recovered", "\n".join(logs.output))
        self.assertNotIn("test-secret-token", "\n".join(logs.output))
        self.assertTrue(all(record.exc_info is None for record in logs.records))

    async def test_conflict_is_visible_and_alert_failure_does_not_stop_polling(self):
        self.monitor._wait_before_retry = AsyncMock()
        self.monitor.on_failure.side_effect = RuntimeError("test-secret-token")
        with self.assertLogs("app.ops.telegram_commands", level="INFO") as logs:
            await self.run_responses([httpx.Response(409, json={"description": "test-secret-token"})])
        self.assertIn("Telegram API 409", "\n".join(logs.output))
        self.assertIn("alert delivery failed", "\n".join(logs.output))
        self.assertNotIn("test-secret-token", "\n".join(logs.output))
        self.monitor.on_failure.assert_awaited_once()

    async def test_rate_limit_honors_retry_after(self):
        self.monitor._wait_before_retry = AsyncMock()
        await self.run_responses([httpx.Response(429, json={"parameters": {"retry_after": 45}})])
        self.monitor._wait_before_retry.assert_awaited_once_with(45)

    async def test_offset_advances_and_unauthorized_chat_cannot_trade(self):
        offsets = []

        def respond(request):
            offsets.append(request.url.params["offset"])
            self.assertEqual(request.url.params["timeout"], "10")
            return httpx.Response(200, json={"ok": True, "result": [
                {"update_id": 12, "message": {"chat": {"id": 999}, "text": "stop"}}
            ]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            for update in await self.monitor._get_updates(client):
                await self.monitor._handle_update(update)
            await self.monitor._get_updates(client)
        self.assertEqual(offsets, ["1", "13"])
        self.assertTrue(self.monitor.trading_service.auto_buy_enabled)
        self.assertEqual(self.monitor.notification_publisher.events, [])

    async def test_cancel_inflight_poll_closes_client(self):
        entered = asyncio.Event()

        async def respond(request):
            entered.set()
            await asyncio.Event().wait()

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("app.ops.telegram_commands.httpx.AsyncClient", return_value=client):
            task = asyncio.create_task(self.monitor.start())
            await asyncio.wait_for(entered.wait(), timeout=1)
            await self.monitor.stop()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(client.is_closed)
        self.assertFalse(self.monitor.running)
        self.monitor.on_failure.assert_not_awaited()

    async def test_stop_interrupts_retry_wait(self):
        self.monitor.running = True
        task = asyncio.create_task(self.monitor._wait_before_retry(30))
        await self.monitor.stop()
        await asyncio.wait_for(task, timeout=1)

    async def test_rejected_and_malformed_responses(self):
        for body, expected in [
            ({"ok": False, "error_code": 401, "description": "test-secret-token"}, TelegramPollingError),
            ({"ok": True, "result": {}}, ValueError),
            ([], ValueError),
        ]:
            with self.subTest(body=body):
                async with httpx.AsyncClient(transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json=body)
                )) as client:
                    with self.assertRaises(expected) as error:
                        await self.monitor._get_updates(client)
                    self.assertNotIn("test-secret-token", str(error.exception))


if __name__ == "__main__":
    unittest.main()

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.disclosure.monitor import DisclosureMonitor
from app.logging_config import configure_logging
from app.notification.publisher import NotificationEvent
from app.notification.telegram import TelegramNotificationPublisher
from app.ops import TelegramCommandMonitor


configure_logging()


telegram_publisher = TelegramNotificationPublisher()
monitor = DisclosureMonitor(
    notification_publisher=telegram_publisher
)
telegram_command_monitor = TelegramCommandMonitor(
    trading_service=monitor.trading_service,
    notification_publisher=telegram_publisher,
    on_failure=monitor._publish_failure_alert,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    polling_task = asyncio.create_task(
        monitor.start()
    )
    command_task = asyncio.create_task(
        telegram_command_monitor.start()
    )

    try:
        yield
    finally:
        await telegram_command_monitor.stop()
        await monitor.stop()
        polling_task.cancel()
        command_task.cancel()

        try:
            await polling_task
        except asyncio.CancelledError:
            pass

        try:
            await command_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="DisclosureEdge",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {
        "status": "UP",
        "dart_polling": monitor.running,
        "telegram_alert_enabled": telegram_publisher.enabled,
        "telegram_configured": telegram_publisher.configured,
        "telegram_commands_running": telegram_command_monitor.running,
        "auto_buy_enabled": monitor.trading_service.auto_buy_enabled,
        "auto_buy_dry_run": monitor.trading_service.dry_run,
    }


@app.get("/dart/status")
async def dart_status():
    return {
        "running": monitor.running,
        "polling_time": monitor.service.is_polling_time(),
        "polling_start": monitor.service.polling_start.strftime("%H:%M"),
        "polling_end": monitor.service.polling_end.strftime("%H:%M"),
        "polling_weekdays_only": monitor.service.polling_weekdays_only,
        "seen_rcp_nos": sorted(monitor.service.seen_rcp_nos),
    }


@app.get("/trading/status")
async def trading_status():
    return monitor.trading_service.status()


@app.post("/ops/auto-buy/enable")
async def enable_auto_buy():
    await monitor.trading_service.set_auto_buy_enabled(True)

    return monitor.trading_service.status()


@app.post("/ops/auto-buy/disable")
async def disable_auto_buy():
    await monitor.trading_service.set_auto_buy_enabled(False)

    return monitor.trading_service.status()


@app.get("/ops/telegram-alert")
async def telegram_alert_status():
    return {
        "enabled": telegram_publisher.enabled,
        "configured": telegram_publisher.configured,
    }


@app.post("/ops/telegram-alert/enable")
async def enable_telegram_alert():
    telegram_publisher.enabled = True

    return {
        "enabled": telegram_publisher.enabled,
        "configured": telegram_publisher.configured,
    }


@app.post("/ops/telegram-alert/disable")
async def disable_telegram_alert():
    telegram_publisher.enabled = False

    return {
        "enabled": telegram_publisher.enabled,
        "configured": telegram_publisher.configured,
    }


@app.post("/ops/telegram-alert/test")
async def test_telegram_alert():
    sent = await telegram_publisher.publish(
        NotificationEvent(
            service="ops",
            event_type="telegram_alert.tested",
            title="[DisclosureEdge] 텔레그램 알림 테스트",
            body="텔레그램 알림 연동이 정상적으로 호출되었습니다.",
        )
    )

    return {
        "sent": sent,
        "enabled": telegram_publisher.enabled,
        "configured": telegram_publisher.configured,
    }

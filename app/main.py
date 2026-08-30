import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.disclosure.monitor import DisclosureMonitor
from app.notification.publisher import NotificationEvent
from app.notification.telegram import TelegramNotificationPublisher


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "[%(levelname)s] "
        "%(name)s - %(message)s"
    )
)


telegram_publisher = TelegramNotificationPublisher()
monitor = DisclosureMonitor(
    notification_publisher=telegram_publisher
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    polling_task = asyncio.create_task(
        monitor.start()
    )

    try:
        yield
    finally:
        await monitor.stop()
        polling_task.cancel()

        try:
            await polling_task
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
    }


@app.get("/dart/status")
async def dart_status():
    browser = monitor.browser

    if not browser.page:
        return {
            "running": False,
        }

    return {
        "running": monitor.running,
        "url": browser.page.url,
        "seen_rcp_nos": sorted(monitor.service.seen_rcp_nos),
    }


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

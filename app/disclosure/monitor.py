import asyncio
import logging

from app.disclosure.service import DisclosureService
from app.notification.publisher import (
    LoggingNotificationPublisher,
    NotificationEvent,
    NotificationPublisher,
)
from app.notification.alert_manager import AlertManager
from app.trading import TradingService
from app.trading.fill_websocket import FillEvent
from app.trading.service import TradingResult


logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 3
OUT_OF_HOURS_MAX_SLEEP_SECONDS = 60


class DisclosureMonitor:

    def __init__(
        self,
        notification_publisher: NotificationPublisher | None = None,
        trading_service: TradingService | None = None,
    ):
        self.service = DisclosureService()
        self.notification_publisher = (
            notification_publisher or LoggingNotificationPublisher()
        )
        self.alert_manager = AlertManager(self.notification_publisher)
        self.trading_service = trading_service or TradingService(
            fill_handler=self._publish_fill_event,
            failure_handler=self._publish_failure_alert,
        )
        self.running = False

    async def start(self):
        self.trading_service.start_fill_stream()

        initial_disclosures = []

        if self.service.is_polling_time():
            try:
                initial_disclosures = await self.service.find_new_disclosures()
            except Exception as exc:
                logger.exception("DART initial polling failed")
                await self._publish_failure_alert(
                    key="dart.initial_polling",
                    title="[장애] DART 초기 조회 실패",
                    body=f"{type(exc).__name__}: {exc}",
                )

        logger.info(
            "DART disclosure monitor initialized | initial_new_count=%s",
            len(initial_disclosures),
        )

        await self._process_disclosures(initial_disclosures)

        self.running = True

        logger.info("DART disclosure monitoring started")

        out_of_hours_logged = False

        while self.running:
            if not self.service.is_polling_time():
                if not out_of_hours_logged:
                    logger.info("DART polling paused outside configured polling time")
                    out_of_hours_logged = True

                await asyncio.sleep(
                    min(
                        OUT_OF_HOURS_MAX_SLEEP_SECONDS,
                        self.service.seconds_until_polling_time(),
                    )
                )
                continue

            out_of_hours_logged = False
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

            try:
                await self._poll_once()

            except Exception:
                logger.exception(
                    "DART polling failed"
                )
                await self._publish_failure_alert(
                    key="dart.polling",
                    title="[장애] DART polling 실패",
                    body="DART 공시 조회 중 오류가 발생했습니다. 로그를 확인하세요.",
                )

    async def _poll_once(self):
        if not self.service.is_polling_time():
            logger.debug("DART polling skipped outside configured polling time")
            return []

        new_disclosures = await self.service.find_new_disclosures()

        if not new_disclosures:
            return []

        return await self._process_disclosures(new_disclosures)

    async def _process_disclosures(
        self,
        disclosures: list[dict],
    ):
        processed_disclosures = []

        for disclosure in disclosures:
            try:
                processed = await self._process_new_disclosure(
                    disclosure
                )
            except Exception as exc:
                logger.exception(
                    "Disclosure processing failed | rcpNo=%s",
                    disclosure.get("rcp_no"),
                )
                await self._publish_failure_alert(
                    key=f"disclosure.process.{disclosure.get('rcp_no')}",
                    title="[장애] 공시 처리 실패",
                    body=(
                        f"접수번호: {disclosure.get('rcp_no') or '-'}\n"
                        f"종목코드: {disclosure.get('stock_code') or '-'}\n"
                        f"오류: {type(exc).__name__}: {exc}"
                    ),
                )
                processed = None

            if processed:
                processed_disclosures.append(processed)

        return processed_disclosures

    async def _process_new_disclosure(
        self,
        disclosure: dict
    ):
        logger.info(
            "New disclosure detected | rcpNo=%s | stock=%s | report=%s",
            disclosure["rcp_no"],
            disclosure["stock_name"],
            disclosure["report_name"],
        )

        if not self.service.is_target_disclosure(disclosure):
            logger.info(
                "Disclosure ignored by target report filter | rcpNo=%s",
                disclosure["rcp_no"],
            )
            return None

        await self.notification_publisher.publish(
            self._build_target_disclosure_event(disclosure)
        )

        trading_result = await self.trading_service.try_auto_buy(disclosure)

        if trading_result.attempted or self.trading_service.auto_buy_enabled:
            await self.notification_publisher.publish(
                self._build_trading_event(trading_result)
            )

        return disclosure

    def _build_target_disclosure_event(self, disclosure: dict) -> NotificationEvent:
        report_name = disclosure.get("report_name", "")
        stock_name = disclosure.get("stock_name", "")
        stock_code = disclosure.get("stock_code", "")
        rcept_dt = disclosure.get("rcept_dt", "")
        rcp_no = disclosure.get("rcp_no", "")
        url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"

        return NotificationEvent(
            service="disclosure",
            event_type="target_disclosure.created",
            title=f"[신규 관심 공시] {stock_name}",
            body=(
                f"보고서: {report_name}\n"
                f"종목코드: {stock_code or '-'}\n"
                f"접수일: {rcept_dt or '-'}\n"
                f"접수번호: {rcp_no}"
            ),
            url=url,
        )

    def _build_trading_event(self, result: TradingResult) -> NotificationEvent:
        status_label = {
            "ordered": "주문접수",
            "dry_run": "dry-run",
            "rejected": "거부",
            "skipped": "스킵",
        }.get(result.status, result.status)
        title = f"[자동매수 {status_label}] {result.stock_name or result.stock_code}"
        body = (
            f"종목코드: {result.stock_code or '-'}\n"
            f"접수번호: {result.rcp_no or '-'}\n"
            f"주문번호: {result.order_no or '-'}\n"
            f"사유: {result.reason}\n"
            f"주문금액: {result.order_amount:,}원\n"
            f"수량: {result.quantity}\n"
            f"가격: {result.price:,}원"
        )

        return NotificationEvent(
            service="trading",
            event_type=f"auto_buy.{result.status}",
            title=title,
            body=body,
        )

    async def _publish_fill_event(self, event: FillEvent) -> None:
        await self.notification_publisher.publish(
            NotificationEvent(
                service="trading",
                event_type="auto_buy.filled",
                title=f"[자동매수 체결] {event.stock_name or event.stock_code}",
                body=(
                    f"종목코드: {event.stock_code or '-'}\n"
                    f"주문번호: {event.order_no or '-'}\n"
                    f"체결수량: {event.filled_quantity}\n"
                    f"체결가: {event.filled_price:,}원\n"
                    f"체결금액: {event.filled_amount:,}원"
                ),
            )
        )

    async def _publish_failure_alert(self, key: str, title: str, body: str) -> None:
        await self.alert_manager.publish_failure(
            key=key,
            title=title,
            body=body,
        )

    async def stop(self):
        self.running = False
        await self.trading_service.stop_fill_stream()

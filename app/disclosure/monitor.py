import asyncio
import logging

from app.disclosure.browser import DartBrowser
from app.disclosure.parser import parse_disclosure_detail
from app.disclosure.service import DisclosureService
from app.notification.publisher import (
    LoggingNotificationPublisher,
    NotificationEvent,
    NotificationPublisher,
)


logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 3


class DisclosureMonitor:

    def __init__(
        self,
        notification_publisher: NotificationPublisher | None = None,
    ):
        self.browser = DartBrowser()
        self.service = DisclosureService()
        self.notification_publisher = (
            notification_publisher or LoggingNotificationPublisher()
        )
        self.running = False

    async def start(self):
        await self.browser.start()

        initial_disclosures = await self.service.find_new_disclosures(
            self.browser.page
        )

        logger.info(
            "DART disclosure monitor initialized | initial_new_count=%s",
            len(initial_disclosures),
        )

        await self._process_disclosures(initial_disclosures)

        self.running = True

        logger.info("DART My disclosure monitoring started")

        while self.running:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

            try:
                await self._poll_once()

            except Exception:
                logger.exception(
                    "DART polling failed"
                )

    async def _poll_once(self):
        response = await self.browser.reload()

        logger.info(
            "DART polling status=%s",
            response.status if response else None
        )

        new_disclosures = (
            await self.service.find_new_disclosures(
                self.browser.page
            )
        )

        if not new_disclosures:
            return []

        return await self._process_disclosures(new_disclosures)

    async def _process_disclosures(
        self,
        disclosures: list[dict],
    ):
        details = []

        for disclosure in disclosures:
            detail = await self._process_new_disclosure(
                disclosure
            )

            if detail:
                details.append(detail)

        return details

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

        if not self.service.is_treasury_stock_disclosure(disclosure):
            logger.info(
                "Disclosure ignored by treasury stock filter | rcpNo=%s",
                disclosure["rcp_no"],
            )
            return None

        detail_page = None

        try:
            detail_page = await self.browser.open_disclosure_detail(
                disclosure["rcp_no"]
            )

            detail = await parse_disclosure_detail(
                detail_page,
                report_name=disclosure["report_name"],
                stock_name=disclosure["stock_name"],
            )

            logger.info(
                "Disclosure detail parsed | detail=%s",
                detail,
            )

            await self.notification_publisher.publish(
                self._build_treasury_stock_event(detail)
            )

            return detail

        finally:
            if detail_page:
                await detail_page.close()

    def _build_treasury_stock_event(self, detail: dict) -> NotificationEvent:
        report_name = detail.get("report_name", "")
        stock_name = detail.get("stock_name", "")
        rcp_no = detail.get("rcp_no", "")
        url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"

        amount = (
            detail.get("acquisition_expected_amount")
            or detail.get("contract_amount")
            or "-"
        )
        purpose = (
            detail.get("acquisition_purpose")
            or detail.get("contract_purpose")
            or "-"
        )

        return NotificationEvent(
            service="disclosure",
            event_type="treasury_stock_disclosure.created",
            title=f"[신규 자사주 공시] {stock_name}",
            body=(
                f"보고서: {report_name}\n"
                f"금액: {amount}\n"
                f"목적: {purpose}\n"
                f"접수번호: {rcp_no}"
            ),
            url=url,
        )

    async def stop(self):
        self.running = False
        await self.browser.stop()

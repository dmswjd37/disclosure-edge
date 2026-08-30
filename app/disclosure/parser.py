import logging
import os
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from playwright.async_api import (
    Page,
    TimeoutError as PlaywrightTimeoutError,
)


load_dotenv()

DETAIL_REPORT_NAME_1 = os.environ["DART_DETAIL_REPORT_NAME_1"]
DETAIL_REPORT_NAME_2 = os.environ["DART_DETAIL_REPORT_NAME_2"]


def _clean_text(value: str | None) -> str:
    if not value:
        return ""

    return " ".join(value.split())


async def parse_disclosures(
    page: Page,
) -> dict[str, dict]:
    links = page.locator(
        'a[href*="/dsaf001/main.do?rcpNo="]'
    )

    try:
        await links.first.wait_for(
            state="attached",
            timeout=5000,
        )
    except PlaywrightTimeoutError:
        stored = await page.evaluate(
            "() => localStorage.getItem('myGosiReport')"
        )

        logging.error("Disclosure links were not found")
        logging.error("Current URL: %s", page.url)
        logging.error("localStorage: %s", stored)

        return {}

    count = await links.count()
    logging.info("Parsed disclosure link count: %s", count)

    disclosures = {}

    for i in range(count):
        link = links.nth(i)
        href = await link.get_attribute("href")

        if not href:
            continue

        report_name = _clean_text(
            await link.inner_text()
        )

        query = parse_qs(
            urlparse(href).query
        )
        rcp_nos = query.get("rcpNo")

        if not rcp_nos:
            continue

        rcp_no = rcp_nos[0]

        try:
            stock_name = await link.evaluate(
                """
                element => {
                    const clean = value => (value || "").replace(/\\s+/g, " ").trim();
                    const row = element.closest("tr");
                    const reportCell = element.closest("td, th");

                    if (!row || !reportCell) {
                        return "";
                    }

                    const cells = Array.from(row.querySelectorAll("td, th"));
                    const reportCellIndex = cells.indexOf(reportCell);
                    const stockCell = cells[reportCellIndex + 1];

                    if (!stockCell) {
                        return "";
                    }

                    const corp = stockCell.querySelector('[onclick*="openCorpInfoNew"]');
                    return clean(corp?.innerText || stockCell.innerText);
                }
                """
            )
        except Exception:
            stock_name = ""

        stock_name = _clean_text(stock_name)

        disclosures[rcp_no] = {
            "rcp_no": rcp_no,
            "stock_name": stock_name,
            "report_name": report_name,
            "href": href,
        }

    return disclosures


async def _extract_table_value(page_or_frame, label: str) -> str:
    try:
        value = await page_or_frame.evaluate(
            """
            label => {
                const clean = value => (value || "").replace(/\\s+/g, " ").trim();
                const rows = Array.from(document.querySelectorAll("tr"));

                for (const row of rows) {
                    const cells = Array.from(row.querySelectorAll("th, td"));
                    const texts = cells.map(cell => clean(cell.innerText));

                    if (!texts.some(text => text.includes(label))) {
                        continue;
                    }

                    for (let i = texts.length - 1; i >= 0; i -= 1) {
                        const text = texts[i];

                        if (text && !text.includes(label)) {
                            return text;
                        }
                    }
                }

                return "";
            }
            """,
            label,
        )
    except Exception:
        return ""

    return _clean_text(value)


async def _extract_first_table_value(page: Page, label: str) -> str:
    value = await _extract_table_value(page, label)

    if value:
        return value

    for frame in page.frames:
        if not frame.url or frame.url == "about:blank":
            continue

        value = await _extract_table_value(frame, label)

        if value:
            return value

    return ""


async def parse_disclosure_detail(
    page: Page,
    report_name: str = "",
    stock_name: str = "",
) -> dict:
    await page.wait_for_load_state("domcontentloaded")

    try:
        await page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass

    await page.wait_for_timeout(500)

    query = parse_qs(urlparse(page.url).query)
    rcp_no = (query.get("rcpNo") or [""])[0]
    report_name = _clean_text(report_name)
    stock_name = _clean_text(stock_name)

    detail = {
        "rcp_no": rcp_no,
        "stock_name": stock_name,
        "report_name": report_name,
    }

    if report_name == DETAIL_REPORT_NAME_1:
        detail["acquisition_expected_amount"] = (
            await _extract_first_table_value(page, "취득예정금액(원)")
        )
        detail["acquisition_purpose"] = (
            await _extract_first_table_value(page, "취득목적")
        )

    elif report_name == DETAIL_REPORT_NAME_2:
        detail["contract_amount"] = (
            await _extract_first_table_value(page, "계약금액(원)")
        )
        detail["contract_purpose"] = (
            await _extract_first_table_value(page, "계약목적")
        )

    return detail

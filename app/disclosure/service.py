import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, time
from urllib.parse import urlencode
from urllib.request import urlopen

from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_TARGET_REPORT_NAMES_ENV = "DART_TARGET_REPORT_NAMES"
DART_POLLING_START_ENV = "DART_POLLING_START"
DART_POLLING_END_ENV = "DART_POLLING_END"
DART_POLLING_WEEKDAYS_ONLY_ENV = "DART_POLLING_WEEKDAYS_ONLY"


def _get_target_report_names() -> set[str]:
    raw_value = os.getenv(DART_TARGET_REPORT_NAMES_ENV)

    if not raw_value:
        raise RuntimeError(
            f"{DART_TARGET_REPORT_NAMES_ENV} environment variable is required"
        )

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        parsed = [
            item.strip()
            for item in raw_value.replace("\n", ",").split(",")
            if item.strip()
        ]

    if not isinstance(parsed, list) or not all(
        isinstance(item, str) and item.strip()
        for item in parsed
    ):
        raise RuntimeError(
            f"{DART_TARGET_REPORT_NAMES_ENV} must be a JSON string array"
        )

    return {item.strip() for item in parsed}


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_time(value: str, env_name: str) -> time:
    match = re.fullmatch(r"(\d{2}):(\d{2})", value.strip())

    if not match:
        raise RuntimeError(f"{env_name} must be HH:MM")

    hour = int(match.group(1))
    minute = int(match.group(2))

    if hour > 23 or minute > 59:
        raise RuntimeError(f"{env_name} must be HH:MM")

    return time(hour=hour, minute=minute)


class DisclosureService:
    """Tracks seen disclosures and returns items that should be processed."""

    def __init__(self):
        self.seen_rcp_nos: set[str] = set()
        self.target_report_names = _get_target_report_names()
        self.polling_start = _parse_time(
            os.getenv(DART_POLLING_START_ENV, "09:00"),
            DART_POLLING_START_ENV,
        )
        self.polling_end = _parse_time(
            os.getenv(DART_POLLING_END_ENV, "15:30"),
            DART_POLLING_END_ENV,
        )
        self.polling_weekdays_only = _env_flag(DART_POLLING_WEEKDAYS_ONLY_ENV, True)
        self.initialized = False

    def is_polling_time(self, now: datetime | None = None) -> bool:
        now = now or datetime.now()

        if self.polling_weekdays_only and now.weekday() >= 5:
            return False

        current_time = now.time()
        return self.polling_start <= current_time <= self.polling_end

    async def find_new_disclosures(self):
        disclosures = await self.get_recent_reports()
        current_rcp_nos = set(disclosures.keys())

        if not self.initialized:
            self.seen_rcp_nos = current_rcp_nos
            self.initialized = True
            return list(disclosures.values())

        new_rcp_nos = current_rcp_nos - self.seen_rcp_nos

        new_disclosures = [
            disclosures[rcp_no]
            for rcp_no in disclosures
            if rcp_no in new_rcp_nos
        ]

        self.seen_rcp_nos.update(current_rcp_nos)

        return new_disclosures

    def is_target_disclosure(self, disclosure: dict) -> bool:
        report_name = disclosure.get("report_name", "")

        return report_name in self.target_report_names

    async def get_recent_reports(self) -> dict[str, dict]:
        return await asyncio.to_thread(self._get_recent_reports_sync)

    def _get_recent_reports_sync(self) -> dict[str, dict]:
        query_date = date.today().strftime("%Y%m%d")
        api_key = os.getenv("DART_API_KEY")

        if not api_key:
            raise RuntimeError("DART_API_KEY environment variable is required")

        query = urlencode(
            {
                "crtfc_key": api_key,
                "bgn_de": query_date,
                "end_de": query_date,
                "pblntf_ty": "B",
                "sort": "date",
                "sort_mth": "desc",
                "page_count": "100",
            }
        )
        url = f"{DART_LIST_URL}?{query}"

        with urlopen(url, timeout=10) as response:
            payload = response.read().decode("utf-8")

        data = json.loads(payload)
        status = data.get("status")

        if status != "000":
            if status == "013":
                return {}

            logger.warning(
                "DART list API failed | status=%s | message=%s",
                status,
                data.get("message"),
            )
            return {}

        disclosures = {}

        for report in data.get("list", []):
            report_name = report.get("report_nm", "")
            rcp_no = report.get("rcept_no")

            if not rcp_no or report_name not in self.target_report_names:
                continue

            disclosures[rcp_no] = {
                "rcp_no": rcp_no,
                "stock_name": report.get("corp_name") or "",
                "stock_code": report.get("stock_code") or "",
                "report_name": report_name,
                "rcept_dt": report.get("rcept_dt") or "",
                "href": (
                    "https://dart.fss.or.kr/dsaf001/main.do"
                    f"?rcpNo={rcp_no}"
                ),
            }

        logger.info(
            "DART recent reports parsed | total=%s | matched=%s",
            len(data.get("list", [])),
            len(disclosures),
        )

        return disclosures

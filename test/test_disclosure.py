import asyncio
import os
import unittest
from datetime import date, datetime

from app.disclosure.service import DisclosureService


class DisclosureServiceTest(unittest.TestCase):
    def test_is_polling_time_uses_configured_dart_window(self):
        service = DisclosureService()

        self.assertTrue(service.is_polling_time(datetime(2026, 9, 4, 9, 0)))
        self.assertTrue(service.is_polling_time(datetime(2026, 9, 4, 15, 30)))
        self.assertFalse(service.is_polling_time(datetime(2026, 9, 4, 15, 31)))


async def main():
    print("query_date:", date.today().strftime("%Y%m%d"))

    if not os.getenv("DART_API_KEY"):
        print("DART_API_KEY is not set. Skipping OpenDART smoke check.")
        return

    if not os.getenv("DART_TARGET_REPORT_NAMES"):
        print("DART_TARGET_REPORT_NAMES is not set. Skipping OpenDART smoke check.")
        return

    service = DisclosureService()

    disclosures = await service.find_new_disclosures()
    print("initial_disclosure_count:", len(disclosures))

    for disclosure in disclosures:
        print("list_item:", disclosure)
        assert disclosure["rcp_no"]
        assert disclosure["stock_name"]
        assert disclosure["report_name"]

    second_disclosures = await service.find_new_disclosures()
    assert second_disclosures == []


if __name__ == "__main__":
    asyncio.run(main())

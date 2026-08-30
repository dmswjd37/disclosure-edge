import asyncio

from app.disclosure.browser import DART_MY_URL, DartBrowser
from app.disclosure.parser import parse_disclosure_detail
from app.disclosure.service import DisclosureService


async def main():
    browser = DartBrowser()
    service = DisclosureService()
    detail_page = None

    try:
        page = await browser.start()
        print("list_url:", page.url)
        assert page.url.startswith(DART_MY_URL)

        disclosures = await service.find_new_disclosures(page)
        print("initial_disclosure_count:", len(disclosures))
        assert disclosures

        disclosure = disclosures[0]
        print("first_list_item:", disclosure)
        assert disclosure["rcp_no"]
        assert disclosure["stock_name"]
        assert disclosure["report_name"]

        detail_page = await browser.open_disclosure_detail(
            disclosure["rcp_no"]
        )

        detail = await parse_disclosure_detail(
            detail_page,
            report_name=disclosure["report_name"],
            stock_name=disclosure["stock_name"],
        )

        print("first_detail:", detail)
        assert detail["rcp_no"] == disclosure["rcp_no"]
        assert detail["stock_name"] == disclosure["stock_name"]
        assert detail["report_name"] == disclosure["report_name"]

    finally:
        if detail_page:
            await detail_page.close()

        await browser.stop()


if __name__ == "__main__":
    asyncio.run(main())

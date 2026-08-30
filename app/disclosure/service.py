from app.disclosure.parser import parse_disclosures


TREASURY_STOCK_REPORT_KEYWORDS = (
    "자기주식취득결정",
    "자기주식취득신탁계약체결결정",
)


class DisclosureService:
    """Tracks seen disclosures and returns items that should be processed."""

    def __init__(self):
        self.seen_rcp_nos: set[str] = set()
        self.initialized = False

    async def find_new_disclosures(self, page):
        disclosures = await parse_disclosures(page)
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

    def is_treasury_stock_disclosure(self, disclosure: dict) -> bool:
        report_name = disclosure.get("report_name", "")

        return any(
            keyword in report_name
            for keyword in TREASURY_STOCK_REPORT_KEYWORDS
        )

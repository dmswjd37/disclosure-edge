import asyncio
import json
import logging
import os

from pathlib import Path
from dotenv import load_dotenv
from playwright.async_api import async_playwright, BrowserContext, Page


load_dotenv()

logger = logging.getLogger(__name__)

DART_MY_URL = "https://dart.fss.or.kr/dsac001/mainMy.do"

BROWSER_DATA_DIR = Path("data/browser")

MY_GOSI_REPORT = json.loads(
    os.environ["MY_GOSI_REPORT"]
)


class DartBrowser:
    def __init__(self):
        self.playwright = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self.running = False

    async def start(self):
        """
        Chromium persistent context 시작
        """
        BROWSER_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        self.playwright = await async_playwright().start()

        self.context = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_DATA_DIR),
            headless=True,
            locale="ko-KR",
            timezone_id="Asia/Seoul",
        )

        # 기존 탭이 있으면 재사용
        if self.context.pages:
            self.page = self.context.pages[0]
        else:
            self.page = await self.context.new_page()

        await self._initialize_local_storage()

        self.running = True

        logger.info("DART browser started")
        return self.page

    async def _initialize_local_storage(self):
        """
        DART origin에 접속한 뒤 localStorage 설정
        """

        # localStorage는 origin별이므로
        # 먼저 dart.fss.or.kr 페이지에 접근해야 한다.
        await self.page.goto(
            "https://dart.fss.or.kr/",
            wait_until="domcontentloaded"
        )

        value = json.dumps(
            MY_GOSI_REPORT,
            ensure_ascii=False
        )

        await self.page.evaluate(
            """
            value => {
                localStorage.setItem(
                    "myGosiReport",
                    value
                );
            }
            """,
            value
        )

        # 제대로 저장됐는지 확인
        stored_count = await self.page.evaluate(
            """
            () => {
                const stored = localStorage.getItem("myGosiReport");

                if (!stored) {
                    return 0;
                }

                return JSON.parse(stored).myGosiReport?.length ?? 0;
            }
            """
        )

        logger.info(
            "myGosiReport localStorage initialized: %s item(s)",
            stored_count
        )

        # My공시 페이지 이동
        await self.page.goto(
            DART_MY_URL,
            wait_until="domcontentloaded"
        )

    async def open_disclosure_detail(self, rcp_no: str):
        detail_url = (
            "https://dart.fss.or.kr/dsaf001/main.do"
            f"?rcpNo={rcp_no}"
        )

        page = await self.context.new_page()

        await page.goto(
            detail_url,
            wait_until="domcontentloaded",
        )

        return page

    async def reload(self):
        if not self.page:
            return None

        if self.page.url != DART_MY_URL:
            return await self.page.goto(
                DART_MY_URL,
                wait_until="domcontentloaded",
            )

        return await self.page.reload(
            wait_until="domcontentloaded",
        )

    async def stop(self):
        self.running = False

        if self.context:
            await self.context.close()

        if self.playwright:
            await self.playwright.stop()

        logger.info("DART browser stopped")

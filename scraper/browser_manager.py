"""
Playwright 瀏覽器管理：建立擬人化的瀏覽器實例，降低被偵測機率。
"""
import random
import time
import logging
from contextlib import contextmanager
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

from config import SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


@contextmanager
def get_browser_context(headless: bool = True):
    """
    Context manager：回傳 Playwright BrowserContext。
    設定擬人化參數，避免被 bot detection 封鎖。
    """
    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        ctx: BrowserContext = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=2,   # 2× DPR：截圖每個字 4 倍像素量，對 Claude Vision OCR 精度幫助明顯
            locale="zh-TW",
            timezone_id="Asia/Taipei",
            java_script_enabled=True,
            accept_downloads=False,
            # 讓 navigator.webdriver 為 false
            extra_http_headers={
                "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        # 覆蓋 navigator.webdriver 屬性
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            window.chrome = { runtime: {} };
        """)
        try:
            yield ctx
        finally:
            browser.close()


def human_delay(min_sec: float = SCRAPE_DELAY_MIN, max_sec: float = SCRAPE_DELAY_MAX) -> None:
    """模擬人類操作的隨機延遲。"""
    time.sleep(random.uniform(min_sec, max_sec))


def safe_click(page: Page, selector: str, timeout: int = 10000) -> bool:
    """安全點擊：等待元素出現後點擊，失敗回傳 False。"""
    try:
        page.wait_for_selector(selector, timeout=timeout)
        page.click(selector)
        return True
    except Exception as e:
        logger.debug(f"safe_click failed for '{selector}': {e}")
        return False


def safe_get_text(page: Page, selector: str, default: str = "") -> str:
    """安全取得元素文字，失敗回傳 default。"""
    try:
        el = page.query_selector(selector)
        if el:
            return (el.inner_text() or "").strip()
    except Exception:
        pass
    return default


def take_screenshot(page: Page, path: str) -> bool:
    """截圖存檔，失敗回傳 False。"""
    try:
        page.screenshot(path=path, full_page=False)
        return True
    except Exception as e:
        logger.error(f"Screenshot failed: {e}")
        return False

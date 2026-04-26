import sys
sys.stdout.reconfigure(encoding="utf-8")
from playwright.sync_api import sync_playwright
import pathlib

p = pathlib.Path("d:/Coding/urban-renewal/frontend/maintenance.html")
url = p.as_uri()
print("URL:", url)

with sync_playwright() as pw:
    b = pw.chromium.launch(headless=True)
    ctx = b.new_context(viewport={"width": 1280, "height": 800})
    pg = ctx.new_page()
    def handle_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body='{"enabled": true, "message": "預計於今晚 23:00 完成升級，將新增「永慶房屋」物件來源。\\n感謝您的耐心等候！"}',
        )
    pg.route("**/api/maintenance_status", handle_route)
    pg.goto(url)
    pg.wait_for_timeout(2000)
    pg.screenshot(path="d:/Coding/urban-renewal/scripts/maintenance_preview.png", full_page=False)
    print("截圖存到 scripts/maintenance_preview.png")
    b.close()

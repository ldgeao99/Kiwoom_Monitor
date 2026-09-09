"""finviz 맵 페이지를 헤드리스 크롬으로 렌더링해 PNG로 캡처한다.

맵은 정적 이미지가 아니라 페이지에서 canvas로 그려지므로, 실제 브라우저로
페이지를 렌더링한 뒤 map canvas 영역을 스크린샷으로 뜬다. 이 방식은 URL의
날짜/시간값을 알 필요가 없어 '항상 최신 맵'을 얻는다.

필요 패키지(서버에 1회 설치):
    pip install playwright
    python -m playwright install chromium
    # (리눅스에서 시스템 라이브러리 필요 시) python -m playwright install-deps chromium

Playwright가 설치돼 있지 않으면 import 시점에 실패하므로, 호출부에서
지연 import(try/except) 하여 미설치 시 전송만 건너뛰도록 한다.
"""
from playwright.sync_api import sync_playwright

# 저사양(≈1GB RAM) 서버용 메모리 절약 플래그
_LAUNCH_ARGS = [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--single-process',
    '--no-zygote',
    '--disable-extensions',
    '--disable-background-networking',
]

# 쿠키/동의 배너가 뜰 경우 눌러볼 후보 셀렉터(있을 때만, best-effort)
_CONSENT_SELECTORS = [
    '#onetrust-accept-btn-handler',
    'button:has-text("Accept all")',
    'button:has-text("Accept")',
    'button:has-text("AGREE")',
    'button:has-text("I Agree")',
    'button:has-text("Consent")',
]


def capture(page_url, viewport=(1440, 900), scale=2, render_wait_ms=3500, timeout_ms=45000):
    """finviz map 페이지(page_url)를 렌더링해 map canvas를 PNG bytes로 반환."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=_LAUNCH_ARGS)
        try:
            ctx = browser.new_context(
                viewport={'width': viewport[0], 'height': viewport[1]},
                device_scale_factor=scale,   # 2배로 선명하게
                user_agent=('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                            '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'),
            )
            page = ctx.new_page()
            page.goto(page_url, wait_until='networkidle', timeout=timeout_ms)

            # 동의 배너 best-effort 닫기(없으면 무시)
            for sel in _CONSENT_SELECTORS:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=600):
                        btn.click(timeout=800)
                        page.wait_for_timeout(300)
                        break
                except Exception:
                    pass

            # 맵 canvas가 그려질 때까지 대기 후 여유를 두고 캡처
            page.wait_for_selector('canvas.chart', timeout=timeout_ms)
            page.wait_for_timeout(render_wait_ms)
            return page.locator('canvas.chart').screenshot()
        finally:
            browser.close()

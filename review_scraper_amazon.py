from __future__ import annotations

import csv
import html
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
TODAY_JST = datetime.now(JST).date()

CUTOFF_DATE = date(2025, 12, 1)
DATA_DIR = Path("review_csv")

PRODUCT_NAME = "ReFa FINE BUBBLE U+"
MALL_NAME = "Amazon"
PRODUCT_URL = "https://www.amazon.co.jp/product-reviews/B0G23Y9C56/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&sortBy=recent&pageNumber=1"
BASE_URL = "https://www.amazon.co.jp/"

CSV_HEADERS = [
    "検索実行日",
    "モール名",
    "対象商品名",
    "ページURL",
    "口コミ投稿日",
    "星の数",
    "注文日",
    "口コミのタイトル",
    "口コミ全文",
]

MALL_ORDER = {
    "Amazon": 1,
    "楽天": 2,
    "Yahoo": 3,
    "ビックカメラ": 4,
}

DATE_PATTERNS = [
    r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})",
    r"(20\d{2})年(\d{1,2})月(\d{1,2})日",
    r"(20\d{2})\.(\d{1,2})\.(\d{1,2})",
]

AMAZON_DATE_PATTERNS = [
    r"日本で\s*(20\d{2})年(\d{1,2})月(\d{1,2})日にレビュー済み",
    r"(20\d{2})年(\d{1,2})月(\d{1,2})日に日本でレビュー済み",
]

ORDER_DATE_RE = re.compile(r"注文日[:：]\s*(20\d{2}[/-]\d{1,2}[/-]\d{1,2})")


@dataclass
class Review:
    run_date: str
    mall: str
    product_name: str
    page_url: str
    review_date: str
    stars: str
    order_date: str
    title: str
    body: str

    def row(self) -> List[str]:
        return [
            self.run_date,
            self.mall,
            self.product_name,
            self.page_url,
            self.review_date,
            self.stars,
            self.order_date,
            self.title,
            self.body,
        ]


def jst_today_str() -> str:
    return TODAY_JST.strftime("%Y/%m/%d")


def clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_text(text: str) -> str:
    text = clean_text(text).lower()
    text = re.sub(r"[!-/:-@[-`{-~]", "", text)
    return text


def parse_date(text: str) -> Optional[date]:
    text = clean_text(text)

    for pattern in AMAZON_DATE_PATTERNS:
        m = re.search(pattern, text)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return date(y, mo, d)
            except ValueError:
                return None

    for pattern in DATE_PATTERNS:
        m = re.search(pattern, text)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return date(y, mo, d)
            except ValueError:
                return None

    return None


def fmt_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def normalize_order_date_text(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""
    d = parse_date(text)
    if d:
        return fmt_date(d)
    if text.startswith("注文日：") or text.startswith("注文日:"):
        suffix = re.sub(r"^注文日[:：]\s*", "", text)
        d = parse_date(suffix)
        if d:
            return fmt_date(d)
    return ""


def extract_order_date_from_text(text: str) -> Tuple[str, str]:
    text = clean_text(text)
    if not text:
        return "", ""
    m = ORDER_DATE_RE.search(text)
    if not m:
        return text, ""
    d = parse_date(m.group(1))
    order_date = fmt_date(d) if d else ""
    cleaned = ORDER_DATE_RE.sub("", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ：:　")
    cleaned = clean_text(cleaned)
    return cleaned, order_date


def build_dedupe_key(mall: str, review_date: str, body: str) -> Optional[Tuple[str, str, str]]:
    normalized_body = normalize_text(body)
    if not normalized_body:
        return None
    return mall, review_date, normalized_body


def star_to_str(text: str) -> str:
    text = clean_text(text)
    m = re.search(r"([0-5](?:\.\d)?)", text)
    return m.group(1) if m else ""


def load_existing_reviews() -> Tuple[List[Review], Dict[str, Set[Tuple[str, str, str]]]]:
    rows: List[Review] = []
    seen_by_mall: Dict[str, Set[Tuple[str, str, str]]] = {}
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for csv_path in DATA_DIR.glob("*_Review.csv"):
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                review = Review(
                    run_date=row.get("検索実行日", ""),
                    mall=row.get("モール名", ""),
                    product_name=row.get("対象商品名", ""),
                    page_url=row.get("ページURL", ""),
                    review_date=row.get("口コミ投稿日", ""),
                    stars=row.get("星の数", ""),
                    order_date=row.get("注文日", ""),
                    title=row.get("口コミのタイトル", ""),
                    body=row.get("口コミ全文", ""),
                )
                rows.append(review)
                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key:
                    seen_by_mall.setdefault(review.mall, set()).add(key)

    return rows, seen_by_mall


def write_reviews(reviews: List[Review]) -> List[Path]:
    by_year: Dict[int, List[Review]] = {}

    for review in reviews:
        d = parse_date(review.review_date)
        if d is None:
            continue
        by_year.setdefault(d.year, []).append(review)

    written_files: List[Path] = []

    for year, year_reviews in sorted(by_year.items()):
        path = DATA_DIR / f"{year}_FBU+_Review.csv"
        year_reviews.sort(
            key=lambda r: (
                parse_date(r.review_date) or date(1900, 1, 1),
                MALL_ORDER.get(r.mall, 999),
                normalize_text(r.body),
                normalize_text(r.title),
            )
        )

        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)
            for review in year_reviews:
                writer.writerow(review.row())

        written_files.append(path)

    return written_files


def log_response(response, label: str) -> None:
    if response is None:
        print(f"[WARN] {label}: response is None", file=sys.stderr)
        return
    try:
        print(f"[INFO] {label}: status={response.status} url={response.url}")
    except Exception as e:
        print(f"[WARN] {label}: response logging failed / {e}", file=sys.stderr)


def goto_with_retry(page, url: str, label: str) -> None:
    last_error: Optional[Exception] = None

    for attempt in range(1, 4):
        for wait_until in ["domcontentloaded", "load"]:
            try:
                print(f"[INFO] goto label={label} attempt={attempt} wait_until={wait_until} url={url}")
                response = page.goto(url, wait_until=wait_until, timeout=60000)
                log_response(response, f"{label} attempt={attempt} wait_until={wait_until}")
                page.wait_for_timeout(5000)
                return
            except PlaywrightTimeoutError as e:
                last_error = e
                print(
                    f"[WARN] timeout label={label} attempt={attempt} wait_until={wait_until} url={url}",
                    file=sys.stderr,
                )
                page.wait_for_timeout(2000)
            except Exception as e:
                last_error = e
                print(
                    f"[WARN] goto失敗 label={label} attempt={attempt} wait_until={wait_until} url={url} / {e}",
                    file=sys.stderr,
                )
                page.wait_for_timeout(2000)

    raise RuntimeError(f"ページ遷移失敗: {label} / {url} / {last_error}")


def detect_blocked_state(page) -> Optional[str]:
    html_text = page.content()
    title = ""
    try:
        title = page.title()
    except Exception:
        pass

    text = clean_text(BeautifulSoup(html_text, "html.parser").get_text(" ", strip=True))

    block_words = [
        "申し訳ございません",
        "問題が発生しました",
        "Service Unavailable",
        "Robot Check",
        "Enter the characters you see below",
        "アクセスできません",
        "ご利用いただけません",
        "503",
    ]

    for word in block_words:
        if word.lower() in title.lower() or word.lower() in text.lower():
            return f"block_detected word={word} title={title}"

    return None


def extract_review_nodes(page) -> List:
    selectors = [
        "div[data-hook='review']",
        "div.review",
        "li.review",
    ]

    for selector in selectors:
        locator = page.locator(selector)
        try:
            count = locator.count()
        except Exception:
            count = 0

        print(f"[INFO] selector={selector} count={count}")
        if count > 0:
            return locator.all()

    return []


def parse_review_node(node, page_url: str) -> Optional[Review]:
    try:
        whole_text = clean_text(node.inner_text())
    except Exception:
        return None

    date_value = parse_date(whole_text)
    if date_value is None:
        return None
    if date_value < CUTOFF_DATE:
        return None

    stars = ""
    title = ""
    body = ""
    order_date = ""

    try:
        title = clean_text(node.locator("[data-hook='review-title']").first.inner_text())
    except Exception:
        title = ""

    try:
        body = clean_text(node.locator("[data-hook='review-body']").first.inner_text())
    except Exception:
        body = ""

    if not body:
        lines = [clean_text(x) for x in whole_text.split("\n") if clean_text(x)]
        body = " ".join(lines)

    try:
        stars_text = clean_text(node.locator("[data-hook='review-star-rating']").first.inner_text())
        stars = star_to_str(stars_text)
    except Exception:
        try:
            stars_text = clean_text(node.locator("[data-hook='cmps-review-star-rating']").first.inner_text())
            stars = star_to_str(stars_text)
        except Exception:
            stars = ""

    title, order_from_title = extract_order_date_from_text(title)
    body, order_from_body = extract_order_date_from_text(body)
    order_date = order_from_title or order_from_body
    order_date = normalize_order_date_text(order_date)

    body = clean_text(body)
    if not body:
        return None

    return Review(
        run_date=jst_today_str(),
        mall=MALL_NAME,
        product_name=PRODUCT_NAME,
        page_url=page_url,
        review_date=fmt_date(date_value),
        stars=stars,
        order_date=order_date,
        title=title,
        body=body,
    )


def get_next_page_url(page) -> Optional[str]:
    selectors = [
        "li.a-last a",
        "a[aria-label='次へ']",
        "a:has-text('次へ')",
    ]

    for selector in selectors:
        locator = page.locator(selector).first
        try:
            count = locator.count()
        except Exception:
            count = 0

        if count == 0:
            continue

        try:
            href = locator.get_attribute("href")
        except Exception:
            href = None

        if not href:
            continue

        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"https://www.amazon.co.jp{href}"
        return f"https://www.amazon.co.jp/{href.lstrip('./')}"

    return None


def scrape_amazon(seen_keys: Set[Tuple[str, str, str]]) -> List[Review]:
    results: List[Review] = []
    visited_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        context = browser.new_context(
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            ignore_https_errors=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
        )

        context.set_extra_http_headers(
            {
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        page = context.new_page()

        try:
            goto_with_retry(page, BASE_URL, "amazon_base")
        except Exception as e:
            context.close()
            browser.close()
            raise RuntimeError(f"Amazonトップ接続失敗: {e}")

        blocked = detect_blocked_state(page)
        if blocked:
            context.close()
            browser.close()
            raise RuntimeError(f"Amazonトップでブロック検知: {blocked}")

        current_url = PRODUCT_URL
        page_count = 0
        success_page_count = 0

        while current_url and current_url not in visited_urls:
            visited_urls.add(current_url)
            page_count += 1

            try:
                goto_with_retry(page, current_url, f"review_page_{page_count}")
            except Exception as e:
                context.close()
                browser.close()
                raise RuntimeError(f"Amazonレビュー接続失敗: {e}")

            success_page_count += 1

            blocked = detect_blocked_state(page)
            if blocked:
                context.close()
                browser.close()
                raise RuntimeError(f"Amazonレビュー画面でブロック検知: {blocked}")

            nodes = extract_review_nodes(page)
            if not nodes:
                title = ""
                try:
                    title = page.title()
                except Exception:
                    pass

                html_text = page.content()
                soup = BeautifulSoup(html_text, "html.parser")
                text_preview = clean_text(soup.get_text(" ", strip=True))[:500]

                context.close()
                browser.close()
                raise RuntimeError(
                    f"レビューDOM抽出0件: page={page_count} title={title} url={page.url} preview={text_preview}"
                )

            page_reviews: List[Review] = []
            for node in nodes:
                review = parse_review_node(node, page.url)
                if review is None:
                    continue
                page_reviews.append(review)

            print(f"[INFO] page={page_count} parsed_reviews={len(page_reviews)}")

            if not page_reviews:
                context.close()
                browser.close()
                raise RuntimeError(f"レビューDOMは存在したが解析結果0件: page={page_count} url={page.url}")

            added_this_page = 0
            old_hit = False

            for review in page_reviews:
                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key is None:
                    continue

                if key in seen_keys:
                    old_hit = True
                    continue

                seen_keys.add(key)
                results.append(review)
                added_this_page += 1

            oldest_date = min((parse_date(r.review_date) for r in page_reviews if parse_date(r.review_date)), default=None)
            if oldest_date and oldest_date < CUTOFF_DATE:
                print("[INFO] cutoff到達のため終了")
                break

            if added_this_page == 0 and old_hit:
                print("[INFO] 既存レビューのみのため終了")
                break

            next_url = get_next_page_url(page)
            if not next_url or next_url in visited_urls:
                break

            current_url = next_url
            time.sleep(1)

        context.close()
        browser.close()

        if success_page_count == 0:
            raise RuntimeError("1ページも取得できていません")

    return results


def main() -> int:
    existing_reviews, seen_by_mall = load_existing_reviews()
    all_reviews = list(existing_reviews)
    seen_keys = seen_by_mall.setdefault(MALL_NAME, set())

    try:
        new_reviews = scrape_amazon(seen_keys)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    all_reviews.extend(new_reviews)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if all_reviews:
        write_reviews(all_reviews)

    if not new_reviews:
        print("[INFO] 追加レビューなし")
        return 0

    print(f"[INFO] Amazon: {len(new_reviews)}件追加")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

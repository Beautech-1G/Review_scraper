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
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
TODAY_JST = datetime.now(JST).date()

CUTOFF_DATE = date(2025, 12, 1)
DATA_DIR = Path("review_csv")

PRODUCT_NAME = "ReFa FINE BUBBLE U+"
START_URL = "https://www.biccamera.com/bc/disp/SfrGoodsPageReview.jsp?GOODS_NO=14676796"
MALL_NAME = "ビックカメラ"

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

STAR_PATTERNS = [
    r"([0-5](?:\.\d)?)\s*/\s*5",
    r"([0-5](?:\.\d)?)\s*点",
    r"星\s*([0-5](?:\.\d)?)",
    r"評価\s*([0-5](?:\.\d)?)",
]

NOISE_EXACT = {
    "このレビューは参考になりましたか？",
    "参考になった",
    "参考になった人",
    "このレビューを報告する",
    "レビューを報告する",
    "投稿する",
    "前へ",
    "次へ",
    "最初へ",
    "最後へ",
}

NOISE_CONTAINS = [
    "ページトップへ",
    "在庫のある店舗",
    "店舗在庫をみる",
    "商品詳細を見る",
    "お気に入りに登録",
    "カートに入れる",
    "ビック特価",
    "レビューを書く",
    "この商品を見た人は",
]


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


def extract_star(text: str) -> str:
    text = clean_text(text)
    for pattern in STAR_PATTERNS:
        m = re.search(pattern, text)
        if m:
            return m.group(1)
    return ""


def is_noise_line(text: str) -> bool:
    text = clean_text(text)
    if not text:
        return True
    if text in NOISE_EXACT:
        return True
    if any(word in text for word in NOISE_CONTAINS):
        return True
    if re.fullmatch(r"\d+", text):
        return True
    return False


def build_dedupe_key(mall: str, review_date: str, body: str) -> Optional[Tuple[str, str, str]]:
    normalized_body = normalize_text(body)
    if not normalized_body:
        return None
    return mall, review_date, normalized_body


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


def extract_candidate_blocks_from_html(html_text: str) -> List[str]:
    soup = BeautifulSoup(html_text, "html.parser")

    selectors = [
        "[class*='review']",
        "[id*='review']",
        "[class*='Review']",
        "[id*='Review']",
        "[class*='kuchikomi']",
        "[id*='kuchikomi']",
        "article",
        "li",
        "section",
        "div",
    ]

    blocks: List[str] = []
    seen_texts: Set[str] = set()

    for selector in selectors:
        for node in soup.select(selector):
            text = clean_text(node.get_text("\n", strip=True))
            if not text:
                continue
            if len(text) < 20:
                continue
            if parse_date(text) is None:
                continue
            if PRODUCT_NAME in text and len(text) < 80:
                continue

            normalized = normalize_text(text)
            if normalized in seen_texts:
                continue

            seen_texts.add(normalized)
            blocks.append(text)

    blocks.sort(key=len)
    return blocks


def parse_review_from_block(block_text: str, page_url: str) -> Optional[Review]:
    lines = [clean_text(x) for x in block_text.split("\n")]
    lines = [x for x in lines if not is_noise_line(x)]

    if not lines:
        return None

    review_date_index = -1
    review_date_value: Optional[date] = None

    for i, line in enumerate(lines):
        d = parse_date(line)
        if d:
            review_date_index = i
            review_date_value = d
            break

    if review_date_value is None:
        return None

    if review_date_value < CUTOFF_DATE:
        return None

    stars = ""
    title = ""
    body_parts: List[str] = []

    window_text = " ".join(lines[max(0, review_date_index - 3): min(len(lines), review_date_index + 4)])
    stars = extract_star(window_text)

    before_lines = [x for x in lines[:review_date_index] if parse_date(x) is None]
    after_lines = [x for x in lines[review_date_index + 1:] if parse_date(x) is None]

    candidate_title = ""
    if before_lines:
        for line in reversed(before_lines):
            if not extract_star(line) and 1 <= len(line) <= 80:
                candidate_title = line
                break

    title = candidate_title

    for line in after_lines:
        if line == title:
            continue
        if extract_star(line):
            continue
        if len(line) <= 2:
            continue
        body_parts.append(line)

    body = clean_text(" ".join(body_parts))

    if not body:
        return None

    return Review(
        run_date=jst_today_str(),
        mall=MALL_NAME,
        product_name=PRODUCT_NAME,
        page_url=page_url,
        review_date=fmt_date(review_date_value),
        stars=stars,
        order_date="",
        title=title,
        body=body,
    )


def parse_reviews_from_page(html_text: str, page_url: str) -> List[Review]:
    blocks = extract_candidate_blocks_from_html(html_text)
    reviews: List[Review] = []
    seen_in_page: Set[Tuple[str, str, str]] = set()

    for block in blocks:
        review = parse_review_from_block(block, page_url)
        if review is None:
            continue

        key = build_dedupe_key(review.mall, review.review_date, review.body)
        if key is None:
            continue
        if key in seen_in_page:
            continue

        seen_in_page.add(key)
        reviews.append(review)

    reviews.sort(key=lambda r: parse_date(r.review_date) or date(1900, 1, 1))
    return reviews


def get_next_page_url(page) -> Optional[str]:
    candidates = [
        "a:has-text('次へ')",
        "a[aria-label='次へ']",
        "a[rel='next']",
        "a:has-text('>')",
    ]

    for selector in candidates:
        try:
            locator = page.locator(selector).first
            if locator.count() == 0:
                continue
            href = locator.get_attribute("href")
            if href:
                if href.startswith("http"):
                    return href
                if href.startswith("/"):
                    return f"https://www.biccamera.com{href}"
                return f"https://www.biccamera.com/bc/disp/{href.lstrip('./')}"
        except Exception:
            continue

    html_text = page.content()
    m = re.search(r'href="([^"]*SfrGoodsPageReview\.jsp[^"]*)"', html_text, flags=re.IGNORECASE)
    if m:
        href = html.unescape(m.group(1))
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"https://www.biccamera.com{href}"
        return f"https://www.biccamera.com/bc/disp/{href.lstrip('./')}"

    return None


def scrape_biccamera(seen_keys: Set[Tuple[str, str, str]]) -> List[Review]:
    results: List[Review] = []
    visited_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        current_url = START_URL
        page_count = 0

        while current_url and current_url not in visited_urls:
            visited_urls.add(current_url)
            page_count += 1

            try:
                print(f"[INFO] fetch page {page_count}: {current_url}")
                page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)
            except PlaywrightTimeoutError:
                print(f"[WARN] timeout: {current_url}", file=sys.stderr)
                break
            except Exception as e:
                print(f"[WARN] fetch失敗: {current_url} / {e}", file=sys.stderr)
                break

            html_text = page.content()
            reviews = parse_reviews_from_page(html_text, page.url)

            if not reviews:
                title = page.title()
                print(f"[WARN] レビュー抽出0件 title={title} url={page.url}", file=sys.stderr)
                break

            added_this_page = 0
            old_hit = False

            for review in reviews:
                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key is None:
                    continue

                if key in seen_keys:
                    old_hit = True
                    continue

                seen_keys.add(key)
                results.append(review)
                added_this_page += 1

            oldest_date = min((parse_date(r.review_date) for r in reviews if parse_date(r.review_date)), default=None)
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

    return results


def main() -> int:
    existing_reviews, seen_by_mall = load_existing_reviews()
    all_reviews = list(existing_reviews)

    seen_keys = seen_by_mall.setdefault(MALL_NAME, set())

    try:
        new_reviews = scrape_biccamera(seen_keys)
    except Exception as e:
        print(f"[ERROR] ビックカメラ取得失敗: {e}", file=sys.stderr)
        return 1

    all_reviews.extend(new_reviews)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if all_reviews:
        write_reviews(all_reviews)

    if not new_reviews:
        print("[INFO] 追加レビューなし")
        return 0

    print(f"[INFO] ビックカメラ: {len(new_reviews)}件追加")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

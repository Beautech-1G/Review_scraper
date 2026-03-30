from __future__ import annotations

import csv
import html
import math
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
TODAY_JST = datetime.now(JST).date()
CUTOFF_DATE = date(2026, 2, 1)
DATA_DIR = Path("review_csv")
TIMEOUT = 30
MAX_PAGES_PER_MALL = 120
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

PRODUCTS = [
    {
        "product_name": "ReFa FINE BUBBLE U+",
        "file_stub": "FBU+",
        "malls": [
            {
                "mall": "Amazon",
                "url": "https://www.amazon.co.jp/product-reviews/B0G23Y9C56/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&sortBy=recent&pageNumber=1",
                "scraper": "amazon",
            },
            {
                "mall": "楽天",
                "url": "https://review.rakuten.co.jp/item/1/262320_10002177?sort=6#itemReviewList",
                "scraper": "rakuten",
            },
            {
                "mall": "Yahoo",
                "url": "https://shopping.yahoo.co.jp/review/item/list?store_id=mtgec&page_key=1579320109&sc_i=shopping-pc-web-list-ranking-crk01_01-rvw&sort=-latest",
                "scraper": "yahoo",
            },
            {
                "mall": "ビックカメラ",
                "url": "https://www.biccamera.com/bc/disp/SfrGoodsPageReview.jsp?GOODS_NO=14676796",
                "scraper": "biccamera",
            },
        ],
    }
]

MALL_ORDER = {"Amazon": 1, "楽天": 2, "Yahoo": 3, "ビックカメラ": 4}
CSV_HEADERS = [
    "検索実行日",
    "モール名",
    "対象商品名",
    "ページURL",
    "口コミ投稿日",
    "星の数",
    "口コミタイトル",
    "口コミ全文",
]


@dataclass
class Review:
    run_date: str
    mall: str
    product_name: str
    page_url: str
    review_date: str
    stars: str
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
            self.title,
            self.body,
        ]


def jst_today_str() -> str:
    return TODAY_JST.strftime("%Y/%m/%d")


def normalize_text(text: str) -> str:
    """
    正規化定義（重複判定用）
    1. HTMLエンティティ解除
    2. Unicode NFKC 正規化
    3. 改行/タブ/全角半角スペースの連続を半角1スペースへ圧縮
    4. 前後空白除去
    5. 英字のみ小文字化
    ※句読点は残す
    """
    if text is None:
        return ""
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u3000", " ")
    text = re.sub(r"[\t\r\n ]+", " ", text)
    text = text.strip()
    text = text.lower()
    return text


def parse_date(text: str) -> Optional[date]:
    text = text.strip()
    m = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", text)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def fmt_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def star_to_str(value: Optional[float]) -> str:
    if value is None:
        return ""
    if math.isclose(value, round(value)):
        return str(int(round(value)))
    return str(value)


def build_dedupe_key(mall: str, review_date: str, body: str) -> Optional[Tuple[str, str, str]]:
    normalized_body = normalize_text(body)
    if not normalized_body:
        return None
    return (mall, review_date, normalized_body)


def load_existing_reviews() -> Tuple[List[Review], Dict[str, set]]:
    rows: List[Review] = []
    seen_by_mall: Dict[str, set] = {}
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
                    title=row.get("口コミタイトル", ""),
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
                MALL_ORDER.get(r.mall, 999),
                parse_date(r.review_date) or date(1900, 1, 1),
                r.title,
                normalize_text(r.body),
            )
        )
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)
            for review in year_reviews:
                writer.writerow(review.row())
        written_files.append(path)

    return written_files


def fetch(session: requests.Session, url: str) -> str:
    resp = session.get(url, timeout=TIMEOUT, headers=HEADERS)
    resp.raise_for_status()
    return resp.text


class BaseScraper:
    def __init__(self, mall: str, product_name: str, start_url: str, session: requests.Session, seen_keys: set):
        self.mall = mall
        self.product_name = product_name
        self.start_url = start_url
        self.session = session
        self.seen_keys = seen_keys

    def scrape(self) -> List[Review]:
        raise NotImplementedError

    def make_review(self, page_url: str, review_date: date, stars: Optional[float], title: str, body: str) -> Review:
        return Review(
            run_date=jst_today_str(),
            mall=self.mall,
            product_name=self.product_name,
            page_url=page_url,
            review_date=fmt_date(review_date),
            stars=star_to_str(stars),
            title=clean_text(title),
            body=clean_text(body),
        )


class RakutenScraper(BaseScraper):
    def scrape(self) -> List[Review]:
        results: List[Review] = []
        is_initial = len(self.seen_keys) == 0

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            page_url = f"https://review.rakuten.co.jp/item/1/262320_10002177?sort=6&page={page}#itemReviewList"
            try:
                html_text = fetch(self.session, page_url)
            except Exception as e:
                print(f"[WARN] 楽天 page={page} fetch失敗: {e}", file=sys.stderr)
                break

            soup = BeautifulSoup(html_text, "html.parser")
            lines = [clean_text(x) for x in soup.get_text("\n").split("\n")]
            lines = [x for x in lines if x]

            page_reviews = self._parse_from_lines(lines, page_url)
            if not page_reviews:
                break

            added_this_page = 0
            old_seen_on_page = False
            for review in page_reviews:
                review_dt = parse_date(review.review_date)
                if review_dt is None:
                    continue
                if review_dt < CUTOFF_DATE:
                    return results

                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key and key in self.seen_keys:
                    old_seen_on_page = True
                    continue
                if key:
                    self.seen_keys.add(key)

                results.append(review)
                added_this_page += 1

            if not is_initial and added_this_page == 0 and old_seen_on_page:
                break

            time.sleep(1)

        return results

    def _parse_from_lines(self, lines: List[str], page_url: str) -> List[Review]:
        reviews: List[Review] = []
        i = 0
        while i < len(lines):
            star_match = re.fullmatch(r"([1-5](?:\.0)?)", lines[i])
            if star_match and i + 1 < len(lines):
                d = parse_date(lines[i + 1])
                if d:
                    stars = float(star_match.group(1))
                    title = ""
                    body = ""
                    j = i + 2
                    while j < len(lines):
                        txt = lines[j]
                        if re.fullmatch(r"([1-5](?:\.0)?)", txt) and j + 1 < len(lines) and parse_date(lines[j + 1]):
                            break
                        if txt.startswith("注文日："):
                            break
                        if txt in {"さらに表示", "参考になった", "不適切レビュー報告"}:
                            j += 1
                            continue
                        if title == "" and len(txt) <= 80 and not re.search(r"さん$|代$|男性$|女性$|商品:|購入者さん$", txt):
                            title = txt
                        elif not body and not re.search(r"さん$|代$|男性$|女性$|商品:|家族へ|実用品|はじめて|注文日：", txt):
                            body = txt
                        elif body and not re.search(r"注文日：", txt):
                            body = f"{body} {txt}"
                        j += 1
                    if title or body:
                        reviews.append(self.make_review(page_url, d, stars, title, body))
                    i = j
                    continue
            i += 1
        return reviews


class YahooScraper(BaseScraper):
    def scrape(self) -> List[Review]:
        results: List[Review] = []
        is_initial = len(self.seen_keys) == 0

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            page_url = f"https://shopping.yahoo.co.jp/review/item/list?store_id=mtgec&page_key=1579320109&sort=-latest&page={page}"
            try:
                html_text = fetch(self.session, page_url)
            except Exception as e:
                print(f"[WARN] Yahoo page={page} fetch失敗: {e}", file=sys.stderr)
                break

            soup = BeautifulSoup(html_text, "html.parser")
            lines = [clean_text(x) for x in soup.get_text("\n").split("\n")]
            lines = [x for x in lines if x]
            page_reviews = self._parse_from_lines(lines, page_url)
            if not page_reviews:
                break

            added_this_page = 0
            old_seen_on_page = False
            for review in page_reviews:
                review_dt = parse_date(review.review_date)
                if review_dt is None:
                    continue
                if review_dt < CUTOFF_DATE:
                    return results

                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key and key in self.seen_keys:
                    old_seen_on_page = True
                    continue
                if key:
                    self.seen_keys.add(key)

                results.append(review)
                added_this_page += 1

            if not is_initial and added_this_page == 0 and old_seen_on_page:
                break

            time.sleep(1)

        return results

    def _parse_from_lines(self, lines: List[str], page_url: str) -> List[Review]:
        reviews: List[Review] = []
        i = 0
        while i < len(lines):
            title = lines[i]
            if i + 2 < len(lines):
                d = parse_date(lines[i + 1])
                star_author = lines[i + 2]
                if d and re.match(r"^[0-5](?:\.\d)?[^\d]?.*さん", star_author):
                    m = re.match(r"^([0-5](?:\.\d)?)", star_author)
                    stars = float(m.group(1)) if m else None
                    j = i + 3
                    body_parts: List[str] = []
                    while j < len(lines):
                        txt = lines[j]
                        if j + 2 < len(lines) and parse_date(lines[j + 1]) and re.match(r"^[0-5](?:\.\d)?[^\d]?.*さん", lines[j + 2]):
                            break
                        if txt in {"購入した商品", "購入したストア"} or txt.startswith("違反報告") or txt.startswith("いいね"):
                            j += 1
                            continue
                        if re.match(r"^(カラー|<商品名>|<カラー>|<商品名>)/", txt):
                            j += 1
                            continue
                        body_parts.append(txt)
                        j += 1
                    body = " ".join(body_parts).strip()
                    reviews.append(self.make_review(page_url, d, stars, title, body))
                    i = j
                    continue
            i += 1
        return reviews


class AmazonScraper(BaseScraper):
    def scrape(self) -> List[Review]:
        results: List[Review] = []
        is_initial = len(self.seen_keys) == 0

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            page_url = (
                "https://www.amazon.co.jp/product-reviews/B0G23Y9C56/"
                f"ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&sortBy=recent&pageNumber={page}"
            )
            try:
                html_text = fetch(self.session, page_url)
            except Exception as e:
                print(f"[WARN] Amazon page={page} fetch失敗: {e}", file=sys.stderr)
                break

            soup = BeautifulSoup(html_text, "html.parser")
            review_nodes = soup.select("[data-hook='review']")
            if not review_nodes:
                title = clean_text(soup.title.get_text(" ")) if soup.title else ""
                print(f"[WARN] Amazon page={page} レビュー要素なし title={title}", file=sys.stderr)
                break

            added_this_page = 0
            old_seen_on_page = False
            for node in review_nodes:
                title = clean_text(_node_text(node.select_one("[data-hook='review-title']")))
                body = clean_text(_node_text(node.select_one("[data-hook='review-body']")))
                date_text = clean_text(_node_text(node.select_one("[data-hook='review-date']")))
                review_dt = parse_date(date_text)
                stars = parse_star_text(_node_text(node.select_one("[data-hook='review-star-rating'], [data-hook='cmps-review-star-rating']")))
                if review_dt is None:
                    continue
                if review_dt < CUTOFF_DATE:
                    return results

                review = self.make_review(page_url, review_dt, stars, title, body)
                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key and key in self.seen_keys:
                    old_seen_on_page = True
                    continue
                if key:
                    self.seen_keys.add(key)

                results.append(review)
                added_this_page += 1

            if not is_initial and added_this_page == 0 and old_seen_on_page:
                break

            time.sleep(1)

        return results


class BicCameraScraper(BaseScraper):
    def scrape(self) -> List[Review]:
        results: List[Review] = []
        is_initial = len(self.seen_keys) == 0

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            page_url = f"https://www.biccamera.com/bc/disp/SfrGoodsPageReview.jsp?GOODS_NO=14676796&page={page}"
            try:
                html_text = fetch(self.session, page_url)
            except Exception as e:
                print(f"[WARN] ビックカメラ page={page} fetch失敗: {e}", file=sys.stderr)
                break

            soup = BeautifulSoup(html_text, "html.parser")
            lines = [clean_text(x) for x in soup.get_text("\n").split("\n")]
            lines = [x for x in lines if x]
            page_reviews = self._parse_from_lines(lines, page_url)
            if not page_reviews:
                title = clean_text(soup.title.get_text(" ")) if soup.title else ""
                print(f"[WARN] ビックカメラ page={page} レビュー抽出0件 title={title}", file=sys.stderr)
                break

            added_this_page = 0
            old_seen_on_page = False
            for review in page_reviews:
                review_dt = parse_date(review.review_date)
                if review_dt is None:
                    continue
                if review_dt < CUTOFF_DATE:
                    return results

                key = build_dedupe_key(review.mall, review.review_date, review.body)
                if key and key in self.seen_keys:
                    old_seen_on_page = True
                    continue
                if key:
                    self.seen_keys.add(key)

                results.append(review)
                added_this_page += 1

            if not is_initial and added_this_page == 0 and old_seen_on_page:
                break

            time.sleep(1)

        return results

    def _parse_from_lines(self, lines: List[str], page_url: str) -> List[Review]:
        reviews: List[Review] = []
        for i, txt in enumerate(lines):
            d = parse_date(txt)
            if not d:
                continue

            title = lines[i - 1] if i - 1 >= 0 else ""
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            star_match = re.match(r"^([0-5](?:\.\d)?)$", next_line)
            stars = float(star_match.group(1)) if star_match else None

            body_parts: List[str] = []
            j = i + 2 if star_match else i + 1
            while j < len(lines):
                probe = lines[j]
                if parse_date(probe):
                    break
                if len(probe) <= 2 and re.fullmatch(r"[0-5](?:\.\d)?", probe):
                    j += 1
                    continue
                if probe in {"参考になった", "このレビューは参考になりましたか？"}:
                    break
                body_parts.append(probe)
                j += 1

            body = " ".join(body_parts).strip()
            if title or body:
                reviews.append(self.make_review(page_url, d, stars, title, body))

        return reviews


SCRAPER_MAP = {
    "rakuten": RakutenScraper,
    "yahoo": YahooScraper,
    "amazon": AmazonScraper,
    "biccamera": BicCameraScraper,
}


def _node_text(node) -> str:
    return node.get_text(" ", strip=True) if node else ""


def parse_star_text(text: str) -> Optional[float]:
    m = re.search(r"([0-5](?:\.\d)?)", text)
    return float(m.group(1)) if m else None


def main() -> int:
    existing_reviews, seen_by_mall = load_existing_reviews()
    all_reviews = list(existing_reviews)
    new_count = 0

    with requests.Session() as session:
        for product in PRODUCTS:
            product_name = product["product_name"]
            for mall_conf in product["malls"]:
                mall = mall_conf["mall"]
                scraper_cls = SCRAPER_MAP[mall_conf["scraper"]]
                seen_keys = seen_by_mall.setdefault(mall, set())
                scraper = scraper_cls(mall, product_name, mall_conf["url"], session, seen_keys)
                try:
                    reviews = scraper.scrape()
                    all_reviews.extend(reviews)
                    new_count += len(reviews)
                    print(f"[INFO] {mall}: {len(reviews)}件追加")
                except Exception as e:
                    print(f"[ERROR] {mall}: {e}", file=sys.stderr)
                    continue

    if new_count == 0:
        print("[INFO] 追加レビューなし")
        return 0

    write_reviews(all_reviews)
    print(f"[INFO] 合計 {new_count} 件追加")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

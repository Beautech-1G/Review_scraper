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
}

PRODUCTS = [
    {
        "product_name": "ReFa FINE BUBBLE U+",
        "file_stub": "FBU+",
        "malls": [
            {
                "mall": "楽天",
                "url": "https://review.rakuten.co.jp/item/1/262320_10002177",
                "scraper": "rakuten",
            },
            {
                "mall": "Yahoo",
                "url": "https://shopping.yahoo.co.jp/review/item/list?store_id=mtgec&page_key=1579320109",
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

MALL_ORDER = {"楽天": 1, "Yahoo": 2, "ビックカメラ": 3}

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


def jst_today_str():
    return TODAY_JST.strftime("%Y/%m/%d")


def clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_date(text: str) -> Optional[date]:
    m = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", text)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def fmt_date(d: date):
    return d.strftime("%Y/%m/%d")


def star_to_str(v):
    return str(int(v)) if v else ""


def fetch(session, url):
    for i in range(3):  # 3回リトライ
        try:
            r = session.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"[WARN] fetch失敗 {i+1}回目: {e}")
            time.sleep(3)
    raise Exception("fetch失敗")


class BaseScraper:
    def __init__(self, mall, product_name, start_url, session):
        self.mall = mall
        self.product_name = product_name
        self.start_url = start_url
        self.session = session

    def make_review(self, page_url, d, stars, title, body):
        return Review(
            jst_today_str(),
            self.mall,
            self.product_name,
            page_url,
            fmt_date(d),
            star_to_str(stars),
            "",  # 注文日は空欄固定
            clean_text(title),
            clean_text(body),
        )


# =========================
# Yahoo（DOM抽出）
# =========================
class YahooScraper(BaseScraper):
    def scrape(self):
        results = []

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            url = f"{self.start_url}&page={page}"
            html = fetch(self.session, url)
            soup = BeautifulSoup(html, "html.parser")

            cards = soup.select("div.style_ReviewDetail__")

            if not cards:
                break

            for c in cards:
                title = c.select_one("p.style_ReviewDetail__reviewTitle__")
                body = c.select_one("p.style_ReviewDetail__reviewBody__")
                date_el = c.select_one("p.style_ReviewDetail__postedTime__")
                star_el = c.select_one("span.Review__stars")

                if not body or not date_el:
                    continue

                d = parse_date(date_el.get_text())
                if not d or d < CUTOFF_DATE:
                    return results

                stars = None
                if star_el and star_el.has_attr("aria-label"):
                    m = re.search(r"(\d)", star_el["aria-label"])
                    if m:
                        stars = int(m.group(1))

                results.append(
                    self.make_review(
                        url,
                        d,
                        stars,
                        title.get_text() if title else "",
                        body.get_text(),
                    )
                )

            time.sleep(1)

        return results


# =========================
# ビックカメラ（DOM抽出）
# =========================
class BicCameraScraper(BaseScraper):
    def scrape(self):
        results = []

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            url = f"{self.start_url}&page={page}"
            html = fetch(self.session, url)
            soup = BeautifulSoup(html, "html.parser")

            cards = soup.select("div.reviewBox")

            if not cards:
                break

            for c in cards:
                title = c.select_one("p.title")
                body = c.select_one("p.content")

                if not body:
                    continue

                text_all = c.get_text()

                d = parse_date(text_all)
                if not d or d < CUTOFF_DATE:
                    return results

                star_img = c.select_one("img[src*='review_']")
                stars = None
                if star_img:
                    m = re.search(r"review_(\d)", star_img["src"])
                    if m:
                        stars = int(m.group(1))

                results.append(
                    self.make_review(
                        url,
                        d,
                        stars,
                        title.get_text() if title else "",
                        body.get_text(),
                    )
                )

            time.sleep(1)

        return results


# =========================
# 楽天（そのまま）
# =========================
class RakutenScraper(BaseScraper):
    def scrape(self):
        results = []

        for page in range(1, MAX_PAGES_PER_MALL + 1):
            url = f"{self.start_url}?page={page}"
            html = fetch(self.session, url)
            soup = BeautifulSoup(html, "html.parser")

            items = soup.select("#itemReviewList li")

            if not items:
                break

            for i in items:
                body = i.select_one("div.review-body--LpVR4")
                if not body:
                    continue

                d = parse_date(i.get_text())
                if not d or d < CUTOFF_DATE:
                    return results

                title = i.select_one("div.type-header--1Weg4")

                stars = None
                for s in i.select("span"):
                    if s.get_text().isdigit():
                        stars = int(s.get_text())
                        break

                results.append(
                    self.make_review(
                        url,
                        d,
                        stars,
                        title.get_text() if title else "",
                        body.get_text(),
                    )
                )

            time.sleep(1)

        return results


SCRAPER_MAP = {
    "rakuten": RakutenScraper,
    "yahoo": YahooScraper,
    "biccamera": BicCameraScraper,
}


def write_reviews(reviews):
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f"{TODAY_JST.year}_Review.csv"

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        for r in reviews:
            writer.writerow(r.row())


def main():
    all_reviews = []

    with requests.Session() as session:
        for p in PRODUCTS:
            for m in p["malls"]:
                scraper = SCRAPER_MAP[m["scraper"]](
                    m["mall"], p["product_name"], m["url"], session
                )
                try:
                    reviews = scraper.scrape()
                    all_reviews.extend(reviews)
                    print(f"{m['mall']}：{len(reviews)}件")
                except Exception as e:
                    print(f"[ERROR] {m['mall']} 失敗: {e}")
                    continue

    write_reviews(all_reviews)


if __name__ == "__main__":
    main()

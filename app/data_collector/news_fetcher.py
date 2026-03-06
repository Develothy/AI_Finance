"""
뉴스 센티먼트 수집 (Phase 4)
===========================

Naver News API를 사용하여 종목별/시장 전체 뉴스를 수집한다.
"""

from datetime import date
from typing import Optional
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
import re
import time

import requests

from config import settings
from core import get_logger
from core.decorators import retry

logger = get_logger("news_fetcher")

# HTML 태그 / 엔티티 제거용
_TAG_RE = re.compile(r"<[^>]+>")
_ENTITY_RE = re.compile(r"&[a-zA-Z]+;|&#\d+;")

# 시장 전체 뉴스 검색 키워드
MARKET_KEYWORDS = [
    "코스피",
    "코스닥",
    "증시",
    "주식시장",
    "한국은행 금리",
    "경기 전망",
]


@dataclass
class NewsFetchResult:
    """뉴스 수집 결과"""

    records: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    message: str = ""


class NewsFetcher:
    """네이버 뉴스 API 수집기"""

    API_URL = "https://openapi.naver.com/v1/search/news.json"
    MAX_DISPLAY = 100
    MAX_START = 1000
    RATE_LIMIT_DELAY = 0.1  # 초

    def __init__(self):
        self.client_id = settings.NAVER_CLIENT_ID
        self.client_secret = settings.NAVER_CLIENT_SECRET
        self.available = bool(self.client_id and self.client_secret)

        if not self.available:
            logger.warning("Naver API 키 미설정 — 뉴스 수집 비활성화", "__init__")

    # ── API 호출 ──────────────────────────────────────────────

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="news_fetcher")
    def _call_api(
        self,
        query: str,
        display: int = 100,
        start: int = 1,
        sort: str = "date",
    ) -> dict:
        """Naver News API 단일 호출"""
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }
        params = {
            "query": query,
            "display": min(display, self.MAX_DISPLAY),
            "start": start,
            "sort": sort,
        }
        resp = requests.get(self.API_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        time.sleep(self.RATE_LIMIT_DELAY)
        return resp.json()

    # ── 공개 메서드 ───────────────────────────────────────────

    def fetch_stock_news(
        self,
        code: str,
        stock_name: str,
        market: str = "KR",
        max_items: int = 100,
    ) -> list[dict]:
        """종목별 뉴스 수집"""
        query = f"{stock_name} 주가"
        return self._fetch_news(query, code=code, market=market, max_items=max_items)

    def fetch_market_news(
        self,
        market: str = "KR",
        max_items: int = 100,
    ) -> list[dict]:
        """시장 전체 뉴스 수집"""
        all_records = []
        per_keyword = max(max_items // len(MARKET_KEYWORDS), 10)
        for keyword in MARKET_KEYWORDS:
            records = self._fetch_news(
                keyword, code=None, market=market, max_items=per_keyword,
            )
            all_records.extend(records)
        return all_records

    # ── 내부 메서드 ───────────────────────────────────────────

    def _fetch_news(
        self,
        query: str,
        code: Optional[str],
        market: str,
        max_items: int,
    ) -> list[dict]:
        """API 호출 → 레코드 변환 (페이지네이션)"""
        if not self.available:
            return []

        records = []
        start = 1

        while len(records) < max_items and start < self.MAX_START:
            try:
                data = self._call_api(query, display=self.MAX_DISPLAY, start=start)
            except Exception as e:
                logger.warning(
                    f"API 호출 실패: {query} start={start} - {e}",
                    "_fetch_news",
                )
                break

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                title = _clean_html(item.get("title", ""))
                desc = _clean_html(item.get("description", ""))
                pub_date = _parse_pub_date(item.get("pubDate", ""))
                url = item.get("originallink") or item.get("link", "")

                if not title or not pub_date:
                    continue

                records.append({
                    "date": pub_date,
                    "market": market,
                    "code": code,
                    "title": title[:500],
                    "description": desc[:2000] if desc else None,
                    "url": url[:1000] if url else None,
                    "source": "naver",
                })

            start += self.MAX_DISPLAY
            if len(items) < self.MAX_DISPLAY:
                break

        return records[:max_items]


# ── 유틸리티 ─────────────────────────────────────────────────


def _clean_html(text: str) -> str:
    """HTML 태그 및 엔티티 제거"""
    text = _TAG_RE.sub("", text)
    text = _ENTITY_RE.sub(" ", text)
    return text.strip()


def _parse_pub_date(pub_date_str: str) -> Optional[date]:
    """RFC 2822 pubDate → date"""
    try:
        dt = parsedate_to_datetime(pub_date_str)
        return dt.date()
    except Exception:
        return None
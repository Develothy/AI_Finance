"""
네이버 종목토론방 크롤링 (Phase 7B)
====================================

https://finance.naver.com/item/board.naver?code=XXXXXX
일별 게시글 수 및 댓글 수를 수집한다.
"""

import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from core import get_logger
from core.decorators import retry

logger = get_logger("naver_community_fetcher")

_BASE_URL = "https://finance.naver.com/item/board.naver"
_RATE_LIMIT_DELAY = 0.4
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class CommunityFetchResult:
    records: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    message: str = ""


class NaverCommunityFetcher:
    """네이버 종목토론방 크롤러"""

    def __init__(self):
        self.available = True
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": _USER_AGENT,
            "Referer": "https://finance.naver.com/",
        })

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="naver_community")
    def _fetch_page(self, code: str, page: int = 1) -> str:
        """종목토론방 단일 페이지 HTML"""
        resp = self.session.get(_BASE_URL, params={"code": code, "page": page}, timeout=10)
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.text

    def _parse_board_page(self, html: str) -> list[dict]:
        """게시판 HTML에서 게시글 파싱"""
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table.type2 tbody tr")

        posts = []
        for row in rows:
            cols = row.select("td")
            if len(cols) < 4:
                continue

            # 날짜
            date_td = cols[0]
            date_text = date_td.get_text(strip=True)
            post_date = self._parse_date(date_text)
            if post_date is None:
                continue

            # 제목 확인 (게시글인지)
            title_td = cols[1]
            title_a = title_td.select_one("a")
            if not title_a:
                continue

            # 댓글 수
            comment_count = 0
            comment_span = title_td.select_one("span.cmt")
            if comment_span:
                cmt_match = re.search(r"\d+", comment_span.get_text(strip=True))
                if cmt_match:
                    comment_count = int(cmt_match.group())

            posts.append({
                "date": post_date,
                "comment_count": comment_count,
            })

        return posts

    @staticmethod
    def _parse_date(date_text: str) -> Optional[date]:
        """날짜 문자열 → date"""
        date_text = date_text.strip()[:10]
        try:
            return datetime.strptime(date_text, "%Y.%m.%d").date()
        except ValueError:
            return None

    def fetch_community_data(
        self, code: str, market: str = "KR",
        start_date: str = None, end_date: str = None,
        days: int = 30, max_pages: int = 20,
    ) -> list[dict]:
        """단일 종목 커뮤니티 데이터 수집 → 일별 집계"""
        if not self.available:
            return []

        end_dt = date.fromisoformat(end_date) if end_date else date.today()
        start_dt = date.fromisoformat(start_date) if start_date else end_dt - timedelta(days=days)

        all_posts = []

        for page_num in range(1, max_pages + 1):
            try:
                html = self._fetch_page(code, page_num)
            except Exception as e:
                logger.warning(f"페이지 조회 실패: {code} p={page_num} - {e}", "fetch_community_data")
                break

            posts = self._parse_board_page(html)
            if not posts:
                break

            reached_start = False
            for post in posts:
                if post["date"] < start_dt:
                    reached_start = True
                    continue
                if post["date"] > end_dt:
                    continue
                all_posts.append(post)

            oldest = min(p["date"] for p in posts)
            if oldest < start_dt or reached_start:
                break

        # 일별 집계
        daily = defaultdict(lambda: {"post_count": 0, "comment_count": 0})
        for post in all_posts:
            daily[post["date"]]["post_count"] += 1
            daily[post["date"]]["comment_count"] += post["comment_count"]

        return [
            {
                "date": dt,
                "market": market,
                "code": code,
                "community_post_count": daily[dt]["post_count"],
                "community_comment_count": daily[dt]["comment_count"],
            }
            for dt in sorted(daily.keys())
        ]

    def fetch_all(
        self, codes: list[str], market: str,
        start_date: str = None, end_date: str = None,
        days: int = 30, max_pages_per_code: int = 20,
    ) -> CommunityFetchResult:
        """전체 종목 커뮤니티 수집"""
        result = CommunityFetchResult(total_count=len(codes))

        if not self.available:
            result.message = "커뮤니티 수집 비활성화"
            return result

        logger.info(f"커뮤니티 수집 시작: {market} ({len(codes)}종목)", "fetch_all")

        for code in codes:
            try:
                records = self.fetch_community_data(
                    code, market, start_date, end_date, days, max_pages_per_code,
                )
                result.records.extend(records)
                result.success_count += 1
            except Exception as e:
                result.failed_count += 1
                logger.warning(f"커뮤니티 실패: {code} - {e}", "fetch_all")

        result.message = f"커뮤니티 완료: {result.success_count}/{result.total_count} 성공, {len(result.records)}건"
        logger.info(result.message, "fetch_all")
        return result

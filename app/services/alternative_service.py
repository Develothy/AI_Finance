"""
대안 데이터 서비스 (Phase 7)
============================

Google Trends + 네이버 커뮤니티 수집 → DB 저장
"""

from core import get_logger
from db import database
from data_collector.google_trends_fetcher import GoogleTrendsFetcher
from data_collector.naver_community_fetcher import NaverCommunityFetcher
from repositories.alternative_repository import AlternativeRepository

logger = get_logger("alternative_service")


class AlternativeService:

    def __init__(self):
        self.trends_fetcher = GoogleTrendsFetcher()
        self.community_fetcher = NaverCommunityFetcher()

    def collect_trends(
        self, market: str, codes: list[tuple[str, str]] = None,
        start_date: str = None, end_date: str = None, days: int = 90,
    ) -> dict:
        """Google Trends 수집 → DB 저장"""
        if codes is None:
            codes = self._get_stock_codes_with_names(market)

        if not codes:
            return {"market": market, "message": "종목 없음", "saved": 0}

        result = self.trends_fetcher.fetch_all(codes, market, start_date, end_date, days)

        for rec in result.records:
            rec["source"] = "google_trends"

        saved = 0
        if result.records:
            with database.session() as session:
                repo = AlternativeRepository(session)
                saved = repo.upsert_trends_data(result.records)

        return {
            "market": market,
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "saved": saved,
            "message": result.message,
        }

    def collect_community(
        self, market: str, codes: list[str] = None,
        start_date: str = None, end_date: str = None, days: int = 30,
    ) -> dict:
        """네이버 커뮤니티 수집 → DB 저장"""
        if codes is None:
            codes = self._get_stock_codes(market)

        if not codes:
            return {"market": market, "message": "종목 없음", "saved": 0}

        result = self.community_fetcher.fetch_all(codes, market, start_date, end_date, days)

        for rec in result.records:
            rec["source"] = "naver_community"

        saved = 0
        if result.records:
            with database.session() as session:
                repo = AlternativeRepository(session)
                saved = repo.upsert_community_data(result.records)

        return {
            "market": market,
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "saved": saved,
            "message": result.message,
        }

    def collect_all(
        self, market: str, codes: list[str] = None,
        start_date: str = None, end_date: str = None, days: int = 90,
    ) -> dict:
        """Google Trends + 커뮤니티 모두 수집"""
        code_names = self._get_stock_codes_with_names(market)
        if codes:
            code_names = [(c, n) for c, n in code_names if c in codes]
        code_only = [c for c, _ in code_names]

        trends_result = self.collect_trends(market, code_names, start_date, end_date, days)
        community_result = self.collect_community(market, code_only, start_date, end_date, min(days, 30))

        total_saved = trends_result.get("saved", 0) + community_result.get("saved", 0)

        return {
            "trends": trends_result,
            "community": community_result,
            "total_saved": total_saved,
            "saved": total_saved,
            "message": f"대안 데이터 수집 완료: Trends {trends_result.get('saved', 0)}건 + 커뮤니티 {community_result.get('saved', 0)}건",
        }

    @staticmethod
    def _get_stock_codes_with_names(market: str) -> list[tuple[str, str]]:
        from repositories import StockRepository

        with database.session() as session:
            return StockRepository(session).get_codes_with_names(market)

    @staticmethod
    def _get_stock_codes(market: str) -> list[str]:
        from repositories import StockRepository

        with database.session() as session:
            return StockRepository(session).get_codes_by_market(market)

"""
뉴스 센티먼트 서비스 (Phase 4)
==============================

뉴스 수집 → 센티먼트 분석 → DB 저장 오케스트레이션
"""

from typing import Optional

from core import get_logger
from db import database
from data_collector.news_fetcher import NewsFetcher
from data_collector.sentiment_analyzer import SentimentAnalyzer
from repositories.news_repository import NewsRepository

logger = get_logger("news_service")


class NewsService:
    # 뉴스 센티먼트 수집/조회

    def __init__(self):
        self.fetcher = NewsFetcher()

    # ============================================================
    # 수집
    # ============================================================

    def collect(
        self,
        market: str = "KR",
        codes: list[tuple[str, str]] = None,
        include_market_news: bool = True,
        max_items_per_code: int = 50,
    ) -> dict:
        """
        뉴스 수집 → 센티먼트 분석 → DB 저장

        Args:
            market: KR
            codes: [(code, name), ...] 리스트 (None이면 DB에서 전체 조회)
            include_market_news: 시장 전체 뉴스 포함 여부
            max_items_per_code: 종목당 최대 뉴스 건수
        """
        if not self.fetcher.available:
            return {
                "total_codes": 0,
                "stock_success": 0,
                "stock_failed": 0,
                "market_news": 0,
                "saved": 0,
                "message": "Naver API 키 미설정",
            }

        analyzer = SentimentAnalyzer.get_instance()

        if codes is None:
            codes = self._get_stock_codes_with_names(market)

        all_records = []
        stock_success = 0
        stock_failed = 0

        # 1. 종목별 뉴스 수집
        for code, name in codes:
            try:
                records = self.fetcher.fetch_stock_news(
                    code=code,
                    stock_name=name,
                    market=market,
                    max_items=max_items_per_code,
                )
                if records:
                    records = self._analyze_records(analyzer, records)
                    all_records.extend(records)
                stock_success += 1
            except Exception as e:
                logger.warning(
                    f"종목 뉴스 수집 실패: {code} ({name}) - {e}",
                    "collect",
                )
                stock_failed += 1

        # 2. 시장 전체 뉴스 수집
        market_count = 0
        if include_market_news:
            try:
                market_records = self.fetcher.fetch_market_news(
                    market=market,
                    max_items=100,
                )
                if market_records:
                    market_records = self._analyze_records(analyzer, market_records)
                    all_records.extend(market_records)
                    market_count = len(market_records)
            except Exception as e:
                logger.warning(f"시장 뉴스 수집 실패: {e}", "collect")

        # 3. DB 저장
        saved = 0
        if all_records:
            with database.session() as session:
                repo = NewsRepository(session)
                saved = repo.upsert_articles(all_records)

        logger.info(
            f"뉴스 센티먼트 수집 완료: 종목 {stock_success}성공/{stock_failed}실패, "
            f"시장뉴스 {market_count}건, 총 {saved}건 저장",
            "collect",
        )

        return {
            "total_codes": len(codes),
            "stock_success": stock_success,
            "stock_failed": stock_failed,
            "market_news": market_count,
            "saved": saved,
            "message": f"총 {saved}건 저장",
        }

    # ============================================================
    # 조회
    # ============================================================

    def get_articles(
        self,
        code: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100,
    ) -> list[dict]:
        with database.session() as session:
            repo = NewsRepository(session)
            rows = repo.get_articles(code, start_date, end_date, limit)
            return [self._to_dict(r) for r in rows]

    def get_sentiment_summary(self, code: str) -> Optional[dict]:
        with database.session() as session:
            repo = NewsRepository(session)
            return repo.get_latest_sentiment(code)

    # ============================================================
    # 내부 헬퍼
    # ============================================================

    @staticmethod
    def _analyze_records(analyzer: SentimentAnalyzer, records: list[dict]) -> list[dict]:
        texts = [
            r["title"] + ". " + (r.get("description") or "")
            for r in records
        ]
        sentiments = analyzer.analyze(texts)
        for rec, sent in zip(records, sentiments):
            rec["sentiment_score"] = sent["sentiment_score"]
            rec["sentiment_label"] = sent["label"]
        return records

    @staticmethod
    def _get_stock_codes_with_names(market: str) -> list[tuple[str, str]]:
        from models import StockInfo

        with database.session() as session:
            rows = (
                session.query(StockInfo.code, StockInfo.name)
                .filter(StockInfo.market == market)
                .all()
            )
        return [(r.code, r.name) for r in rows]

    @staticmethod
    def _to_dict(n) -> dict:
        return {
            "id": n.id,
            "date": str(n.date) if n.date else None,
            "market": n.market,
            "code": n.code,
            "title": n.title,
            "description": n.description,
            "url": n.url,
            "source": n.source,
            "sentiment_score": float(n.sentiment_score) if n.sentiment_score else None,
            "sentiment_label": n.sentiment_label,
        }
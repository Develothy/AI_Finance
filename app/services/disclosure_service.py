"""
공시 + 수급 서비스 (Phase 5)
============================

DART 공시 수집/센티먼트 + KRX 수급 수집 오케스트레이션
"""

from typing import Optional

from core import get_logger
from db import database
from data_collector.disclosure_fetcher import DisclosureFetcher
from data_collector.krx_fetcher import KRXSupplyFetcher
from repositories.disclosure_repository import DisclosureRepository

logger = get_logger("disclosure_service")


class DisclosureService:
    """공시 + 수급 데이터 수집/조회 서비스"""

    def __init__(self):
        self.disclosure_fetcher = DisclosureFetcher()
        self.supply_fetcher = KRXSupplyFetcher()

    # ============================================================
    # DART 공시 수집
    # ============================================================

    def collect_disclosures(
        self,
        market: str,
        codes: list[str] = None,
        start_date: str = None,
        end_date: str = None,
        days: int = 60,
        analyze_sentiment: bool = True,
    ) -> dict:
        """
        DART 공시 수집 → 센티먼트 분석(선택) → DB 저장

        Args:
            market: KOSPI, KOSDAQ
            codes: 종목코드 리스트 (None이면 DB에서 전체 조회)
            start_date: 시작일
            end_date: 종료일
            days: lookback 일수
            analyze_sentiment: 공시 제목 센티먼트 분석 여부
        """
        if codes is None:
            codes = self._get_stock_codes(market)

        if not codes:
            return {"market": market, "message": "종목 없음", "saved": 0}

        result = self.disclosure_fetcher.fetch_all(
            codes, market, start_date, end_date, days,
        )

        # 센티먼트 분석 (KR-FinBert-SC 재사용)
        if analyze_sentiment and result.records:
            result.records = self._analyze_sentiment(result.records)

        # DB 저장
        saved = 0
        if result.records:
            with database.session() as session:
                repo = DisclosureRepository(session)
                saved = repo.upsert_disclosures(result.records)

        return {
            "market": market,
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "saved": saved,
            "message": result.message,
        }

    # ============================================================
    # KRX 수급 수집
    # ============================================================

    def collect_supply_demand(
        self,
        market: str,
        codes: list[str] = None,
        start_date: str = None,
        end_date: str = None,
        days: int = 60,
    ) -> dict:
        """
        KRX 수급 데이터 수집 → DB 저장

        Args:
            market: KOSPI, KOSDAQ
            codes: 종목코드 리스트 (None이면 DB에서 전체 조회)
            start_date: 시작일
            end_date: 종료일
            days: lookback 일수
        """
        if codes is None:
            codes = self._get_stock_codes(market)

        if not codes:
            return {"market": market, "message": "종목 없음", "saved": 0}

        result = self.supply_fetcher.fetch_all(
            codes, market, start_date, end_date, days,
        )

        # DB 저장
        saved = 0
        if result.records:
            with database.session() as session:
                repo = DisclosureRepository(session)
                saved = repo.upsert_supply_demand(result.records)

        return {
            "market": market,
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "saved": saved,
            "message": result.message,
        }

    # ============================================================
    # 조회
    # ============================================================

    def get_disclosures(
        self,
        market: str,
        code: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100,
    ) -> list[dict]:
        with database.session() as session:
            repo = DisclosureRepository(session)
            rows = repo.get_disclosures(market, code, start_date, end_date, limit)
            return [self._disclosure_to_dict(r) for r in rows]

    def get_supply_demand(
        self,
        market: str,
        code: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100,
    ) -> list[dict]:
        with database.session() as session:
            repo = DisclosureRepository(session)
            rows = repo.get_supply_demand(market, code, start_date, end_date, limit)
            return [self._supply_to_dict(r) for r in rows]

    # ============================================================
    # 내부 헬퍼
    # ============================================================

    @staticmethod
    def _analyze_sentiment(records: list[dict]) -> list[dict]:
        """공시 제목에 센티먼트 분석 적용"""
        try:
            from data_collector.sentiment_analyzer import SentimentAnalyzer
            analyzer = SentimentAnalyzer.get_instance()

            texts = [r["report_nm"] for r in records]
            sentiments = analyzer.analyze(texts)

            for rec, sent in zip(records, sentiments):
                rec["sentiment_score"] = sent["sentiment_score"]
                rec["sentiment_label"] = sent["label"]
        except Exception as e:
            logger.warning(f"공시 센티먼트 분석 실패 (건너뜀): {e}", "_analyze_sentiment")

        return records

    @staticmethod
    def _get_stock_codes(market: str) -> list[str]:
        from models import StockInfo

        with database.session() as session:
            codes = [
                r[0] for r in
                session.query(StockInfo.code)
                .filter(StockInfo.market == market)
                .all()
            ]
        return codes

    @staticmethod
    def _disclosure_to_dict(d) -> dict:
        return {
            "id": d.id,
            "date": str(d.date) if d.date else None,
            "market": d.market,
            "code": d.code,
            "corp_name": d.corp_name,
            "report_nm": d.report_nm,
            "rcept_no": d.rcept_no,
            "flr_nm": d.flr_nm,
            "rcept_dt": d.rcept_dt,
            "report_type": d.report_type,
            "type_score": float(d.type_score) if d.type_score else None,
            "sentiment_score": float(d.sentiment_score) if d.sentiment_score else None,
            "sentiment_label": d.sentiment_label,
        }

    @staticmethod
    def _supply_to_dict(s) -> dict:
        return {
            "id": s.id,
            "date": str(s.date) if s.date else None,
            "market": s.market,
            "code": s.code,
            "short_selling_volume": int(s.short_selling_volume) if s.short_selling_volume else None,
            "short_selling_ratio": float(s.short_selling_ratio) if s.short_selling_ratio else None,
            "program_buy_volume": int(s.program_buy_volume) if s.program_buy_volume else None,
            "program_sell_volume": int(s.program_sell_volume) if s.program_sell_volume else None,
            "program_net_volume": (
                int(s.program_buy_volume or 0) - int(s.program_sell_volume or 0)
            ) if s.program_buy_volume is not None or s.program_sell_volume is not None else None,
            "source": s.source,
        }

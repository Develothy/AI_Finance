"""
재무 데이터 서비스 (Phase 2)
===========================

KIS API 기초정보 + DART 재무제표 수집 오케스트레이션
"""

from datetime import datetime
from typing import Optional

from core import get_logger
from db import database
from data_collector.kis_fetcher import KISClient
from data_collector.dart_fetcher import DARTClient, get_current_quarter
from repositories import FundamentalRepository

logger = get_logger("fundamental_service")


class FundamentalService:
    """재무 데이터 수집/조회 서비스"""

    def __init__(self):
        self.kis_client = KISClient()
        self.dart_client = DARTClient()

    # ============================================================
    # KIS 기초정보 수집
    # ============================================================

    def collect_fundamentals(
        self,
        market: str,
        codes: list[str] = None,
        date: str = None,
    ) -> dict:
        """
        KIS API로 기초정보 수집 → DB 저장

        Args:
            market: KOSPI, KOSDAQ
            codes: 종목코드 리스트 (None이면 DB에서 전체 조회)
            date: 수집 날짜 (기본: 오늘)
        """
        if codes is None:
            codes = self._get_stock_codes(market)

        if not codes:
            return {"market": market, "message": "종목 없음", "saved": 0}

        result = self.kis_client.fetch_all(codes, market, date)

        if result.skipped:
            return {
                "market": market,
                "skipped": True,
                "message": result.message,
                "saved": 0,
            }

        # DB 저장
        saved = 0
        if result.fundamentals:
            with database.session() as session:
                repo = FundamentalRepository(session)
                saved = repo.upsert_fundamentals(result.fundamentals)

        return {
            "market": market,
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "saved": saved,
            "message": result.message,
        }

    # ============================================================
    # DART 재무제표 수집
    # ============================================================

    def collect_financial_statements(
        self,
        market: str,
        codes: list[str] = None,
        year: int = None,
        quarter: str = None,
    ) -> dict:
        """
        DART API로 재무제표 수집 → DB 저장

        Args:
            market: KOSPI, KOSDAQ
            codes: 종목코드 리스트 (None이면 DB에서 전체 조회)
            year: 사업연도 (None이면 자동 판단)
            quarter: Q1, Q2, Q3, A (None이면 자동 판단)
        """
        if codes is None:
            codes = self._get_stock_codes(market)

        if not codes:
            return {"market": market, "message": "종목 없음", "saved": 0}

        # 자동 분기 판단
        if year is None or quarter is None:
            auto_year, auto_quarter = get_current_quarter()
            year = year or auto_year
            quarter = quarter or auto_quarter

        result = self.dart_client.fetch_all(codes, market, year, quarter)

        if result.skipped:
            return {
                "market": market,
                "year": year,
                "quarter": quarter,
                "skipped": True,
                "message": result.message,
                "saved": 0,
            }

        # DB 저장
        saved = 0
        if result.statements:
            with database.session() as session:
                repo = FundamentalRepository(session)
                saved = repo.upsert_financial_statements(result.statements)

        return {
            "market": market,
            "year": year,
            "quarter": quarter,
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "saved": saved,
            "message": result.message,
        }

    # ============================================================
    # 조회
    # ============================================================

    def get_fundamentals(
        self,
        market: str,
        code: str,
        start_date: str = None,
        end_date: str = None,
    ) -> list[dict]:
        """종목 기초정보 조회"""
        with database.session() as session:
            repo = FundamentalRepository(session)
            rows = repo.get_fundamentals(market, code, start_date, end_date)
            return [self._fundamental_to_dict(r) for r in rows]

    def get_financial_statements(
        self,
        market: str,
        code: str,
        limit: int = 20,
    ) -> list[dict]:
        """종목 재무제표 조회"""
        with database.session() as session:
            repo = FundamentalRepository(session)
            rows = repo.get_financial_statements(market, code, limit)
            return [self._statement_to_dict(r) for r in rows]

    def get_summary(self, market: str, code: str) -> dict:
        """종목 재무 종합 요약"""
        with database.session() as session:
            repo = FundamentalRepository(session)
            fund = repo.get_latest_fundamental(market, code)
            stmt = repo.get_latest_financial_statement(market, code)

            return {
                "market": market,
                "code": code,
                "fundamental": self._fundamental_to_dict(fund) if fund else None,
                "financial_statement": self._statement_to_dict(stmt) if stmt else None,
            }

    # ============================================================
    # 내부 헬퍼
    # ============================================================

    @staticmethod
    def _get_stock_codes(market: str) -> list[str]:
        """DB에서 마켓별 종목 코드 조회"""
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
    def _fundamental_to_dict(f) -> dict:
        return {
            "market": f.market,
            "code": f.code,
            "date": str(f.date) if f.date else None,
            "per": float(f.per) if f.per else None,
            "pbr": float(f.pbr) if f.pbr else None,
            "eps": float(f.eps) if f.eps else None,
            "bps": float(f.bps) if f.bps else None,
            "market_cap": int(f.market_cap) if f.market_cap else None,
            "div_yield": float(f.div_yield) if f.div_yield else None,
            "foreign_ratio": float(f.foreign_ratio) if f.foreign_ratio else None,
            "inst_net_buy": int(f.inst_net_buy) if f.inst_net_buy else None,
            "foreign_net_buy": int(f.foreign_net_buy) if f.foreign_net_buy else None,
            "individual_net_buy": int(f.individual_net_buy) if f.individual_net_buy else None,
        }

    @staticmethod
    def _statement_to_dict(s) -> dict:
        return {
            "market": s.market,
            "code": s.code,
            "period": s.period,
            "period_date": str(s.period_date) if s.period_date else None,
            "revenue": int(s.revenue) if s.revenue else None,
            "operating_profit": int(s.operating_profit) if s.operating_profit else None,
            "net_income": int(s.net_income) if s.net_income else None,
            "roe": float(s.roe) if s.roe else None,
            "roa": float(s.roa) if s.roa else None,
            "debt_ratio": float(s.debt_ratio) if s.debt_ratio else None,
            "operating_margin": float(s.operating_margin) if s.operating_margin else None,
            "net_margin": float(s.net_margin) if s.net_margin else None,
            "source": s.source,
        }

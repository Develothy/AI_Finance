"""
재무 데이터 Repository
"""

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models import StockFundamental, FinancialStatement


class FundamentalRepository:

    def __init__(self, session: Session):
        self.session = session

    def _upsert(self, model, records, constraint, index_elements, update_fields, extra_set=None) -> int:
        if not records:
            return 0

        if settings.DB_TYPE == "postgresql":
            stmt = pg_insert(model).values(records)
            set_ = {col: getattr(stmt.excluded, col) for col in update_fields}
            if extra_set:
                set_.update(extra_set)
            stmt = stmt.on_conflict_do_update(constraint=constraint, set_=set_)
        else:
            stmt = sqlite_insert(model).values(records)
            set_ = {col: getattr(stmt.excluded, col) for col in update_fields}
            if extra_set:
                set_.update(extra_set)
            stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)

        self.session.execute(stmt)
        return len(records)

    # ============================================================
    # StockFundamental
    # ============================================================

    def upsert_fundamentals(self, records: list[dict]) -> int:
        # 기초정보 Upsert
        if not records:
            return 0

        update_fields = [
            "per", "pbr", "eps", "bps", "market_cap", "div_yield",
            "foreign_ratio", "inst_net_buy", "foreign_net_buy", "individual_net_buy",
        ]

        return self._upsert(
            StockFundamental, records, "uq_stock_fundamental",
            ["market", "code", "date"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    def get_fundamentals(
        self,
        market: str,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[StockFundamental]:
        # 종목별 기초정보 조회
        query = self.session.query(StockFundamental).filter(
            StockFundamental.market == market,
            StockFundamental.code == code,
        )
        if start_date:
            query = query.filter(StockFundamental.date >= start_date)
        if end_date:
            query = query.filter(StockFundamental.date <= end_date)
        return query.order_by(StockFundamental.date).all()

    def get_latest_fundamental(self, market: str, code: str) -> Optional[StockFundamental]:
        # 최신 기초정보 1건
        return self.session.query(StockFundamental).filter(
            StockFundamental.market == market,
            StockFundamental.code == code,
        ).order_by(StockFundamental.date.desc()).first()

    # ============================================================
    # FinancialStatement
    # ============================================================

    def upsert_financial_statements(self, records: list[dict]) -> int:
        # 재무제표 Upsert
        if not records:
            return 0

        update_fields = [
            "period_date", "revenue", "operating_profit", "net_income",
            "roe", "roa", "debt_ratio", "operating_margin", "net_margin", "source",
        ]

        return self._upsert(
            FinancialStatement, records, "uq_financial_statement",
            ["market", "code", "period"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    def get_financial_statements(
        self,
        market: str,
        code: str,
        limit: int = 20,
    ) -> list[FinancialStatement]:
        # 종목별 재무제표 조회 (최신순)
        return self.session.query(FinancialStatement).filter(
            FinancialStatement.market == market,
            FinancialStatement.code == code,
        ).order_by(FinancialStatement.period_date.desc()).limit(limit).all()

    def get_latest_financial_statement(self, market: str, code: str) -> Optional[FinancialStatement]:
        # 최신 분기 재무제표 1건
        return self.session.query(FinancialStatement).filter(
            FinancialStatement.market == market,
            FinancialStatement.code == code,
        ).order_by(FinancialStatement.period_date.desc()).first()

    def get_financial_statements_for_features(
        self,
        market: str,
        code: str,
    ) -> list[FinancialStatement]:
        #피처 계산용 재무제표 조회 (오래된 순)
        return self.session.query(FinancialStatement).filter(
            FinancialStatement.market == market,
            FinancialStatement.code == code,
        ).order_by(FinancialStatement.period_date.asc()).all()
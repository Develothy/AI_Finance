"""
재무 데이터
- StockFundamental: KIS API 일별 기초정보 + 투자자별매매
- FinancialStatement: DART 분기별 재무제표
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)

from db import ModelBase


class StockFundamental(ModelBase):
    """종목 기초정보 + 투자자별 매매 (KIS API 일별)"""

    __tablename__ = "stock_fundamental"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)

    # 밸류에이션
    per = Column(Numeric(10, 2))
    pbr = Column(Numeric(10, 2))
    eps = Column(Numeric(15, 2))
    bps = Column(Numeric(15, 2))
    market_cap = Column(BigInteger)
    div_yield = Column(Numeric(8, 4))

    # 외국인 보유
    foreign_ratio = Column(Numeric(8, 4))

    # 투자자별 순매수
    inst_net_buy = Column(BigInteger)
    foreign_net_buy = Column(BigInteger)
    individual_net_buy = Column(BigInteger)

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("market", "code", "date", name="uq_stock_fundamental"),
        Index("idx_fundamental_lookup", "market", "code", "date"),
    )

    def __repr__(self):
        return f"<StockFundamental({self.market}:{self.code} {self.date})>"


class FinancialStatement(ModelBase):
    # 분기별 재무제표 (DART API)

    __tablename__ = "financial_statement"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)       # 2024Q4, 2024A
    period_date = Column(Date, nullable=False)         # 분기 말일

    # 손익계산서
    revenue = Column(BigInteger)
    operating_profit = Column(BigInteger)
    net_income = Column(BigInteger)

    # 수익성 지표
    roe = Column(Numeric(10, 2))
    roa = Column(Numeric(10, 2))
    operating_margin = Column(Numeric(10, 2))
    net_margin = Column(Numeric(10, 2))

    # 안정성 지표
    debt_ratio = Column(Numeric(10, 2))

    source = Column(String(30), default="dart")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("market", "code", "period", name="uq_financial_statement"),
        Index("idx_financial_lookup", "market", "code", "period_date"),
    )

    def __repr__(self):
        return f"<FinancialStatement({self.market}:{self.code} {self.period})>"
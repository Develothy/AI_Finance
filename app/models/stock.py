"""
주식 데이터 모델
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    BigInteger,
    DateTime,
    Index,
    UniqueConstraint,
)

from db import ModelBase


class StockPrice(ModelBase):
    """주가 데이터 (일봉)"""

    __tablename__ = "stock_price"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(10), nullable=False)      # 'KOSPI', 'KOSDAQ', 'NYSE', 'NASDAQ'
    code = Column(String(20), nullable=False)        # '005930', 'AAPL'
    date = Column(Date, nullable=False)
    open = Column(Numeric(15, 2))
    high = Column(Numeric(15, 2))
    low = Column(Numeric(15, 2))
    close = Column(Numeric(15, 2))
    volume = Column(BigInteger)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('market', 'code', 'date', name='uq_stock_price'),
        Index('idx_stock_price_lookup', 'market', 'code', 'date'),
    )

    def __repr__(self):
        return f"<StockPrice({self.market}:{self.code} {self.date})>"


class StockInfo(ModelBase):
    """종목 정보"""

    __tablename__ = "stock_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    name = Column(String(100))
    sector = Column(String(50))
    industry = Column(String(100))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('market', 'code', name='uq_stock_info'),
        Index('idx_stock_info_sector', 'market', 'sector'),
    )

    def __repr__(self):
        return f"<StockInfo({self.market}:{self.code} {self.name})>"
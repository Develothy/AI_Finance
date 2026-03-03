"""
거시경제 지표 테이블 (Phase 3)
"""

from datetime import datetime

from sqlalchemy import (
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


class MacroIndicator(ModelBase):
    """거시경제 지표 (EAV 구조)"""

    __tablename__ = "macro_indicator"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    indicator_name = Column(String(50), nullable=False)  # KRW_USD, VIX, KOSPI, SP500, US_10Y, KR_3Y, WTI, GOLD
    value = Column(Numeric(15, 4), nullable=False)
    change_pct = Column(Numeric(10, 6))  # 전일 대비 변화율
    source = Column(String(30))  # yfinance / fdr / fred
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("date", "indicator_name", name="uq_macro_indicator"),
        Index("idx_macro_lookup", "indicator_name", "date"),
    )

    def __repr__(self):
        return f"<MacroIndicator({self.indicator_name} {self.date} = {self.value})>"

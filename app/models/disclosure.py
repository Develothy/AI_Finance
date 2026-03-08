"""
공시 + 수급 데이터 (Phase 5)
- DartDisclosure: DART 공시 목록
- KrxSupplyDemand: KRX 공매도/프로그램매매
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


class DartDisclosure(ModelBase):
    """DART 전자공시 목록"""

    __tablename__ = "dart_disclosure"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    corp_name = Column(String(100), nullable=True)
    report_nm = Column(String(500), nullable=False)    # 공시 제목
    rcept_no = Column(String(20), nullable=False)       # 접수번호
    flr_nm = Column(String(100), nullable=True)         # 공시 제출인
    rcept_dt = Column(String(10), nullable=True)        # 접수일 (YYYYMMDD)

    # 공시 유형 분류
    report_type = Column(String(50), nullable=True)     # 실적, 지분, 기타 등
    type_score = Column(Numeric(4, 2), default=0.2)     # 유형 가중치 (실적=1.0)

    # 센티먼트 (KR-FinBert-SC 재사용)
    sentiment_score = Column(Numeric(6, 4), nullable=True)
    sentiment_label = Column(String(20), nullable=True)

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("rcept_no", "code", name="uq_dart_disclosure"),
        Index("idx_disclosure_lookup", "market", "code", "date"),
        Index("idx_disclosure_date", "date"),
    )

    def __repr__(self):
        return f"<DartDisclosure({self.code} {self.date} {self.report_nm[:30]})>"


class KrxSupplyDemand(ModelBase):
    """KRX 수급 데이터 (공매도 + 프로그램매매)"""

    __tablename__ = "krx_supply_demand"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)

    # 공매도
    short_selling_volume = Column(BigInteger, nullable=True)
    short_selling_ratio = Column(Numeric(8, 4), nullable=True)  # %

    # 프로그램매매
    program_buy_volume = Column(BigInteger, nullable=True)
    program_sell_volume = Column(BigInteger, nullable=True)

    # 신용잔고 (추후 데이터소스 확보 시 구현)
    margin_balance = Column(BigInteger, nullable=True)

    source = Column(String(30), default="pykrx")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("market", "code", "date", name="uq_krx_supply_demand"),
        Index("idx_supply_demand_lookup", "market", "code", "date"),
    )

    def __repr__(self):
        return f"<KrxSupplyDemand({self.market}:{self.code} {self.date})>"

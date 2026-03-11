"""
대안 데이터 테이블 (Phase 7)
- Google Trends + 네이버 종목토론방
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


class AlternativeData(ModelBase):
    """대안 데이터 (Google Trends + 커뮤니티)"""

    __tablename__ = "alternative_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)

    # Google Trends
    google_trend_value = Column(Numeric(6, 2))         # 0~100 주간 원본
    google_trend_interpolated = Column(Numeric(6, 2))  # 일별 보간값

    # 네이버 종목토론방
    community_post_count = Column(Integer)             # 일별 게시글 수
    community_comment_count = Column(Integer)           # 일별 댓글 수

    source = Column(String(30), default="mixed")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("market", "code", "date", name="uq_alternative_data"),
        Index("idx_alternative_lookup", "market", "code", "date"),
        Index("idx_alternative_date", "date"),
    )

    def __repr__(self):
        return f"<AlternativeData({self.market}:{self.code} {self.date})>"

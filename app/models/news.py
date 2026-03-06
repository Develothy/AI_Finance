"""
뉴스 센티먼트 테이블 (Phase 4)
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
    Text,
    UniqueConstraint,
)

from db import ModelBase


class NewsSentiment(ModelBase):
    """뉴스 기사별 센티먼트 스코어"""

    __tablename__ = "news_sentiment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    market = Column(String(10), nullable=False, default="KR")
    code = Column(String(20), nullable=True)  # NULL = 시장 전체 뉴스
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    url = Column(String(1000), nullable=True)
    source = Column(String(30), default="naver")
    sentiment_score = Column(Numeric(6, 4), nullable=True)  # -1.0 ~ +1.0
    sentiment_label = Column(String(20), nullable=True)  # positive / negative / neutral
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("url", "code", name="uq_news_sentiment"),
        Index("idx_news_lookup", "market", "code", "date"),
        Index("idx_news_date", "date"),
        Index("idx_news_code_date", "code", "date"),
    )

    def __repr__(self):
        return f"<NewsSentiment({self.code or 'MARKET'} {self.date} {self.sentiment_label})>"
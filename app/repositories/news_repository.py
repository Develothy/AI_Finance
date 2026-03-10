"""
뉴스 센티먼트 Repository (Phase 4)
"""

from datetime import datetime, date
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models.news import NewsSentiment


class NewsRepository:

    def __init__(self, session: Session):
        self.session = session

    def _upsert(self, model, records, constraint, index_elements,
                update_fields, extra_set=None) -> int:
        # pg/sqlite 분기 upsert
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
            stmt = stmt.on_conflict_do_update(
                index_elements=index_elements, set_=set_,
            )

        self.session.execute(stmt)
        return len(records)

    # ============================================================
    # Upsert
    # ============================================================

    def upsert_articles(self, records: list[dict]) -> int:
        if not records:
            return 0

        update_fields = [
            "title", "description", "sentiment_score",
            "sentiment_label", "source",
        ]
        return self._upsert(
            NewsSentiment, records, "uq_news_sentiment",
            ["url", "code"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    # ============================================================
    # 집계 조회 (feature_engineer 용)
    # ============================================================

    def _stddev_expr(self):
        # SQLite/PostgreSQL 호환 표준편차 표현식
        if settings.DB_TYPE == "sqlite":
            # SQLite에는 stddev 없음 → sqrt(avg(x²) - avg(x)²)
            avg_sq = func.avg(
                NewsSentiment.sentiment_score * NewsSentiment.sentiment_score,
            )
            sq_avg = (
                func.avg(NewsSentiment.sentiment_score)
                * func.avg(NewsSentiment.sentiment_score)
            )
            # abs()로 부동소수점 오차 방지
            return func.coalesce(func.sqrt(func.abs(avg_sq - sq_avg)), 0)
        else:
            return func.coalesce(func.stddev(NewsSentiment.sentiment_score), 0)

    def get_daily_sentiment(
        self,
        code: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        종목별 일별 센티먼트 집계

        Returns:
            [{"date": ..., "news_sentiment": ..., "news_volume": ...,
              "news_sentiment_std": ...}, ...]
        """
        std_expr = self._stddev_expr()

        rows = (
            self.session.query(
                NewsSentiment.date,
                func.avg(NewsSentiment.sentiment_score).label("avg_score"),
                func.count(NewsSentiment.id).label("volume"),
                std_expr.label("std_score"),
            )
            .filter(
                NewsSentiment.code == code,
                NewsSentiment.date >= start_date,
                NewsSentiment.date <= end_date,
            )
            .group_by(NewsSentiment.date)
            .order_by(NewsSentiment.date)
            .all()
        )

        return [{
            "date": r.date,
            "news_sentiment": round(float(r.avg_score), 4) if r.avg_score else None,
            "news_volume": int(r.volume),
            "news_sentiment_std": round(float(r.std_score), 4) if r.std_score else None,
        } for r in rows]

    def get_daily_market_sentiment(
        self,
        start_date: date,
        end_date: date,
        market: str = "KR",
    ) -> list[dict]:
        """
        시장 전체 일별 센티먼트 집계 (code IS NULL)

        Returns:
            [{"date": ..., "market_sentiment": ..., "market_news_volume": ...}, ...]
        """
        rows = (
            self.session.query(
                NewsSentiment.date,
                func.avg(NewsSentiment.sentiment_score).label("avg_score"),
                func.count(NewsSentiment.id).label("volume"),
            )
            .filter(
                NewsSentiment.code.is_(None),
                NewsSentiment.market == market,
                NewsSentiment.date >= start_date,
                NewsSentiment.date <= end_date,
            )
            .group_by(NewsSentiment.date)
            .order_by(NewsSentiment.date)
            .all()
        )

        return [{
            "date": r.date,
            "market_sentiment": round(float(r.avg_score), 4) if r.avg_score else None,
            "market_news_volume": int(r.volume),
        } for r in rows]

    def get_daily_sentiment_filtered(
        self,
        code: str,
        stock_name: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        종목별 일별 센티먼트 (제목에 종목명 포함 필터링, Phase 6B)

        Returns:
            [{"date": ..., "news_sentiment_filtered": ...,
              "news_relevance_ratio": ...}, ...]
        """
        from sqlalchemy import case

        is_relevant = NewsSentiment.title.contains(stock_name)

        rows = (
            self.session.query(
                NewsSentiment.date,
                func.count(NewsSentiment.id).label("total_count"),
                func.sum(case((is_relevant, 1), else_=0)).label("filtered_count"),
                func.avg(
                    case(
                        (is_relevant, NewsSentiment.sentiment_score),
                        else_=None,
                    )
                ).label("filtered_avg_score"),
            )
            .filter(
                NewsSentiment.code == code,
                NewsSentiment.date >= start_date,
                NewsSentiment.date <= end_date,
            )
            .group_by(NewsSentiment.date)
            .order_by(NewsSentiment.date)
            .all()
        )

        return [{
            "date": r.date,
            "news_sentiment_filtered": (
                round(float(r.filtered_avg_score), 4)
                if r.filtered_avg_score else None
            ),
            "news_relevance_ratio": (
                round(int(r.filtered_count) / int(r.total_count), 4)
                if r.total_count and int(r.total_count) > 0 else None
            ),
        } for r in rows]

    # ============================================================
    # 일반 조회
    # ============================================================

    def get_articles(
        self,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
    ) -> list[NewsSentiment]:
        query = self.session.query(NewsSentiment)
        if code is not None:
            query = query.filter(NewsSentiment.code == code)
        if start_date:
            query = query.filter(NewsSentiment.date >= start_date)
        if end_date:
            query = query.filter(NewsSentiment.date <= end_date)
        return query.order_by(NewsSentiment.date.desc()).limit(limit).all()

    def get_latest_sentiment(self, code: str) -> Optional[dict]:
        row = (
            self.session.query(
                NewsSentiment.date,
                func.avg(NewsSentiment.sentiment_score).label("avg_score"),
                func.count(NewsSentiment.id).label("volume"),
            )
            .filter(NewsSentiment.code == code)
            .group_by(NewsSentiment.date)
            .order_by(NewsSentiment.date.desc())
            .first()
        )
        if not row:
            return None
        return {
            "date": str(row.date) if row.date else None,
            "sentiment": round(float(row.avg_score), 4) if row.avg_score else None,
            "volume": int(row.volume),
        }
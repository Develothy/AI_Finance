"""
대안 데이터 Repository (Phase 7)
"""

from datetime import date, datetime
from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from config import settings
from models.alternative import AlternativeData


class AlternativeRepository:

    def __init__(self, session: Session):
        self.session = session

    def _upsert(self, model, records, constraint, index_elements, update_fields) -> int:
        if not records:
            return 0

        if settings.DB_TYPE == "postgresql":
            stmt = pg_insert(model).values(records)
            set_ = {col: getattr(stmt.excluded, col) for col in update_fields}
            stmt = stmt.on_conflict_do_update(constraint=constraint, set_=set_)
        else:
            stmt = sqlite_insert(model).values(records)
            set_ = {col: getattr(stmt.excluded, col) for col in update_fields}
            stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)

        self.session.execute(stmt)
        return len(records)

    def upsert_trends_data(self, records: list[dict]) -> int:
        """Google Trends 데이터만 upsert (커뮤니티 컬럼 보존)"""
        return self._upsert(
            AlternativeData, records, "uq_alternative_data",
            ["market", "code", "date"],
            ["google_trend_value", "google_trend_interpolated", "source"],
        )

    def upsert_community_data(self, records: list[dict]) -> int:
        """커뮤니티 데이터만 upsert (트렌드 컬럼 보존)"""
        return self._upsert(
            AlternativeData, records, "uq_alternative_data",
            ["market", "code", "date"],
            ["community_post_count", "community_comment_count", "source"],
        )

    def get_alternative_for_features(
        self, market: str, code: str,
        start_date: date = None, end_date: date = None,
    ) -> list[AlternativeData]:
        """피처 계산용 조회 (오래된 순)"""
        query = self.session.query(AlternativeData).filter(
            AlternativeData.market == market,
            AlternativeData.code == code,
        )
        if start_date:
            query = query.filter(AlternativeData.date >= start_date)
        if end_date:
            query = query.filter(AlternativeData.date <= end_date)
        return query.order_by(AlternativeData.date.asc()).all()

    def get_alternative_data(
        self, market: str, code: str,
        start_date: Optional[date] = None, end_date: Optional[date] = None,
        limit: int = 100,
    ) -> list[AlternativeData]:
        """일반 조회 (최신순)"""
        query = self.session.query(AlternativeData).filter(
            AlternativeData.market == market,
            AlternativeData.code == code,
        )
        if start_date:
            query = query.filter(AlternativeData.date >= start_date)
        if end_date:
            query = query.filter(AlternativeData.date <= end_date)
        return query.order_by(AlternativeData.date.desc()).limit(limit).all()

"""
거시경제 지표 Repository (Phase 3)
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models import MacroIndicator


class MacroRepository:

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
    # Upsert
    # ============================================================

    def upsert_indicators(self, records: list[dict]) -> int:
        """거시지표 Upsert"""
        if not records:
            return 0

        update_fields = ["value", "change_pct", "source"]

        return self._upsert(
            MacroIndicator, records, "uq_macro_indicator",
            ["date", "indicator_name"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    # ============================================================
    # 조회
    # ============================================================

    def get_indicators(
        self,
        indicator_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[MacroIndicator]:
        """특정 지표 시계열 조회"""
        query = self.session.query(MacroIndicator).filter(
            MacroIndicator.indicator_name == indicator_name,
        )
        if start_date:
            query = query.filter(MacroIndicator.date >= start_date)
        if end_date:
            query = query.filter(MacroIndicator.date <= end_date)
        return query.order_by(MacroIndicator.date).all()

    def get_latest(self) -> list[MacroIndicator]:
        """지표별 최신 데이터 조회"""
        # 서브쿼리: 지표별 최신 날짜
        subq = (
            self.session.query(
                MacroIndicator.indicator_name,
                func.max(MacroIndicator.date).label("max_date"),
            )
            .group_by(MacroIndicator.indicator_name)
            .subquery()
        )

        return (
            self.session.query(MacroIndicator)
            .join(
                subq,
                (MacroIndicator.indicator_name == subq.c.indicator_name)
                & (MacroIndicator.date == subq.c.max_date),
            )
            .order_by(MacroIndicator.indicator_name)
            .all()
        )

    def get_all_by_date_range(
        self,
        start_date: str,
        end_date: str,
    ) -> list[MacroIndicator]:
        """날짜 범위 내 전체 지표 조회 (feature_engineer 용)"""
        return (
            self.session.query(MacroIndicator)
            .filter(
                MacroIndicator.date >= start_date,
                MacroIndicator.date <= end_date,
            )
            .order_by(MacroIndicator.date)
            .all()
        )

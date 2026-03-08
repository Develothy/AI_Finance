"""
공시 + 수급 Repository (Phase 5)
"""

from datetime import datetime, date
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models.disclosure import DartDisclosure, KrxSupplyDemand


class DisclosureRepository:

    def __init__(self, session: Session):
        self.session = session

    def _upsert(self, model, records, constraint, index_elements,
                update_fields, extra_set=None) -> int:
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
    # DartDisclosure
    # ============================================================

    def upsert_disclosures(self, records: list[dict]) -> int:
        if not records:
            return 0

        update_fields = [
            "date", "market", "corp_name", "report_nm", "flr_nm",
            "rcept_dt", "report_type", "type_score",
            "sentiment_score", "sentiment_label",
        ]
        return self._upsert(
            DartDisclosure, records, "uq_dart_disclosure",
            ["rcept_no", "code"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    def get_disclosures(
        self,
        market: str,
        code: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
    ) -> list[DartDisclosure]:
        query = self.session.query(DartDisclosure).filter(
            DartDisclosure.market == market,
            DartDisclosure.code == code,
        )
        if start_date:
            query = query.filter(DartDisclosure.date >= start_date)
        if end_date:
            query = query.filter(DartDisclosure.date <= end_date)
        return query.order_by(DartDisclosure.date.desc()).limit(limit).all()

    def get_disclosures_for_features(
        self,
        market: str,
        code: str,
        start_date: date = None,
        end_date: date = None,
    ) -> list[DartDisclosure]:
        """피처 계산용 공시 조회 (오래된 순)"""
        query = self.session.query(DartDisclosure).filter(
            DartDisclosure.market == market,
            DartDisclosure.code == code,
        )
        if start_date:
            query = query.filter(DartDisclosure.date >= start_date)
        if end_date:
            query = query.filter(DartDisclosure.date <= end_date)
        return query.order_by(DartDisclosure.date.asc()).all()

    # ============================================================
    # KrxSupplyDemand
    # ============================================================

    def upsert_supply_demand(self, records: list[dict]) -> int:
        if not records:
            return 0

        update_fields = [
            "short_selling_volume", "short_selling_ratio",
            "program_buy_volume", "program_sell_volume",
            "margin_balance", "source",
        ]
        return self._upsert(
            KrxSupplyDemand, records, "uq_krx_supply_demand",
            ["market", "code", "date"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    def get_supply_demand(
        self,
        market: str,
        code: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
    ) -> list[KrxSupplyDemand]:
        query = self.session.query(KrxSupplyDemand).filter(
            KrxSupplyDemand.market == market,
            KrxSupplyDemand.code == code,
        )
        if start_date:
            query = query.filter(KrxSupplyDemand.date >= start_date)
        if end_date:
            query = query.filter(KrxSupplyDemand.date <= end_date)
        return query.order_by(KrxSupplyDemand.date.desc()).limit(limit).all()

    def get_supply_demand_for_features(
        self,
        market: str,
        code: str,
        start_date: date = None,
        end_date: date = None,
    ) -> list[KrxSupplyDemand]:
        """피처 계산용 수급 데이터 조회 (오래된 순)"""
        query = self.session.query(KrxSupplyDemand).filter(
            KrxSupplyDemand.market == market,
            KrxSupplyDemand.code == code,
        )
        if start_date:
            query = query.filter(KrxSupplyDemand.date >= start_date)
        if end_date:
            query = query.filter(KrxSupplyDemand.date <= end_date)
        return query.order_by(KrxSupplyDemand.date.asc()).all()

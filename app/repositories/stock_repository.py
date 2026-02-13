"""
주식 데이터 Repository
"""

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models import StockPrice, StockInfo


class StockRepository:
    """주식 데이터 DB 작업"""

    def __init__(self, session: Session):
        self.session = session

    # ============================================================
    # Upsert 헬퍼
    # ============================================================

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
    # StockPrice
    # ============================================================

    def upsert_prices(self, records: list[dict]) -> int:
        """
        주가 데이터 Upsert (Insert or Update)

        Args:
            records: [{"market": "KOSPI", "code": "005930", "date": ..., ...}]

        Returns:
            처리된 레코드 수
        """
        return self._upsert(
            StockPrice, records, 'uq_stock_price',
            ['market', 'code', 'date'],
            ['open', 'high', 'low', 'close', 'volume'],
            extra_set={'created_at': datetime.now()}
        )

    def get_prices(
            self,
            code: str,
            market: str = "KOSPI",
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
    ) -> list[StockPrice]:
        """종목별 주가 조회"""
        query = self.session.query(StockPrice).filter(
            StockPrice.code == code,
            StockPrice.market == market
        )

        if start_date:
            query = query.filter(StockPrice.date >= start_date)
        if end_date:
            query = query.filter(StockPrice.date <= end_date)

        return query.order_by(StockPrice.date).all()

    def get_latest_price(self, code: str, market: str = "KOSPI") -> Optional[StockPrice]:
        """최신 주가 조회"""
        return self.session.query(StockPrice).filter(
            StockPrice.code == code,
            StockPrice.market == market
        ).order_by(StockPrice.date.desc()).first()

    def delete_prices(self, code: str, market: str = "KOSPI") -> int:
        """종목 주가 삭제"""
        deleted = self.session.query(StockPrice).filter(
            StockPrice.code == code,
            StockPrice.market == market
        ).delete()
        return deleted

    # ============================================================
    # StockInfo
    # ============================================================

    def upsert_info(self, records: list[dict]) -> int:
        """종목 정보 Upsert"""
        return self._upsert(
            StockInfo, records, 'uq_stock_info',
            ['market', 'code'],
            ['name', 'sector', 'industry'],
            extra_set={'updated_at': datetime.now()}
        )

    def get_info(self, code: str, market: str) -> Optional[StockInfo]:
        """종목 정보 조회"""
        return self.session.query(StockInfo).filter(
            StockInfo.code == code,
            StockInfo.market == market
        ).first()

    def get_all_codes(self, market: Optional[str] = None) -> list[str]:
        """전체 종목 코드 조회"""
        query = self.session.query(StockInfo.code)
        if market:
            query = query.filter(StockInfo.market == market)
        return [row[0] for row in query.all()]

    def get_by_sector(self, sector: str, market: Optional[str] = None) -> list[StockInfo]:
        """섹터별 종목 조회"""
        query = self.session.query(StockInfo).filter(
            StockInfo.sector.contains(sector)
        )
        if market:
            query = query.filter(StockInfo.market == market)
        return query.all()
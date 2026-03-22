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

    def get_info_by_code(self, code: str, market: str = "KOSPI") -> Optional[StockInfo]:
        """종목코드로 종목 정보 조회"""
        return self.session.query(StockInfo).filter(
            StockInfo.code == code,
            StockInfo.market == market,
        ).first()

    def get_by_sector(self, sector: str, market: Optional[str] = None) -> list[StockInfo]:
        """섹터별 종목 조회 (부분 매칭)"""
        query = self.session.query(StockInfo).filter(
            StockInfo.sector.contains(sector)
        )
        if market:
            query = query.filter(StockInfo.market == market)
        return query.all()

    def get_sector_for_code(self, code: str, market: str) -> Optional[str]:
        """종목코드의 섹터 조회"""
        result = self.session.query(StockInfo.sector).filter(
            StockInfo.code == code,
            StockInfo.market == market,
        ).first()
        return result[0] if result else None

    def get_codes_in_sector(self, sector: str, market: str) -> list[str]:
        """동일 섹터 종목코드 목록 (정확 매칭)"""
        rows = self.session.query(StockInfo.code).filter(
            StockInfo.sector == sector,
            StockInfo.market == market,
        ).all()
        return [r[0] for r in rows]

    def get_industry_for_code(self, code: str, market: str) -> Optional[str]:
        """종목코드의 산업분류 조회"""
        result = self.session.query(StockInfo.industry).filter(
            StockInfo.code == code,
            StockInfo.market == market,
        ).first()
        return result[0] if result else None

    def get_codes_by_industry_keyword(self, industry: str, market: str) -> list[str]:
        """동일 산업 키워드를 가진 종목코드 목록 (LIKE 매칭)

        industry 값에서 첫 번째 주요 키워드를 추출하여 부분 매칭.
        예: "반도체 제조업" → "%반도체%" 매칭
        """
        # 첫 번째 주요 키워드 추출 (2자 이상)
        keyword = None
        for word in industry.replace(",", " ").split():
            if len(word) >= 2:
                keyword = word
                break

        if not keyword:
            return []

        rows = self.session.query(StockInfo.code).filter(
            StockInfo.industry.ilike(f"%{keyword}%"),
            StockInfo.market == market,
        ).all()
        return [r[0] for r in rows]

    def get_codes_by_market(self, market: str) -> list[str]:
        """마켓별 종목 코드 목록 (KR이면 KOSPI+KOSDAQ)"""
        query = self.session.query(StockInfo.code)
        if market == "KR":
            query = query.filter(StockInfo.market.in_(["KOSPI", "KOSDAQ"]))
        else:
            query = query.filter(StockInfo.market == market)
        return [r[0] for r in query.all()]

    def search_stocks(self, keyword: str, market: Optional[str] = None, limit: int = 50) -> list[StockInfo]:
        """종목 검색 (code 또는 name 부분 매칭)"""
        query = self.session.query(StockInfo).filter(
            (StockInfo.code.ilike(f"%{keyword}%")) |
            (StockInfo.name.ilike(f"%{keyword}%"))
        )
        if market:
            if market == "KR":
                query = query.filter(StockInfo.market.in_(["KOSPI", "KOSDAQ"]))
            else:
                query = query.filter(StockInfo.market == market)
        return query.order_by(StockInfo.name).limit(limit).all()

    def search_by_sector_or_industry(self, keyword: str, market: Optional[str] = None) -> list[StockInfo]:
        """섹터 또는 산업분류 부분 매칭으로 종목 검색"""
        query = self.session.query(StockInfo).filter(
            (StockInfo.sector.ilike(f"%{keyword}%")) |
            (StockInfo.industry.ilike(f"%{keyword}%"))
        )
        if market:
            if market == "KR":
                query = query.filter(StockInfo.market.in_(["KOSPI", "KOSDAQ"]))
            else:
                query = query.filter(StockInfo.market == market)
        return query.order_by(StockInfo.name).all()

    def get_codes_with_names(self, market: str) -> list[tuple[str, str]]:
        """마켓별 (code, name) 목록 (KR이면 KOSPI+KOSDAQ)"""
        query = self.session.query(StockInfo.code, StockInfo.name)
        if market == "KR":
            query = query.filter(StockInfo.market.in_(["KOSPI", "KOSDAQ"]))
        else:
            query = query.filter(StockInfo.market == market)
        return [(r.code, r.name) for r in query.all()]
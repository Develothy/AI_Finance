"""
주식 데이터 서비스
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from fastapi import HTTPException

from db import database
from repositories import StockRepository
from data_collector import DataPipeline
from core import get_logger, log_execution
from api.schemas import CollectRequest, CollectResponse, StockPriceResponse, StockInfoResponse

logger = get_logger("service")


def _resolve_date_range(
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """
    날짜 조건 정규화.

    - 둘 다 없음  → (None, None)  — 호출자가 latest 로직 처리
    - start만     → (start, 오늘)
    - end만       → (end, end)   — 하루
    - 둘 다 있음  → 그대로
    """
    if not start_date and not end_date:
        return None, None

    if start_date and not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    if end_date and not start_date:
        start_date = end_date

    return start_date, end_date


class StockService:

    def __init__(self, pipeline=None, db=None):
        self.pipeline = pipeline or DataPipeline()
        self.database = db or database

    # ── DB 저장 ───────────────────────────────────────────────

    @staticmethod
    def _df_to_records(df: pd.DataFrame, code: str, market: str) -> list[dict]:
        """DataFrame을 DB 레코드로 변환"""
        records = []
        for _, row in df.iterrows():
            try:
                dt = row.get('date')
                if dt is None:
                    continue

                if isinstance(dt, str):
                    dt = datetime.strptime(dt, '%Y-%m-%d').date()
                elif isinstance(dt, datetime):
                    dt = dt.date()
                elif isinstance(dt, pd.Timestamp):
                    dt = dt.date()

                record = {
                    'market': market,
                    'code': code,
                    'date': dt,
                    'open': float(row.get('open', 0)) if pd.notna(row.get('open')) else None,
                    'high': float(row.get('high', 0)) if pd.notna(row.get('high')) else None,
                    'low': float(row.get('low', 0)) if pd.notna(row.get('low')) else None,
                    'close': float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
                    'volume': int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                }
                records.append(record)
            except Exception as e:
                logger.debug(
                    f"레코드 변환 실패",
                    "_df_to_records",
                    {"code": code, "error": str(e)}
                )
        return records

    @log_execution(module="service")
    def save_to_db(self, data: dict[str, pd.DataFrame], market: str) -> int:
        """
        데이터 DB 저장

        Args:
            data: {종목코드: DataFrame} 딕셔너리
            market: 'KOSPI', 'KOSDAQ', 'NYSE', 'NASDAQ'

        Returns:
            저장된 레코드 수
        """
        total_saved = 0
        try:
            with self.database.session() as session:
                repo = StockRepository(session)
                for code, df in data.items():
                    try:
                        records = self._df_to_records(df, code, market)
                        if records:
                            repo.upsert_prices(records)
                            total_saved += len(records)
                    except Exception as e:
                        logger.error(
                            f"종목 저장 실패",
                            "save_to_db",
                            {"code": code, "error": str(e)}
                        )

            logger.info(
                f"DB 저장 완료",
                "save_to_db",
                {"market": market, "codes": len(data), "records": total_saved}
            )
        except Exception as e:
            logger.critical(
                f"DB 저장 실패",
                "save_to_db",
                {"error": str(e)}
            )
            raise

        return total_saved

    # ── 종목 정보 저장 ──────────────────────────────────────────

    def _save_stock_info(self, records: list[dict]) -> int:
        """종목 메타데이터(이름, 섹터, 산업) DB 저장"""
        try:
            with self.database.session() as session:
                repo = StockRepository(session)
                saved = repo.upsert_info(records)
                logger.info(
                    "종목 정보 저장 완료",
                    "_save_stock_info",
                    {"count": saved}
                )
                return saved
        except Exception as e:
            logger.error(
                "종목 정보 저장 실패",
                "_save_stock_info",
                {"error": str(e)}
            )
            return 0

    # ── 데이터 수집 ──────────────────────────────────────────

    @log_execution(module="service")
    def collect(self, request: CollectRequest) -> CollectResponse:
        end_date = request.end_date or datetime.now().strftime('%Y-%m-%d')

        if request.start_date:
            start_date = request.start_date
        else:
            start_date = (datetime.now() - timedelta(days=request.days)).strftime('%Y-%m-%d')

        result = self.pipeline.fetch(
            start_date=start_date,
            end_date=end_date,
            codes=request.codes,
            market=request.market,
            sector=request.sector,
        )

        if result.data:
            result.db_saved_count = self.save_to_db(result.data, result.market)

        if result.stock_info:
            self._save_stock_info(result.stock_info)

        return CollectResponse(
            success=result.success,
            message=result.message,
            total_codes=result.total_codes,
            success_count=result.success_count,
            failed_count=result.failed_count,
            db_saved_count=result.db_saved_count,
            elapsed_seconds=round(result.total_elapsed, 2),
        )

    # ── 주가 조회 (종목) ─────────────────────────────────────

    @log_execution(module="service")
    def get_prices_by_code(
        self,
        code: str,
        market: str,
        start_date: Optional[str],
        end_date: Optional[str],
        limit: int,
    ) -> list[StockPriceResponse]:
        start, end = _resolve_date_range(start_date, end_date)

        with self.database.session() as session:
            repo = StockRepository(session)

            if start is None and end is None:
                p = repo.get_latest_price(code, market)
                if not p:
                    raise HTTPException(status_code=404, detail="데이터 없음")
                return [StockPriceResponse.from_model(p)]

            prices = repo.get_prices(code, market, start, end)
            prices = prices[-limit:] if len(prices) > limit else prices
            return [StockPriceResponse.from_model(p) for p in prices]

    # ── 주가 조회 (섹터) ─────────────────────────────────────

    @log_execution(module="service")
    def get_prices_by_sector(
        self,
        sector: str,
        market: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        limit: int,
    ) -> list[StockPriceResponse]:
        start, end = _resolve_date_range(start_date, end_date)

        with self.database.session() as session:
            repo = StockRepository(session)
            stocks = repo.get_by_sector(sector, market)

            if not stocks:
                raise HTTPException(status_code=404, detail=f"섹터 '{sector}'에 해당하는 종목 없음")

            if start is None and end is None:
                return [
                    StockPriceResponse.from_model(p)
                    for s in stocks
                    if (p := repo.get_latest_price(s.code, s.market))
                ]

            result = []
            for stock in stocks:
                prices = repo.get_prices(stock.code, stock.market, start, end)
                prices = prices[-limit:] if len(prices) > limit else prices
                result.extend(StockPriceResponse.from_model(p) for p in prices)
            return result

    # ── 주가 삭제 ────────────────────────────────────────────

    @log_execution(module="service")
    def delete_prices(self, code: str, market: str) -> dict:
        with self.database.session() as session:
            repo = StockRepository(session)
            deleted = repo.delete_prices(code, market)
            return {"deleted": deleted, "code": code, "market": market}

    # ── 종목 정보 조회 ──────────────────────────────────────

    @log_execution(module="service")
    def get_stock_info(self, code: str, market: str) -> Optional[StockInfoResponse]:
        with self.database.session() as session:
            repo = StockRepository(session)
            info = repo.get_info_by_code(code, market)
            if not info:
                return None
            return StockInfoResponse.from_model(info)

    # ── 종목 조회 (섹터) ─────────────────────────────────────

    @log_execution(module="service")
    def get_stocks_by_sector(
        self,
        sector: str,
        market: Optional[str],
    ) -> list[StockInfoResponse]:
        with self.database.session() as session:
            repo = StockRepository(session)
            stocks = repo.get_by_sector(sector, market)

            if not stocks:
                raise HTTPException(status_code=404, detail=f"섹터 '{sector}'에 해당하는 종목 없음")

            return [StockInfoResponse.from_model(s) for s in stocks]

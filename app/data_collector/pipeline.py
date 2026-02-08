"""
데이터 수집 파이프라인
===================

한국/미국 주식 데이터 통합 수집 및 DB 저장

Usage:
    from data_collector import DataPipeline

    pipeline = DataPipeline()

    # 종목 코드 직접 지정
    result = pipeline.fetch(
        start_date='2024-01-01',
        end_date='2024-12-31',
        codes=['005930', '000660']
    )

    # 마켓/섹터 기준 자동 조회
    result = pipeline.fetch(
        start_date='2024-01-01',
        end_date='2024-12-31',
        market='KOSPI',
        sector='반도체'
    )
"""

import time
from datetime import datetime, date
from typing import Optional, Union
from dataclasses import dataclass, field

import pandas as pd

from config import settings
from db import database
from models import StockPrice
from repositories import StockRepository
from core import get_logger, log_execution, handle_exception, DataFetchError

from data_collector.stock_codes import (
    get_kr_codes,
    get_us_codes,
    is_korean_market,
    is_us_market
)
from data_collector.kr_fetcher import fetch_kr_stocks, FetchResult as KRFetchResult
from data_collector.us_fetcher import fetch_us_stocks, FetchResult as USFetchResult

logger = get_logger("pipeline")


@dataclass
class PipelineResult:
    """파이프라인 실행 결과"""

    # 수집 결과
    data: dict[str, pd.DataFrame] = field(default_factory=dict)

    # 통계
    total_codes: int = 0
    success_count: int = 0
    failed_count: int = 0
    db_saved_count: int = 0

    # 실패 목록
    failed_codes: list[str] = field(default_factory=list)

    # 시간
    fetch_elapsed: float = 0.0
    save_elapsed: float = 0.0
    total_elapsed: float = 0.0

    # 상태
    success: bool = True
    message: str = ""

    @property
    def success_rate(self) -> float:
        if self.total_codes == 0:
            return 0.0
        return self.success_count / self.total_codes * 100

    def to_dict(self) -> dict:
        return {
            "total_codes": self.total_codes,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "db_saved_count": self.db_saved_count,
            "success_rate": f"{self.success_rate:.1f}%",
            "fetch_elapsed_sec": round(self.fetch_elapsed, 2),
            "save_elapsed_sec": round(self.save_elapsed, 2),
            "total_elapsed_sec": round(self.total_elapsed, 2),
            "success": self.success,
            "message": self.message
        }


class DataPipeline:
    """데이터 수집 파이프라인"""

    def __init__(self, init_db: bool = True):
        """
        Args:
            init_db: DB 테이블 자동 생성 여부
        """
        if init_db:
            database.create_tables()

        logger.info("DataPipeline 초기화 완료", "__init__")

    @log_execution(module="pipeline")
    def fetch(
            self,
            start_date: Union[str, date],
            end_date: Union[str, date],
            codes: Optional[list[str]] = None,
            market: Optional[str] = None,
            sector: Optional[str] = None,
            save_to_db: bool = True,
            max_workers: int = 8,
            show_progress: bool = True
    ) -> PipelineResult:
        """
        데이터 수집 실행

        Args:
            start_date: 시작일
            end_date: 종료일
            codes: 종목 코드 리스트 (옵션)
            market: 마켓 - KOSPI, KOSDAQ, NYSE, NASDAQ, S&P500 (옵션)
            sector: 섹터명 (옵션)
            save_to_db: DB 저장 여부
            max_workers: 병렬 작업 수 (한국 주식)
            show_progress: 진행률 표시

        Returns:
            PipelineResult 객체
        """
        result = PipelineResult()
        start_time = time.perf_counter()

        # 날짜 문자열 변환
        if isinstance(start_date, date):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, date):
            end_date = end_date.strftime('%Y-%m-%d')

        logger.info(
            "데이터 수집 파이프라인 시작",
            "fetch",
            {
                "start_date": start_date,
                "end_date": end_date,
                "codes": codes[:5] if codes else None,
                "market": market,
                "sector": sector
            }
        )

        try:
            # 1. 종목 코드 결정
            if codes:
                # 종목 코드가 주어진 경우
                target_codes = codes
                # 한국/미국 구분 (첫 번째 코드로 판단)
                is_kr = target_codes[0].isdigit()

            elif market:
                # 마켓이 주어진 경우
                if is_korean_market(market):
                    target_codes = get_kr_codes(market, sector)
                    is_kr = True
                elif is_us_market(market):
                    target_codes = get_us_codes(market, sector)
                    is_kr = False
                else:
                    raise DataFetchError(f"알 수 없는 마켓: {market}")

            else:
                # 아무것도 없으면 기본값 (한국 전체)
                target_codes = get_kr_codes()
                is_kr = True

            result.total_codes = len(target_codes)

            if result.total_codes == 0:
                result.message = "수집할 종목이 없습니다"
                result.success = False
                return result

            logger.info(
                f"종목 코드 {result.total_codes}개 확인",
                "fetch",
                {"is_kr": is_kr, "sample": target_codes[:5]}
            )

            # 2. 데이터 수집
            fetch_start = time.perf_counter()

            if is_kr:
                fetch_result = fetch_kr_stocks(
                    codes=target_codes,
                    start_date=start_date,
                    end_date=end_date,
                    max_workers=max_workers,
                    show_progress=show_progress
                )
                market_type = "KR"
            else:
                fetch_result = fetch_us_stocks(
                    tickers=target_codes,
                    start_date=start_date,
                    end_date=end_date,
                    show_progress=show_progress
                )
                market_type = "US"

            result.fetch_elapsed = time.perf_counter() - fetch_start
            result.data = fetch_result.success
            result.success_count = fetch_result.success_count
            result.failed_count = fetch_result.failed_count
            result.failed_codes = fetch_result.failed

            # 3. DB 저장
            if save_to_db and result.data:
                save_start = time.perf_counter()
                result.db_saved_count = self._save_to_db(result.data, market_type)
                result.save_elapsed = time.perf_counter() - save_start

            # 4. 결과 정리
            result.total_elapsed = time.perf_counter() - start_time
            result.success = result.success_rate >= 50
            result.message = f"수집 완료: {result.success_count}/{result.total_codes} 성공"

            logger.info(
                "데이터 수집 파이프라인 완료",
                "fetch",
                result.to_dict()
            )

        except Exception as e:
            result.success = False
            result.message = f"파이프라인 실패: {str(e)}"
            result.total_elapsed = time.perf_counter() - start_time

            logger.exception(
                f"파이프라인 예외 발생",
                "fetch",
                {"error": str(e)}
            )

        return result

    def _save_to_db(self, data: dict[str, pd.DataFrame], market: str) -> int:
        """
        데이터 DB 저장

        Args:
            data: {종목코드: DataFrame} 딕셔너리
            market: 'KR' 또는 'US'

        Returns:
            저장된 레코드 수
        """
        total_saved = 0

        try:
            with database.session() as session:
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
                            "_save_to_db",
                            {"code": code, "error": str(e)}
                        )

            logger.info(
                f"DB 저장 완료",
                "_save_to_db",
                {"market": market, "codes": len(data), "records": total_saved}
            )

        except Exception as e:
            logger.critical(
                f"DB 저장 실패",
                "_save_to_db",
                {"error": str(e)}
            )
            raise

        return total_saved

    def _df_to_records(
            self,
            df: pd.DataFrame,
            code: str,
            market: str
    ) -> list[dict]:
        """DataFrame을 DB 레코드로 변환"""
        records = []

        for _, row in df.iterrows():
            try:
                # date 컬럼 처리
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

    def fetch_kr(
            self,
            start_date: str,
            end_date: str,
            codes: Optional[list[str]] = None,
            market: Optional[str] = None,
            sector: Optional[str] = None,
            **kwargs
    ) -> PipelineResult:
        """한국 주식 수집 (편의 메서드)"""
        if codes is None:
            codes = get_kr_codes(market, sector)

        return self.fetch(
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            **kwargs
        )

    def fetch_us(
            self,
            start_date: str,
            end_date: str,
            codes: Optional[list[str]] = None,
            market: Optional[str] = None,
            sector: Optional[str] = None,
            **kwargs
    ) -> PipelineResult:
        """미국 주식 수집 (편의 메서드)"""
        if codes is None:
            codes = get_us_codes(market, sector)

        return self.fetch(
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            **kwargs
        )
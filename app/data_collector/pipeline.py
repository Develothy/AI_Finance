"""
데이터 수집 파이프라인
===================

한국/미국 주식 데이터 통합 수집

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

from core import get_logger, log_execution, DataFetchError

from data_collector.stock_codes import (
    get_kr_codes,
    get_us_codes,
    is_korean_market,
    is_us_market
)
from data_collector.kr_fetcher import fetch_kr_stocks
from data_collector.us_fetcher import fetch_us_stocks

logger = get_logger("pipeline")


@dataclass
class PipelineResult:
    """파이프라인 실행 결과"""

    # 수집 결과
    data: dict[str, pd.DataFrame] = field(default_factory=dict)
    market: str = ""

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

    def __init__(self):
        logger.info("DataPipeline 초기화 완료", "__init__")

    @log_execution(module="pipeline")
    def fetch(
            self,
            start_date: Union[str, date],
            end_date: Union[str, date],
            codes: Optional[list[str]] = None,
            market: Optional[str] = None,
            sector: Optional[str] = None,
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
            # market 검증
            valid_markets = ['KOSPI', 'KOSDAQ', 'NYSE', 'NASDAQ', 'S&P500', 'SP500']
            if market and market.upper() not in valid_markets:
                raise DataFetchError(f"알 수 없는 마켓: {market}. 지원: {valid_markets}")

            # 1. 종목 코드 결정
            if codes:
                # 종목 코드가 주어진 경우
                target_codes = codes
                # 한국/미국 구분 (첫 번째 코드로 판단)
                is_kr = target_codes[0].isdigit()
                # market 지정 안 됐으면 default값
                if not market:
                    market = "KOSPI" if is_kr else "NASDAQ"

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
                # 아무것도 없으면 default값 (KOSPI)
                market = "KOSPI"
                target_codes = get_kr_codes(market, sector)
                is_kr = True

            result.total_codes = len(target_codes)

            if result.total_codes == 0:
                result.message = "수집할 종목이 없습니다"
                result.success = False
                return result

            logger.info(
                f"종목 코드 {result.total_codes}개 확인",
                "fetch",
                {"market": market, "is_kr": is_kr, "sample": target_codes[:5]}
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
            else:
                fetch_result = fetch_us_stocks(
                    tickers=target_codes,
                    start_date=start_date,
                    end_date=end_date,
                    show_progress=show_progress
                )

            result.market = market.upper()

            result.fetch_elapsed = time.perf_counter() - fetch_start
            result.data = fetch_result.success
            result.success_count = fetch_result.success_count
            result.failed_count = fetch_result.failed_count
            result.failed_codes = fetch_result.failed

            # 3. 결과 정리
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
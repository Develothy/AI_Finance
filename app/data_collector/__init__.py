"""
데이터 수집 모듈 (Module 0)
========================

한국/미국 주식 데이터 수집 파이프라인

Usage:
    from data_collector import DataPipeline, DataScheduler

    # 수동 수집
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

    # 미국 주식
    result = pipeline.fetch(
        start_date='2024-01-01',
        end_date='2024-12-31',
        market='S&P500'
    )

    # 결과 확인
    print(result.to_dict())
    print(result.data['005930'])  # DataFrame

    # 스케줄러 사용
    scheduler = DataScheduler()
    scheduler.add_kr_daily_job(hour=18, minute=0)
    scheduler.add_us_daily_job(hour=7, minute=0)
    scheduler.start()
"""

from .pipeline import DataPipeline, PipelineResult
from .stock_codes import (
    get_kr_codes,
    get_us_codes,
    get_stock_codes,
    get_kr_stock_list,
    get_us_stock_list,
    is_korean_market,
    is_us_market,
)
from .kr_fetcher import fetch_kr_stocks
from .us_fetcher import fetch_us_stocks
from .kis_fetcher import KISClient
from .dart_fetcher import DARTClient

# 스케줄러는 선택적 import (apscheduler 필요)
try:
    from .scheduler import DataScheduler
    SCHEDULER_AVAILABLE = True
except ImportError:
    DataScheduler = None
    SCHEDULER_AVAILABLE = False


__all__ = [
    # 메인
    "DataPipeline",
    "PipelineResult",

    # 종목 코드
    "get_kr_codes",
    "get_us_codes",
    "get_stock_codes",
    "get_kr_stock_list",
    "get_us_stock_list",
    "is_korean_market",
    "is_us_market",

    # Fetcher
    "fetch_kr_stocks",
    "fetch_us_stocks",

    # Phase 2 Fetcher
    "KISClient",
    "DARTClient",

    # 스케줄러
    "DataScheduler",
    "SCHEDULER_AVAILABLE",
]

__version__ = "1.0.0"
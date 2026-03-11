"""
데이터 수집 모듈 (Module 0)
========================

한국/미국 주식 데이터 수집 파이프라인

Usage:
    from data_collector import DataPipeline

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

    # 스케줄러 사용 (scheduler 모듈에서 import)
    from scheduler import JobScheduler
    scheduler = JobScheduler()
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
from .news_fetcher import NewsFetcher
from .sentiment_analyzer import SentimentAnalyzer
from .disclosure_fetcher import DisclosureFetcher
from .krx_fetcher import KRXSupplyFetcher
from .google_trends_fetcher import GoogleTrendsFetcher
from .naver_community_fetcher import NaverCommunityFetcher


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

    # Phase 4 News
    "NewsFetcher",
    "SentimentAnalyzer",

    # Phase 5 Disclosure + Supply/Demand
    "DisclosureFetcher",
    "KRXSupplyFetcher",

    # Phase 7 Alternative Data
    "GoogleTrendsFetcher",
    "NaverCommunityFetcher",
]

__version__ = "1.0.0"
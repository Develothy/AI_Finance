"""
주식 데이터 엔드포인트
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    CollectRequest,
    CollectResponse,
    StockPriceResponse,
    StockInfoResponse,
    StockSearchResponse,
)
from services import stock_service

router = APIRouter(prefix="/stocks", tags=["주식 데이터"])


@router.post("/collect", response_model=CollectResponse)
def collect_data(request: CollectRequest):
    """
    데이터 수집

    - codes: 종목 코드 리스트 ["005930", "000660"]
    - market: 마켓 (KOSPI, KOSDAQ, S&P500 등)
    - sector: 섹터
    - days: 수집 기간 (일)
    """
    return stock_service.collect(request)


@router.get("/prices/code/{code}", response_model=list[StockPriceResponse])
def get_prices_by_code(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
    limit: int = Query(default=100, le=1000),
):
    """
    종목 주가 조회

    날짜 조건:
    - 둘 다 없음: 최신 1개
    - start_date만: start_date ~ 오늘
    - end_date만: end_date 하루
    - 둘 다 있음: start_date ~ end_date
    """
    return stock_service.get_prices_by_code(code, market, start_date, end_date, limit)


@router.get("/prices/sector/{sector}", response_model=list[StockPriceResponse])
def get_prices_by_sector(
    sector: str,
    market: Optional[str] = Query(default=None, description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
    limit: int = Query(default=100, le=1000, description="종목당 최대 개수"),
):
    """
    섹터별 주가 조회

    날짜 조건:
    - 둘 다 없음: 최신 1개
    - start_date만: start_date ~ 오늘
    - end_date만: end_date 하루
    - 둘 다 있음: start_date ~ end_date
    """
    return stock_service.get_prices_by_sector(sector, market, start_date, end_date, limit)


@router.delete("/prices/code/{code}")
def delete_prices(
    code: str,
    market: str = Query(default="KOSPI"),
):
    """종목 주가 삭제"""
    return stock_service.delete_prices(code, market)


@router.get("/info/{code}", response_model=StockInfoResponse)
def get_stock_info(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
):
    """종목 정보 조회 (이름, 섹터, 산업)"""
    info = stock_service.get_stock_info(code, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목 정보 없음: {code}")
    return info


@router.get("/search", response_model=list[StockSearchResponse])
def search_stocks(
    keyword: str = Query(description="종목코드 또는 종목명 검색어"),
    market: Optional[str] = Query(default=None, description="KOSPI, KOSDAQ, KR"),
    limit: int = Query(default=50, le=200),
):
    """종목 검색 (code 또는 name 부분 매칭)"""
    return stock_service.search_stocks(keyword, market, limit)


@router.get("/search/sector", response_model=list[StockSearchResponse])
def search_stocks_by_sector(
    keyword: str = Query(description="섹터 또는 산업분류 검색어 (예: 반도체)"),
    market: Optional[str] = Query(default=None, description="KOSPI, KOSDAQ, KR"),
):
    """섹터/산업분류 검색으로 종목 일괄 조회"""
    return stock_service.search_stocks_by_sector(keyword, market)


@router.get("/stocks/sector/{sector}", response_model=list[StockInfoResponse])
def get_stocks_by_sector(
    sector: str,
    market: Optional[str] = Query(default=None, description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
):
    """
    섹터별 종목 조회

    - sector: 섹터명 (반도체, IT, 금융 등)
    - market: 마켓 필터 (옵션)
    """
    return stock_service.get_stocks_by_sector(sector, market)

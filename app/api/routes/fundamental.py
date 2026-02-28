"""
재무 데이터 API 엔드포인트 (Phase 2)
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    FundamentalCollectRequest,
    FundamentalCollectResponse,
    FinancialCollectRequest,
    FinancialCollectResponse,
    StockFundamentalResponse,
    FinancialStatementResponse,
    FundamentalSummaryResponse,
)
from services import fundamental_service

router = APIRouter(prefix="/fundamental", tags=["Fundamental"])


# ============================================================
# 수집
# ============================================================

@router.post("/collect", response_model=FundamentalCollectResponse)
def collect_fundamentals(req: FundamentalCollectRequest):
    """KIS API 기초정보 수집 실행"""
    result = fundamental_service.collect_fundamentals(
        market=req.market,
        codes=req.codes,
        date=req.date,
    )
    return FundamentalCollectResponse(**result)


@router.post("/collect/financial", response_model=FinancialCollectResponse)
def collect_financial_statements(req: FinancialCollectRequest):
    """DART 재무제표 수집 실행"""
    result = fundamental_service.collect_financial_statements(
        market=req.market,
        codes=req.codes,
        year=req.year,
        quarter=req.quarter,
    )
    return FinancialCollectResponse(**result)


# ============================================================
# 조회
# ============================================================

@router.get("/summary/{code}", response_model=FundamentalSummaryResponse)
def get_summary(
    code: str,
    market: str = Query(default="KOSPI"),
):
    """종목 재무 종합 요약"""
    result = fundamental_service.get_summary(market, code)
    return FundamentalSummaryResponse(**result)


@router.get("/{code}", response_model=list[StockFundamentalResponse])
def get_fundamentals(
    code: str,
    market: str = Query(default="KOSPI"),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    """종목 기초정보 조회"""
    rows = fundamental_service.get_fundamentals(market, code, start_date, end_date)
    if not rows:
        raise HTTPException(status_code=404, detail=f"기초정보 없음: {market}:{code}")
    return [StockFundamentalResponse(**r) for r in rows]


@router.get("/{code}/financial", response_model=list[FinancialStatementResponse])
def get_financial_statements(
    code: str,
    market: str = Query(default="KOSPI"),
    limit: int = Query(default=20, le=100),
):
    """종목 재무제표 조회"""
    rows = fundamental_service.get_financial_statements(market, code, limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"재무제표 없음: {market}:{code}")
    return [FinancialStatementResponse(**r) for r in rows]

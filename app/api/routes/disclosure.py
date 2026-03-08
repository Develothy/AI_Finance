"""
공시 + 수급 API 엔드포인트 (Phase 5)
"""

from typing import Optional

from fastapi import APIRouter, Query

from api.schemas import (
    DisclosureCollectRequest,
    DisclosureCollectResponse,
    SupplyDemandCollectRequest,
    SupplyDemandCollectResponse,
    DisclosureResponse,
    SupplyDemandResponse,
)
from services import disclosure_service

router = APIRouter(prefix="/disclosure", tags=["Disclosure & Supply"])


# ── 공시 ──────────────────────────────────────────────

@router.post("/collect", response_model=DisclosureCollectResponse)
def collect_disclosures(req: DisclosureCollectRequest):
    """DART 공시 목록 수집"""
    result = disclosure_service.collect_disclosures(
        market=req.market,
        codes=req.codes,
        start_date=req.start_date,
        end_date=req.end_date,
        days=req.days,
        analyze_sentiment=req.analyze_sentiment,
    )
    return DisclosureCollectResponse(**result)


@router.get("/list", response_model=list[DisclosureResponse])
def get_disclosures(
    market: str = Query(default="KOSPI"),
    code: str = Query(...),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
):
    """종목별 공시 목록 조회"""
    rows = disclosure_service.get_disclosures(
        market, code, start_date, end_date, limit,
    )
    return [DisclosureResponse(**r) for r in rows]


# ── 수급 ──────────────────────────────────────────────

@router.post("/supply/collect", response_model=SupplyDemandCollectResponse)
def collect_supply_demand(req: SupplyDemandCollectRequest):
    """KRX 수급 데이터 수집"""
    result = disclosure_service.collect_supply_demand(
        market=req.market,
        codes=req.codes,
        start_date=req.start_date,
        end_date=req.end_date,
        days=req.days,
    )
    return SupplyDemandCollectResponse(**result)


@router.get("/supply/{market}/{code}", response_model=list[SupplyDemandResponse])
def get_supply_demand(
    market: str,
    code: str,
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
):
    """종목별 수급 시계열 조회"""
    rows = disclosure_service.get_supply_demand(
        market, code, start_date, end_date, limit,
    )
    return [SupplyDemandResponse(**r) for r in rows]

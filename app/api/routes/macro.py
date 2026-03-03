"""
거시경제 지표 API 엔드포인트 (Phase 3)
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    MacroCollectRequest,
    MacroCollectResponse,
    MacroIndicatorResponse,
)
from services import macro_service

router = APIRouter(prefix="/macro", tags=["Macro"])


# ============================================================
# 수집
# ============================================================

@router.post("/collect", response_model=MacroCollectResponse)
def collect_macro(req: MacroCollectRequest):
    """거시경제 지표 수집 실행"""
    result = macro_service.collect(
        start_date=req.start_date,
        end_date=req.end_date,
        days_back=req.days_back,
    )
    return MacroCollectResponse(**result)


# ============================================================
# 조회
# ============================================================

@router.get("/latest", response_model=list[MacroIndicatorResponse])
def get_latest():
    """지표별 최신값 조회"""
    rows = macro_service.get_latest()
    if not rows:
        return []
    return [MacroIndicatorResponse(**r) for r in rows]


@router.get("/{indicator_name}", response_model=list[MacroIndicatorResponse])
def get_indicators(
    indicator_name: str,
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    """특정 지표 시계열 조회"""
    rows = macro_service.get_indicators(indicator_name, start_date, end_date)
    if not rows:
        raise HTTPException(status_code=404, detail=f"지표 데이터 없음: {indicator_name}")
    return [MacroIndicatorResponse(**r) for r in rows]

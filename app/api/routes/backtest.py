"""
백테스트 API 엔드포인트
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    BacktestRunRequest,
    BacktestRunResponse,
    BacktestTradeResponse,
    BacktestDailyResponse,
    BacktestCompareRequest,
)
from services import backtest_service

router = APIRouter(prefix="/backtest", tags=["Backtest"])


# ============================================================
# 실행
# ============================================================

@router.post("/run", response_model=BacktestRunResponse)
def run_backtest(req: BacktestRunRequest):
    """백테스트 실행"""
    try:
        result = backtest_service.run_backtest(
            market=req.market,
            codes=req.codes,
            start_date=req.start_date,
            end_date=req.end_date,
            model_ids=req.model_ids,
            aggregation_method=req.aggregation_method,
            initial_capital=req.initial_capital,
            transaction_fee=req.transaction_fee,
            tax_rate=req.tax_rate,
            max_position_pct=req.max_position_pct,
            name=req.name,
        )
        return BacktestRunResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"백테스트 실패: {e}")


# ============================================================
# 조회
# ============================================================

@router.get("/runs", response_model=list[BacktestRunResponse])
def list_runs(
    market: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=200),
):
    """백테스트 이력 조회"""
    runs = backtest_service.get_runs(market=market, limit=limit)
    return [BacktestRunResponse(**r) for r in runs]


@router.get("/runs/{run_id}", response_model=BacktestRunResponse)
def get_run(run_id: int):
    """백테스트 상세 조회"""
    result = backtest_service.get_run_detail(run_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"백테스트 없음: id={run_id}")
    return BacktestRunResponse(**result)


@router.get("/runs/{run_id}/trades", response_model=list[BacktestTradeResponse])
def get_trades(
    run_id: int,
    code: Optional[str] = Query(default=None),
    limit: int = Query(default=500, le=2000),
):
    """거래 로그 조회"""
    trades = backtest_service.get_trades(run_id, code=code, limit=limit)
    return [BacktestTradeResponse(**t) for t in trades]


@router.get("/runs/{run_id}/equity", response_model=list[BacktestDailyResponse])
def get_equity_curve(run_id: int):
    """에쿼티 커브 조회"""
    daily = backtest_service.get_equity_curve(run_id)
    return [BacktestDailyResponse(**d) for d in daily]


# ============================================================
# 삭제
# ============================================================

@router.delete("/runs/{run_id}")
def delete_run(run_id: int):
    """백테스트 삭제"""
    deleted = backtest_service.delete_run(run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"백테스트 없음: id={run_id}")
    return {"deleted": True, "id": run_id}


# ============================================================
# 비교
# ============================================================

@router.post("/compare", response_model=list[BacktestRunResponse])
def compare_runs(req: BacktestCompareRequest):
    """복수 백테스트 비교"""
    results = backtest_service.compare_runs(req.run_ids)
    return [BacktestRunResponse(**r) for r in results]

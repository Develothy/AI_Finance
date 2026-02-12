"""
퀀트 플랫폼 API 서버
"""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from db import database
from models import StockPrice, StockInfo
from repositories import StockRepository
from data_collector import DataPipeline

app = FastAPI(
    title="퀀트 플랫폼 API",
    description="주식 데이터 수집 및 조회 API",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 파이프라인 인스턴스
pipeline = DataPipeline(init_db=False)


# ============================================================
# Request/Response 모델
# ============================================================

class CollectRequest(BaseModel):
    codes: Optional[list[str]] = None
    market: Optional[str] = None
    sector: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: int = 30


class CollectResponse(BaseModel):
    success: bool
    message: str
    total_codes: int
    success_count: int
    failed_count: int
    db_saved_count: int
    elapsed_seconds: float


class StockPriceResponse(BaseModel):
    market: str
    code: str
    date: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]


# ============================================================
# 엔드포인트
# ============================================================

@app.get("/")
def root():
    """헬스 체크"""
    return {"status": "ok", "message": "퀀트 플랫폼 API"}


@app.post("/collect", response_model=CollectResponse)
def collect_data(request: CollectRequest):
    """
    데이터 수집

    - codes: 종목 코드 리스트 ["005930", "000660"]
    - market: 마켓 (KOSPI, KOSDAQ, S&P500 등)
    - sector: 섹터
    - days: 수집 기간 (일)
    """
    try:
        end_date = request.end_date or datetime.now().strftime('%Y-%m-%d')

        if request.start_date:
            start_date = request.start_date
        else:
            start_date = (datetime.now() - timedelta(days=request.days)).strftime('%Y-%m-%d')

        result = pipeline.fetch(
            start_date=start_date,
            end_date=end_date,
            codes=request.codes,
            market=request.market,
            sector=request.sector
        )

        return CollectResponse(
            success=result.success,
            message=result.message,
            total_codes=result.total_codes,
            success_count=result.success_count,
            failed_count=result.failed_count,
            db_saved_count=result.db_saved_count,
            elapsed_seconds=round(result.total_elapsed, 2)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/{code}", response_model=list[StockPriceResponse])
def get_prices(
        code: str,
        market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
        start_date: Optional[str] = Query(default=None),
        end_date: Optional[str] = Query(default=None),
        limit: int = Query(default=100, le=1000)
):
    """
    종목 주가 조회

    - code: 종목 코드 (005930, AAPL 등)
    - market: KOSPI, KOSDAQ, NYSE, NASDAQ
    - start_date: 시작일 (YYYY-MM-DD)
    - end_date: 종료일 (YYYY-MM-DD)
    - limit: 최대 조회 개수
    """
    try:
        with database.session() as session:
            repo = StockRepository(session)
            prices = repo.get_prices(code, market, start_date, end_date)

            # limit 적용
            prices = prices[-limit:] if len(prices) > limit else prices

            return [
                StockPriceResponse(
                    market=p.market,
                    code=p.code,
                    date=p.date.strftime('%Y-%m-%d'),
                    open=float(p.open) if p.open else None,
                    high=float(p.high) if p.high else None,
                    low=float(p.low) if p.low else None,
                    close=float(p.close) if p.close else None,
                    volume=int(p.volume) if p.volume else None
                )
                for p in prices
            ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/{code}/latest", response_model=Optional[StockPriceResponse])
def get_latest_price(
        code: str,
        market: str = Query(default="KOSPI")
):
    """최신 주가 조회"""
    try:
        with database.session() as session:
            repo = StockRepository(session)
            p = repo.get_latest_price(code, market)

            if not p:
                raise HTTPException(status_code=404, detail="데이터 없음")

            return StockPriceResponse(
                market=p.market,
                code=p.code,
                date=p.date.strftime('%Y-%m-%d'),
                open=float(p.open) if p.open else None,
                high=float(p.high) if p.high else None,
                low=float(p.low) if p.low else None,
                close=float(p.close) if p.close else None,
                volume=int(p.volume) if p.volume else None
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/prices/{code}")
def delete_prices(
        code: str,
        market: str = Query(default="KOSPI")
):
    """종목 주가 삭제"""
    try:
        with database.session() as session:
            repo = StockRepository(session)
            deleted = repo.delete_prices(code, market)

            return {"deleted": deleted, "code": code, "market": market}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 실행
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
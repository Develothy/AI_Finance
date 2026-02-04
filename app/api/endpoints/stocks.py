from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query

from app.services.fdr_service import FDRService
from app.schemas.stock_schemas import StockListResponse, StockDataResponse, MultipleStockDataResponse, StockPriceRequest

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/list", response_model=StockListResponse,
            summary="전체 종목 리스트 조회")
async def get_stock_list(market: str = 'KRX'):
    try:
        stocks = FDRService.get_stock_list(market)
        return StockListResponse(
            stocks=stocks,
            total_count=len(stocks),
            market=market
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stock list: {str(e)}")


@router.get("/{symbol}",
            response_model=StockDataResponse,
            summary="단일 종목 주가 데이터 조회")
async def get_stock_data(
        symbol: str = Path(..., description="종목코드", example="316140"),
        start_date: Optional[str] = Query(None, description="조회 시작일 (YYYY-MM-DD)"),
        end_date: Optional[str] = Query(None, description="조회 종료일 (YYYY-MM-DD)"),
        days: Optional[int] = Query(30, description="조회 기간 (일 단위)", example=3, ge=1, le=1825)  # 최대 5년
):
    try:
        stock_data = FDRService.get_stock_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            days=days
        )

        if stock_data.total_count == 0:
            raise HTTPException(
                status_code=404,
                detail=f"종목코드 '{symbol}'에 대한 데이터를 찾을 수 없습니다. 종목코드를 확인해주세요."
            )

        return stock_data

    except HTTPException:
        # HTTPException은 그대로 다시 raise
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"종목 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.post("/multiple", response_model=MultipleStockDataResponse,
             summary="다중 종목 주가 데이터 조회")
async def get_multiple_stocks(request: StockPriceRequest):
    if len(request.symbols) > 50:
        raise HTTPException(status_code=400, detail="최대 50개 종목까지 가능")

    try:
        result = await FDRService.get_multiple_stocks(
            symbols=request.symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            days=request.days
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


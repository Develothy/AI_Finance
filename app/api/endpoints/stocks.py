from fastapi import APIRouter, HTTPException
from app.services.finance_service import FinanceService
from app.schemas.stock_schemas import StockListResponse

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/list", response_model=StockListResponse)
async def get_stock_list(market: str = 'KRX'):
    """전체 종목 리스트 조회"""
    try:
        stocks = FinanceService.get_stock_list(market)
        return StockListResponse(
            stocks=stocks,
            total_count=len(stocks),
            market=market
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stock list: {str(e)}")
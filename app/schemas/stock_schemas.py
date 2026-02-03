from typing import Optional, List

from pydantic import BaseModel


class StockItem(BaseModel):
    symbol: str
    name: str
    market: Optional[str] = None

class StockListResponse(BaseModel):
    stocks: list[StockItem]
    total_count: int
    market: str

class StockPriceRequest(BaseModel):
    symbols: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: Optional[int] = 30

class StockDataPoint(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int = 0

class StockDataResponse(BaseModel):
    symbol: str
    name: Optional[str] = None
    data: list[StockDataPoint]
    start_date: str
    end_date: str
    total_count: int

class MultipleStockDataResponse(BaseModel):
    stocks_data: list[StockDataResponse]
    request_count: int
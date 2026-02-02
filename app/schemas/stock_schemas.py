from typing import Optional

from pydantic import BaseModel


class StockItem(BaseModel):
    symbol: str
    name: str
    market: Optional[str] = None

class StockListResponse(BaseModel):
    stocks: list[StockItem]
    total_count: int
    market: str
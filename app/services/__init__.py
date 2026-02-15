from .stock_service import StockService
from .indicator_service import IndicatorService

stock_service = StockService()
indicator_service = IndicatorService()

__all__ = [
    "StockService", "stock_service",
    "IndicatorService", "indicator_service",
]

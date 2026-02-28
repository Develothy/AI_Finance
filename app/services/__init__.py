from .stock_service import StockService
from .indicator_service import IndicatorService
from .ml_service import MLService
from .fundamental_service import FundamentalService

stock_service = StockService()
indicator_service = IndicatorService()
ml_service = MLService()
fundamental_service = FundamentalService()

__all__ = [
    "StockService", "stock_service",
    "IndicatorService", "indicator_service",
    "MLService", "ml_service",
    "FundamentalService", "fundamental_service",
]

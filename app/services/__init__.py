from .stock_service import StockService
from .indicator_service import IndicatorService
from .ml_service import MLService
from .fundamental_service import FundamentalService
from .macro_service import MacroService
from .news_service import NewsService

stock_service = StockService()
indicator_service = IndicatorService()
ml_service = MLService()
fundamental_service = FundamentalService()
macro_service = MacroService()
news_service = NewsService()

__all__ = [
    "StockService", "stock_service",
    "IndicatorService", "indicator_service",
    "MLService", "ml_service",
    "FundamentalService", "fundamental_service",
    "MacroService", "macro_service",
    "NewsService", "news_service",
]

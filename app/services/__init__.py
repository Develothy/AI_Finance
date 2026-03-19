from .stock_service import StockService
from .indicator_service import IndicatorService
from .ml_service import MLService
from .fundamental_service import FundamentalService
from .macro_service import MacroService
from .news_service import NewsService
from .disclosure_service import DisclosureService
from .admin_service import AdminService
from .scheduler_service import SchedulerService
from .alternative_service import AlternativeService
from .backtest_service import BacktestService

stock_service = StockService()
indicator_service = IndicatorService()
ml_service = MLService()
fundamental_service = FundamentalService()
macro_service = MacroService()
news_service = NewsService()
disclosure_service = DisclosureService()
admin_service = AdminService()
scheduler_service = SchedulerService()
alternative_service = AlternativeService()
backtest_service = BacktestService()

__all__ = [
    "StockService", "stock_service",
    "IndicatorService", "indicator_service",
    "MLService", "ml_service",
    "FundamentalService", "fundamental_service",
    "MacroService", "macro_service",
    "NewsService", "news_service",
    "DisclosureService", "disclosure_service",
    "AdminService", "admin_service",
    "SchedulerService", "scheduler_service",
    "AlternativeService", "alternative_service",
    "BacktestService", "backtest_service",
]

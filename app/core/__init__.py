"""
로깅, 예외처리, 데코레이터를 제공하는 핵심 모듈

Usage:
    # config는 최상위에서 import
    from config import settings

    # core 모듈
    from core import get_logger, log_execution, handle_exception, retry
    from core import DataFetchError, APIConnectionError, OrderError

    # 로거 사용
    logger = get_logger("data_fetcher")
    logger.info("데이터 수집 시작", "fetch_stock", {"code": "005930"})

    # 데코레이터 사용
    @log_execution(module="data_fetcher")
    @retry(max_attempts=3, delay=1)
    @handle_exception(default_return=None, notify=True)
    def fetch_stock(code: str):
        ...
"""

from .logging import (
    get_logger,
    AppLogger,
    LogConfig,
    LoggerSetup,
    slack_notifier,
)

from .decorators import (
    log_execution,
    handle_exception,
    retry,
    robust_execution,
)

from .exceptions import (
    # Base
    BaseAppException,

    # Data Fetch
    DataFetchError,
    APIConnectionError,
    DataValidationError,
    CrawlingError,

    # Strategy
    StrategyError,
    BacktestError,
    SignalGenerationError,

    # Trade
    TradeError,
    OrderError,
    PositionError,

    # Sentiment
    SentimentError,
    ModelLoadError,
    InferenceError,

    # Config
    ConfigError,
)


__all__ = [
    # Logging
    "get_logger",
    "AppLogger",
    "LogConfig",
    "LoggerSetup",
    "slack_notifier",

    # Decorators
    "log_execution",
    "handle_exception",
    "retry",
    "robust_execution",

    # Exceptions - Base
    "BaseAppException",

    # Exceptions - Data Fetch
    "DataFetchError",
    "APIConnectionError",
    "DataValidationError",
    "CrawlingError",

    # Exceptions - Strategy
    "StrategyError",
    "BacktestError",
    "SignalGenerationError",

    # Exceptions - Trade
    "TradeError",
    "OrderError",
    "PositionError",

    # Exceptions - Sentiment
    "SentimentError",
    "ModelLoadError",
    "InferenceError",

    # Exceptions - Config
    "ConfigError",
]

__version__ = "1.0.0"
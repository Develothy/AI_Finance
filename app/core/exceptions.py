"""

예외 계층 구조:
BaseAppException
├── DataFetchError
│   ├── APIConnectionError
│   ├── DataValidationError
│   └── CrawlingError
├── StrategyError
│   ├── BacktestError
│   └── SignalGenerationError
├── TradeError
│   ├── OrderError
│   └── PositionError
├── SentimentError
│   ├── ModelLoadError
│   └── InferenceError
└── ConfigError
"""

from typing import Any, Optional
from datetime import datetime


class BaseAppException(Exception):
    """기본 애플리케이션 예외 클래스"""

    def __init__(
            self,
            message: str,
            code: Optional[str] = None,
            context: Optional[dict[str, Any]] = None
    ):
        self.message = message
        self.code = code or self.__class__.__name__
        self.context = context or {}
        self.timestamp = datetime.now()
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "code": self.code,
            "context": self.context,
            "timestamp": self.timestamp.isoformat()
        }

    def __str__(self) -> str:
        if self.context:
            return f"{self.message} | context: {self.context}"
        return self.message


# ============================================================
# 데이터 수집 관련 예외
# ============================================================

class DataFetchError(BaseAppException):
    """데이터 수집 실패"""
    pass


class APIConnectionError(DataFetchError):
    """API 연결 실패"""

    def __init__(
            self,
            message: str,
            api_name: Optional[str] = None,
            endpoint: Optional[str] = None,
            status_code: Optional[int] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "api_name": api_name,
            "endpoint": endpoint,
            "status_code": status_code
        })
        super().__init__(message, context=context, **kwargs)


class DataValidationError(DataFetchError):
    """데이터 검증 실패"""

    def __init__(
            self,
            message: str,
            field: Optional[str] = None,
            expected: Optional[Any] = None,
            actual: Optional[Any] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "field": field,
            "expected": str(expected) if expected else None,
            "actual": str(actual) if actual else None
        })
        super().__init__(message, context=context, **kwargs)


class CrawlingError(DataFetchError):
    """크롤링 실패"""

    def __init__(
            self,
            message: str,
            url: Optional[str] = None,
            status_code: Optional[int] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "url": url,
            "status_code": status_code
        })
        super().__init__(message, context=context, **kwargs)


# ============================================================
# 전략 관련 예외
# ============================================================

class StrategyError(BaseAppException):
    """전략 실행 실패"""
    pass


class BacktestError(StrategyError):
    """백테스트 실패"""

    def __init__(
            self,
            message: str,
            strategy_name: Optional[str] = None,
            period: Optional[str] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "strategy_name": strategy_name,
            "period": period
        })
        super().__init__(message, context=context, **kwargs)


class SignalGenerationError(StrategyError):
    """시그널 생성 실패"""

    def __init__(
            self,
            message: str,
            strategy_name: Optional[str] = None,
            stock_code: Optional[str] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "strategy_name": strategy_name,
            "stock_code": stock_code
        })
        super().__init__(message, context=context, **kwargs)


# ============================================================
# 매매 관련 예외
# ============================================================

class TradeError(BaseAppException):
    """매매 실패"""
    pass


class OrderError(TradeError):
    """주문 실패"""

    def __init__(
            self,
            message: str,
            order_id: Optional[str] = None,
            stock_code: Optional[str] = None,
            order_type: Optional[str] = None,
            quantity: Optional[int] = None,
            price: Optional[float] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "order_id": order_id,
            "stock_code": stock_code,
            "order_type": order_type,
            "quantity": quantity,
            "price": price
        })
        super().__init__(message, context=context, **kwargs)


class PositionError(TradeError):
    """포지션 관리 실패"""

    def __init__(
            self,
            message: str,
            stock_code: Optional[str] = None,
            current_position: Optional[int] = None,
            requested_quantity: Optional[int] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "stock_code": stock_code,
            "current_position": current_position,
            "requested_quantity": requested_quantity
        })
        super().__init__(message, context=context, **kwargs)


# ============================================================
# 센티먼트 분석 관련 예외
# ============================================================

class SentimentError(BaseAppException):
    """센티먼트 분석 실패"""
    pass


class ModelLoadError(SentimentError):
    """모델 로드 실패"""

    def __init__(
            self,
            message: str,
            model_name: Optional[str] = None,
            model_path: Optional[str] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "model_name": model_name,
            "model_path": model_path
        })
        super().__init__(message, context=context, **kwargs)


class InferenceError(SentimentError):
    """추론 실패"""

    def __init__(
            self,
            message: str,
            model_name: Optional[str] = None,
            input_text: Optional[str] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        if input_text and len(input_text) > 100:
            input_text = input_text[:100] + "..."
        context.update({
            "model_name": model_name,
            "input_text": input_text
        })
        super().__init__(message, context=context, **kwargs)


# ============================================================
# 설정 관련 예외
# ============================================================

class ConfigError(BaseAppException):
    """설정 오류"""

    def __init__(
            self,
            message: str,
            config_key: Optional[str] = None,
            config_file: Optional[str] = None,
            **kwargs
    ):
        context = kwargs.pop("context", {})
        context.update({
            "config_key": config_key,
            "config_file": config_file
        })
        super().__init__(message, context=context, **kwargs)
"""

@log_execution - 함수 실행 시간 + 결과 로깅
@handle_exception - 예외 처리 + 알림
@retry - 재시도 로직
@robust_execution - 올인원 조합
"""

import time
import asyncio
from typing import Any, Callable, Optional, Type
from functools import wraps

from .logging import get_logger, slack_notifier
from .exceptions import BaseAppException


def _truncate(text: str, max_length: int) -> str:
    """텍스트 자르기"""
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


# ============================================================
# @log_execution
# ============================================================

def log_execution(
        module: Optional[str] = None,
        log_args: bool = True,
        log_result: bool = False,
        max_result_length: int = 200
):
    """
    함수 실행 시간 및 결과 로깅 데코레이터

    Usage:
        @log_execution(module="data_fetcher")
        def fetch_stock(code: str) -> pd.DataFrame:
            ...
    """
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                _module = module or func.__module__.split(".")[-1]
                logger = get_logger(_module)
                func_name = func.__name__

                context = {}
                if log_args:
                    context["args"] = _truncate(str(args), 200)
                    context["kwargs"] = _truncate(str(kwargs), 200)

                logger.debug(f"함수 시작", func_name, context if log_args else None)

                start_time = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    elapsed = time.perf_counter() - start_time

                    result_context = {"elapsed_sec": round(elapsed, 4)}
                    if log_result:
                        result_context["result"] = _truncate(str(result), max_result_length)

                    logger.info(f"함수 완료", func_name, result_context)
                    return result

                except Exception as e:
                    elapsed = time.perf_counter() - start_time
                    logger.error(
                        f"함수 실패: {type(e).__name__}: {str(e)}",
                        func_name,
                        {"elapsed_sec": round(elapsed, 4)}
                    )
                    raise

            return async_wrapper

        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                _module = module or func.__module__.split(".")[-1]
                logger = get_logger(_module)
                func_name = func.__name__

                context = {}
                if log_args:
                    context["args"] = _truncate(str(args), 200)
                    context["kwargs"] = _truncate(str(kwargs), 200)

                logger.debug(f"함수 시작", func_name, context if log_args else None)

                start_time = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                    elapsed = time.perf_counter() - start_time

                    result_context = {"elapsed_sec": round(elapsed, 4)}
                    if log_result:
                        result_context["result"] = _truncate(str(result), max_result_length)

                    logger.info(f"함수 완료", func_name, result_context)
                    return result

                except Exception as e:
                    elapsed = time.perf_counter() - start_time
                    logger.error(
                        f"함수 실패: {type(e).__name__}: {str(e)}",
                        func_name,
                        {"elapsed_sec": round(elapsed, 4)}
                    )
                    raise

            return sync_wrapper

    return decorator


# ============================================================
# @handle_exception
# ============================================================

def handle_exception(
        default_return: Any = None,
        notify: bool = False,
        exceptions: tuple[Type[Exception], ...] = (Exception,),
        reraise: bool = False,
        module: Optional[str] = None
):
    """
    예외 처리 데코레이터

    Usage:
        @handle_exception(default_return=[], notify=True)
        def fetch_all_stocks():
            ...
    """
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                _module = module or func.__module__.split(".")[-1]
                logger = get_logger(_module)
                func_name = func.__name__

                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    context = {}
                    if isinstance(e, BaseAppException):
                        context = e.context

                    context["exception_type"] = type(e).__name__
                    context["exception_msg"] = str(e)

                    logger.exception(f"예외 발생", func_name, context)

                    if notify:
                        message = f"[{_module}:{func_name}] {type(e).__name__}: {str(e)}"
                        await slack_notifier.send_async(message, "ERROR")

                    if reraise:
                        raise

                    if callable(default_return):
                        return default_return()
                    return default_return

            return async_wrapper

        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                _module = module or func.__module__.split(".")[-1]
                logger = get_logger(_module)
                func_name = func.__name__

                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    context = {}
                    if isinstance(e, BaseAppException):
                        context = e.context

                    context["exception_type"] = type(e).__name__
                    context["exception_msg"] = str(e)

                    logger.exception(f"예외 발생", func_name, context)

                    if notify:
                        message = f"[{_module}:{func_name}] {type(e).__name__}: {str(e)}"
                        slack_notifier.send(message, "ERROR")

                    if reraise:
                        raise

                    if callable(default_return):
                        return default_return()
                    return default_return

            return sync_wrapper

    return decorator


# ============================================================
# @retry
# ============================================================

def retry(
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions: tuple[Type[Exception], ...] = (Exception,),
        on_retry: Optional[Callable[[Exception, int], None]] = None,
        module: Optional[str] = None
):
    """
    재시도 데코레이터 (지수 백오프)

    Usage:
        @retry(max_attempts=3, delay=1, backoff=2)
        def fetch_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                _module = module or func.__module__.split(".")[-1]
                logger = get_logger(_module)
                func_name = func.__name__

                last_exception = None
                current_delay = delay

                for attempt in range(1, max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e

                        if attempt == max_attempts:
                            logger.error(
                                f"최대 재시도 횟수 초과",
                                func_name,
                                {"max_attempts": max_attempts, "exception": str(e)}
                            )
                            raise

                        logger.warning(
                            f"재시도 예정",
                            func_name,
                            {
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "delay_sec": current_delay,
                                "exception": str(e)
                            }
                        )

                        if on_retry:
                            on_retry(e, attempt)

                        await asyncio.sleep(current_delay)
                        current_delay *= backoff

                raise last_exception

            return async_wrapper

        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                _module = module or func.__module__.split(".")[-1]
                logger = get_logger(_module)
                func_name = func.__name__

                last_exception = None
                current_delay = delay

                for attempt in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e

                        if attempt == max_attempts:
                            logger.error(
                                f"최대 재시도 횟수 초과",
                                func_name,
                                {"max_attempts": max_attempts, "exception": str(e)}
                            )
                            raise

                        logger.warning(
                            f"재시도 예정",
                            func_name,
                            {
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "delay_sec": current_delay,
                                "exception": str(e)
                            }
                        )

                        if on_retry:
                            on_retry(e, attempt)

                        time.sleep(current_delay)
                        current_delay *= backoff

                raise last_exception

            return sync_wrapper

    return decorator


# ============================================================
# @robust_execution - 올인원
# ============================================================

def robust_execution(
        module: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        default_return: Any = None,
        notify_on_failure: bool = True
):
    """
    로깅 + 재시도 + 예외처리 조합 데코레이터

    Usage:
        @robust_execution(module="data_fetcher", max_retries=3)
        def fetch_stock(code: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        decorated = func
        decorated = log_execution(module=module)(decorated)
        decorated = retry(max_attempts=max_retries, delay=retry_delay, module=module)(decorated)
        decorated = handle_exception(
            default_return=default_return,
            notify=notify_on_failure,
            module=module
        )(decorated)
        return decorated

    return decorator
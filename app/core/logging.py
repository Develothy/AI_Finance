"""

Features:
- loguru 기반 통합 로깅
- 파일별 분리: app.log (전체), error.log (ERROR+), trade.log (매매)
- 로그 로테이션: 일별 + 100MB 초과 시
- 보관 기간: 30일
- 비동기 로깅
- 민감 정보 마스킹
- 슬랙 알림 (CRITICAL)
"""

import os
import re
import sys
import json
import asyncio
from pathlib import Path
from typing import Any, Optional
from functools import wraps

from loguru import logger

try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False


# ============================================================
# 설정 로딩 (순환 참조 방지)
# ============================================================

_settings = None

def _get_settings():
    """설정 싱글톤 지연 로딩"""
    global _settings
    if _settings is None:
        try:
            from config import settings
            _settings = settings
        except ImportError:
            _settings = None
    return _settings


# ============================================================
# 로그 설정 클래스
# ============================================================

class _LogConfig:
    """로깅 설정"""

    @property
    def LOG_DIR(self) -> Path:
        settings = _get_settings()
        if settings:
            return Path(settings.LOG_DIR)
        return Path("logs")

    @property
    def APP_LOG(self) -> Path:
        return self.LOG_DIR / "app.log"

    @property
    def ERROR_LOG(self) -> Path:
        return self.LOG_DIR / "error.log"

    @property
    def TRADE_LOG(self) -> Path:
        return self.LOG_DIR / "trade.log"

    @property
    def ROTATION_SIZE(self) -> str:
        settings = _get_settings()
        if settings:
            return settings.LOG_ROTATION_SIZE
        return "100 MB"

    ROTATION_TIME = "00:00"

    @property
    def RETENTION(self) -> str:
        settings = _get_settings()
        if settings:
            return f"{settings.LOG_RETENTION_DAYS} days"
        return "30 days"

    LOG_FORMAT = (
        "[{time:YYYY-MM-DD HH:mm:ss}] [{level}] [{extra[trace_id]}] [{extra[module]}] [{extra[function]}] "
        "{message}"
    )

    CONSOLE_FORMAT = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<dim>{extra[trace_id]}</dim> | "
        "<cyan>{extra[module]}</cyan>:<cyan>{extra[function]}</cyan> | "
        "<level>{message}</level>"
    )

    SENSITIVE_PATTERNS = [
        (r'(api[_-]?key["\s:=]+)["\']?[\w\-]+["\']?', r'\1***MASKED***'),
        (r'(password["\s:=]+)["\']?[\w\-]+["\']?', r'\1***MASKED***'),
        (r'(secret["\s:=]+)["\']?[\w\-]+["\']?', r'\1***MASKED***'),
        (r'(token["\s:=]+)["\']?[\w\-]+["\']?', r'\1***MASKED***'),
        (r'(authorization["\s:=]+)["\']?[\w\-]+["\']?', r'\1***MASKED***'),
    ]

    @property
    def SLACK_ENABLED(self) -> bool:
        settings = _get_settings()
        if settings:
            return settings.SLACK_ENABLED
        return False

    @property
    def SLACK_TOKEN(self) -> Optional[str]:
        settings = _get_settings()
        if settings:
            return settings.SLACK_TOKEN
        return os.getenv("SLACK_TOKEN")

    @property
    def SLACK_CHANNEL(self) -> str:
        settings = _get_settings()
        if settings:
            return settings.SLACK_CHANNEL
        return os.getenv("SLACK_CHANNEL", "#alerts")

    @property
    def DEV_MODE(self) -> bool:
        settings = _get_settings()
        if settings:
            return settings.DEV_MODE
        return os.getenv("DEV_MODE", "true").lower() == "true"


LogConfig = _LogConfig()


# ============================================================
# 유틸리티 함수
# ============================================================

def mask_sensitive_data(message: str) -> str:
    """민감 정보 마스킹"""
    for pattern, replacement in LogConfig.SENSITIVE_PATTERNS:
        message = re.sub(pattern, replacement, message, flags=re.IGNORECASE)
    return message


def format_context(context: dict[str, Any]) -> str:
    """컨텍스트를 JSON 문자열로 포맷팅"""
    if not context:
        return ""
    try:
        json_str = json.dumps(context, ensure_ascii=False, default=str)
        return mask_sensitive_data(json_str)
    except Exception:
        return str(context)


# ============================================================
# 슬랙 알림
# ============================================================

class SlackNotifier:
    """슬랙 알림 발송"""

    def __init__(self):
        self.client = None
        self._initialized = False

    def _ensure_initialized(self):
        if self._initialized:
            return

        if SLACK_AVAILABLE and LogConfig.SLACK_ENABLED and LogConfig.SLACK_TOKEN:
            self.client = WebClient(token=LogConfig.SLACK_TOKEN)

        self._initialized = True

    def send(self, message: str, level: str = "CRITICAL") -> bool:
        self._ensure_initialized()

        if not self.client:
            return False

        try:
            emoji_map = {
                "CRITICAL": "🚨",
                "ERROR": "❌",
                "WARNING": "⚠️",
                "INFO": "ℹ️"
            }
            emoji = emoji_map.get(level, "📝")

            self.client.chat_postMessage(
                channel=LogConfig.SLACK_CHANNEL,
                text=f"{emoji} *[{level}]* {message}",
                mrkdwn=True
            )
            return True
        except SlackApiError as e:
            print(f"Slack notification failed: {e}")
            return False

    async def send_async(self, message: str, level: str = "CRITICAL") -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.send, message, level)


slack_notifier = SlackNotifier()


# ============================================================
# 커스텀 싱크
# ============================================================

def slack_sink(message):
    """CRITICAL 레벨 로그를 슬랙으로 발송"""
    record = message.record
    if record["level"].name == "CRITICAL":
        text = f"[{record['extra'].get('module', 'unknown')}] {record['message']}"
        slack_notifier.send(text, "CRITICAL")


def trade_filter(record):
    """매매 관련 로그만 필터링"""
    return record["extra"].get("trade", False)


def error_filter(record):
    """ERROR 이상 레벨 필터링"""
    return record["level"].no >= logger.level("ERROR").no


# ============================================================
# 로거 설정
# ============================================================

class LoggerSetup:
    """로거 초기화 및 설정"""

    _initialized = False

    @classmethod
    def setup(cls, dev_mode: Optional[bool] = None):
        if cls._initialized:
            return

        if dev_mode is None:
            dev_mode = LogConfig.DEV_MODE

        LogConfig.LOG_DIR.mkdir(parents=True, exist_ok=True)

        # extra 기본값 설정 (contextualize로 오버라이드 가능)
        logger.configure(extra={"trace_id": "-", "module": "", "function": "", "trade": False})

        logger.remove()

        # 콘솔 출력 (개발 모드)
        if dev_mode:
            logger.add(
                sys.stdout,
                format=LogConfig.CONSOLE_FORMAT,
                level="DEBUG",
                colorize=True,
                filter=lambda record: not record["extra"].get("trade", False)
            )

        # app.log
        logger.add(
            LogConfig.APP_LOG,
            format=LogConfig.LOG_FORMAT,
            level="DEBUG",
            rotation=LogConfig.ROTATION_SIZE,
            retention=LogConfig.RETENTION,
            compression="gz",
            enqueue=True,
            filter=lambda record: not record["extra"].get("trade", False)
        )

        # error.log
        logger.add(
            LogConfig.ERROR_LOG,
            format=LogConfig.LOG_FORMAT,
            level="ERROR",
            rotation=LogConfig.ROTATION_SIZE,
            retention=LogConfig.RETENTION,
            compression="gz",
            enqueue=True,
            filter=lambda record: (
                    error_filter(record) and
                    not record["extra"].get("trade", False)
            )
        )

        # trade.log
        logger.add(
            LogConfig.TRADE_LOG,
            format=LogConfig.LOG_FORMAT,
            level="INFO",
            rotation=LogConfig.ROTATION_TIME,
            retention=LogConfig.RETENTION,
            compression="gz",
            enqueue=True,
            filter=trade_filter
        )

        # 슬랙 알림
        logger.add(
            slack_sink,
            level="CRITICAL",
            enqueue=True
        )

        cls._initialized = True
        logger.bind(module="core", function="setup", trace_id="-").info("Logger initialized")


# ============================================================
# 애플리케이션 로거
# ============================================================

class AppLogger:
    """애플리케이션 로거 래퍼"""

    def __init__(self, module: str):
        LoggerSetup.setup()
        self.module = module
        self._logger = logger.bind(module=module, function="")

    def _log(
            self,
            level: str,
            message: str,
            function: str = "",
            context: Optional[dict[str, Any]] = None,
            trade: bool = False
    ):
        if context:
            context_str = format_context(context)
            message = f"{message} {context_str}"

        message = mask_sensitive_data(message)
        bound_logger = self._logger.bind(function=function, trade=trade)
        getattr(bound_logger, level.lower())(message)

    def debug(self, message: str, function: str = "", context: Optional[dict] = None):
        self._log("DEBUG", message, function, context)

    def info(self, message: str, function: str = "", context: Optional[dict] = None):
        self._log("INFO", message, function, context)

    def warning(self, message: str, function: str = "", context: Optional[dict] = None):
        self._log("WARNING", message, function, context)

    def error(self, message: str, function: str = "", context: Optional[dict] = None):
        self._log("ERROR", message, function, context)

    def critical(self, message: str, function: str = "", context: Optional[dict] = None):
        self._log("CRITICAL", message, function, context)

    def trade(self, message: str, function: str = "", context: Optional[dict] = None):
        """매매 로그 (trade.log에 기록)"""
        self._log("INFO", message, function, context, trade=True)

    def exception(self, message: str, function: str = "", context: Optional[dict] = None):
        """예외 로그 (스택 트레이스 포함)"""
        if context:
            context_str = format_context(context)
            message = f"{message} {context_str}"
        message = mask_sensitive_data(message)
        bound_logger = self._logger.bind(function=function, trade=False)
        bound_logger.exception(message)


def get_logger(module: str) -> AppLogger:
    """모듈별 로거 생성"""
    return AppLogger(module)
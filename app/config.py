"""
퀀트 플랫폼 설정 관리
==================

.env 파일 기반 설정 관리 (자바 properties와 유사)

Usage:
    from config import settings

    # 설정값 접근
    print(settings.SLACK_TOKEN)
    print(settings.DB_HOST)
    print(settings.is_dev_mode)

    # 설정 검증
    settings.validate()
"""

import os
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass
from functools import lru_cache

# dotenv 로드
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


@dataclass
class Settings:
    """애플리케이션 설정"""

    # ----------------------------------------------------------
    # 앱 설정
    # ----------------------------------------------------------
    APP_ENV: str = "development"
    DEV_MODE: bool = True
    DEBUG: bool = True

    # ----------------------------------------------------------
    # 로깅 설정
    # ----------------------------------------------------------
    LOG_LEVEL: str = "DEBUG"
    LOG_DIR: str = "logs"
    LOG_RETENTION_DAYS: int = 30
    LOG_ROTATION_SIZE: str = "100MB"

    # ----------------------------------------------------------
    # 슬랙 알림
    # ----------------------------------------------------------
    SLACK_ENABLED: bool = False
    SLACK_TOKEN: Optional[str] = None
    SLACK_CHANNEL: str = "#alerts"
    SLACK_WEBHOOK_URL: Optional[str] = None

    # ----------------------------------------------------------
    # 텔레그램 알림
    # ----------------------------------------------------------
    TELEGRAM_ENABLED: bool = False
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    # ----------------------------------------------------------
    # 데이터베이스
    # ----------------------------------------------------------
    DB_TYPE: str = "sqlite"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "quant_platform"
    DB_USER: str = "postgres"
    DB_PASSWORD: Optional[str] = None
    SQLITE_PATH: str = "data/quant.db"

    # ----------------------------------------------------------
    # 한국투자증권 API
    # ----------------------------------------------------------
    KIS_APP_KEY: Optional[str] = None
    KIS_APP_SECRET: Optional[str] = None
    KIS_ACCOUNT_NO: Optional[str] = None
    KIS_MOCK_MODE: bool = True

    # ----------------------------------------------------------
    # Alpaca API
    # ----------------------------------------------------------
    ALPACA_API_KEY: Optional[str] = None
    ALPACA_SECRET_KEY: Optional[str] = None
    ALPACA_PAPER: bool = True

    # ----------------------------------------------------------
    # OpenAI API
    # ----------------------------------------------------------
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_MODEL: str = "gpt-4o-mini"

    # ----------------------------------------------------------
    # 스케줄러
    # ----------------------------------------------------------
    SCHEDULER_TIMEZONE: str = "Asia/Seoul"
    DATA_FETCH_HOUR: int = 18
    DATA_FETCH_MINUTE: int = 0

    # ----------------------------------------------------------
    # 프로퍼티
    # ----------------------------------------------------------

    @property
    def is_dev_mode(self) -> bool:
        return self.DEV_MODE or self.APP_ENV == "development"

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def db_url(self) -> str:
        if self.DB_TYPE == "sqlite":
            return f"sqlite:///{self.SQLITE_PATH}"
        elif self.DB_TYPE == "postgresql":
            return (
                f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            )
        else:
            raise ValueError(f"지원하지 않는 DB 타입: {self.DB_TYPE}")

    @property
    def log_dir_path(self) -> Path:
        return Path(self.LOG_DIR)

    # ----------------------------------------------------------
    # 검증
    # ----------------------------------------------------------

    def validate(self) -> list[str]:
        """설정 검증. 경고 메시지 리스트 반환"""
        warnings = []

        if self.SLACK_ENABLED and not self.SLACK_TOKEN:
            warnings.append("SLACK_ENABLED=true이지만 SLACK_TOKEN이 설정되지 않음")

        if self.TELEGRAM_ENABLED:
            if not self.TELEGRAM_BOT_TOKEN:
                warnings.append("TELEGRAM_ENABLED=true이지만 TELEGRAM_BOT_TOKEN이 설정되지 않음")
            if not self.TELEGRAM_CHAT_ID:
                warnings.append("TELEGRAM_ENABLED=true이지만 TELEGRAM_CHAT_ID가 설정되지 않음")

        if self.DB_TYPE == "postgresql" and not self.DB_PASSWORD:
            warnings.append("DB_TYPE=postgresql이지만 DB_PASSWORD가 설정되지 않음")

        if self.is_production:
            if self.DEBUG:
                warnings.append("프로덕션 환경에서 DEBUG=true는 권장하지 않음")
            if self.KIS_MOCK_MODE:
                warnings.append("프로덕션 환경에서 KIS_MOCK_MODE=true 확인 필요")

        return warnings

    def print_config(self, show_secrets: bool = False):
        """설정값 출력 (디버깅용)"""
        print("=" * 60)
        print("현재 설정")
        print("=" * 60)

        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue

            if not show_secrets and any(s in key.upper() for s in ["PASSWORD", "SECRET", "TOKEN", "KEY"]):
                if value:
                    value = "***MASKED***"

            print(f"{key}: {value}")

        print("=" * 60)


class ConfigLoader:
    """설정 파일 로더"""

    @staticmethod
    def load_from_env(env_file: Optional[str] = None) -> Settings:
        if DOTENV_AVAILABLE:
            if env_file:
                load_dotenv(env_file)
            else:
                load_dotenv()

        settings = Settings()

        for field_name in settings.__dataclass_fields__:
            env_value = os.getenv(field_name)
            if env_value is not None:
                field_type = type(getattr(settings, field_name))

                if field_type == bool:
                    converted_value = env_value.lower() in ("true", "1", "yes", "on")
                elif field_type == int:
                    converted_value = int(env_value)
                elif field_type == float:
                    converted_value = float(env_value)
                else:
                    converted_value = env_value

                setattr(settings, field_name, converted_value)

        return settings

    @staticmethod
    def load_from_dict(config_dict: dict[str, Any]) -> Settings:
        settings = Settings()
        for key, value in config_dict.items():
            if hasattr(settings, key):
                setattr(settings, key, value)
        return settings


@lru_cache()
def get_settings(env_file: Optional[str] = None) -> Settings:
    """설정 싱글톤 인스턴스 반환"""
    return ConfigLoader.load_from_env(env_file)


def reload_settings(env_file: Optional[str] = None) -> Settings:
    """설정 다시 로드 (캐시 무효화)"""
    get_settings.cache_clear()
    return get_settings(env_file)


# 기본 설정 인스턴스
settings = get_settings()
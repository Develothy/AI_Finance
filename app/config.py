"""
Usage:
    from config import settings

    print(settings.DB_TYPE)
    print(settings.db_url)
    print(settings.SLACK_ENABLED)
"""

import os
from typing import Optional
from dataclasses import dataclass

# dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class _Settings:

    # 앱
    APP_ENV: str = "development"
    DEV_MODE: bool = True
    DEBUG: bool = True

    # 로깅
    LOG_LEVEL: str = "DEBUG"
    LOG_DIR: str = "logs"
    LOG_RETENTION_DAYS: int = 30
    LOG_ROTATION_SIZE: str = "100MB"

    # 슬랙
    SLACK_ENABLED: bool = False
    SLACK_TOKEN: Optional[str] = None
    SLACK_CHANNEL: str = "#alerts"
    SLACK_WEBHOOK_URL: Optional[str] = None

    # DB
    DB_TYPE: str = "sqlite"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "quant_platform"
    DB_USER: str = "postgres"
    DB_PASSWORD: Optional[str] = None
    SQLITE_PATH: str = "data/quant.db"

    # 한국투자증권
    KIS_APP_KEY: Optional[str] = None
    KIS_APP_SECRET: Optional[str] = None
    KIS_ACCOUNT_NO: Optional[str] = None
    KIS_MOCK_MODE: bool = True

    # Alpaca
    ALPACA_API_KEY: Optional[str] = None
    ALPACA_SECRET_KEY: Optional[str] = None
    ALPACA_PAPER: bool = True

    # OpenAI
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_MODEL: str = "gpt-4o-mini"

    # 스케줄러
    SCHEDULER_TIMEZONE: str = "Asia/Seoul"
    DATA_FETCH_HOUR: int = 18
    DATA_FETCH_MINUTE: int = 0

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def db_url(self) -> str:
        if self.DB_TYPE == "sqlite":
            return f"sqlite:///{self.SQLITE_PATH}"
        else:
            return (
                f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            )

    def validate(self) -> list[str]:
        warnings = []
        if self.SLACK_ENABLED and not self.SLACK_TOKEN:
            warnings.append("SLACK_ENABLED=true but SLACK_TOKEN is not set")
        if self.DB_TYPE == "postgresql" and not self.DB_PASSWORD:
            warnings.append("DB_TYPE=postgresql but DB_PASSWORD is not set")
        return warnings


def _load_settings() -> _Settings:
    s = _Settings()
    for field in s.__dataclass_fields__:
        val = os.getenv(field)
        if val is not None:
            field_type = type(getattr(s, field))
            if field_type == bool:
                setattr(s, field, val.lower() in ("true", "1", "yes"))
            elif field_type == int:
                setattr(s, field, int(val))
            else:
                setattr(s, field, val)
    return s


# 설정 인스턴스 (싱글톤)
settings = _load_settings()
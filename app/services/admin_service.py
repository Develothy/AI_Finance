"""
어드민 모니터링 서비스
======================

DB 통계, 로그 조회, 설정 조회
"""

import re
from collections import deque
from pathlib import Path
from typing import Optional

from api.schemas import (
    ConfigGroup,
    ConfigResponse,
    DBResponse,
    LogEntry,
    LogResponse,
    TableStats,
)
from config import settings
from core import get_logger
from db import database
from repositories.admin_repository import AdminRepository

logger = get_logger("admin_service")


class AdminService:

    def get_db_status(self) -> DBResponse:
        """DB 상태 + 테이블 통계"""
        try:
            with database.session() as session:
                repo = AdminRepository(session)
                tables = {
                    "stock_price": TableStats(**repo.stock_price_stats()),
                    "stock_info": TableStats(**repo.stock_info_stats()),
                    "stock_fundamental": TableStats(**repo.fundamental_stats()),
                    "financial_statement": TableStats(**repo.financial_stmt_stats()),
                    "feature_store": TableStats(**repo.feature_store_stats()),
                    "news_sentiment": TableStats(**repo.news_stats()),
                    "ml_model": TableStats(**repo.ml_model_stats()),
                    "ml_prediction": TableStats(**repo.ml_prediction_stats()),
                    "dart_disclosure": TableStats(**repo.dart_stats()),
                    "krx_supply_demand": TableStats(**repo.krx_stats()),
                }
                return DBResponse(connected=True, db_type=settings.DB_TYPE, tables=tables)
        except Exception as e:
            return DBResponse(connected=False, db_type=settings.DB_TYPE, error=str(e))

    def get_logs(
        self, file: str, lines: int, level: Optional[str], search: Optional[str],
    ) -> LogResponse:
        """로그 파일 파싱 + 필터링"""
        log_map = {
            "app": Path(settings.LOG_DIR) / "app.log",
            "error": Path(settings.LOG_DIR) / "error.log",
            "trade": Path(settings.LOG_DIR) / "trade.log",
        }

        log_path = log_map.get(file)
        if not log_path or not log_path.exists():
            return LogResponse(file=file, total=0, entries=[])

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            raw_lines = list(deque(f, maxlen=lines))

        pattern = re.compile(
            r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] \[(\w+)\] \[(\w*)\] \[(\w*)\] (.*)'
        )

        entries = []
        for raw in raw_lines:
            raw = raw.strip()
            if not raw:
                continue

            m = pattern.match(raw)
            if m:
                entry = LogEntry(
                    time=m.group(1), level=m.group(2),
                    module=m.group(3), function=m.group(4), message=m.group(5),
                )
            else:
                entry = LogEntry(time="", level="", module="", function="", message=raw)

            if level and entry.level and entry.level.upper() != level.upper():
                continue
            if search and search.lower() not in raw.lower():
                continue

            entries.append(entry)

        entries.reverse()
        return LogResponse(file=file, total=len(entries), entries=entries)

    def get_config(self) -> ConfigResponse:
        """설정 조회 (민감정보 마스킹)"""
        masked_keys = {
            "DB_PASSWORD", "SLACK_TOKEN", "SLACK_WEBHOOK_URL",
            "KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO",
            "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET", "KIS_MOCK_ACCOUNT_NO",
            "ALPACA_API_KEY", "ALPACA_SECRET_KEY",
            "OPENAI_API_KEY",
            "DART_API_KEY", "FRED_API_KEY",
            "NAVER_CLIENT_ID", "NAVER_CLIENT_SECRET",
        }

        groups_def = {
            "app": ["APP_ENV", "DEV_MODE", "DEBUG"],
            "logging": ["LOG_LEVEL", "LOG_DIR", "LOG_RETENTION_DAYS", "LOG_ROTATION_SIZE"],
            "database": ["DB_TYPE", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "SQLITE_PATH"],
            "scheduler": ["SCHEDULER_TIMEZONE", "DATA_FETCH_HOUR", "DATA_FETCH_MINUTE"],
            "slack": ["SLACK_ENABLED", "SLACK_TOKEN", "SLACK_CHANNEL", "SLACK_WEBHOOK_URL"],
            "kis": ["KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO",
                    "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET", "KIS_MOCK_ACCOUNT_NO", "KIS_MOCK_MODE"],
            "alpaca": ["ALPACA_API_KEY", "ALPACA_SECRET_KEY", "ALPACA_PAPER"],
            "openai": ["OPENAI_API_KEY", "OPENAI_MODEL"],
            "dart": ["DART_API_KEY"],
            "fred": ["FRED_API_KEY"],
            "naver": ["NAVER_CLIENT_ID", "NAVER_CLIENT_SECRET"],
        }

        groups = {}
        for group_name, keys in groups_def.items():
            items = {}
            for key in keys:
                val = getattr(settings, key, None)
                if key in masked_keys and val:
                    items[key] = "***MASKED***"
                else:
                    items[key] = str(val) if val is not None else ""
            groups[group_name] = ConfigGroup(items=items)

        warnings = settings.validate()
        return ConfigResponse(warnings=warnings, groups=groups)


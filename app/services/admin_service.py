"""
어드민 모니터링 서비스
======================

DB 통계, 로그 조회, 설정 조회
"""

import re
from collections import deque
from pathlib import Path
from typing import Optional

from sqlalchemy import func

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
from models import (
    StockPrice, StockInfo, StockFundamental, FinancialStatement,
    FeatureStore, MLModel, MLPrediction,
    NewsSentiment, DartDisclosure, KrxSupplyDemand,
)

logger = get_logger("admin_service")


class AdminService:

    def get_db_status(self) -> DBResponse:
        """DB 상태 + 테이블 통계"""
        try:
            with database.session() as session:
                tables = {}
                tables["stock_price"] = self._stock_price_stats(session)
                tables["stock_info"] = self._stock_info_stats(session)
                tables["stock_fundamental"] = self._fundamental_stats(session)
                tables["financial_statement"] = self._financial_stmt_stats(session)
                tables["feature_store"] = self._feature_store_stats(session)
                tables["news_sentiment"] = self._news_stats(session)
                tables["ml_model"] = self._ml_model_stats(session)
                tables["ml_prediction"] = self._ml_prediction_stats(session)
                tables["dart_disclosure"] = self._dart_stats(session)
                tables["krx_supply_demand"] = self._krx_stats(session)

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

    # ── 내부 통계 헬퍼 ──

    def _stock_price_stats(self, session) -> TableStats:
        count = session.query(func.count(StockPrice.id)).scalar() or 0
        if count == 0:
            return TableStats(row_count=0)
        earliest = session.query(func.min(StockPrice.date)).scalar()
        latest = session.query(func.max(StockPrice.date)).scalar()
        markets = [r[0] for r in session.query(StockPrice.market).distinct().all()]
        code_count = session.query(func.count(func.distinct(StockPrice.code))).scalar() or 0
        return TableStats(
            row_count=count,
            earliest_date=earliest.strftime("%Y-%m-%d") if earliest else None,
            latest_date=latest.strftime("%Y-%m-%d") if latest else None,
            markets=markets, code_count=code_count,
        )

    def _stock_info_stats(self, session) -> TableStats:
        count = session.query(func.count(StockInfo.id)).scalar() or 0
        if count == 0:
            return TableStats(row_count=0)
        markets = [r[0] for r in session.query(StockInfo.market).distinct().all()]
        sector_count = session.query(func.count(func.distinct(StockInfo.sector))).scalar() or 0
        return TableStats(row_count=count, markets=markets, sector_count=sector_count)

    def _fundamental_stats(self, session) -> TableStats:
        count = session.query(func.count(StockFundamental.id)).scalar() or 0
        if count == 0:
            return TableStats(row_count=0)
        earliest = session.query(func.min(StockFundamental.date)).scalar()
        latest = session.query(func.max(StockFundamental.date)).scalar()
        markets = [r[0] for r in session.query(StockFundamental.market).distinct().all()]
        code_count = session.query(func.count(func.distinct(StockFundamental.code))).scalar() or 0
        return TableStats(
            row_count=count,
            earliest_date=earliest.strftime("%Y-%m-%d") if earliest else None,
            latest_date=latest.strftime("%Y-%m-%d") if latest else None,
            markets=markets, code_count=code_count,
        )

    def _financial_stmt_stats(self, session) -> TableStats:
        count = session.query(func.count(FinancialStatement.id)).scalar() or 0
        if count == 0:
            return TableStats(row_count=0)
        markets = [r[0] for r in session.query(FinancialStatement.market).distinct().all()]
        code_count = session.query(func.count(func.distinct(FinancialStatement.code))).scalar() or 0
        period_count = session.query(func.count(func.distinct(FinancialStatement.period_date))).scalar() or 0
        return TableStats(row_count=count, markets=markets, code_count=code_count, period_count=period_count)

    def _feature_store_stats(self, session) -> TableStats:
        count = session.query(func.count(FeatureStore.id)).scalar() or 0
        if count == 0:
            return TableStats(row_count=0)
        earliest = session.query(func.min(FeatureStore.date)).scalar()
        latest = session.query(func.max(FeatureStore.date)).scalar()
        markets = [r[0] for r in session.query(FeatureStore.market).distinct().all()]
        code_count = session.query(func.count(func.distinct(FeatureStore.code))).scalar() or 0
        phase6_count = session.query(func.count(FeatureStore.id)).filter(
            FeatureStore.sector_return_1d.isnot(None),
        ).scalar() or 0
        phase6_code_count = session.query(
            func.count(func.distinct(FeatureStore.code))
        ).filter(
            FeatureStore.sector_return_1d.isnot(None),
        ).scalar() or 0
        return TableStats(
            row_count=count,
            earliest_date=earliest.strftime("%Y-%m-%d") if earliest else None,
            latest_date=latest.strftime("%Y-%m-%d") if latest else None,
            markets=markets, code_count=code_count,
            phase6_count=phase6_count, phase6_code_count=phase6_code_count,
        )

    def _news_stats(self, session) -> TableStats:
        count = session.query(func.count(NewsSentiment.id)).scalar() or 0
        if count == 0:
            return TableStats(row_count=0)
        earliest = session.query(func.min(NewsSentiment.date)).scalar()
        latest = session.query(func.max(NewsSentiment.date)).scalar()
        code_count = session.query(
            func.count(func.distinct(NewsSentiment.code))
        ).filter(NewsSentiment.code.isnot(None)).scalar() or 0
        return TableStats(
            row_count=count,
            earliest_date=earliest.strftime("%Y-%m-%d") if earliest else None,
            latest_date=latest.strftime("%Y-%m-%d") if latest else None,
            code_count=code_count,
        )

    def _ml_model_stats(self, session) -> TableStats:
        count = session.query(func.count(MLModel.id)).scalar() or 0
        active = session.query(func.count(MLModel.id)).filter(MLModel.is_active.is_(True)).scalar() or 0
        return TableStats(row_count=count, active_count=active)

    def _ml_prediction_stats(self, session) -> TableStats:
        count = session.query(func.count(MLPrediction.id)).scalar() or 0
        return TableStats(row_count=count)

    def _dart_stats(self, session) -> TableStats:
        return TableStats(
            row_count=session.query(func.count(DartDisclosure.id)).scalar() or 0,
            code_count=session.query(func.count(func.distinct(DartDisclosure.code))).scalar() or 0,
        )

    def _krx_stats(self, session) -> TableStats:
        return TableStats(
            row_count=session.query(func.count(KrxSupplyDemand.id)).scalar() or 0,
            code_count=session.query(func.count(func.distinct(KrxSupplyDemand.code))).scalar() or 0,
        )

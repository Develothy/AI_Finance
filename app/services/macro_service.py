"""
거시경제 지표 서비스 (Phase 3)
==============================

거시지표 수집/조회 오케스트레이션
"""

from datetime import datetime, timedelta
from typing import Optional

from core import get_logger
from db import database
from data_collector.macro_fetcher import MacroFetcher
from repositories import MacroRepository

logger = get_logger("macro_service")


class MacroService:
    """거시경제 지표 수집/조회 서비스"""

    def __init__(self):
        self.fetcher = MacroFetcher()

    # ============================================================
    # 수집
    # ============================================================

    def collect(
        self,
        start_date: str = None,
        end_date: str = None,
        days_back: int = 30,
    ) -> dict:
        """
        거시지표 수집 → DB 저장

        Args:
            start_date: 시작일 (None이면 days_back일 전)
            end_date: 종료일 (None이면 오늘)
            days_back: start_date 미지정 시 수집 기간
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_dt = datetime.now() - timedelta(days=days_back)
            start_date = start_dt.strftime("%Y-%m-%d")

        logger.info(
            f"거시지표 수집 시작: {start_date} ~ {end_date}",
            "collect",
        )

        result = self.fetcher.fetch_all(start_date, end_date)

        if not result.records:
            return {
                "total": result.total_count,
                "success": result.success_count,
                "failed": result.failed_count,
                "skipped": len(result.skipped_indicators),
                "saved": 0,
                "message": result.message or "수집 데이터 없음",
            }

        # DB 저장
        saved = 0
        with database.session() as session:
            repo = MacroRepository(session)
            saved = repo.upsert_indicators(result.records)

        logger.info(
            f"거시지표 수집 완료: {saved}건 저장",
            "collect",
        )

        return {
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "skipped": len(result.skipped_indicators),
            "saved": saved,
            "message": result.message,
        }

    # ============================================================
    # 조회
    # ============================================================

    def get_indicators(
        self,
        indicator_name: str,
        start_date: str = None,
        end_date: str = None,
    ) -> list[dict]:
        """특정 지표 시계열 조회"""
        with database.session() as session:
            repo = MacroRepository(session)
            rows = repo.get_indicators(indicator_name, start_date, end_date)
            return [self._to_dict(r) for r in rows]

    def get_latest(self) -> list[dict]:
        """지표별 최신값 조회"""
        with database.session() as session:
            repo = MacroRepository(session)
            rows = repo.get_latest()
            return [self._to_dict(r) for r in rows]

    # ============================================================
    # 헬퍼
    # ============================================================

    @staticmethod
    def _to_dict(m) -> dict:
        return {
            "indicator_name": m.indicator_name,
            "date": str(m.date) if m.date else None,
            "value": float(m.value) if m.value else None,
            "change_pct": float(m.change_pct) if m.change_pct else None,
            "source": m.source,
        }

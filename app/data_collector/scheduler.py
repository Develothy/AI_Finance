"""
데이터 수집 스케줄러
=================

매일 자동 실행
"""

from datetime import datetime, timedelta
from typing import Optional, Callable

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

import sys
sys.path.append('..')
from config import settings
from core import get_logger

from .pipeline import DataPipeline

logger = get_logger("scheduler")


class DataScheduler:
    """데이터 수집 스케줄러"""

    def __init__(self):
        if not SCHEDULER_AVAILABLE:
            raise ImportError("apscheduler가 설치되어 있지 않습니다. pip install apscheduler")

        self.scheduler = BackgroundScheduler(
            timezone=settings.SCHEDULER_TIMEZONE
        )
        self.pipeline = DataPipeline()
        self._jobs = {}

        logger.info("DataScheduler 초기화 완료", "__init__")

    def add_daily_job(
            self,
            job_id: str,
            hour: int = None,
            minute: int = None,
            market: Optional[str] = None,
            sector: Optional[str] = None,
            days_back: int = 7,
            callback: Optional[Callable] = None
    ):
        """
        일별 수집 작업 추가

        Args:
            job_id: 작업 ID
            hour: 실행 시각 (시)
            minute: 실행 시각 (분)
            market: 마켓
            sector: 섹터
            days_back: 수집 기간 (오늘 기준 N일 전부터)
            callback: 완료 후 콜백 함수
        """
        if hour is None:
            hour = settings.DATA_FETCH_HOUR
        if minute is None:
            minute = settings.DATA_FETCH_MINUTE

        def job_func():
            logger.info(
                f"스케줄 작업 시작",
                "daily_job",
                {"job_id": job_id, "market": market, "sector": sector}
            )

            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

            result = self.pipeline.fetch(
                start_date=start_date,
                end_date=end_date,
                market=market,
                sector=sector
            )

            logger.info(
                f"스케줄 작업 완료",
                "daily_job",
                {"job_id": job_id, "result": result.to_dict()}
            )

            if callback:
                callback(result)

        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            timezone=settings.SCHEDULER_TIMEZONE
        )

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            replace_existing=True
        )

        self._jobs[job_id] = job

        logger.info(
            f"스케줄 작업 등록",
            "add_daily_job",
            {
                "job_id": job_id,
                "time": f"{hour:02d}:{minute:02d}",
                "market": market
            }
        )

    def add_kr_daily_job(
            self,
            hour: int = None,
            minute: int = None,
            market: str = None,
            sector: str = None
    ):
        """한국 주식 일별 수집 작업"""
        self.add_daily_job(
            job_id="kr_daily",
            hour=hour,
            minute=minute,
            market=market or "KOSPI",
            sector=sector
        )

    def add_us_daily_job(
            self,
            hour: int = None,
            minute: int = None,
            market: str = None,
            sector: str = None
    ):
        """미국 주식 일별 수집 작업"""
        # 미국은 한국 시간 기준 아침에 수집 (장 마감 후)
        if hour is None:
            hour = 7  # 한국 시간 오전 7시

        self.add_daily_job(
            job_id="us_daily",
            hour=hour,
            minute=minute or 0,
            market=market or "S&P500",
            sector=sector
        )

    def remove_job(self, job_id: str):
        """작업 제거"""
        if job_id in self._jobs:
            self.scheduler.remove_job(job_id)
            del self._jobs[job_id]
            logger.info(f"작업 제거: {job_id}", "remove_job")

    def start(self):
        """스케줄러 시작"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("스케줄러 시작됨", "start")

    def stop(self):
        """스케줄러 중지"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("스케줄러 중지됨", "stop")

    def get_jobs(self) -> list[dict]:
        """등록된 작업 목록"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run": str(job.next_run_time),
                "trigger": str(job.trigger)
            })
        return jobs

    def run_now(self, job_id: str):
        """작업 즉시 실행"""
        if job_id in self._jobs:
            job = self._jobs[job_id]
            job.func()
            logger.info(f"작업 즉시 실행: {job_id}", "run_now")
        else:
            logger.warning(f"작업을 찾을 수 없음: {job_id}", "run_now")
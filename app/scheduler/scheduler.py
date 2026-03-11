"""
잡 스케줄러
==========

크론식 기반 자동 실행
"""

from datetime import datetime, timedelta
from typing import Optional, Callable

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

from config import settings
from core import get_logger
from db import database

from data_collector.pipeline import DataPipeline

logger = get_logger("scheduler")


def parse_cron_expr(cron_expr: str) -> CronTrigger:
    """
    크론식을 APScheduler CronTrigger로 변환

    지원 형식:
        5필드: 분 시 일 월 요일          (표준 crontab)
        6필드: 초 분 시 일 월 요일        (Quartz 스타일)

    예시:
        "0 18 * * *"     → 매일 18:00
        "*/10 * * * *"   → 10분마다
        "0 */6 * * *"    → 6시간마다
        "0 18 */7 * *"   → 7일마다 18:00
        "0 0 18 * * *"   → 매일 18:00 (6필드, 초=0)
        "0 */10 * * * *" → 10분마다 (6필드, 초=0)
    """
    parts = cron_expr.strip().split()

    if len(parts) == 5:
        # 표준 crontab: 분 시 일 월 요일
        return CronTrigger.from_crontab(cron_expr, timezone=settings.SCHEDULER_TIMEZONE)

    elif len(parts) == 6:
        # Quartz 스타일: 초 분 시 일 월 요일
        second, minute, hour, day, month, day_of_week = parts
        # '?' → '*' 변환 (Quartz 호환)
        day_of_week = day_of_week.replace("?", "*")
        day = day.replace("?", "*")
        return CronTrigger(
            second=second,
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=settings.SCHEDULER_TIMEZONE,
        )

    raise ValueError(f"잘못된 크론식: '{cron_expr}' (5 또는 6필드)")


class JobScheduler:
    """잡 스케줄러"""

    _instance: "JobScheduler | None" = None

    @classmethod
    def get_instance(cls) -> "JobScheduler":
        # 싱글톤 인스턴스
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def get_running_instance(cls) -> "JobScheduler | None":
        # 실행 중인 인스턴스 반환 (없거나 미실행이면 None)
        if cls._instance is None:
            return None
        return cls._instance if cls._instance.scheduler.running else None

    def __init__(self):
        if not SCHEDULER_AVAILABLE:
            raise ImportError("apscheduler가 설치되어 있지 않습니다. pip install apscheduler")

        self.scheduler = BackgroundScheduler(
            timezone=settings.SCHEDULER_TIMEZONE
        )
        database.create_tables()
        self.pipeline = DataPipeline()
        self._jobs = {}

        logger.info("JobScheduler 초기화 완료", "__init__")

    def load_jobs_from_db(self):
        # enabled 스케줄 APScheduler에 등록
        from repositories.scheduler_repository import SchedulerRepository

        with database.session() as session:
            repo = SchedulerRepository(session)
            jobs = repo.get_all_jobs()
            enabled_ids = [j.id for j in jobs if j.enabled]
            steps_by_job = repo.get_steps_for_jobs(enabled_ids)
            loaded = 0
            for job_model in jobs:
                if not job_model.enabled:
                    continue
                try:
                    steps = steps_by_job.get(job_model.id, [])
                    self._add_scheduled_job(job_model, steps=steps)
                    loaded += 1
                except Exception as e:
                    logger.warning(f"잡 등록 실패: {job_model.job_name} - {e}", "load_jobs_from_db")

        logger.info(f"DB에서 {loaded}개 잡 로드 완료", "load_jobs_from_db")
        return loaded

    @staticmethod
    def _create_schedule_log(job_name: str) -> int | None:
        """스케줄 실행 이력 생성 (공통 헬퍼)"""
        from repositories.scheduler_repository import SchedulerRepository

        with database.session() as session:
            repo = SchedulerRepository(session)
            job_row = repo.get_job_by_name(job_name)
            if job_row:
                log = repo.create_log({
                    "job_id": job_row.id,
                    "started_at": datetime.now(),
                    "status": "running",
                    "trigger_by": "scheduler",
                })
                return log.id
        return None

    @staticmethod
    def _update_schedule_log(log_id: int, data: dict):
        """스케줄 실행 이력 업데이트 (공통 헬퍼)"""
        if not log_id:
            return
        from repositories.scheduler_repository import SchedulerRepository

        with database.session() as session:
            repo = SchedulerRepository(session)
            log_entry = repo.get_log(log_id)
            if log_entry:
                data.setdefault("finished_at", datetime.now())
                repo.update_log(log_entry, data)

    def add_cron_job(
            self,
            job_id: str,
            cron_expr: str,
            market: Optional[str] = None,
            sector: Optional[str] = None,
            days_back: int = 7,
            callback: Optional[Callable] = None
    ):
        """
        크론식 기반 수집 작업 추가

        Args:
            job_id: 작업 ID
            cron_expr: 크론 표현식 (5필드 또는 6필드)
            market: 마켓
            sector: 섹터
            days_back: 수집 기간 (오늘 기준 N일 전부터)
            callback: 완료 후 콜백 함수
        """
        def job_func():
            logger.info(
                f"스케줄 작업 시작",
                "cron_job",
                {"job_id": job_id, "market": market, "sector": sector, "cron": cron_expr}
            )

            log_id = self._create_schedule_log(job_id)

            try:
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

                result = self.pipeline.fetch(
                    start_date=start_date,
                    end_date=end_date,
                    market=market,
                    sector=sector
                )

                if result.data or result.stock_info:
                    from services import StockService
                    svc = StockService(pipeline=self.pipeline)
                    if result.data:
                        result.db_saved_count = svc.save_to_db(result.data, result.market)
                    if result.stock_info:
                        svc._save_stock_info(result.stock_info)

                self._update_schedule_log(log_id, {
                    "status": "success" if result.success else "failed",
                    "total_codes": result.total_codes,
                    "success_count": result.success_count,
                    "failed_count": result.failed_count,
                    "db_saved_count": result.db_saved_count,
                    "message": result.message,
                })

                logger.info(
                    f"스케줄 작업 완료",
                    "cron_job",
                    {"job_id": job_id, "result": result.to_dict()}
                )

            except Exception as e:
                self._update_schedule_log(log_id, {
                    "status": "failed",
                    "message": str(e)[:500],
                })
                logger.error(f"스케줄 작업 실패", "cron_job", {"job_id": job_id, "error": str(e)})

            if callback:
                callback(result)

        trigger = parse_cron_expr(cron_expr)

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            max_instances=1,
        )

        self._jobs[job_id] = job

        logger.info(
            f"스케줄 작업 등록",
            "add_cron_job",
            {"job_id": job_id, "cron_expr": cron_expr, "market": market}
        )

    def add_kr_daily_job(
            self,
            hour: int = None,
            minute: int = None,
            market: str = None,
            sector: str = None
    ):
        # 한국 주식 일별 수집 작업
        h = hour if hour is not None else settings.DATA_FETCH_HOUR
        m = minute if minute is not None else settings.DATA_FETCH_MINUTE
        self.add_cron_job(
            job_id="kr_daily",
            cron_expr=f"{m} {h} * * *",
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
        # 미국 주식 일별 수집 작업
        h = hour if hour is not None else 7  # 한국 시간 오전 7시
        m = minute if minute is not None else 0
        self.add_cron_job(
            job_id="us_daily",
            cron_expr=f"{m} {h} * * *",
            market=market or "S&P500",
            sector=sector
        )

    def _add_scheduled_job(self, job_model, steps=None):
        """step 기반 통합 크론 잡 등록"""
        job_name = job_model.job_name
        cron_expr = job_model.cron_expr
        market = job_model.market or "KOSPI"
        sector = job_model.sector or None
        days_back = job_model.days_back or 7

        # step 데이터 직렬화 (ORM 세션 분리)
        steps_data = [
            {"step_type": s.step_type, "step_order": s.step_order,
             "enabled": s.enabled, "config": s.get_config()}
            for s in (steps or [])
        ]

        enabled_types = [s["step_type"] for s in steps_data if s["enabled"]]

        def scheduled_job_func():
            from services.scheduler_service import _execute_pipeline

            logger.info(f"스케줄 잡 시작", "scheduled_job",
                        {"job_id": job_name, "market": market, "steps": enabled_types})

            log_id = self._create_schedule_log(job_name)

            try:
                result = _execute_pipeline(market, sector, days_back, steps_data)

                self._update_schedule_log(log_id, {
                    "status": "success" if result.get("failed", 0) == 0 else "partial",
                    "success_count": result.get("success", 0),
                    "failed_count": result.get("failed", 0),
                    "db_saved_count": result.get("saved", 0),
                    "message": result.get("message", "")[:500],
                })

            except Exception as e:
                self._update_schedule_log(log_id, {
                    "status": "failed",
                    "message": str(e)[:500],
                })
                logger.error(f"스케줄 잡 실패", "scheduled_job",
                             {"job_id": job_name, "error": str(e)})

        trigger = parse_cron_expr(cron_expr)
        job = self.scheduler.add_job(
            scheduled_job_func, trigger=trigger, id=job_name,
            replace_existing=True, max_instances=1,
        )
        self._jobs[job_name] = job
        logger.info(f"스케줄 잡 등록", "_add_scheduled_job",
                    {"job_id": job_name, "market": market, "steps": enabled_types})

    def add_job_from_model(self, job_model):
        # DB ScheduleJob 모델에서 작업 등록
        self.add_cron_job(
            job_id=job_model.job_name,
            cron_expr=job_model.cron_expr,
            market=job_model.market,
            sector=job_model.sector,
            days_back=job_model.days_back,
        )

    def remove_job(self, job_id: str):
        if job_id in self._jobs:
            self.scheduler.remove_job(job_id)
            del self._jobs[job_id]
            logger.info(f"작업 제거: {job_id}", "remove_job")

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("스케줄러 시작됨", "start")

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("스케줄러 중지됨", "stop")

    def get_jobs(self) -> list[dict]:
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run": str(job.next_run_time),
                "trigger": str(job.trigger)
            })
        return jobs

    def run_now(self, job_id: str):
        if job_id in self._jobs:
            job = self._jobs[job_id]
            job.func()
            logger.info(f"작업 즉시 실행: {job_id}", "run_now")
        else:
            logger.warning(f"작업을 찾을 수 없음: {job_id}", "run_now")

    def get_next_runs(self) -> dict[str, str]:
        if not self.scheduler.running:
            return {}
        return {
            job.id: str(job.next_run_time) if job.next_run_time else None
            for job in self.scheduler.get_jobs()
        }

    def sync_job(self, job_name: str, action: str = "add", job_model=None, steps=None):
        if not self.scheduler.running:
            return
        try:
            if action == "remove":
                self.remove_job(job_name)
            elif action == "add" and job_model and job_model.enabled:
                self._add_scheduled_job(job_model, steps=steps)
            elif action == "update":
                self.remove_job(job_name)
                if job_model and job_model.enabled:
                    self._add_scheduled_job(job_model, steps=steps)
        except Exception as e:
            logger.warning(f"스케줄러 동기화 실패 ({action} {job_name}): {e}", "sync_job")
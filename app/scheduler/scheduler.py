"""
잡 스케줄러
==========

크론식 기반 자동 실행
"""

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

from config import settings
from core import get_logger
from db import database

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

    def _add_scheduled_job(self, job_model, steps=None):
        """step 기반 통합 크론 잡 등록 — SchedulerService.run_job() 위임"""
        job_name = job_model.job_name
        job_db_id = job_model.id
        cron_expr = job_model.cron_expr

        enabled_types = [
            s.step_type for s in (steps or []) if s.enabled
        ] if steps else []

        def scheduled_job_func():
            from services.scheduler_service import SchedulerService
            logger.info(f"크론 잡 시작: {job_name}", "scheduled_job")
            SchedulerService().run_job(job_db_id, trigger_by="scheduler")

        trigger = parse_cron_expr(cron_expr)
        job = self.scheduler.add_job(
            scheduled_job_func, trigger=trigger, id=job_name,
            replace_existing=True, max_instances=1,
        )
        self._jobs[job_name] = job
        logger.info(f"스케줄 잡 등록", "_add_scheduled_job",
                    {"job_id": job_name, "steps": enabled_types})

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
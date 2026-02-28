"""
데이터 수집 스케줄러
=================

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

from .pipeline import DataPipeline

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


class DataScheduler:
    """데이터 수집 스케줄러"""

    _instance: "DataScheduler | None" = None

    @classmethod
    def get_instance(cls) -> "DataScheduler":
        # 싱글톤 인스턴스
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def get_running_instance(cls) -> "DataScheduler | None":
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

        logger.info("DataScheduler 초기화 완료", "__init__")

    def load_jobs_from_db(self):
        """DB에서 enabled 스케줄을 읽어 APScheduler에 등록"""
        from models import ScheduleJob, MLTrainConfig

        with database.session() as session:
            jobs = session.query(ScheduleJob).filter(ScheduleJob.enabled == True).all()
            loaded = 0
            for job_model in jobs:
                try:
                    if job_model.job_type == "ml_train":
                        config = session.query(MLTrainConfig).filter(
                            MLTrainConfig.job_id == job_model.id
                        ).first()
                        self.add_ml_train_job(job_model, config)
                    elif job_model.job_type == "fundamental_collect":
                        self.add_fundamental_job(job_model)
                    else:
                        self.add_job_from_model(job_model)
                    loaded += 1
                except Exception as e:
                    logger.warning(f"잡 등록 실패: {job_model.job_name} - {e}", "load_jobs_from_db")

        logger.info(f"DB에서 {loaded}개 잡 로드 완료", "load_jobs_from_db")
        return loaded

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
            from models import ScheduleJob, ScheduleLog

            logger.info(
                f"스케줄 작업 시작",
                "cron_job",
                {"job_id": job_id, "market": market, "sector": sector, "cron": cron_expr}
            )

            # 실행 이력 생성
            log_id = None
            with database.session() as session:
                job_row = session.query(ScheduleJob).filter(ScheduleJob.job_name == job_id).first()
                if job_row:
                    log = ScheduleLog(job_id=job_row.id, started_at=datetime.now(), status="running", trigger_by="scheduler")
                    session.add(log)
                    session.flush()
                    log_id = log.id

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

                # 실행 이력 업데이트 (성공)
                if log_id:
                    with database.session() as session:
                        log_entry = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
                        if log_entry:
                            log_entry.finished_at = datetime.now()
                            log_entry.status = "success" if result.success else "failed"
                            log_entry.total_codes = result.total_codes
                            log_entry.success_count = result.success_count
                            log_entry.failed_count = result.failed_count
                            log_entry.db_saved_count = result.db_saved_count
                            log_entry.message = result.message

                logger.info(
                    f"스케줄 작업 완료",
                    "cron_job",
                    {"job_id": job_id, "result": result.to_dict()}
                )

            except Exception as e:
                # 실행 이력 업데이트 (실패)
                if log_id:
                    with database.session() as session:
                        log_entry = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
                        if log_entry:
                            log_entry.finished_at = datetime.now()
                            log_entry.status = "failed"
                            log_entry.message = str(e)[:500]

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
        """한국 주식 일별 수집 작업"""
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
        """미국 주식 일별 수집 작업"""
        h = hour if hour is not None else 7  # 한국 시간 오전 7시
        m = minute if minute is not None else 0
        self.add_cron_job(
            job_id="us_daily",
            cron_expr=f"{m} {h} * * *",
            market=market or "S&P500",
            sector=sector
        )

    def add_ml_train_job(self, job_model, config):
        """ML 학습 크론 잡 등록

        Args:
            job_model: ScheduleJob 모델
            config: MLTrainConfig 모델 (markets, algorithms, target_days 등)
        """
        from ml.training_scheduler import run_training_schedule

        job_name = job_model.job_name
        cron_expr = job_model.cron_expr

        # config에서 ML 전용 설정 추출 (세션 밖에서 사용하기 위해 값 복사)
        markets = config.get_markets() if config else ["KOSPI", "KOSDAQ"]
        algorithms = config.get_algorithms() if config else None
        target_days = config.get_target_days() if config else None
        include_feature = config.include_feature_compute if config else True
        optuna_trials = config.optuna_trials if config else 50

        def ml_job_func():
            from models import ScheduleJob as SJ, ScheduleLog

            logger.info(f"ML 학습 스케줄 시작", "ml_cron_job",
                        {"job_id": job_name, "markets": markets})

            # 실행 이력 생성
            log_id = None
            with database.session() as session:
                job_row = session.query(SJ).filter(SJ.job_name == job_name).first()
                if job_row:
                    log = ScheduleLog(
                        job_id=job_row.id,
                        started_at=datetime.now(),
                        status="running",
                        trigger_by="scheduler",
                    )
                    session.add(log)
                    session.flush()
                    log_id = log.id

            try:
                result = run_training_schedule(
                    markets=markets,
                    algorithms=algorithms,
                    target_days=target_days,
                    include_feature_compute=include_feature,
                    optuna_trials=optuna_trials,
                )

                if log_id:
                    with database.session() as session:
                        log_entry = session.query(ScheduleLog).filter(
                            ScheduleLog.id == log_id
                        ).first()
                        if log_entry:
                            log_entry.finished_at = datetime.now()
                            log_entry.status = "success" if result["failed"] == 0 else "partial"
                            log_entry.success_count = result["trained"]
                            log_entry.failed_count = result["failed"]
                            log_entry.message = result.get("summary", "")[:500]

            except Exception as e:
                if log_id:
                    with database.session() as session:
                        log_entry = session.query(ScheduleLog).filter(
                            ScheduleLog.id == log_id
                        ).first()
                        if log_entry:
                            log_entry.finished_at = datetime.now()
                            log_entry.status = "failed"
                            log_entry.message = str(e)[:500]
                logger.error(f"ML 학습 스케줄 실패", "ml_cron_job", {"error": str(e)})

        trigger = parse_cron_expr(cron_expr)
        job = self.scheduler.add_job(
            ml_job_func, trigger=trigger, id=job_name,
            replace_existing=True, max_instances=1,
        )
        self._jobs[job_name] = job
        logger.info(f"ML 학습 스케줄 등록", "add_ml_train_job",
                    {"job_id": job_name, "markets": markets})

    def add_fundamental_job(self, job_model):
        """재무 데이터 수집 크론 잡 등록 (Phase 2)

        Args:
            job_model: ScheduleJob 모델 (job_type="fundamental_collect")
        """
        job_name = job_model.job_name
        cron_expr = job_model.cron_expr
        market = job_model.market or "KOSPI"

        def fundamental_job_func():
            from models import ScheduleJob as SJ, ScheduleLog
            from services import FundamentalService

            logger.info(f"재무 데이터 수집 스케줄 시작", "fundamental_cron_job",
                        {"job_id": job_name, "market": market})

            log_id = None
            with database.session() as session:
                job_row = session.query(SJ).filter(SJ.job_name == job_name).first()
                if job_row:
                    log = ScheduleLog(
                        job_id=job_row.id,
                        started_at=datetime.now(),
                        status="running",
                        trigger_by="scheduler",
                    )
                    session.add(log)
                    session.flush()
                    log_id = log.id

            try:
                svc = FundamentalService()
                result = svc.collect_fundamentals(market=market)

                if log_id:
                    with database.session() as session:
                        log_entry = session.query(ScheduleLog).filter(
                            ScheduleLog.id == log_id
                        ).first()
                        if log_entry:
                            log_entry.finished_at = datetime.now()
                            log_entry.status = "success"
                            log_entry.success_count = result.get("success", 0)
                            log_entry.failed_count = result.get("failed", 0)
                            log_entry.db_saved_count = result.get("saved", 0)
                            log_entry.message = result.get("message", "")[:500]

            except Exception as e:
                if log_id:
                    with database.session() as session:
                        log_entry = session.query(ScheduleLog).filter(
                            ScheduleLog.id == log_id
                        ).first()
                        if log_entry:
                            log_entry.finished_at = datetime.now()
                            log_entry.status = "failed"
                            log_entry.message = str(e)[:500]
                logger.error(f"재무 데이터 수집 스케줄 실패", "fundamental_cron_job",
                             {"error": str(e)})

        trigger = parse_cron_expr(cron_expr)
        job = self.scheduler.add_job(
            fundamental_job_func, trigger=trigger, id=job_name,
            replace_existing=True, max_instances=1,
        )
        self._jobs[job_name] = job
        logger.info(f"재무 데이터 수집 스케줄 등록", "add_fundamental_job",
                    {"job_id": job_name, "market": market})

    def add_job_from_model(self, job_model):
        """DB ScheduleJob 모델에서 작업 등록"""
        self.add_cron_job(
            job_id=job_model.job_name,
            cron_expr=job_model.cron_expr,
            market=job_model.market,
            sector=job_model.sector,
            days_back=job_model.days_back,
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

    def get_next_runs(self) -> dict[str, str]:
        """등록된 잡별 다음 실행 시각 조회"""
        if not self.scheduler.running:
            return {}
        return {
            job.id: str(job.next_run_time) if job.next_run_time else None
            for job in self.scheduler.get_jobs()
        }

    def sync_job(self, job_name: str, action: str = "add", job_model=None, ml_config=None):
        """DB 변경사항을 APScheduler에 즉시 반영"""
        if not self.scheduler.running:
            return
        try:
            if action == "remove":
                self.remove_job(job_name)
            elif action == "add" and job_model and job_model.enabled:
                job_type = getattr(job_model, "job_type", "data_collect")
                if job_type == "ml_train":
                    self.add_ml_train_job(job_model, ml_config)
                elif job_type == "fundamental_collect":
                    self.add_fundamental_job(job_model)
                else:
                    self.add_job_from_model(job_model)
            elif action == "update":
                self.remove_job(job_name)
                if job_model and job_model.enabled:
                    job_type = getattr(job_model, "job_type", "data_collect")
                    if job_type == "ml_train":
                        self.add_ml_train_job(job_model, ml_config)
                    elif job_type == "fundamental_collect":
                        self.add_fundamental_job(job_model)
                    else:
                        self.add_job_from_model(job_model)
        except Exception as e:
            logger.warning(f"스케줄러 동기화 실패 ({action} {job_name}): {e}", "sync_job")
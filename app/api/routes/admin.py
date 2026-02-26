"""
어드민 API 엔드포인트 (모니터링 + 스케줄러)
"""

import platform
import re
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import func

from api.schemas import (
    ConfigGroup,
    ConfigResponse,
    DBResponse,
    HealthResponse,
    LogEntry,
    LogResponse,
    ScheduleJobRequest,
    ScheduleJobResponse,
    ScheduleLogResponse,
    TableStats,
)
from config import settings
from core import get_logger
from db import database
from data_collector import DataScheduler, SCHEDULER_AVAILABLE
from models import StockPrice, StockInfo, ScheduleJob, ScheduleLog, MLTrainConfig

logger = get_logger("admin")

router = APIRouter(prefix="/admin", tags=["관리자"])

_START_TIME = time.time()


# ============================================================
# 모니터링 엔드포인트
# ============================================================

@router.get("/health", response_model=HealthResponse)
def health_check():
    """상세 헬스 체크"""
    started_at = datetime.fromtimestamp(_START_TIME)
    return HealthResponse(
        status="ok",
        uptime_seconds=round(time.time() - _START_TIME, 1),
        started_at=started_at.strftime("%Y-%m-%d %H:%M:%S"),
        version="1.0.0",
        python_version=platform.python_version(),
        db_type=settings.DB_TYPE,
    )


@router.get("/db", response_model=DBResponse)
def db_status():
    """DB 상태 + 테이블 통계"""
    try:
        with database.session() as session:
            # stock_price 통계
            sp_count = session.query(func.count(StockPrice.id)).scalar() or 0
            sp_earliest = None
            sp_latest = None
            sp_markets = []
            sp_code_count = 0

            if sp_count > 0:
                sp_earliest_dt = session.query(func.min(StockPrice.date)).scalar()
                sp_latest_dt = session.query(func.max(StockPrice.date)).scalar()
                sp_earliest = sp_earliest_dt.strftime("%Y-%m-%d") if sp_earliest_dt else None
                sp_latest = sp_latest_dt.strftime("%Y-%m-%d") if sp_latest_dt else None
                sp_markets = [
                    r[0] for r in session.query(StockPrice.market).distinct().all()
                ]
                sp_code_count = session.query(
                    func.count(func.distinct(StockPrice.code))
                ).scalar() or 0

            # stock_info 통계
            si_count = session.query(func.count(StockInfo.id)).scalar() or 0
            si_markets = []
            si_sector_count = 0

            if si_count > 0:
                si_markets = [
                    r[0] for r in session.query(StockInfo.market).distinct().all()
                ]
                si_sector_count = session.query(
                    func.count(func.distinct(StockInfo.sector))
                ).scalar() or 0

            return DBResponse(
                connected=True,
                db_type=settings.DB_TYPE,
                tables={
                    "stock_price": TableStats(
                        row_count=sp_count,
                        earliest_date=sp_earliest,
                        latest_date=sp_latest,
                        markets=sp_markets,
                        code_count=sp_code_count,
                    ),
                    "stock_info": TableStats(
                        row_count=si_count,
                        markets=si_markets,
                        sector_count=si_sector_count,
                    ),
                },
            )
    except Exception as e:
        return DBResponse(connected=False, db_type=settings.DB_TYPE, error=str(e))


@router.get("/logs", response_model=LogResponse)
def get_logs(
    file: str = Query(default="app", description="app / error / trade"),
    lines: int = Query(default=100, le=500),
    level: Optional[str] = Query(default=None, description="DEBUG, INFO, WARNING, ERROR, CRITICAL"),
    search: Optional[str] = Query(default=None, description="텍스트 검색"),
):
    """로그 조회"""
    log_map = {
        "app": Path(settings.LOG_DIR) / "app.log",
        "error": Path(settings.LOG_DIR) / "error.log",
        "trade": Path(settings.LOG_DIR) / "trade.log",
    }

    log_path = log_map.get(file)
    if not log_path:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 로그 파일: {file}")

    if not log_path.exists():
        return LogResponse(file=file, total=0, entries=[])

    # 파일 끝에서 lines만큼 읽기
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
                time=m.group(1),
                level=m.group(2),
                module=m.group(3),
                function=m.group(4),
                message=m.group(5),
            )
        else:
            entry = LogEntry(
                time="", level="", module="", function="", message=raw
            )

        # 레벨 필터
        if level and entry.level and entry.level.upper() != level.upper():
            continue

        # 텍스트 검색
        if search and search.lower() not in raw.lower():
            continue

        entries.append(entry)

    # 최신순
    entries.reverse()

    return LogResponse(file=file, total=len(entries), entries=entries)


@router.get("/config", response_model=ConfigResponse)
def get_config():
    """설정 확인 (민감정보 마스킹)"""
    masked_keys = {
        "DB_PASSWORD", "SLACK_TOKEN", "SLACK_WEBHOOK_URL",
        "KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO",
        "ALPACA_API_KEY", "ALPACA_SECRET_KEY",
        "OPENAI_API_KEY",
    }

    groups_def = {
        "app": ["APP_ENV", "DEV_MODE", "DEBUG"],
        "logging": ["LOG_LEVEL", "LOG_DIR", "LOG_RETENTION_DAYS", "LOG_ROTATION_SIZE"],
        "database": ["DB_TYPE", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "SQLITE_PATH"],
        "scheduler": ["SCHEDULER_TIMEZONE", "DATA_FETCH_HOUR", "DATA_FETCH_MINUTE"],
        "slack": ["SLACK_ENABLED", "SLACK_TOKEN", "SLACK_CHANNEL", "SLACK_WEBHOOK_URL"],
        "kis": ["KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_MOCK_MODE"],
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


# ============================================================
# 스케줄러 엔드포인트
# ============================================================

@router.get("/scheduler/jobs", response_model=list[ScheduleJobResponse])
def list_schedule_jobs():
    """등록된 스케줄 목록"""
    with database.session() as session:
        jobs = session.query(ScheduleJob).order_by(ScheduleJob.id).all()
        scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
        next_runs = scheduler.get_next_runs() if scheduler else {}

        # ML 학습 잡의 설정 조회
        ml_job_ids = [j.id for j in jobs if getattr(j, "job_type", "") == "ml_train"]
        ml_configs = {}
        if ml_job_ids:
            configs = session.query(MLTrainConfig).filter(
                MLTrainConfig.job_id.in_(ml_job_ids)
            ).all()
            ml_configs = {c.job_id: c for c in configs}

        return [
            ScheduleJobResponse.from_model(
                j,
                next_runs.get(j.job_name),
                ml_config=ml_configs.get(j.id),
            )
            for j in jobs
        ]


@router.post("/scheduler/jobs", response_model=ScheduleJobResponse)
def create_schedule_job(req: ScheduleJobRequest):
    """스케줄 추가 (데이터 수집 / ML 학습 통합)"""
    with database.session() as session:
        exists = session.query(ScheduleJob).filter(
            ScheduleJob.job_name == req.job_name
        ).first()
        if exists:
            raise HTTPException(status_code=409, detail=f"이미 존재하는 job_name: {req.job_name}")

        job = ScheduleJob(
            job_name=req.job_name,
            job_type=req.job_type,
            market=req.market,
            sector=req.sector,
            cron_expr=req.cron_expr,
            days_back=req.days_back,
            enabled=req.enabled,
            description=req.description,
        )
        session.add(job)
        session.flush()

        # ML 학습인 경우 전용 설정 테이블에 저장
        ml_config = None
        if req.job_type == "ml_train":
            import json
            ml_config = MLTrainConfig(
                job_id=job.id,
                markets=json.dumps(req.ml_markets),
                algorithms=json.dumps(req.ml_algorithms),
                target_days=json.dumps(req.ml_target_days),
                include_feature_compute=req.ml_include_feature_compute,
                optuna_trials=req.ml_optuna_trials,
            )
            session.add(ml_config)
            session.flush()

        scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
        if scheduler:
            scheduler.sync_job(job.job_name, "add", job, ml_config=ml_config)
        return ScheduleJobResponse.from_model(job)


@router.put("/scheduler/jobs/{job_id}", response_model=ScheduleJobResponse)
def update_schedule_job(job_id: int, req: ScheduleJobRequest):
    """스케줄 수정"""
    with database.session() as session:
        job = session.query(ScheduleJob).filter(ScheduleJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")

        old_job_name = job.job_name

        # job_name 변경 시 중복 체크
        if req.job_name != job.job_name:
            dup = session.query(ScheduleJob).filter(
                ScheduleJob.job_name == req.job_name,
                ScheduleJob.id != job_id,
            ).first()
            if dup:
                raise HTTPException(status_code=409, detail=f"이미 존재하는 job_name: {req.job_name}")

        job.job_name = req.job_name
        job.job_type = req.job_type
        job.market = req.market
        job.sector = req.sector
        job.cron_expr = req.cron_expr
        job.days_back = req.days_back
        job.enabled = req.enabled
        job.description = req.description
        session.flush()

        # ML 학습 설정 업데이트
        ml_config = None
        if req.job_type == "ml_train":
            import json
            ml_config = session.query(MLTrainConfig).filter(
                MLTrainConfig.job_id == job.id
            ).first()
            if ml_config:
                ml_config.markets = json.dumps(req.ml_markets)
                ml_config.algorithms = json.dumps(req.ml_algorithms)
                ml_config.target_days = json.dumps(req.ml_target_days)
                ml_config.include_feature_compute = req.ml_include_feature_compute
                ml_config.optuna_trials = req.ml_optuna_trials
            else:
                ml_config = MLTrainConfig(
                    job_id=job.id,
                    markets=json.dumps(req.ml_markets),
                    algorithms=json.dumps(req.ml_algorithms),
                    target_days=json.dumps(req.ml_target_days),
                    include_feature_compute=req.ml_include_feature_compute,
                    optuna_trials=req.ml_optuna_trials,
                )
                session.add(ml_config)
            session.flush()

        # 이전 잡 제거 → 새 설정으로 등록
        scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
        if scheduler:
            scheduler.sync_job(old_job_name, "remove")
            scheduler.sync_job(job.job_name, "add", job, ml_config=ml_config)
        return ScheduleJobResponse.from_model(job)


@router.delete("/scheduler/jobs/{job_id}")
def delete_schedule_job(job_id: int):
    """스케줄 삭제"""
    with database.session() as session:
        job = session.query(ScheduleJob).filter(ScheduleJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")
        job_name = job.job_name
        session.delete(job)
    scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
    if scheduler:
        scheduler.sync_job(job_name, "remove")
    return {"deleted": True, "id": job_id, "job_name": job_name}


@router.post("/scheduler/jobs/{job_id}/run")
def run_schedule_job(job_id: int):
    """스케줄 즉시 실행 (백그라운드) - 데이터 수집 / ML 학습 통합"""
    with database.session() as session:
        job = session.query(ScheduleJob).filter(ScheduleJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")

        # 공통 값 추출
        job_type = job.job_type
        job_days_back = job.days_back
        job_market = job.market
        job_sector = job.sector

        # ML 전용 설정 조회
        ml_markets = None
        ml_algorithms = None
        ml_target_days = None
        ml_include_feature = True
        ml_optuna_trials = 50
        if job_type == "ml_train":
            config = session.query(MLTrainConfig).filter(
                MLTrainConfig.job_id == job.id
            ).first()
            if config:
                ml_markets = config.get_markets()
                ml_algorithms = config.get_algorithms()
                ml_target_days = config.get_target_days()
                ml_include_feature = config.include_feature_compute
                ml_optuna_trials = config.optuna_trials

        # 실행 이력 생성
        log = ScheduleLog(
            job_id=job.id,
            started_at=datetime.now(),
            status="running",
        )
        session.add(log)
        session.flush()
        log_id = log.id

    if job_type == "ml_train":
        from ml.training_scheduler import run_training_schedule

        def _run_ml():
            try:
                result = run_training_schedule(
                    markets=ml_markets or ["KOSPI", "KOSDAQ"],
                    algorithms=ml_algorithms,
                    target_days=ml_target_days,
                    include_feature_compute=ml_include_feature,
                    optuna_trials=ml_optuna_trials,
                )
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
                with database.session() as session:
                    log_entry = session.query(ScheduleLog).filter(
                        ScheduleLog.id == log_id
                    ).first()
                    if log_entry:
                        log_entry.finished_at = datetime.now()
                        log_entry.status = "failed"
                        log_entry.message = str(e)[:500]

        thread = threading.Thread(target=_run_ml, daemon=True)
        thread.start()
    else:
        from services import StockService
        svc = StockService()
        thread = threading.Thread(
            target=svc.run_schedule_job,
            args=(log_id, job_market, job_sector, job_days_back),
            daemon=True,
        )
        thread.start()

    return {
        "success": True,
        "log_id": log_id,
        "message": f"실행 시작 (log_id={log_id})",
    }


@router.get("/scheduler/logs", response_model=list[ScheduleLogResponse])
def list_schedule_logs(
    job_id: Optional[int] = Query(default=None, description="스케줄 ID 필터"),
    limit: int = Query(default=20, le=100),
):
    """실행 이력 조회"""
    with database.session() as session:
        query = session.query(ScheduleLog, ScheduleJob.job_name).outerjoin(
            ScheduleJob, ScheduleLog.job_id == ScheduleJob.id
        )

        if job_id:
            query = query.filter(ScheduleLog.job_id == job_id)

        query = query.order_by(ScheduleLog.started_at.desc()).limit(limit)
        rows = query.all()

        return [
            ScheduleLogResponse(
                id=log.id,
                job_id=log.job_id,
                job_name=job_name,
                started_at=log.started_at.strftime("%Y-%m-%d %H:%M:%S") if log.started_at else "",
                finished_at=log.finished_at.strftime("%Y-%m-%d %H:%M:%S") if log.finished_at else None,
                status=log.status,
                total_codes=log.total_codes or 0,
                success_count=log.success_count or 0,
                failed_count=log.failed_count or 0,
                db_saved_count=log.db_saved_count or 0,
                trigger_by=log.trigger_by or "manual",
                message=log.message,
            )
            for log, job_name in rows
        ]
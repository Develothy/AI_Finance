"""
어드민 API 엔드포인트 (모니터링 + 스케줄러)
"""

import platform
import re
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
from models import StockPrice, StockInfo, ScheduleJob, ScheduleLog

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

def _get_scheduler_next_runs() -> dict[str, str]:
    """APScheduler에서 next_run_time 조회"""
    try:
        from data_collector import DataScheduler, SCHEDULER_AVAILABLE
        if not SCHEDULER_AVAILABLE:
            return {}
        scheduler = DataScheduler.get_instance()
        if not scheduler.scheduler.running:
            return {}
        runs = {}
        for job in scheduler.scheduler.get_jobs():
            runs[job.id] = str(job.next_run_time) if job.next_run_time else None
        return runs
    except Exception:
        return {}


@router.get("/scheduler/jobs", response_model=list[ScheduleJobResponse])
def list_schedule_jobs():
    """등록된 스케줄 목록"""
    with database.session() as session:
        jobs = session.query(ScheduleJob).order_by(ScheduleJob.id).all()
        next_runs = _get_scheduler_next_runs()
        return [
            ScheduleJobResponse.from_model(j, next_runs.get(j.job_name))
            for j in jobs
        ]


def _sync_scheduler(job_name: str, action: str = "add", job_model=None):
    """APScheduler에 즉시 반영"""
    try:
        from data_collector import DataScheduler, SCHEDULER_AVAILABLE
        if not SCHEDULER_AVAILABLE:
            return
        scheduler = DataScheduler.get_instance()
        if not scheduler.scheduler.running:
            return

        if action == "remove":
            scheduler.remove_job(job_name)
        elif action == "add" and job_model and job_model.enabled:
            scheduler.add_job_from_model(job_model)
        elif action == "update":
            scheduler.remove_job(job_name)
            if job_model and job_model.enabled:
                scheduler.add_job_from_model(job_model)
    except Exception as e:
        logger.warning(f"스케줄러 동기화 실패 ({action} {job_name}): {e}")


@router.post("/scheduler/jobs", response_model=ScheduleJobResponse)
def create_schedule_job(req: ScheduleJobRequest):
    """스케줄 추가"""
    with database.session() as session:
        exists = session.query(ScheduleJob).filter(
            ScheduleJob.job_name == req.job_name
        ).first()
        if exists:
            raise HTTPException(status_code=409, detail=f"이미 존재하는 job_name: {req.job_name}")

        job = ScheduleJob(
            job_name=req.job_name,
            market=req.market,
            sector=req.sector,
            cron_expr=req.cron_expr,
            days_back=req.days_back,
            enabled=req.enabled,
            description=req.description,
        )
        session.add(job)
        session.flush()
        _sync_scheduler(job.job_name, "add", job)
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
        job.market = req.market
        job.sector = req.sector
        job.cron_expr = req.cron_expr
        job.days_back = req.days_back
        job.enabled = req.enabled
        job.description = req.description
        session.flush()

        # 이전 잡 제거 → 새 설정으로 등록
        _sync_scheduler(old_job_name, "remove")
        _sync_scheduler(job.job_name, "add", job)
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
    _sync_scheduler(job_name, "remove")
    return {"deleted": True, "id": job_id, "job_name": job_name}


@router.post("/scheduler/jobs/{job_id}/run")
def run_schedule_job(job_id: int):
    """스케줄 즉시 실행"""
    with database.session() as session:
        job = session.query(ScheduleJob).filter(ScheduleJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")

        # 실행 이력 생성
        log = ScheduleLog(
            job_id=job.id,
            started_at=datetime.now(),
            status="running",
        )
        session.add(log)
        session.flush()
        log_id = log.id

    # 세션 밖에서 실행 (장시간 소요 가능)
    try:
        from datetime import timedelta
        from data_collector import DataPipeline
        from services import StockService

        pipeline = DataPipeline()
        svc = StockService(pipeline=pipeline)

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=job.days_back)).strftime("%Y-%m-%d")

        from api.schemas import CollectRequest
        req = CollectRequest(
            market=job.market,
            sector=job.sector,
            start_date=start_date,
            end_date=end_date,
        )
        result = svc.collect(req)

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

        return {
            "success": result.success,
            "log_id": log_id,
            "message": result.message,
        }

    except Exception as e:
        with database.session() as session:
            log_entry = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
            if log_entry:
                log_entry.finished_at = datetime.now()
                log_entry.status = "failed"
                log_entry.message = str(e)[:500]

        raise HTTPException(status_code=500, detail=f"실행 실패: {e}")


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
"""
어드민 API 엔드포인트 (모니터링 + 스케줄러)
"""

import platform
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Body, HTTPException, Query

from api.schemas import (
    ConfigResponse,
    DBResponse,
    HealthResponse,
    LogResponse,
    RunJobRequest,
    RunStepRequest,
    RunFromStepRequest,
    ScheduleJobRequest,
    ScheduleJobResponse,
    ScheduleLogResponse,
    PipelineStepLogResponse,
)
from config import settings
from services import admin_service, scheduler_service

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
    return admin_service.get_db_status()


@router.get("/logs", response_model=LogResponse)
def get_logs(
    file: str = Query(default="app", description="app / error / trade"),
    lines: int = Query(default=100, le=500),
    level: Optional[str] = Query(default=None, description="DEBUG, INFO, WARNING, ERROR, CRITICAL"),
    search: Optional[str] = Query(default=None, description="텍스트 검색"),
):
    """로그 조회"""
    log_path = {"app", "error", "trade"}
    if file not in log_path:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 로그 파일: {file}")
    return admin_service.get_logs(file=file, lines=lines, level=level, search=search)


@router.get("/config", response_model=ConfigResponse)
def get_config():
    """설정 확인 (민감정보 마스킹)"""
    return admin_service.get_config()


# ============================================================
# 스케줄러 엔드포인트
# ============================================================

@router.get("/scheduler/jobs", response_model=list[ScheduleJobResponse])
def list_schedule_jobs():
    """등록된 스케줄 목록"""
    return scheduler_service.list_jobs()


@router.post("/scheduler/jobs", response_model=ScheduleJobResponse)
def create_schedule_job(req: ScheduleJobRequest):
    """스케줄 추가"""
    return scheduler_service.create_job(req)


@router.put("/scheduler/jobs/{job_id}", response_model=ScheduleJobResponse)
def update_schedule_job(job_id: int, req: ScheduleJobRequest):
    """스케줄 수정"""
    return scheduler_service.update_job(job_id, req)


@router.delete("/scheduler/jobs/{job_id}")
def delete_schedule_job(job_id: int):
    """스케줄 삭제"""
    return scheduler_service.delete_job(job_id)


@router.post("/scheduler/jobs/{job_id}/run")
def run_schedule_job(job_id: int, req: Optional[RunJobRequest] = Body(default=None)):
    """스케줄 즉시 실행 (백그라운드)"""
    base_date = req.base_date if req else None
    return scheduler_service.run_job(job_id, base_date=base_date)


@router.post("/scheduler/jobs/{job_id}/run-step")
def run_single_step(job_id: int, req: RunStepRequest):
    """단일 스텝만 실행"""
    return scheduler_service.run_job(job_id, only_step=req.step_type)


@router.post("/scheduler/jobs/{job_id}/run-from")
def run_from_step(job_id: int, req: RunFromStepRequest):
    """지정 스텝부터 이후 전체 실행"""
    return scheduler_service.run_job(job_id, from_step=req.from_step)


@router.get("/scheduler/logs", response_model=list[ScheduleLogResponse])
def list_schedule_logs(
    job_id: Optional[int] = Query(default=None, description="스케줄 ID 필터"),
    limit: int = Query(default=20, le=100),
):
    """실행 이력 조회"""
    return scheduler_service.list_logs(job_id, limit)


@router.get("/scheduler/logs/{log_id}/steps")
def get_step_logs(log_id: int):
    """특정 실행의 스텝별 로그 조회"""
    return scheduler_service.get_step_logs(log_id)


@router.get("/scheduler/logs/{log_id}/steps/{step_type}/log")
def get_step_log_text(log_id: int, step_type: str):
    """특정 스텝의 로그 텍스트 조회"""
    step_logs = scheduler_service.get_step_logs(log_id)
    for sl in step_logs:
        if sl["step_type"] == step_type:
            return {"step_type": step_type, "log_text": sl.get("log_text", "")}
    raise HTTPException(status_code=404, detail=f"스텝 로그 없음: {step_type}")

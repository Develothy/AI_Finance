"""
스케줄러 모델
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from db import ModelBase


class ScheduleJob(ModelBase):
    """스케줄 설정"""

    __tablename__ = "schedule_job"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(50), unique=True, nullable=False)
    market = Column(String(10), nullable=False)
    sector = Column(String(50), nullable=True)
    cron_expr = Column(String(100), nullable=False)
    days_back = Column(Integer, nullable=False, default=7)
    enabled = Column(Boolean, nullable=False, default=True)
    description = Column(String(200), nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("job_name", name="uq_schedule_job_name"),
    )

    def __repr__(self):
        return f"<ScheduleJob({self.job_name} {self.market} [{self.cron_expr}])>"


class JobStep(ModelBase):
    """스케줄 잡의 파이프라인 단계 (정규화)"""

    __tablename__ = "job_step"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(
        Integer,
        ForeignKey("schedule_job.id", ondelete="CASCADE"),
        nullable=False,
    )
    step_type = Column(String(30), nullable=False)
    step_order = Column(Integer, nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)
    config = Column(String(2000), nullable=True)  # JSON

    __table_args__ = (
        UniqueConstraint("job_id", "step_type", name="uq_job_step_type"),
    )

    def get_config(self) -> dict | None:
        import json
        return json.loads(self.config) if self.config else None

    def __repr__(self):
        return f"<JobStep(job_id={self.job_id} {self.step_type} order={self.step_order})>"


class ScheduleLog(ModelBase):
    """스케줄 실행 이력"""

    __tablename__ = "schedule_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("schedule_job.id", ondelete="CASCADE"), nullable=False)
    trace_id = Column(String(36), nullable=True)
    status = Column(String(20), nullable=False, default="running")
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    finished_at = Column(DateTime, nullable=True)
    total_codes = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    db_saved_count = Column(Integer, default=0)
    trigger_by = Column(String(20), nullable=False, default="manual")
    message = Column(String(500), nullable=True)

    def __repr__(self):
        return f"<ScheduleLog(job_id={self.job_id} {self.status} {self.started_at})>"


class PipelineStepLog(ModelBase):
    """파이프라인 스텝별 실행 이력"""

    __tablename__ = "pipeline_step_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    log_id = Column(Integer, ForeignKey("schedule_log.id", ondelete="CASCADE"), nullable=False)
    trace_id = Column(String(36), nullable=False)
    step_type = Column(String(30), nullable=False)
    step_order = Column(Integer, nullable=False)
    status = Column(String(20), default="pending")  # pending/running/success/failed/skipped
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    duration_sec = Column(Integer, nullable=True)
    saved_count = Column(Integer, default=0)
    summary = Column(String(500), nullable=True)
    error_message = Column(String(2000), nullable=True)
    log_text = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("log_id", "step_type", name="uq_step_log_type"),
    )

    def __repr__(self):
        return f"<PipelineStepLog(log_id={self.log_id} {self.step_type} {self.status})>"

"""
스케줄러 모델
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)

from db import ModelBase


class ScheduleJob(ModelBase):
    """스케줄 설정"""

    __tablename__ = "schedule_job"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(50), unique=True, nullable=False)  # APScheduler 등록용
    market = Column(String(10), nullable=False)
    sector = Column(String(50), nullable=True)
    cron_expr = Column(String(100), nullable=False)  # 크론식: "0 18 * * *", "*/10 * * * *" 등
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


class ScheduleLog(ModelBase):
    """스케줄 실행 이력"""

    __tablename__ = "schedule_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("schedule_job.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(20), nullable=False, default="running")  # running / success / failed
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    finished_at = Column(DateTime, nullable=True)
    total_codes = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    db_saved_count = Column(Integer, default=0)
    trigger_by = Column(String(20), nullable=False, default="manual")  # manual / scheduler
    message = Column(String(500), nullable=True)

    def __repr__(self):
        return f"<ScheduleLog(job_id={self.job_id} {self.status} {self.started_at})>"
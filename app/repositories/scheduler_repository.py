"""
스케줄러 레포지토리
===================

ScheduleJob / ScheduleLog / JobStep CRUD
"""

from collections import defaultdict

from sqlalchemy.orm import Session

from models import ScheduleJob, ScheduleLog, JobStep, JobTargetCode, PipelineStepLog


class SchedulerRepository:
    def __init__(self, session: Session):
        self.session = session

    # ── ScheduleJob ──

    def get_all_jobs(self) -> list[ScheduleJob]:
        return self.session.query(ScheduleJob).order_by(ScheduleJob.id).all()

    def get_job(self, job_id: int) -> ScheduleJob | None:
        return self.session.query(ScheduleJob).filter(ScheduleJob.id == job_id).first()

    def get_job_by_name(self, name: str) -> ScheduleJob | None:
        return self.session.query(ScheduleJob).filter(ScheduleJob.job_name == name).first()

    def find_duplicate_name(self, name: str, exclude_id: int | None = None) -> ScheduleJob | None:
        q = self.session.query(ScheduleJob).filter(ScheduleJob.job_name == name)
        if exclude_id is not None:
            q = q.filter(ScheduleJob.id != exclude_id)
        return q.first()

    def create_job(self, data: dict) -> ScheduleJob:
        job = ScheduleJob(**data)
        self.session.add(job)
        self.session.flush()
        return job

    def update_job(self, job: ScheduleJob, data: dict) -> ScheduleJob:
        for key, val in data.items():
            setattr(job, key, val)
        self.session.flush()
        return job

    def delete_job(self, job: ScheduleJob):
        self.session.delete(job)

    # ── ScheduleLog ──

    def create_log(self, data: dict) -> ScheduleLog:
        log = ScheduleLog(**data)
        self.session.add(log)
        self.session.flush()
        return log

    def get_log(self, log_id: int) -> ScheduleLog | None:
        return self.session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()

    def update_log(self, log: ScheduleLog, data: dict):
        for key, val in data.items():
            setattr(log, key, val)
        self.session.flush()

    def get_stale_running_logs(self) -> list[ScheduleLog]:
        # status='running'인 고아 로그 전체 반환
        return self.session.query(ScheduleLog).filter(
            ScheduleLog.status == "running"
        ).all()

    def get_logs(self, job_id: int | None = None, limit: int = 20) -> list[tuple[ScheduleLog, str | None]]:
        query = self.session.query(ScheduleLog, ScheduleJob.job_name).outerjoin(
            ScheduleJob, ScheduleLog.job_id == ScheduleJob.id
        )
        if job_id:
            query = query.filter(ScheduleLog.job_id == job_id)
        return query.order_by(ScheduleLog.started_at.desc()).limit(limit).all()

    # ── JobStep ──

    def get_steps_for_job(self, job_id: int) -> list[JobStep]:
        return (self.session.query(JobStep)
                .filter(JobStep.job_id == job_id)
                .order_by(JobStep.step_order)
                .all())

    def get_steps_for_jobs(self, job_ids: list[int]) -> dict[int, list[JobStep]]:
        if not job_ids:
            return {}
        rows = (self.session.query(JobStep)
                .filter(JobStep.job_id.in_(job_ids))
                .order_by(JobStep.step_order)
                .all())
        result = defaultdict(list)
        for step in rows:
            result[step.job_id].append(step)
        return dict(result)

    def replace_steps(self, job_id: int, steps_data: list[dict]) -> list[JobStep]:
        self.session.query(JobStep).filter(JobStep.job_id == job_id).delete()
        steps = []
        for sd in steps_data:
            sd["job_id"] = job_id
            step = JobStep(**sd)
            self.session.add(step)
            steps.append(step)
        self.session.flush()
        return steps

    # ── JobTargetCode ──

    def get_target_codes_for_job(self, job_id: int) -> list[JobTargetCode]:
        return (self.session.query(JobTargetCode)
                .filter(JobTargetCode.job_id == job_id)
                .order_by(JobTargetCode.id)
                .all())

    def get_target_codes_for_jobs(self, job_ids: list[int]) -> dict[int, list[JobTargetCode]]:
        if not job_ids:
            return {}
        rows = (self.session.query(JobTargetCode)
                .filter(JobTargetCode.job_id.in_(job_ids))
                .order_by(JobTargetCode.id)
                .all())
        result = defaultdict(list)
        for tc in rows:
            result[tc.job_id].append(tc)
        return dict(result)

    def replace_target_codes(self, job_id: int, codes_data: list[dict]) -> list[JobTargetCode]:
        self.session.query(JobTargetCode).filter(JobTargetCode.job_id == job_id).delete()
        items = []
        for cd in codes_data:
            cd["job_id"] = job_id
            tc = JobTargetCode(**cd)
            self.session.add(tc)
            items.append(tc)
        self.session.flush()
        return items

    # ── PipelineStepLog ──

    def create_step_log(self, data: dict) -> PipelineStepLog:
        step_log = PipelineStepLog(**data)
        self.session.add(step_log)
        self.session.flush()
        return step_log

    def get_step_logs_for_log(self, log_id: int) -> list[PipelineStepLog]:
        return (self.session.query(PipelineStepLog)
                .filter(PipelineStepLog.log_id == log_id)
                .order_by(PipelineStepLog.step_order)
                .all())

    def update_step_log(self, step_log: PipelineStepLog, data: dict):
        for key, val in data.items():
            setattr(step_log, key, val)
        self.session.flush()

    def get_step_log_by_log_and_type(self, log_id: int, step_type: str) -> PipelineStepLog | None:
        return (self.session.query(PipelineStepLog)
                .filter(PipelineStepLog.log_id == log_id, PipelineStepLog.step_type == step_type)
                .first())

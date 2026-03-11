"""
Boolean 플래그 → JobStep 마이그레이션
=====================================

기존 schedule_job의 include_* boolean 컬럼 → job_step 행으로 변환.
MLTrainConfig 데이터 → ml step의 config JSON으로 복사.

Usage:
    cd /Users/rothy/backend/AI_Finance
    .venv/bin/python scripts/migrate_boolean_to_steps.py
"""

import json
import sys

sys.path.insert(0, "app")

from db import database
from models import ScheduleJob, MLTrainConfig, JobStep

BOOLEAN_TO_STEP = [
    ("include_price", "price", 1),
    ("include_fundamental", "fundamental", 2),
    ("include_market_investor", "market_investor", 3),
    ("include_macro", "macro", 4),
    ("include_news", "news", 5),
    ("include_disclosure", "disclosure", 6),
    ("include_supply", "supply", 7),
    ("include_alternative", "alternative", 8),
    ("include_feature", "feature", 9),
    ("include_ml", "ml", 10),
]

# include_ml 이외는 기본 True
DEFAULTS = {"include_ml": False}


def migrate():
    database.create_tables()

    with database.session() as session:
        jobs = session.query(ScheduleJob).all()
        print(f"Found {len(jobs)} jobs to migrate")

        for job in jobs:
            existing_steps = {
                s.step_type
                for s in session.query(JobStep).filter(JobStep.job_id == job.id).all()
            }

            # MLTrainConfig 존재 확인 (include_ml 와 무관하게)
            ml_config = session.query(MLTrainConfig).filter(
                MLTrainConfig.job_id == job.id
            ).first()

            created = 0
            for bool_col, step_type, order in BOOLEAN_TO_STEP:
                if step_type in existing_steps:
                    continue

                default = DEFAULTS.get(bool_col, True)
                flag_val = getattr(job, bool_col, default)

                # MLTrainConfig가 있으면 ml step 강제 생성
                if step_type == "ml" and ml_config and not flag_val:
                    flag_val = True

                if not flag_val:
                    continue

                config_json = None
                if step_type == "ml" and ml_config:
                    config_json = json.dumps({
                        "markets": ml_config.get_markets(),
                        "algorithms": ml_config.get_algorithms(),
                        "target_days": ml_config.get_target_days(),
                        "optuna_trials": ml_config.optuna_trials,
                    })

                step = JobStep(
                    job_id=job.id,
                    step_type=step_type,
                    step_order=order,
                    enabled=True,
                    config=config_json,
                )
                session.add(step)
                created += 1

            if created:
                print(f"  OK {job.job_name}: created {created} steps")
            else:
                print(f"  SKIP {job.job_name}: already fully migrated ({len(existing_steps)} steps)")

    print("Migration complete.")


if __name__ == "__main__":
    migrate()

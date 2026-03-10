"""
스케줄러 서비스
================

스케줄 CRUD + 잡 실행 (백그라운드 스레드)
"""

import json
import threading
from datetime import datetime, timedelta
from typing import Optional

from api.schemas import ScheduleJobRequest, ScheduleJobResponse, ScheduleLogResponse
from core import get_logger
from db import database
from data_collector import DataScheduler, SCHEDULER_AVAILABLE
from repositories.scheduler_repository import SchedulerRepository

logger = get_logger("scheduler_service")


class SchedulerService:

    # ── CRUD ──

    def list_jobs(self) -> list[ScheduleJobResponse]:
        with database.session() as session:
            repo = SchedulerRepository(session)
            jobs = repo.get_all_jobs()
            scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
            next_runs = scheduler.get_next_runs() if scheduler else {}

            ml_job_ids = [j.id for j in jobs if getattr(j, "job_type", "") == "ml_train"]
            ml_configs = repo.get_ml_configs_by_ids(ml_job_ids)

            return [
                ScheduleJobResponse.from_model(j, next_runs.get(j.job_name), ml_config=ml_configs.get(j.id))
                for j in jobs
            ]

    def create_job(self, req: ScheduleJobRequest) -> ScheduleJobResponse:
        with database.session() as session:
            repo = SchedulerRepository(session)

            if repo.find_duplicate_name(req.job_name):
                from fastapi import HTTPException
                raise HTTPException(status_code=409, detail=f"이미 존재하는 job_name: {req.job_name}")

            job = repo.create_job({
                "job_name": req.job_name,
                "job_type": req.job_type,
                "market": req.market,
                "sector": req.sector,
                "cron_expr": req.cron_expr,
                "days_back": req.days_back,
                "enabled": req.enabled,
                "description": req.description,
            })

            ml_config = None
            if req.job_type == "ml_train":
                ml_config = repo.create_ml_config({
                    "job_id": job.id,
                    "markets": json.dumps(req.ml_markets),
                    "algorithms": json.dumps(req.ml_algorithms),
                    "target_days": json.dumps(req.ml_target_days),
                    "include_price_collect": req.ml_include_price_collect,
                    "include_kis_collect": req.ml_include_kis_collect,
                    "include_dart_collect": req.ml_include_dart_collect,
                    "include_feature_compute": req.ml_include_feature_compute,
                    "optuna_trials": req.ml_optuna_trials,
                })

            scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
            if scheduler:
                scheduler.sync_job(job.job_name, "add", job, ml_config=ml_config)
            return ScheduleJobResponse.from_model(job)

    def update_job(self, job_id: int, req: ScheduleJobRequest) -> ScheduleJobResponse:
        with database.session() as session:
            repo = SchedulerRepository(session)
            job = repo.get_job(job_id)
            if not job:
                from fastapi import HTTPException
                raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")

            old_job_name = job.job_name

            if req.job_name != job.job_name:
                if repo.find_duplicate_name(req.job_name, exclude_id=job_id):
                    from fastapi import HTTPException
                    raise HTTPException(status_code=409, detail=f"이미 존재하는 job_name: {req.job_name}")

            repo.update_job(job, {
                "job_name": req.job_name,
                "job_type": req.job_type,
                "market": req.market,
                "sector": req.sector,
                "cron_expr": req.cron_expr,
                "days_back": req.days_back,
                "enabled": req.enabled,
                "description": req.description,
            })

            ml_config = None
            if req.job_type == "ml_train":
                ml_config = repo.get_ml_config(job.id)
                ml_data = {
                    "markets": json.dumps(req.ml_markets),
                    "algorithms": json.dumps(req.ml_algorithms),
                    "target_days": json.dumps(req.ml_target_days),
                    "include_price_collect": req.ml_include_price_collect,
                    "include_kis_collect": req.ml_include_kis_collect,
                    "include_dart_collect": req.ml_include_dart_collect,
                    "include_feature_compute": req.ml_include_feature_compute,
                    "optuna_trials": req.ml_optuna_trials,
                }
                if ml_config:
                    repo.update_ml_config(ml_config, ml_data)
                else:
                    ml_data["job_id"] = job.id
                    ml_config = repo.create_ml_config(ml_data)

            scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
            if scheduler:
                scheduler.sync_job(old_job_name, "remove")
                scheduler.sync_job(job.job_name, "add", job, ml_config=ml_config)
            return ScheduleJobResponse.from_model(job)

    def delete_job(self, job_id: int) -> dict:
        with database.session() as session:
            repo = SchedulerRepository(session)
            job = repo.get_job(job_id)
            if not job:
                from fastapi import HTTPException
                raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")
            job_name = job.job_name
            repo.delete_job(job)

        scheduler = DataScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
        if scheduler:
            scheduler.sync_job(job_name, "remove")
        return {"deleted": True, "id": job_id, "job_name": job_name}

    # ── 실행 ──

    def run_job(self, job_id: int, base_date: Optional[str] = None) -> dict:
        """잡 즉시 실행 (백그라운드 스레드)"""
        with database.session() as session:
            repo = SchedulerRepository(session)
            job = repo.get_job(job_id)
            if not job:
                from fastapi import HTTPException
                raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")

            job_type = job.job_type
            job_days_back = job.days_back
            job_market = job.market
            job_sector = job.sector

            # ML 전용 설정
            ml_markets = None
            ml_algorithms = None
            ml_target_days = None
            ml_include_price_collect = False
            ml_include_kis_collect = False
            ml_include_dart_collect = False
            ml_include_feature = True
            ml_optuna_trials = 50
            if job_type == "ml_train":
                config = repo.get_ml_config(job.id)
                if config:
                    ml_markets = config.get_markets()
                    ml_algorithms = config.get_algorithms()
                    ml_target_days = config.get_target_days()
                    ml_include_price_collect = config.include_price_collect
                    ml_include_kis_collect = config.include_kis_collect
                    ml_include_dart_collect = config.include_dart_collect
                    ml_include_feature = config.include_feature_compute
                    ml_optuna_trials = config.optuna_trials

            log = repo.create_log({"job_id": job.id, "started_at": datetime.now(), "status": "running"})
            log_id = log.id

        if job_type == "ml_train":
            thread = threading.Thread(
                target=self._run_ml_train,
                args=(log_id, ml_markets, ml_algorithms, ml_target_days,
                      ml_include_price_collect, ml_include_kis_collect,
                      ml_include_dart_collect, ml_include_feature,
                      ml_optuna_trials, job_days_back, base_date),
                daemon=True,
            )
        else:
            thread = threading.Thread(
                target=self._run_job_by_type,
                args=(log_id, job_type, job_market, job_sector, job_days_back),
                daemon=True,
            )
        thread.start()

        return {"success": True, "log_id": log_id, "message": f"실행 시작 (log_id={log_id})"}

    def list_logs(self, job_id: Optional[int], limit: int) -> list[ScheduleLogResponse]:
        with database.session() as session:
            repo = SchedulerRepository(session)
            rows = repo.get_logs(job_id, limit)
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

    # ── 내부 실행 로직 (백그라운드 스레드) ──

    def _run_job_by_type(self, log_id: int, job_type: str, market: str, sector: str, days_back: int):
        """job_type별 서비스 호출 + ScheduleLog 업데이트"""
        try:
            if job_type == "fundamental_collect":
                from services import FundamentalService
                result = FundamentalService().collect_fundamentals(market=market)

            elif job_type == "market_investor_collect":
                from services import FundamentalService
                result = FundamentalService().collect_market_investor_trading()

            elif job_type == "macro_collect":
                from services import MacroService
                result = MacroService().collect(days_back=days_back or 30)

            elif job_type == "news_collect":
                from services import NewsService
                news_market = market if market in ("KR", "US") else "KR"
                result = NewsService().collect(market=news_market)

            elif job_type == "disclosure_collect":
                from services import DisclosureService
                result = DisclosureService().collect_disclosures(market=market, days=days_back or 60)

            elif job_type == "supply_collect":
                from services import DisclosureService
                result = DisclosureService().collect_supply_demand(market=market, days=days_back or 60)

            elif job_type == "full_collect":
                result = self._run_full_collect(market, days_back, sector)

            else:
                from services import StockService
                StockService().run_schedule_job(log_id, market, sector, days_back)
                return

            with database.session() as session:
                repo = SchedulerRepository(session)
                log_entry = repo.get_log(log_id)
                if log_entry:
                    repo.update_log(log_entry, {
                        "finished_at": datetime.now(),
                        "status": "success",
                        "success_count": result.get("success", result.get("stock_success", 0)),
                        "failed_count": result.get("failed", result.get("stock_failed", 0)),
                        "db_saved_count": result.get("saved", 0),
                        "message": result.get("message", "")[:500],
                    })

        except Exception as e:
            logger.error(f"잡 실행 실패: {job_type}", "run_job_by_type", {"error": str(e)})
            with database.session() as session:
                repo = SchedulerRepository(session)
                log_entry = repo.get_log(log_id)
                if log_entry:
                    repo.update_log(log_entry, {
                        "finished_at": datetime.now(),
                        "status": "failed",
                        "message": str(e)[:500],
                    })

    def _run_ml_train(
        self, log_id, ml_markets, ml_algorithms, ml_target_days,
        include_price, include_kis, include_dart, include_feature,
        optuna_trials, days_back, base_date,
    ):
        """ML 학습 실행 (백그라운드 스레드)"""
        try:
            from ml.training_scheduler import run_training_schedule

            result = run_training_schedule(
                markets=ml_markets or ["KOSPI", "KOSDAQ"],
                algorithms=ml_algorithms,
                target_days=ml_target_days,
                include_price_collect=include_price,
                include_kis_collect=include_kis,
                include_dart_collect=include_dart,
                include_feature_compute=include_feature,
                optuna_trials=optuna_trials,
                days_back=days_back,
                base_date=base_date,
            )
            with database.session() as session:
                repo = SchedulerRepository(session)
                log_entry = repo.get_log(log_id)
                if log_entry:
                    repo.update_log(log_entry, {
                        "finished_at": datetime.now(),
                        "status": "success" if result["failed"] == 0 else "partial",
                        "success_count": result["trained"],
                        "failed_count": result["failed"],
                        "message": result.get("summary", "")[:500],
                    })
        except Exception as e:
            with database.session() as session:
                repo = SchedulerRepository(session)
                log_entry = repo.get_log(log_id)
                if log_entry:
                    repo.update_log(log_entry, {
                        "finished_at": datetime.now(),
                        "status": "failed",
                        "message": str(e)[:500],
                    })

    def _run_full_collect(self, market: str, days_back: int, sector: str = None) -> dict:
        """전체 데이터 일괄 수집 + 피처 계산 (sector 지정 시 해당 섹터만)"""
        from services import StockService, FundamentalService, MacroService
        from services import NewsService, DisclosureService
        from data_collector import DataPipeline

        steps = []
        total_saved = 0
        total_failed = 0
        target_codes = []

        # 1) 가격 — sector 필터 적용, target_codes 확정
        try:
            pipeline = DataPipeline()
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days_back or 7)).strftime("%Y-%m-%d")
            fetch_result = pipeline.fetch(start_date=start_date, end_date=end_date, market=market, sector=sector)
            if fetch_result.data or fetch_result.stock_info:
                svc = StockService(pipeline=pipeline)
                if fetch_result.data:
                    total_saved += svc.save_to_db(fetch_result.data, fetch_result.market)
                if fetch_result.stock_info:
                    svc._save_stock_info(fetch_result.stock_info)
                    target_codes = [s["code"] for s in fetch_result.stock_info]
            if not target_codes and fetch_result.data:
                target_codes = list({d["code"] for d in fetch_result.data})
            steps.append(f"가격: {fetch_result.success_count}종목")
        except Exception as e:
            steps.append(f"가격: 실패({e})")
            total_failed += 1

        if not target_codes:
            return {"saved": 0, "failed": 1, "success": 0, "message": "대상 종목 없음"}

        # target_codes로 (code, name) 매핑 (뉴스용)
        from models import StockInfo
        with database.session() as session:
            code_names = [
                (r.code, r.name) for r in
                session.query(StockInfo.code, StockInfo.name)
                .filter(StockInfo.market == market, StockInfo.code.in_(target_codes))
                .all()
            ]

        logger.info(f"일괄수집 대상: {len(target_codes)}종목 (sector={sector})", "_run_full_collect")

        # 2) 재무 — target_codes만
        try:
            r = FundamentalService().collect_fundamentals(market=market, codes=target_codes)
            total_saved += r.get("saved", 0)
            steps.append(f"재무: {r.get('saved', 0)}건")
        except Exception as e:
            steps.append(f"재무: 실패({e})")
            total_failed += 1

        # 3) 거시지표 (종목 무관)
        try:
            r = MacroService().collect(days_back=days_back or 30)
            total_saved += r.get("saved", 0)
            steps.append(f"거시: {r.get('saved', 0)}건")
        except Exception as e:
            steps.append(f"거시: 실패({e})")
            total_failed += 1

        # 4) 뉴스 — target_codes만
        try:
            news_market = market if market in ("KR", "US") else "KR"
            r = NewsService().collect(market=news_market, codes=code_names)
            total_saved += r.get("saved", 0)
            steps.append(f"뉴스: {r.get('saved', 0)}건")
        except Exception as e:
            steps.append(f"뉴스: 실패({e})")
            total_failed += 1

        # 5) 공시 — target_codes만
        try:
            r = DisclosureService().collect_disclosures(market=market, codes=target_codes, days=days_back or 60)
            total_saved += r.get("saved", 0)
            steps.append(f"공시: {r.get('saved', 0)}건")
        except Exception as e:
            steps.append(f"공시: 실패({e})")
            total_failed += 1

        # 6) 수급 — target_codes만
        try:
            r = DisclosureService().collect_supply_demand(market=market, codes=target_codes, days=days_back or 60)
            total_saved += r.get("saved", 0)
            steps.append(f"수급: {r.get('saved', 0)}건")
        except Exception as e:
            steps.append(f"수급: 실패({e})")
            total_failed += 1

        # 7) 피처 계산 — target_codes만
        try:
            from ml.feature_engineer import FeatureEngineer
            feat_result = FeatureEngineer().compute_all(market=market, codes=target_codes)
            feat_saved = feat_result.get("success", 0)
            total_saved += feat_saved
            steps.append(f"피처: {feat_saved}/{feat_result.get('total', 0)}종목")
        except Exception as e:
            steps.append(f"피처: 실패({e})")
            total_failed += 1

        total_steps = 7
        msg = " | ".join(steps)
        return {
            "saved": total_saved,
            "failed": total_failed,
            "success": total_steps - total_failed,
            "message": f"일괄 수집+피처 완료: {msg}",
        }

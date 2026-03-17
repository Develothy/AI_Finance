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
from scheduler import JobScheduler, SCHEDULER_AVAILABLE
from repositories.scheduler_repository import SchedulerRepository

logger = get_logger("scheduler_service")

# ── Step 레지스트리 ──

STEP_REGISTRY = {
    "price":           {"order": 1,  "label": "가격"},
    "fundamental":     {"order": 2,  "label": "재무"},
    "market_investor": {"order": 3,  "label": "시장수급"},
    "macro":           {"order": 4,  "label": "거시지표"},
    "news":            {"order": 5,  "label": "뉴스"},
    "disclosure":      {"order": 6,  "label": "공시"},
    "supply":          {"order": 7,  "label": "수급"},
    "alternative":     {"order": 8,  "label": "대안"},
    "feature":         {"order": 9,  "label": "피처"},
    "ml":              {"order": 10, "label": "ML학습"},
}


# ── Step 핸들러 ──

def _handle_price(market, sector, days_back, config, ctx):
    from services import StockService
    from data_collector import DataPipeline

    pipeline = DataPipeline()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back or 7)).strftime("%Y-%m-%d")
    fetch_result = pipeline.fetch(start_date=start_date, end_date=end_date, market=market, sector=sector)
    saved = 0
    if fetch_result.data or fetch_result.stock_info:
        svc = StockService(pipeline=pipeline)
        if fetch_result.data:
            saved = svc.save_to_db(fetch_result.data, fetch_result.market)
        if fetch_result.stock_info:
            svc._save_stock_info(fetch_result.stock_info)
            ctx["target_codes"] = [s["code"] for s in fetch_result.stock_info]
    if not ctx.get("target_codes") and fetch_result.data:
        ctx["target_codes"] = list(fetch_result.data.keys())
    # target_codes → code_names 매핑 (ctx 초기값 덮어쓰기)
    if ctx.get("target_codes"):
        from repositories import StockRepository
        with database.session() as session:
            all_cn = StockRepository(session).get_codes_with_names(market)
            code_set = set(ctx["target_codes"])
            ctx["code_names"] = [(c, n) for c, n in all_cn if c in code_set]
    return {"saved": saved, "summary": f"{fetch_result.success_count}종목"}


def _handle_fundamental(market, sector, days_back, config, ctx):
    from services import FundamentalService
    r = FundamentalService().collect_fundamentals(market=market, codes=ctx.get("target_codes"))
    return {"saved": r.get("saved", 0), "summary": f"{r.get('saved', 0)}건"}


def _handle_market_investor(market, sector, days_back, config, ctx):
    from services import FundamentalService
    r = FundamentalService().collect_market_investor_trading()
    return {"saved": r.get("saved", 0), "summary": f"{r.get('saved', 0)}건"}


def _handle_macro(market, sector, days_back, config, ctx):
    from services import MacroService
    r = MacroService().collect(days_back=days_back or 30)
    return {"saved": r.get("saved", 0), "summary": f"{r.get('saved', 0)}건"}


def _handle_news(market, sector, days_back, config, ctx):
    from services import NewsService
    news_market = market if market in ("KR", "US") else "KR"
    r = NewsService().collect(market=news_market, codes=ctx.get("code_names"))
    return {"saved": r.get("saved", 0), "summary": f"{r.get('saved', 0)}건"}


def _handle_disclosure(market, sector, days_back, config, ctx):
    from services import DisclosureService
    r = DisclosureService().collect_disclosures(market=market, codes=ctx.get("target_codes"), days=days_back or 60)
    return {"saved": r.get("saved", 0), "summary": f"{r.get('saved', 0)}건"}


def _handle_supply(market, sector, days_back, config, ctx):
    from services import DisclosureService
    r = DisclosureService().collect_supply_demand(market=market, codes=ctx.get("target_codes"), days=days_back or 60)
    return {"saved": r.get("saved", 0), "summary": f"{r.get('saved', 0)}건"}


def _handle_alternative(market, sector, days_back, config, ctx):
    from services import AlternativeService
    code_names = ctx.get("code_names")
    target_codes = ctx.get("target_codes")
    if code_names:
        code_names_for_alt = [(c, n) for c, n in code_names if c in (target_codes or [])]
        code_only_for_alt = [c for c, _ in code_names_for_alt]
    else:
        # ctx에 종목 정보 없으면 빈 리스트 → 전체 종목 fallback 방지
        code_names_for_alt = []
        code_only_for_alt = []
    r = AlternativeService().collect_trends(market=market, codes=code_names_for_alt, days=days_back or 90)
    r2 = AlternativeService().collect_community(market=market, codes=code_only_for_alt, days=min(days_back or 30, 30))
    saved = r.get("saved", 0) + r2.get("saved", 0)
    return {"saved": saved, "summary": f"{saved}건"}


def _handle_feature(market, sector, days_back, config, ctx):
    from ml.feature_engineer import FeatureEngineer
    feat_result = FeatureEngineer().compute_all(market=market, codes=ctx.get("target_codes") or None)
    saved = feat_result.get("success", 0)
    return {"saved": saved, "summary": f"{saved}/{feat_result.get('total', 0)}종목"}


def _handle_ml(market, sector, days_back, config, ctx):
    from ml.training_scheduler import run_training_schedule
    config = config or {}
    ml_result = run_training_schedule(
        markets=config.get("markets", [market]),
        algorithms=config.get("algorithms"),
        target_days=config.get("target_days"),
        include_price_collect=False,
        include_kis_collect=False,
        include_dart_collect=False,
        include_feature_compute=False,
        optuna_trials=config.get("optuna_trials", 50),
        days_back=days_back,
    )
    return {"saved": ml_result.get("trained", 0), "summary": f"{ml_result.get('trained', 0)}모델"}


STEP_HANDLERS = {
    "price": _handle_price,
    "fundamental": _handle_fundamental,
    "market_investor": _handle_market_investor,
    "macro": _handle_macro,
    "news": _handle_news,
    "disclosure": _handle_disclosure,
    "supply": _handle_supply,
    "alternative": _handle_alternative,
    "feature": _handle_feature,
    "ml": _handle_ml,
}


class SchedulerService:

    def cleanup_stale_logs(self):
        # 앱 재시작 시 status='running'인 좀비 로그를 'failed'로 변경
        try:
            with database.session() as session:
                repo = SchedulerRepository(session)
                stale = repo.get_stale_running_logs()
                for log_entry in stale:
                    repo.update_log(log_entry, {
                        "finished_at": datetime.now(),
                        "status": "failed",
                        "message": f"앱 재시작으로 중단 (원래 시작: {log_entry.started_at})",
                    })
                if stale:
                    logger.info(f"고아 로그 {len(stale)}건 정리", "cleanup_stale_logs")
        except Exception as e:
            logger.warning(f"고아 로그 정리 실패: {e}", "cleanup_stale_logs")

    # ── CRUD ──

    def list_jobs(self) -> list[ScheduleJobResponse]:
        with database.session() as session:
            repo = SchedulerRepository(session)
            jobs = repo.get_all_jobs()
            scheduler = JobScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
            next_runs = scheduler.get_next_runs() if scheduler else {}

            all_job_ids = [j.id for j in jobs]
            steps_by_job = repo.get_steps_for_jobs(all_job_ids)

            return [
                ScheduleJobResponse.from_model(
                    j, next_runs.get(j.job_name),
                    steps=steps_by_job.get(j.id, []),
                )
                for j in jobs
            ]

    def create_job(self, req: ScheduleJobRequest) -> ScheduleJobResponse:
        with database.session() as session:
            repo = SchedulerRepository(session)

            if repo.find_duplicate_name(req.job_name):
                from fastapi import HTTPException
                raise HTTPException(status_code=409, detail=f"이미 존재하는 job_name: {req.job_name}")

            job_data = {
                "job_name": req.job_name,
                "market": req.market,
                "sector": req.sector,
                "cron_expr": req.cron_expr,
                "days_back": req.days_back,
                "enabled": req.enabled,
                "description": req.description,
            }
            job = repo.create_job(job_data)

            steps_data = []
            for s in req.steps:
                sd = {
                    "step_type": s.step_type,
                    "step_order": s.step_order,
                    "enabled": s.enabled,
                    "config": json.dumps(s.config) if s.config else None,
                }
                steps_data.append(sd)
            steps = repo.replace_steps(job.id, steps_data)

            scheduler = JobScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
            if scheduler:
                scheduler.sync_job(job.job_name, "add", job, steps=steps)
            return ScheduleJobResponse.from_model(job, steps=steps)

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

            update_data = {
                "job_name": req.job_name,
                "market": req.market,
                "sector": req.sector,
                "cron_expr": req.cron_expr,
                "days_back": req.days_back,
                "enabled": req.enabled,
                "description": req.description,
            }
            repo.update_job(job, update_data)

            steps_data = []
            for s in req.steps:
                sd = {
                    "step_type": s.step_type,
                    "step_order": s.step_order,
                    "enabled": s.enabled,
                    "config": json.dumps(s.config) if s.config else None,
                }
                steps_data.append(sd)
            steps = repo.replace_steps(job.id, steps_data)

            scheduler = JobScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
            if scheduler:
                scheduler.sync_job(old_job_name, "remove")
                scheduler.sync_job(job.job_name, "add", job, steps=steps)
            return ScheduleJobResponse.from_model(job, steps=steps)

    def delete_job(self, job_id: int) -> dict:
        with database.session() as session:
            repo = SchedulerRepository(session)
            job = repo.get_job(job_id)
            if not job:
                from fastapi import HTTPException
                raise HTTPException(status_code=404, detail=f"스케줄 없음: id={job_id}")
            job_name = job.job_name
            repo.delete_job(job)

        scheduler = JobScheduler.get_running_instance() if SCHEDULER_AVAILABLE else None
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

            steps = repo.get_steps_for_job(job.id)
            steps_data = [
                {"step_type": s.step_type, "step_order": s.step_order,
                 "enabled": s.enabled, "config": s.get_config()}
                for s in steps
            ]

            log = repo.create_log({"job_id": job.id, "started_at": datetime.now(), "status": "running"})
            log_id = log.id
            job_market = job.market
            job_sector = job.sector
            job_days_back = job.days_back

        thread = threading.Thread(
            target=self._run_job,
            args=(log_id, job_market, job_sector, job_days_back, steps_data),
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

    # ── 통합 실행 로직 (백그라운드 스레드) ──

    def _update_log_safe(self, log_id: int, data: dict):
        try:
            with database.session() as session:
                repo = SchedulerRepository(session)
                log_entry = repo.get_log(log_id)
                if log_entry:
                    repo.update_log(log_entry, data)
        except Exception as e:
            logger.error(
                f"스케줄 로그 업데이트 실패 (log_id={log_id})",
                "_update_log_safe",
                {"error": str(e), "data": str(data)[:200]},
            )

    def _run_job(self, log_id: int, market: str, sector: str, days_back: int,
                 steps_data: list[dict]):
        """step 기반 통합 잡 실행"""
        try:
            result = _execute_pipeline(market, sector, days_back, steps_data)
            self._update_log_safe(log_id, {
                "finished_at": datetime.now(),
                "status": "success" if result.get("failed", 0) == 0 else "partial",
                "success_count": result.get("success", 0),
                "failed_count": result.get("failed", 0),
                "db_saved_count": result.get("saved", 0),
                "message": result.get("message", "")[:500],
            })
        except Exception as e:
            logger.error(f"잡 실행 실패", "_run_job", {"error": str(e)})
            self._update_log_safe(log_id, {
                "finished_at": datetime.now(),
                "status": "failed",
                "message": str(e)[:500],
            })


def _init_ctx(market: str, sector: str) -> dict:
    """market/sector 기반으로 ctx 사전 초기화 (price step 실패 대비)"""
    ctx = {}
    try:
        from data_collector.stock_codes import (
            get_kr_stock_list, filter_kr_stocks_by_sector, is_korean_market,
        )
        from repositories import StockRepository

        if is_korean_market(market):
            listing = get_kr_stock_list(market)
            filtered = filter_kr_stocks_by_sector(listing, sector)
            codes = filtered["code"].tolist()
        else:
            codes = []

        if codes:
            ctx["target_codes"] = codes
            with database.session() as session:
                all_cn = StockRepository(session).get_codes_with_names(market)
                code_set = set(codes)
                ctx["code_names"] = [(c, n) for c, n in all_cn if c in code_set]
    except Exception as e:
        logger.warning(f"ctx 사전 초기화 실패: {e}", "_init_ctx")

    return ctx


def _execute_pipeline(market: str, sector: str, days_back: int,
                      steps_data: list[dict]) -> dict:
    """Step 핸들러 기반 파이프라인 실행"""
    results = []
    total_saved = 0
    total_failed = 0
    ctx = _init_ctx(market, sector)

    sorted_steps = sorted(
        [s for s in steps_data if s.get("enabled", True)],
        key=lambda s: s["step_order"],
    )

    for step in sorted_steps:
        handler = STEP_HANDLERS.get(step["step_type"])
        if not handler:
            results.append(f"{step['step_type']}: 알 수 없는 단계")
            continue
        label = STEP_REGISTRY.get(step["step_type"], {}).get("label", step["step_type"])
        try:
            r = handler(market, sector, days_back, step.get("config"), ctx)
            total_saved += r.get("saved", 0)
            results.append(f"{label}: {r.get('summary', 'OK')}")
        except Exception as e:
            results.append(f"{label}: 실패({e})")
            total_failed += 1

    msg = " | ".join(results)
    return {
        "saved": total_saved,
        "failed": total_failed,
        "success": len(sorted_steps) - total_failed,
        "message": f"파이프라인 완료: {msg}" if results else "실행할 단계 없음",
    }

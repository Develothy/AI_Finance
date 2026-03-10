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

from fastapi import APIRouter, Body, HTTPException, Query
from sqlalchemy import func

from api.schemas import (
    ConfigGroup,
    ConfigResponse,
    DBResponse,
    HealthResponse,
    LogEntry,
    LogResponse,
    RunJobRequest,
    ScheduleJobRequest,
    ScheduleJobResponse,
    ScheduleLogResponse,
    TableStats,
)
from config import settings
from core import get_logger
from db import database
from data_collector import DataScheduler, SCHEDULER_AVAILABLE
from models import (
    StockPrice, StockInfo, ScheduleJob, ScheduleLog, MLTrainConfig,
    StockFundamental, FinancialStatement, FeatureStore, MLModel, MLPrediction,
    NewsSentiment, DartDisclosure, KrxSupplyDemand,
)

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

            # stock_fundamental 통계
            sf_count = session.query(func.count(StockFundamental.id)).scalar() or 0
            sf_earliest = sf_latest = None
            sf_markets = []
            sf_code_count = 0
            if sf_count > 0:
                sf_earliest_dt = session.query(func.min(StockFundamental.date)).scalar()
                sf_latest_dt = session.query(func.max(StockFundamental.date)).scalar()
                sf_earliest = sf_earliest_dt.strftime("%Y-%m-%d") if sf_earliest_dt else None
                sf_latest = sf_latest_dt.strftime("%Y-%m-%d") if sf_latest_dt else None
                sf_markets = [r[0] for r in session.query(StockFundamental.market).distinct().all()]
                sf_code_count = session.query(func.count(func.distinct(StockFundamental.code))).scalar() or 0

            # financial_statement 통계
            fs_count = session.query(func.count(FinancialStatement.id)).scalar() or 0
            fs_markets = []
            fs_code_count = 0
            fs_period_count = 0
            if fs_count > 0:
                fs_markets = [r[0] for r in session.query(FinancialStatement.market).distinct().all()]
                fs_code_count = session.query(func.count(func.distinct(FinancialStatement.code))).scalar() or 0
                fs_period_count = session.query(func.count(func.distinct(FinancialStatement.period_date))).scalar() or 0

            # feature_store 통계
            feat_count = session.query(func.count(FeatureStore.id)).scalar() or 0
            feat_earliest = feat_latest = None
            feat_markets = []
            feat_code_count = 0
            feat_phase6_count = 0
            feat_phase6_code_count = 0
            if feat_count > 0:
                feat_earliest_dt = session.query(func.min(FeatureStore.date)).scalar()
                feat_latest_dt = session.query(func.max(FeatureStore.date)).scalar()
                feat_earliest = feat_earliest_dt.strftime("%Y-%m-%d") if feat_earliest_dt else None
                feat_latest = feat_latest_dt.strftime("%Y-%m-%d") if feat_latest_dt else None
                feat_markets = [r[0] for r in session.query(FeatureStore.market).distinct().all()]
                feat_code_count = session.query(func.count(func.distinct(FeatureStore.code))).scalar() or 0
                # Phase 6 커버리지 (섹터/상대강도 피처 계산 완료 여부)
                feat_phase6_count = session.query(func.count(FeatureStore.id)).filter(
                    FeatureStore.sector_return_1d.isnot(None),
                ).scalar() or 0
                feat_phase6_code_count = session.query(
                    func.count(func.distinct(FeatureStore.code))
                ).filter(
                    FeatureStore.sector_return_1d.isnot(None),
                ).scalar() or 0

            # news_sentiment 통계
            ns_count = session.query(func.count(NewsSentiment.id)).scalar() or 0
            ns_earliest = ns_latest = None
            ns_code_count = 0
            if ns_count > 0:
                ns_earliest_dt = session.query(func.min(NewsSentiment.date)).scalar()
                ns_latest_dt = session.query(func.max(NewsSentiment.date)).scalar()
                ns_earliest = ns_earliest_dt.strftime("%Y-%m-%d") if ns_earliest_dt else None
                ns_latest = ns_latest_dt.strftime("%Y-%m-%d") if ns_latest_dt else None
                ns_code_count = session.query(
                    func.count(func.distinct(NewsSentiment.code))
                ).filter(NewsSentiment.code.isnot(None)).scalar() or 0

            # ml_model / ml_prediction 통계
            ml_model_count = session.query(func.count(MLModel.id)).scalar() or 0
            ml_active_count = session.query(func.count(MLModel.id)).filter(MLModel.is_active.is_(True)).scalar() or 0
            ml_pred_count = session.query(func.count(MLPrediction.id)).scalar() or 0

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
                    "stock_fundamental": TableStats(
                        row_count=sf_count,
                        earliest_date=sf_earliest,
                        latest_date=sf_latest,
                        markets=sf_markets,
                        code_count=sf_code_count,
                    ),
                    "financial_statement": TableStats(
                        row_count=fs_count,
                        markets=fs_markets,
                        code_count=fs_code_count,
                        period_count=fs_period_count,
                    ),
                    "feature_store": TableStats(
                        row_count=feat_count,
                        earliest_date=feat_earliest,
                        latest_date=feat_latest,
                        markets=feat_markets,
                        code_count=feat_code_count,
                        phase6_count=feat_phase6_count,
                        phase6_code_count=feat_phase6_code_count,
                    ),
                    "news_sentiment": TableStats(
                        row_count=ns_count,
                        earliest_date=ns_earliest,
                        latest_date=ns_latest,
                        code_count=ns_code_count,
                    ),
                    "ml_model": TableStats(
                        row_count=ml_model_count,
                        active_count=ml_active_count,
                    ),
                    "ml_prediction": TableStats(
                        row_count=ml_pred_count,
                    ),
                    "dart_disclosure": TableStats(
                        row_count=session.query(func.count(DartDisclosure.id)).scalar() or 0,
                        code_count=session.query(func.count(func.distinct(DartDisclosure.code))).scalar() or 0,
                    ),
                    "krx_supply_demand": TableStats(
                        row_count=session.query(func.count(KrxSupplyDemand.id)).scalar() or 0,
                        code_count=session.query(func.count(func.distinct(KrxSupplyDemand.code))).scalar() or 0,
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
        "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET", "KIS_MOCK_ACCOUNT_NO",
        "ALPACA_API_KEY", "ALPACA_SECRET_KEY",
        "OPENAI_API_KEY",
    }

    groups_def = {
        "app": ["APP_ENV", "DEV_MODE", "DEBUG"],
        "logging": ["LOG_LEVEL", "LOG_DIR", "LOG_RETENTION_DAYS", "LOG_ROTATION_SIZE"],
        "database": ["DB_TYPE", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "SQLITE_PATH"],
        "scheduler": ["SCHEDULER_TIMEZONE", "DATA_FETCH_HOUR", "DATA_FETCH_MINUTE"],
        "slack": ["SLACK_ENABLED", "SLACK_TOKEN", "SLACK_CHANNEL", "SLACK_WEBHOOK_URL"],
        "kis": ["KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO",
                "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET", "KIS_MOCK_ACCOUNT_NO", "KIS_MOCK_MODE"],
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
                include_price_collect=req.ml_include_price_collect,
                include_kis_collect=req.ml_include_kis_collect,
                include_dart_collect=req.ml_include_dart_collect,
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
                ml_config.include_price_collect = req.ml_include_price_collect
                ml_config.include_kis_collect = req.ml_include_kis_collect
                ml_config.include_dart_collect = req.ml_include_dart_collect
                ml_config.include_feature_compute = req.ml_include_feature_compute
                ml_config.optuna_trials = req.ml_optuna_trials
            else:
                ml_config = MLTrainConfig(
                    job_id=job.id,
                    markets=json.dumps(req.ml_markets),
                    algorithms=json.dumps(req.ml_algorithms),
                    target_days=json.dumps(req.ml_target_days),
                    include_price_collect=req.ml_include_price_collect,
                    include_kis_collect=req.ml_include_kis_collect,
                    include_dart_collect=req.ml_include_dart_collect,
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


def _run_job_by_type(log_id: int, job_type: str, market: str, sector: str, days_back: int):
    """job_type별 올바른 서비스를 호출하고 ScheduleLog 업데이트 (백그라운드 스레드)"""
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
            result = _run_full_collect(market, days_back)

        else:
            # data_collect (기본) — 자체적으로 log 업데이트
            from services import StockService
            StockService().run_schedule_job(log_id, market, sector, days_back)
            return

        # ScheduleLog 업데이트
        with database.session() as session:
            log_entry = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
            if log_entry:
                log_entry.finished_at = datetime.now()
                log_entry.status = "success"
                log_entry.success_count = result.get("success", result.get("stock_success", 0))
                log_entry.failed_count = result.get("failed", result.get("stock_failed", 0))
                log_entry.db_saved_count = result.get("saved", 0)
                log_entry.message = result.get("message", "")[:500]

    except Exception as e:
        logger.error(f"잡 실행 실패: {job_type}", "run_job_by_type", {"error": str(e)})
        with database.session() as session:
            log_entry = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
            if log_entry:
                log_entry.finished_at = datetime.now()
                log_entry.status = "failed"
                log_entry.message = str(e)[:500]


def _run_full_collect(market: str, days_back: int) -> dict:
    """전체 데이터 일괄 수집 + 피처 계산 (가격→재무→거시→뉴스→공시→수급→피처)"""
    from datetime import timedelta
    from services import StockService, FundamentalService, MacroService
    from services import NewsService, DisclosureService
    from data_collector import DataPipeline

    steps = []
    total_saved = 0
    total_failed = 0

    # 1) 가격 데이터 수집
    try:
        pipeline = DataPipeline()
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back or 7)).strftime("%Y-%m-%d")
        fetch_result = pipeline.fetch(start_date=start_date, end_date=end_date, market=market)
        if fetch_result.data or fetch_result.stock_info:
            svc = StockService(pipeline=pipeline)
            if fetch_result.data:
                total_saved += svc.save_to_db(fetch_result.data, fetch_result.market)
            if fetch_result.stock_info:
                svc._save_stock_info(fetch_result.stock_info)
        steps.append(f"가격: {fetch_result.success_count}종목")
    except Exception as e:
        steps.append(f"가격: 실패({e})")
        total_failed += 1

    # 2) 재무(KIS) 수집
    try:
        r = FundamentalService().collect_fundamentals(market=market)
        total_saved += r.get("saved", 0)
        steps.append(f"재무: {r.get('saved', 0)}건")
    except Exception as e:
        steps.append(f"재무: 실패({e})")
        total_failed += 1

    # 3) 거시지표 수집
    try:
        r = MacroService().collect(days_back=days_back or 30)
        total_saved += r.get("saved", 0)
        steps.append(f"거시: {r.get('saved', 0)}건")
    except Exception as e:
        steps.append(f"거시: 실패({e})")
        total_failed += 1

    # 4) 뉴스 수집
    try:
        news_market = market if market in ("KR", "US") else "KR"
        r = NewsService().collect(market=news_market)
        total_saved += r.get("saved", 0)
        steps.append(f"뉴스: {r.get('saved', 0)}건")
    except Exception as e:
        steps.append(f"뉴스: 실패({e})")
        total_failed += 1

    # 5) 공시(DART) 수집
    try:
        r = DisclosureService().collect_disclosures(market=market, days=days_back or 60)
        total_saved += r.get("saved", 0)
        steps.append(f"공시: {r.get('saved', 0)}건")
    except Exception as e:
        steps.append(f"공시: 실패({e})")
        total_failed += 1

    # 6) 수급(KRX) 수집
    try:
        r = DisclosureService().collect_supply_demand(market=market, days=days_back or 60)
        total_saved += r.get("saved", 0)
        steps.append(f"수급: {r.get('saved', 0)}건")
    except Exception as e:
        steps.append(f"수급: 실패({e})")
        total_failed += 1

    # 7) 피처 계산 (Phase 1~6, 2-pass)
    try:
        from ml.feature_engineer import FeatureEngineer
        feat_result = FeatureEngineer().compute_all(market=market)
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


@router.post("/scheduler/jobs/{job_id}/run")
def run_schedule_job(job_id: int, req: Optional[RunJobRequest] = Body(default=None)):
    # 스케줄 즉시 실행 (백그라운드) - 모든 job_type 지원
    base_date = req.base_date if req else None

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
        ml_include_price_collect = False
        ml_include_kis_collect = False
        ml_include_dart_collect = False
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
                ml_include_price_collect = config.include_price_collect
                ml_include_kis_collect = config.include_kis_collect
                ml_include_dart_collect = config.include_dart_collect
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
                    include_price_collect=ml_include_price_collect,
                    include_kis_collect=ml_include_kis_collect,
                    include_dart_collect=ml_include_dart_collect,
                    include_feature_compute=ml_include_feature,
                    optuna_trials=ml_optuna_trials,
                    days_back=job_days_back,
                    base_date=base_date,
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
        thread = threading.Thread(
            target=_run_job_by_type,
            args=(log_id, job_type, job_market, job_sector, job_days_back),
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
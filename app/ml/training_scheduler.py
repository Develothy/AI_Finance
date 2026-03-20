"""
ML 학습 스케줄러
=================

설정된 파라미터(시장/알고리즘/타겟일수)로 모델을 자동 학습.
모든 알고리즘을 활성 상태로 유지하여 대시보드에서 정확도 비교 가능.

학습 결과는 기존 ml_model, ml_training_log 테이블에 자동 저장됨.
"""

from datetime import date, datetime, timedelta
from typing import Optional

from core import get_logger
from ml.feature_engineer import FeatureEngineer
from ml.trainer import ModelTrainer

logger = get_logger("training_scheduler")


def run_training_schedule(
    markets: list[str],
    algorithms: list[str] = None,
    target_days: list[int] = None,
    include_price_collect: bool = False,
    include_kis_collect: bool = False,
    include_dart_collect: bool = False,
    include_feature_compute: bool = True,
    optuna_trials: int = 50,
    days_back: int = 7,
    base_date: Optional[str] = None,
    target_codes: list[str] = None,
) -> dict:
    """
    ML 학습 스케줄 실행 (4-Step 파이프라인 + 학습)

    Args:
        markets: 학습 대상 시장 목록 (예: ["KOSPI", "KOSDAQ"])
        algorithms: 학습 알고리즘 목록 (예: ["random_forest", "xgboost", "lightgbm"])
        target_days: 예측 타겟 일수 목록 (예: [1, 5, 10])
        include_price_collect: Step 1 - 가격 데이터 수집
        include_kis_collect: Step 2 - KIS 기초정보 수집
        include_dart_collect: Step 3 - DART 재무제표 수집
        include_feature_compute: Step 4 - 피처 계산
        optuna_trials: Optuna 하이퍼파라미터 튜닝 횟수
        days_back: Step 1 가격 수집 일수
        base_date: 기준일 (YYYY-MM-DD). None이면 오늘 기준.
    """
    if algorithms is None:
        algorithms = ["random_forest", "xgboost", "lightgbm"]
    if target_days is None:
        target_days = [1, 5]

    # 기준일 결정
    if base_date:
        _base = datetime.strptime(base_date, "%Y-%m-%d").date()
    else:
        _base = date.today()
    _base_str = _base.strftime("%Y-%m-%d")

    logger.info(f"ML 학습 파이프라인 시작 (base_date={_base_str})", "training_schedule")

    # 타겟 컬럼명 동적 생성
    targets = [f"target_class_{d}d" for d in target_days]

    # Step 1) 가격 데이터 수집
    if include_price_collect:
        logger.info(f"Step 1: 가격 데이터 수집 시작 (base_date={_base_str})", "training_schedule")
        from services import StockService
        from api.schemas import CollectRequest
        svc = StockService()

        end_date_str = _base_str
        start_date_str = (_base - timedelta(days=days_back)).strftime("%Y-%m-%d")

        for market in markets:
            try:
                result = svc.collect(CollectRequest(
                    market=market,
                    start_date=start_date_str,
                    end_date=end_date_str,
                ))
                logger.info(
                    f"Step 1 완료: {market} (saved={result.db_saved_count})",
                    "training_schedule",
                )
            except Exception as e:
                logger.error(f"Step 1 실패: {market} - {e}", "training_schedule")

    # Step 2) KIS 기초정보 수집
    if include_kis_collect:
        logger.info(f"Step 2: KIS 기초정보 수집 시작 (base_date={_base_str})", "training_schedule")
        from services import fundamental_service as fund_svc

        # base_date를 영업일로 보정
        kis_date = _base_str
        try:
            from core.market_calendar import previous_trading_day
            kis_date = previous_trading_day(markets[0], _base).strftime("%Y-%m-%d")
        except Exception:
            pass

        for market in markets:
            try:
                result = fund_svc.collect_fundamentals(market=market, date=kis_date)
                logger.info(
                    f"Step 2 완료: {market} (saved={result.get('saved', 0)})",
                    "training_schedule",
                )
            except Exception as e:
                logger.error(f"Step 2 실패: {market} - {e}", "training_schedule")

    # Step 3) DART 재무제표 수집
    if include_dart_collect:
        logger.info(f"Step 3: DART 재무제표 수집 시작 (base_date={_base_str})", "training_schedule")
        from services import fundamental_service as fund_svc
        from data_collector.dart_fetcher import get_quarter_for_date

        year, quarter = get_quarter_for_date(_base)
        logger.info(f"Step 3: 기준분기 = {year}/{quarter}", "training_schedule")

        for market in markets:
            try:
                result = fund_svc.collect_financial_statements(
                    market=market, year=year, quarter=quarter,
                )
                logger.info(
                    f"Step 3 완료: {market} (saved={result.get('saved', 0)})",
                    "training_schedule",
                )
            except Exception as e:
                logger.error(f"Step 3 실패: {market} - {e}", "training_schedule")

    # Step 4) 피처 계산 - target_days 전달
    #   DART 새로 수집했으면 forward-fill 범위 변경 → 전체 재계산
    #   그 외에는 증분 계산 (신규 날짜만)
    if include_feature_compute:
        incremental = not include_dart_collect
        mode = "증분" if incremental else "전체(DART 수집 후)"
        logger.info(f"Step 4: 피처 계산 시작 ({mode})", "training_schedule")
        fe = FeatureEngineer()
        for market in markets:
            try:
                result = fe.compute_all(
                    market=market, target_days=target_days,
                    incremental=incremental,
                )
                logger.info(
                    f"Step 4 완료: {market} ({mode}, "
                    f"success={result['success']}, skipped={result['skipped']})",
                    "training_schedule",
                )
            except Exception as e:
                logger.error(f"Step 4 실패: {market} - {e}", "training_schedule")

    # Step 5) 조합별 순차 학습
    trainer = ModelTrainer()
    results = []
    trained = 0
    failed = 0

    for market in markets:
        for target in targets:
            for algo in algorithms:
                combo = f"{market}/{target}/{algo}"
                logger.info(f"학습 시작: {combo}", "training_schedule")
                try:
                    result = trainer.train(
                        market=market,
                        algorithm=algo,
                        target_column=target,
                        optuna_trials=optuna_trials,
                        target_codes=target_codes,
                    )
                    trained += 1
                    metrics = result.get("metrics", {})
                    results.append({
                        "combo": combo,
                        "status": "success",
                        "model_id": result.get("model_id"),
                        "accuracy": metrics.get("accuracy", 0),
                        "f1_score": metrics.get("f1", 0),
                    })
                    logger.info(f"학습 완료: {combo}", "training_schedule")
                except Exception as e:
                    failed += 1
                    results.append({
                        "combo": combo,
                        "status": "failed",
                        "error": str(e),
                    })
                    logger.error(f"학습 실패: {combo} - {e}", "training_schedule")

    total = trained + failed
    summary = f"ML 학습 완료: {trained}/{total} 성공 (markets={markets}, base_date={_base_str})"
    logger.info(summary, "training_schedule")

    return {
        "trained": trained,
        "failed": failed,
        "total": total,
        "base_date": _base_str,
        "details": results,
        "summary": summary,
    }
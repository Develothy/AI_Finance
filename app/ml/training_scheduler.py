"""
ML 학습 스케줄러
=================

설정된 파라미터(시장/알고리즘/타겟일수)로 모델을 자동 학습.
모든 알고리즘을 활성 상태로 유지하여 대시보드에서 정확도 비교 가능.

학습 결과는 기존 ml_model, ml_training_log 테이블에 자동 저장됨.
"""

from core import get_logger
from ml.feature_engineer import FeatureEngineer
from ml.trainer import ModelTrainer

logger = get_logger("training_scheduler")


def run_training_schedule(
    markets: list[str],
    algorithms: list[str] = None,
    target_days: list[int] = None,
    include_feature_compute: bool = True,
    optuna_trials: int = 50,
) -> dict:
    """
    ML 학습 스케줄 실행

    Args:
        markets: 학습 대상 시장 목록 (예: ["KOSPI", "KOSDAQ"])
        algorithms: 학습 알고리즘 목록 (예: ["random_forest", "xgboost", "lightgbm"])
        target_days: 예측 타겟 일수 목록 (예: [1, 5, 10])
        include_feature_compute: 피처 계산 포함 여부
        optuna_trials: Optuna 하이퍼파라미터 튜닝 횟수
    """
    if algorithms is None:
        algorithms = ["random_forest", "xgboost", "lightgbm"]
    if target_days is None:
        target_days = [1, 5]

    # 타겟 컬럼명 동적 생성
    targets = [f"target_class_{d}d" for d in target_days]

    # 1) 피처 계산 (옵션) - target_days 전달
    if include_feature_compute:
        logger.info("피처 계산 시작", "training_schedule")
        fe = FeatureEngineer()
        for market in markets:
            try:
                fe.compute_all(market=market, target_days=target_days)
                logger.info(f"피처 계산 완료: {market}", "training_schedule")
            except Exception as e:
                logger.error(f"피처 계산 실패: {market} - {e}", "training_schedule")

    # 2) 조합별 순차 학습
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
    summary = f"ML 학습 완료: {trained}/{total} 성공 (markets={markets})"
    logger.info(summary, "training_schedule")

    return {
        "trained": trained,
        "failed": failed,
        "total": total,
        "details": results,
        "summary": summary,
    }

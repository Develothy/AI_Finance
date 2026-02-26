"""
Optuna 하이퍼파라미터 튜닝
===========================

YAML 설정 기반 제네릭 objective 함수로 모든 알고리즘 대응
"""

import optuna
from sklearn.metrics import f1_score

from core import get_logger
from .ml_config_loader import get_search_space, get_algorithm_defaults, get_classifier_class

logger = get_logger("tuner")

# Optuna 로그 레벨 조정
optuna.logging.set_verbosity(optuna.logging.WARNING)


def _build_params_from_space(trial, algorithm: str) -> dict:
    """YAML search_space를 읽어 Optuna trial.suggest_* 호출"""
    space = get_search_space(algorithm)
    params = {}
    for name, spec in space.items():
        if spec["type"] == "int":
            kwargs = {"name": name, "low": spec["low"], "high": spec["high"]}
            if "step" in spec:
                kwargs["step"] = spec["step"]
            params[name] = trial.suggest_int(**kwargs)
        elif spec["type"] == "float":
            kwargs = {"name": name, "low": spec["low"], "high": spec["high"]}
            if spec.get("log"):
                kwargs["log"] = True
            params[name] = trial.suggest_float(**kwargs)
    return params


def _generic_objective(trial, algorithm, X_train, y_train, X_val, y_val):
    """모든 알고리즘에 대응하는 제네릭 objective"""
    # YAML에서 탐색 파라미터 생성
    params = _build_params_from_space(trial, algorithm)
    # YAML에서 기본값 병합
    defaults = get_algorithm_defaults(algorithm).copy()
    defaults.update(params)
    # 분류기 생성 + 학습
    clf_class = get_classifier_class(algorithm)
    model = clf_class(**defaults)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    return f1_score(y_val, y_pred, average="binary")


def tune_hyperparameters(
    algorithm: str,
    X_train,
    y_train,
    X_val,
    y_val,
    n_trials: int = 50,
) -> dict:
    """
    Optuna로 하이퍼파라미터 튜닝

    Args:
        algorithm: random_forest / xgboost / lightgbm
        X_train, y_train: 학습 데이터
        X_val, y_val: 검증 데이터
        n_trials: 시도 횟수

    Returns:
        {"best_params": {...}, "best_value": float, "n_trials": int}
    """
    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: _generic_objective(
            trial, algorithm, X_train, y_train, X_val, y_val
        ),
        n_trials=n_trials,
        show_progress_bar=False,
    )

    logger.info(
        f"튜닝 완료: {algorithm} (best_f1={study.best_value:.4f}, trials={n_trials})",
        "tune_hyperparameters",
    )

    return {
        "best_params": study.best_params,
        "best_value": study.best_value,
        "n_trials": n_trials,
    }

"""
Optuna 하이퍼파라미터 튜닝
===========================

각 알고리즘별 탐색 공간 정의 + 최적 파라미터 반환
"""

import optuna
from sklearn.metrics import f1_score

from core import get_logger

logger = get_logger("tuner")

# Optuna 로그 레벨 조정
optuna.logging.set_verbosity(optuna.logging.WARNING)


def _rf_objective(trial, X_train, y_train, X_val, y_val):
    """RandomForest 탐색 공간"""
    from sklearn.ensemble import RandomForestClassifier

    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500, step=50),
        "max_depth": trial.suggest_int("max_depth", 5, 20),
        "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
        "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
        "class_weight": "balanced",
        "random_state": 42,
        "n_jobs": -1,
    }

    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    return f1_score(y_val, y_pred, average="binary")


def _xgb_objective(trial, X_train, y_train, X_val, y_val):
    """XGBoost 탐색 공간"""
    from xgboost import XGBClassifier

    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500, step=50),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 0.5, 2.0),
        "random_state": 42,
        "eval_metric": "logloss",
        "verbosity": 0,
    }

    model = XGBClassifier(**params)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    return f1_score(y_val, y_pred, average="binary")


def _lgbm_objective(trial, X_train, y_train, X_val, y_val):
    """LightGBM 탐색 공간"""
    from lightgbm import LGBMClassifier

    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500, step=50),
        "num_leaves": trial.suggest_int("num_leaves", 15, 63),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "feature_fraction": trial.suggest_float("feature_fraction", 0.6, 1.0),
        "bagging_fraction": trial.suggest_float("bagging_fraction", 0.6, 1.0),
        "bagging_freq": trial.suggest_int("bagging_freq", 1, 7),
        "is_unbalance": True,
        "random_state": 42,
        "verbose": -1,
    }

    model = LGBMClassifier(**params)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    return f1_score(y_val, y_pred, average="binary")


_OBJECTIVE_MAP = {
    "random_forest": _rf_objective,
    "xgboost": _xgb_objective,
    "lightgbm": _lgbm_objective,
}


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
    objective_fn = _OBJECTIVE_MAP.get(algorithm)
    if not objective_fn:
        raise ValueError(f"지원하지 않는 알고리즘: {algorithm}")

    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: objective_fn(trial, X_train, y_train, X_val, y_val),
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

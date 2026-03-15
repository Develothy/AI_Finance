"""
Optuna RL 하이퍼파라미터 튜닝
==============================

축소된 timesteps로 SB3 학습 → val 에피소드 수익률 최대화.
"""

import numpy as np
import optuna
import pandas as pd
from stable_baselines3 import DQN, PPO
from stable_baselines3.common.vec_env import DummyVecEnv

from core import get_logger

from .environment import StockTradingEnv

logger = get_logger("rl_tuner")
optuna.logging.set_verbosity(optuna.logging.WARNING)

# 튜닝 시 축소 timesteps
_TUNE_TIMESTEPS = 20_000

_SB3_MODELS = {
    "dqn": DQN,
    "ppo": PPO,
}


def _rl_objective(
    trial,
    algorithm: str,
    train_df: pd.DataFrame,
    feature_columns: list[str],
    base_params: dict,
):
    """단일 Optuna trial — RL 모델 학습 후 val 수익률 반환."""

    params = base_params.copy()

    # 알고리즘별 탐색 공간
    if algorithm == "dqn":
        params["learning_rate"] = trial.suggest_float("learning_rate", 5e-5, 1e-3, log=True)
        params["buffer_size"] = trial.suggest_categorical("buffer_size", [50000, 100000, 200000])
        params["batch_size"] = trial.suggest_categorical("batch_size", [32, 64, 128])
        params["gamma"] = trial.suggest_float("gamma", 0.95, 0.999)
        params["target_update_interval"] = trial.suggest_categorical(
            "target_update_interval", [500, 1000, 2000],
        )
    elif algorithm == "ppo":
        params["learning_rate"] = trial.suggest_float("learning_rate", 5e-5, 1e-3, log=True)
        params["n_steps"] = trial.suggest_categorical("n_steps", [1024, 2048, 4096])
        params["batch_size"] = trial.suggest_categorical("batch_size", [32, 64, 128])
        params["n_epochs"] = trial.suggest_int("n_epochs", 3, 15)
        params["gamma"] = trial.suggest_float("gamma", 0.95, 0.999)
        params["clip_range"] = trial.suggest_float("clip_range", 0.1, 0.3)
        params["ent_coef"] = trial.suggest_float("ent_coef", 1e-3, 5e-2, log=True)

    transaction_fee = params.pop("transaction_fee", 0.00015)
    tax_rate = params.pop("tax_rate", 0.0023)
    params.pop("total_timesteps", None)

    # 환경 구성 (학습 데이터에서 최대 4개 종목)
    codes = train_df["code"].unique().tolist()
    valid_codes = [
        c for c in codes
        if len(train_df[train_df["code"] == c]) >= 20
    ]

    if not valid_codes:
        raise optuna.TrialPruned()

    np.random.shuffle(valid_codes)
    selected_codes = valid_codes[:4]

    def _make_env(code):
        def _init():
            code_df = train_df[train_df["code"] == code].sort_values("date").reset_index(drop=True)
            return StockTradingEnv(
                df=code_df,
                feature_columns=feature_columns,
                transaction_fee=transaction_fee,
                tax_rate=tax_rate,
            )
        return _init

    env = DummyVecEnv([_make_env(c) for c in selected_codes])

    # SB3 파라미터 필터링
    dqn_keys = {
        "learning_rate", "buffer_size", "learning_starts", "batch_size",
        "gamma", "target_update_interval", "exploration_fraction",
        "exploration_final_eps",
    }
    ppo_keys = {
        "learning_rate", "n_steps", "batch_size", "n_epochs",
        "gamma", "gae_lambda", "clip_range", "ent_coef",
    }
    allowed = dqn_keys if algorithm == "dqn" else ppo_keys
    sb3_params = {k: v for k, v in params.items() if k in allowed}

    # 학습
    try:
        model_cls = _SB3_MODELS[algorithm]
        model = model_cls("MlpPolicy", env, verbose=0, **sb3_params)
        model.learn(total_timesteps=_TUNE_TIMESTEPS)
    except Exception:
        env.close()
        raise optuna.TrialPruned()

    env.close()

    # 평가 — 학습 데이터의 마지막 20%를 val로 사용
    returns = []
    for code in selected_codes:
        code_df = train_df[train_df["code"] == code].sort_values("date").reset_index(drop=True)
        n = len(code_df)
        val_start = int(n * 0.8)
        val_df = code_df.iloc[val_start:].reset_index(drop=True)

        if len(val_df) < 10:
            continue

        eval_env = StockTradingEnv(
            df=val_df,
            feature_columns=feature_columns,
            transaction_fee=transaction_fee,
            tax_rate=tax_rate,
        )

        obs, _ = eval_env.reset()
        while True:
            action, _ = model.predict(obs, deterministic=True)
            obs, reward, terminated, truncated, info = eval_env.step(int(action))
            if terminated or truncated:
                break
        returns.append(info["total_return"])

    if not returns:
        raise optuna.TrialPruned()

    avg_return = float(np.mean(returns))
    return avg_return


def tune_rl_hyperparameters(
    algorithm: str,
    train_df: pd.DataFrame,
    feature_columns: list[str],
    base_params: dict,
    n_trials: int = 10,
) -> dict:
    """
    Optuna RL 하이퍼파라미터 튜닝.

    Returns:
        {"best_params": dict, "best_value": float, "n_trials": int}
    """
    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: _rl_objective(
            trial, algorithm, train_df, feature_columns, base_params,
        ),
        n_trials=n_trials,
        show_progress_bar=False,
    )

    logger.info(
        f"RL 튜닝 완료: {algorithm} "
        f"(best_return={study.best_value:.4f}, trials={n_trials})",
        "tune_rl",
    )

    return {
        "best_params": study.best_params,
        "best_value": study.best_value,
        "n_trials": n_trials,
    }

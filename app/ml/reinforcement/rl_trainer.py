"""
강화학습 학습 파이프라인
========================

FeatureStore 데이터 로드 → 환경 구성 → DQN/PPO 학습
→ 평가 → .zip + _meta.joblib 저장 → DB 저장
"""

import json
import os
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler
from stable_baselines3 import DQN, PPO
from stable_baselines3.common.vec_env import DummyVecEnv

from core import get_logger
from db import database
from repositories import MLRepository

from ..feature_engineer import PHASE7_FEATURE_COLUMNS
from ..ml_config_loader import get_algorithm_defaults
from .environment import StockTradingEnv

logger = get_logger("rl_trainer")

# 저장 디렉토리
SAVED_MODELS_DIR = Path(
    os.environ.get(
        "MODEL_SAVE_DIR",
        Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "saved_models",
    )
)

# SB3 모델 매핑
_SB3_MODELS = {
    "dqn": DQN,
    "ppo": PPO,
}


class RLTrainer:
    """강화학습 모델 학습기 (DQN / PPO)."""

    def __init__(self):
        SAVED_MODELS_DIR.mkdir(parents=True, exist_ok=True)

    def train(
        self,
        market: str,
        algorithm: str,
        target_column: str = "target_class_1d",
        train_ratio: float = 0.85,
        val_ratio: float = 0.15,
        optuna_trials: int = 10,
        feature_columns: list[str] = None,
    ) -> dict:
        """
        RL 모델 학습 실행.

        ModelTrainer.train()과 동일한 시그니처·반환값 유지.

        Returns:
            {"model_id": int, "model_name": str, "metrics": dict, "feature_importance": dict}
        """
        features = feature_columns or PHASE7_FEATURE_COLUMNS
        model_type = "reinforcement"

        # 기본 파라미터 로드
        params = get_algorithm_defaults(algorithm).copy()

        # 1. 학습 이력 생성
        training_log_id = self._create_training_log(
            algorithm=algorithm,
            model_type=model_type,
            market=market,
            target_column=target_column,
        )

        try:
            # 2. 데이터 로드 (code + close 컬럼 필요)
            df = self._load_data(market, features)
            if len(df) < 200:
                raise ValueError(f"RL 학습 데이터 부족: {len(df)}행 (최소 200행)")

            # 2.5 NaN 비율 높은 피처 제외
            features, dropped = self._filter_features_by_nan(df, features)
            if not features:
                raise ValueError("사용 가능한 피처 없음 (모든 피처 NaN 비율 초과)")

            # 3. 시계열 분할
            train_df, val_df = self._split_data(df, train_ratio, val_ratio)

            # 4. NaN imputation
            imputer = SimpleImputer(strategy="median")
            train_df = train_df.copy()
            val_df = val_df.copy()
            train_df[features] = imputer.fit_transform(train_df[features])
            val_df[features] = imputer.transform(val_df[features])

            # 5. 스케일링 (close는 스케일링하지 않음 — 환경에서 원가 사용)
            scaler = RobustScaler()
            train_df[features] = scaler.fit_transform(train_df[features])
            val_df[features] = scaler.transform(val_df[features])

            # 6. Optuna 튜닝 (선택)
            best_params = {}
            tune_result = None
            if optuna_trials > 0:
                from .rl_tuner import tune_rl_hyperparameters

                tune_result = tune_rl_hyperparameters(
                    algorithm=algorithm,
                    train_df=train_df,
                    feature_columns=features,
                    base_params=params,
                    n_trials=optuna_trials,
                )
                best_params = tune_result["best_params"]
                params.update(best_params)

            # 7. 환경 구성 + SB3 학습
            transaction_fee = params.pop("transaction_fee", 0.00015)
            tax_rate = params.pop("tax_rate", 0.0023)
            total_timesteps = params.pop("total_timesteps", 100_000)

            env = self._make_vec_env(
                train_df, features, transaction_fee, tax_rate,
            )

            # SB3 모델에 전달할 파라미터 필터링
            sb3_params = self._filter_sb3_params(algorithm, params)

            model_cls = _SB3_MODELS[algorithm]
            model = model_cls("MlpPolicy", env, verbose=0, **sb3_params)

            logger.info(
                f"RL 학습 시작: {algorithm} "
                f"(timesteps={total_timesteps}, features={len(features)})",
                "train",
            )
            model.learn(total_timesteps=total_timesteps)
            env.close()

            # 8. 평가
            metrics = self._evaluate(model, val_df, features, transaction_fee, tax_rate)

            # 9. 모델 저장
            model_name = f"{algorithm}_rl_{market.lower()}"
            version = self._get_next_version(model_name)
            file_name = f"{model_name}_v{version}"
            model_path = str(SAVED_MODELS_DIR / f"{file_name}.zip")
            meta_path = str(SAVED_MODELS_DIR / f"{file_name}_meta.joblib")

            model.save(model_path.replace(".zip", ""))  # SB3 자동 .zip 추가

            meta_data = {
                "scaler": scaler,
                "imputer": imputer,
                "feature_columns": features,
                "algorithm": algorithm,
                "transaction_fee": transaction_fee,
                "tax_rate": tax_rate,
                "params": params,
            }
            joblib.dump(meta_data, meta_path)

            # 10. DB 저장
            model_id = self._save_model_to_db(
                model_name=model_name,
                model_type=model_type,
                algorithm=algorithm,
                market=market,
                target_column=target_column,
                hyperparameters=params,
                feature_columns=features,
                train_df=train_df,
                metrics=metrics,
                model_path=model_path,
                version=version,
            )

            # 11. 학습 이력 업데이트
            self._update_training_log(
                log_id=training_log_id,
                model_id=model_id,
                status="success",
                train_df=train_df,
                val_df=val_df,
                feature_count=len(features),
                metrics=metrics,
                hyperparameters=params,
                optuna_trials=optuna_trials if optuna_trials > 0 else None,
                best_trial_value=tune_result["best_value"] if tune_result else None,
            )

            logger.info(
                f"RL 학습 완료: {model_name} v{version} "
                f"(return={metrics.get('total_return', 0):.4f}, "
                f"sharpe={metrics.get('sharpe_ratio', 0):.4f})",
                "train",
            )

            return {
                "model_id": model_id,
                "model_name": f"{model_name}_v{version}",
                "metrics": metrics,
                "feature_importance": {},
            }

        except Exception as e:
            self._update_training_log(
                log_id=training_log_id,
                status="failed",
                error_message=str(e)[:1000],
            )
            logger.error(f"RL 학습 실패: {e}", "train")
            raise

    # ------------------------------------------------------------------
    # 환경 생성
    # ------------------------------------------------------------------

    def _make_vec_env(
        self,
        df: pd.DataFrame,
        features: list[str],
        transaction_fee: float,
        tax_rate: float,
    ) -> DummyVecEnv:
        """종목별 환경을 DummyVecEnv로 래핑."""
        codes = df["code"].unique().tolist()

        def _make_env(code):
            def _init():
                code_df = df[df["code"] == code].sort_values("date").reset_index(drop=True)
                return StockTradingEnv(
                    df=code_df,
                    feature_columns=features,
                    transaction_fee=transaction_fee,
                    tax_rate=tax_rate,
                )
            return _init

        # 유효한 종목만 (최소 20행)
        valid_codes = [
            c for c in codes
            if len(df[df["code"] == c]) >= 20
        ]

        if not valid_codes:
            raise ValueError("유효한 종목 없음 (최소 20행 필요)")

        # 최대 8개 종목으로 제한 (메모리/속도)
        if len(valid_codes) > 8:
            np.random.shuffle(valid_codes)
            valid_codes = valid_codes[:8]

        logger.info(
            f"VecEnv 구성: {len(valid_codes)}개 종목",
            "_make_vec_env",
        )

        env_fns = [_make_env(c) for c in valid_codes]
        return DummyVecEnv(env_fns)

    def _filter_sb3_params(self, algorithm: str, params: dict) -> dict:
        """SB3 모델 생성에 사용할 파라미터만 필터링."""
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
        return {k: v for k, v in params.items() if k in allowed}

    # ------------------------------------------------------------------
    # 평가
    # ------------------------------------------------------------------

    def _evaluate(
        self,
        model,
        val_df: pd.DataFrame,
        features: list[str],
        transaction_fee: float,
        tax_rate: float,
    ) -> dict:
        """val 데이터로 에피소드 실행하여 RL 지표 계산."""
        codes = val_df["code"].unique().tolist()
        all_returns = []
        all_daily_returns = []
        total_trades = 0
        winning_trades = 0

        for code in codes:
            code_df = val_df[val_df["code"] == code].sort_values("date").reset_index(drop=True)
            if len(code_df) < 10:
                continue

            env = StockTradingEnv(
                df=code_df,
                feature_columns=features,
                transaction_fee=transaction_fee,
                tax_rate=tax_rate,
            )

            obs, _ = env.reset()
            prev_value = env.portfolio_value
            daily_returns = []

            while True:
                action, _ = model.predict(obs, deterministic=True)
                obs, reward, terminated, truncated, info = env.step(int(action))

                # 일간 수익률 기록
                curr_value = info["portfolio_value"]
                if prev_value > 0:
                    daily_returns.append(curr_value / prev_value - 1.0)
                prev_value = curr_value

                if terminated or truncated:
                    break

            episode_return = info["total_return"]
            all_returns.append(episode_return)
            all_daily_returns.extend(daily_returns)

            # 거래 승률 계산
            for i, trade in enumerate(env.trades):
                if trade[0] == "SELL":
                    total_trades += 1
                    # 직전 BUY 찾기
                    for j in range(i - 1, -1, -1):
                        if env.trades[j][0] == "BUY":
                            if trade[2] > env.trades[j][2]:  # sell_price > buy_price
                                winning_trades += 1
                            break

        if not all_returns:
            return {
                "total_return": 0.0,
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                "win_rate": 0.0,
            }

        # 총 수익률 (평균)
        avg_return = float(np.mean(all_returns))

        # 샤프 비율 (연환산)
        if all_daily_returns and len(all_daily_returns) > 1:
            daily_arr = np.array(all_daily_returns)
            sharpe = float(
                np.mean(daily_arr) / (np.std(daily_arr) + 1e-8) * np.sqrt(252)
            )
        else:
            sharpe = 0.0

        # 최대 낙폭
        if all_daily_returns:
            cumulative = np.cumprod(1 + np.array(all_daily_returns))
            peak = np.maximum.accumulate(cumulative)
            drawdowns = (peak - cumulative) / peak
            max_dd = float(np.max(drawdowns))
        else:
            max_dd = 0.0

        # 승률
        win_rate = float(winning_trades / total_trades) if total_trades > 0 else 0.0

        metrics = {
            "total_return": round(avg_return, 4),
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown": round(max_dd, 4),
            "win_rate": round(win_rate, 4),
        }

        logger.info(
            f"RL 평가 완료: return={avg_return:.4f}, sharpe={sharpe:.4f}, "
            f"mdd={max_dd:.4f}, win_rate={win_rate:.4f}",
            "_evaluate",
        )

        return metrics

    # ------------------------------------------------------------------
    # 데이터 로드 / 분할
    # ------------------------------------------------------------------

    def _load_data(
        self, market: str, features: list[str],
    ) -> pd.DataFrame:
        """feature_store + 외부 소스에서 학습 데이터 로드 (code + close 컬럼 포함)."""
        from ml.feature_loader import EXTERNAL_FEATURE_NAMES, merge_external_features

        with database.session() as session:
            repo = MLRepository(session)
            rows = repo.get_features_by_market(market)

            if not rows:
                raise ValueError(f"피처 데이터 없음: {market}")

            fs_features = [c for c in features if c not in EXTERNAL_FEATURE_NAMES]

            data = []
            for r in rows:
                row_dict = {"date": r.date, "code": r.code}
                # close는 환경에서 원가로 사용
                close_val = getattr(r, "close", None)
                row_dict["close"] = float(close_val) if close_val is not None else None
                for col in fs_features:
                    val = getattr(r, col, None)
                    row_dict[col] = float(val) if val is not None else None
                data.append(row_dict)

            df = pd.DataFrame(data)
            # close가 없으면 사용 불가
            df = df.dropna(subset=["close"])

            # 외부 피처 병합 (거시지표 + 시장센티먼트)
            df = merge_external_features(df, session, market, features)

            nan_counts = df[features].isna().sum()
            nan_features = nan_counts[nan_counts > 0]
            if len(nan_features) > 0:
                logger.info(
                    f"NaN 피처 {len(nan_features)}개 (imputer로 처리 예정)",
                    "_load_data",
                )
            return df

    def _filter_features_by_nan(
        self,
        df: pd.DataFrame,
        features: list[str],
        threshold: float = 0.5,
    ) -> tuple[list[str], list[str]]:
        """NaN 비율 threshold 이상인 피처 제외."""
        n_rows = len(df)
        usable = []
        dropped = []

        for col in features:
            nan_ratio = df[col].isna().sum() / n_rows
            if nan_ratio >= threshold:
                dropped.append(col)
            else:
                usable.append(col)

        if dropped:
            logger.info(
                f"NaN 비율 초과로 피처 제외 ({len(dropped)}개): {dropped}",
                "_filter_features",
            )

        return usable, dropped

    def _split_data(
        self,
        df: pd.DataFrame,
        train_ratio: float,
        val_ratio: float,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """시계열 기반 데이터 분할 (train / val)."""
        n = len(df)
        train_end = int(n * train_ratio)

        train_df = df.iloc[:train_end].copy()
        val_df = df.iloc[train_end:].copy()

        logger.info(
            f"데이터 분할: train={len(train_df)}, val={len(val_df)}",
            "_split_data",
        )
        return train_df, val_df

    # ------------------------------------------------------------------
    # DB 헬퍼
    # ------------------------------------------------------------------

    def _get_next_version(self, model_name: str) -> int:
        """다음 버전 번호."""
        with database.session() as session:
            repo = MLRepository(session)
            existing = repo.get_model_by_name(model_name)
            if existing:
                return existing.version + 1
            return 1

    def _save_model_to_db(self, **kwargs) -> int:
        """모델 메타데이터 DB 저장."""
        with database.session() as session:
            repo = MLRepository(session)

            repo.deactivate_models(
                market=kwargs["market"],
                model_type=kwargs["model_type"],
                target_column=kwargs["target_column"],
                algorithm=kwargs["algorithm"],
            )

            metrics = kwargs["metrics"]
            hp = {}
            for k, v in kwargs["hyperparameters"].items():
                if isinstance(v, (int, float, str, bool, type(None))):
                    hp[k] = v

            model = repo.save_model({
                "model_name": kwargs["model_name"],
                "model_type": kwargs["model_type"],
                "algorithm": kwargs["algorithm"],
                "market": kwargs["market"],
                "target_column": kwargs["target_column"],
                "hyperparameters": json.dumps(hp),
                "feature_columns": json.dumps(kwargs["feature_columns"]),
                "train_start_date": kwargs["train_df"]["date"].min(),
                "train_end_date": kwargs["train_df"]["date"].max(),
                "train_sample_count": len(kwargs["train_df"]),
                # RL 전용 지표 — 기존 분류 지표 필드는 NULL
                "accuracy": None,
                "precision_score": None,
                "recall": None,
                "f1_score": None,
                "auc_roc": None,
                "model_path": kwargs["model_path"],
                "is_active": True,
                "version": kwargs["version"],
            })
            return model.id

    def _create_training_log(self, **kwargs) -> int:
        """학습 이력 생성 (running 상태)."""
        with database.session() as session:
            repo = MLRepository(session)
            log = repo.save_training_log({
                "algorithm": kwargs["algorithm"],
                "model_type": kwargs["model_type"],
                "market": kwargs["market"],
                "target_column": kwargs["target_column"],
                "status": "running",
                "started_at": datetime.now(),
            })
            return log.id

    def _update_training_log(self, log_id: int, status: str = None, **kwargs):
        """학습 이력 업데이트."""
        with database.session() as session:
            repo = MLRepository(session)
            updates = {"finished_at": datetime.now()}

            if status:
                updates["status"] = status
            if kwargs.get("model_id"):
                updates["model_id"] = kwargs["model_id"]
            if kwargs.get("error_message"):
                updates["error_message"] = kwargs["error_message"]
            if kwargs.get("train_df") is not None:
                updates["train_start_date"] = kwargs["train_df"]["date"].min()
                updates["train_end_date"] = kwargs["train_df"]["date"].max()
                updates["train_samples"] = len(kwargs["train_df"])
            if kwargs.get("val_df") is not None:
                updates["val_start_date"] = kwargs["val_df"]["date"].min()
                updates["val_end_date"] = kwargs["val_df"]["date"].max()
                updates["val_samples"] = len(kwargs["val_df"])
            if kwargs.get("feature_count"):
                updates["feature_count"] = kwargs["feature_count"]
            if kwargs.get("metrics"):
                updates["metrics_json"] = json.dumps(kwargs["metrics"])
            if kwargs.get("hyperparameters"):
                hp = {}
                for k, v in kwargs["hyperparameters"].items():
                    if isinstance(v, (int, float, str, bool, type(None))):
                        hp[k] = v
                updates["hyperparameters_json"] = json.dumps(hp)
            if kwargs.get("optuna_trials") is not None:
                updates["optuna_trials"] = kwargs["optuna_trials"]
            if kwargs.get("best_trial_value") is not None:
                updates["best_trial_value"] = kwargs["best_trial_value"]

            repo.update_training_log(log_id, updates)

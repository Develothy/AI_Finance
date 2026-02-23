"""
모델 학습 파이프라인
====================

feature_store에서 데이터 로드 → 시계열 분할 → 학습 → 평가 → 저장
"""

import json
import os
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import RobustScaler

from config import settings
from core import get_logger
from db import database
from models import FeatureStore, MLModel, MLTrainingLog
from repositories import MLRepository

from .feature_engineer import PHASE1_FEATURE_COLUMNS
from .tuner import tune_hyperparameters

logger = get_logger("trainer")

# 저장 디렉토리
SAVED_MODELS_DIR = Path(os.path.dirname(os.path.dirname(__file__))) / "saved_models"


def _get_classifier(algorithm: str, params: dict):
    """알고리즘별 분류기 생성"""
    if algorithm == "random_forest":
        defaults = {"class_weight": "balanced", "random_state": 42, "n_jobs": -1}
        defaults.update(params)
        return RandomForestClassifier(**defaults)

    elif algorithm == "xgboost":
        from xgboost import XGBClassifier
        defaults = {"random_state": 42, "eval_metric": "logloss", "verbosity": 0}
        defaults.update(params)
        return XGBClassifier(**defaults)

    elif algorithm == "lightgbm":
        from lightgbm import LGBMClassifier
        defaults = {"is_unbalance": True, "random_state": 42, "verbose": -1}
        defaults.update(params)
        return LGBMClassifier(**defaults)

    raise ValueError(f"지원하지 않는 알고리즘: {algorithm}")


class ModelTrainer:
    """ML 모델 학습기"""

    def __init__(self):
        SAVED_MODELS_DIR.mkdir(parents=True, exist_ok=True)

    def train(
        self,
        market: str,
        algorithm: str = "random_forest",
        target_column: str = "target_class_1d",
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        optuna_trials: int = 50,
        feature_columns: list[str] = None,
    ) -> dict:
        """
        모델 학습 실행

        Args:
            market: 대상 마켓 (KOSPI 등)
            algorithm: random_forest / xgboost / lightgbm
            target_column: 타겟 컬럼 (target_class_1d, target_class_5d)
            train_ratio: 학습 데이터 비율
            val_ratio: 검증 데이터 비율
            optuna_trials: Optuna 시도 횟수 (0이면 기본 파라미터)
            feature_columns: 사용할 피처 컬럼 목록 (None이면 PHASE1_FEATURE_COLUMNS)

        Returns:
            {"model_id": int, "metrics": dict, "model_name": str}
        """
        features = feature_columns or PHASE1_FEATURE_COLUMNS
        model_type = "classification"  # Phase 1은 분류만

        # 1. 학습 이력 생성
        training_log_id = self._create_training_log(
            algorithm=algorithm,
            model_type=model_type,
            market=market,
            target_column=target_column,
        )

        try:
            # 2. 데이터 로드
            df = self._load_data(market, features, target_column)
            if len(df) < 100:
                raise ValueError(f"학습 데이터 부족: {len(df)}행 (최소 100행)")

            # 3. 시계열 분할
            train_df, val_df, test_df = self._split_data(df, train_ratio, val_ratio)

            X_train = train_df[features].values
            y_train = train_df[target_column].values
            X_val = val_df[features].values
            y_val = val_df[target_column].values
            X_test = test_df[features].values
            y_test = test_df[target_column].values

            # 4. 스케일링
            scaler = RobustScaler()
            X_train = scaler.fit_transform(X_train)
            X_val = scaler.transform(X_val)
            X_test = scaler.transform(X_test)

            # 5. 하이퍼파라미터 튜닝
            best_params = {}
            tune_result = None
            if optuna_trials > 0:
                tune_result = tune_hyperparameters(
                    algorithm, X_train, y_train, X_val, y_val, n_trials=optuna_trials
                )
                best_params = tune_result["best_params"]

            # 6. 최적 파라미터로 학습
            model = _get_classifier(algorithm, best_params)
            model.fit(X_train, y_train)

            # 7. 평가 (테스트 세트)
            y_pred = model.predict(X_test)
            y_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

            metrics = {
                "accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
                "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
                "recall": round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
                "f1": round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
            }
            if y_proba is not None:
                try:
                    metrics["auc_roc"] = round(float(roc_auc_score(y_test, y_proba)), 4)
                except ValueError:
                    metrics["auc_roc"] = None

            # 8. 피처 중요도
            feature_importance = {}
            if hasattr(model, "feature_importances_"):
                for fname, imp in zip(features, model.feature_importances_):
                    feature_importance[fname] = round(float(imp), 6)

            # 9. 모델 저장
            model_name = f"{algorithm[:2]}_class_{market.lower()}_{target_column.replace('target_class_', '')}d"
            version = self._get_next_version(model_name)
            file_name = f"{model_name}_v{version}.joblib"
            model_path = str(SAVED_MODELS_DIR / file_name)

            # 모델 + 스케일러 함께 저장
            joblib.dump({
                "model": model,
                "scaler": scaler,
                "feature_columns": features,
                "algorithm": algorithm,
                "target_column": target_column,
            }, model_path)

            # 10. DB 저장
            model_id = self._save_model_to_db(
                model_name=model_name,
                model_type=model_type,
                algorithm=algorithm,
                market=market,
                target_column=target_column,
                hyperparameters=best_params,
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
                feature_importance=feature_importance,
                hyperparameters=best_params,
                optuna_trials=optuna_trials if optuna_trials > 0 else None,
                best_trial_value=tune_result["best_value"] if tune_result else None,
            )

            logger.info(
                f"학습 완료: {model_name} v{version} (f1={metrics['f1']}, acc={metrics['accuracy']})",
                "train",
            )

            return {
                "model_id": model_id,
                "model_name": f"{model_name}_v{version}",
                "metrics": metrics,
                "feature_importance": feature_importance,
            }

        except Exception as e:
            self._update_training_log(
                log_id=training_log_id,
                status="failed",
                error_message=str(e)[:1000],
            )
            logger.error(f"학습 실패: {e}", "train")
            raise

    def _load_data(self, market: str, features: list[str], target_column: str) -> pd.DataFrame:
        """feature_store에서 학습 데이터 로드"""
        with database.session() as session:
            repo = MLRepository(session)
            rows = repo.get_features_by_market(market)

            if not rows:
                raise ValueError(f"피처 데이터 없음: {market}")

            data = []
            for r in rows:
                row_dict = {"date": r.date}
                for col in features:
                    val = getattr(r, col, None)
                    row_dict[col] = float(val) if val is not None else None
                target_val = getattr(r, target_column, None)
                row_dict[target_column] = int(target_val) if target_val is not None else None
                data.append(row_dict)

            df = pd.DataFrame(data)
            # NaN 있는 행 제거
            df = df.dropna(subset=features + [target_column])
            return df

    def _split_data(
        self, df: pd.DataFrame, train_ratio: float, val_ratio: float
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """시계열 기반 데이터 분할"""
        n = len(df)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_df = df.iloc[:train_end].copy()
        val_df = df.iloc[train_end:val_end].copy()
        test_df = df.iloc[val_end:].copy()

        logger.info(
            f"데이터 분할: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}",
            "_split_data",
        )
        return train_df, val_df, test_df

    def _get_next_version(self, model_name: str) -> int:
        """다음 버전 번호"""
        with database.session() as session:
            repo = MLRepository(session)
            existing = repo.get_model_by_name(model_name)
            if existing:
                return existing.version + 1
            return 1

    def _save_model_to_db(self, **kwargs) -> int:
        """모델 메타데이터 DB 저장"""
        with database.session() as session:
            repo = MLRepository(session)

            # 동일 타겟의 기존 활성 모델 비활성화
            repo.deactivate_models(
                market=kwargs["market"],
                model_type=kwargs["model_type"],
                target_column=kwargs["target_column"],
            )

            metrics = kwargs["metrics"]
            model = repo.save_model({
                "model_name": kwargs["model_name"],
                "model_type": kwargs["model_type"],
                "algorithm": kwargs["algorithm"],
                "market": kwargs["market"],
                "target_column": kwargs["target_column"],
                "hyperparameters": json.dumps(kwargs["hyperparameters"]),
                "feature_columns": json.dumps(kwargs["feature_columns"]),
                "train_start_date": kwargs["train_df"]["date"].min(),
                "train_end_date": kwargs["train_df"]["date"].max(),
                "train_sample_count": len(kwargs["train_df"]),
                "accuracy": metrics.get("accuracy"),
                "precision_score": metrics.get("precision"),
                "recall": metrics.get("recall"),
                "f1_score": metrics.get("f1"),
                "auc_roc": metrics.get("auc_roc"),
                "model_path": kwargs["model_path"],
                "is_active": True,
                "version": kwargs["version"],
            })
            return model.id

    def _create_training_log(self, **kwargs) -> int:
        """학습 이력 생성 (running 상태)"""
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
        """학습 이력 업데이트"""
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
            if kwargs.get("feature_importance"):
                updates["feature_importance_json"] = json.dumps(kwargs["feature_importance"])
            if kwargs.get("hyperparameters"):
                updates["hyperparameters_json"] = json.dumps(kwargs["hyperparameters"])
            if kwargs.get("optuna_trials") is not None:
                updates["optuna_trials"] = kwargs["optuna_trials"]
            if kwargs.get("best_trial_value") is not None:
                updates["best_trial_value"] = kwargs["best_trial_value"]

            repo.update_training_log(log_id, updates)

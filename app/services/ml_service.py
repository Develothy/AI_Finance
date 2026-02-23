"""
ML 서비스
==========

피처 계산 / 모델 학습 / 예측 오케스트레이션
"""

import json

from core import get_logger
from db import database
from ml.feature_engineer import FeatureEngineer
from ml.predictor import Predictor
from ml.trainer import ModelTrainer
from repositories import MLRepository

logger = get_logger("ml_service")


class MLService:
    """ML 기능 통합 서비스"""

    def __init__(self):
        self.feature_engineer = FeatureEngineer()
        self.trainer = ModelTrainer()
        self.predictor = Predictor()

    # ============================================================
    # 피처
    # ============================================================

    def compute_features(self, market: str, code: str = None, start_date: str = None, end_date: str = None) -> dict:
        """피처 계산"""
        if code:
            count = self.feature_engineer.compute_features(market, code, start_date, end_date)
            return {"market": market, "code": code, "saved_count": count}
        else:
            result = self.feature_engineer.compute_all(market, start_date, end_date)
            return {"market": market, **result}

    # ============================================================
    # 학습
    # ============================================================

    def train_model(
        self,
        market: str,
        algorithm: str = "random_forest",
        target_column: str = "target_class_1d",
        optuna_trials: int = 50,
    ) -> dict:
        """모델 학습"""
        return self.trainer.train(
            market=market,
            algorithm=algorithm,
            target_column=target_column,
            optuna_trials=optuna_trials,
        )

    # ============================================================
    # 예측
    # ============================================================

    def predict(self, code: str, market: str, model_id: int = None) -> list[dict]:
        """단일 종목 예측"""
        return self.predictor.predict_single(code, market, model_id)

    def predict_market(self, market: str) -> dict:
        """마켓 전체 예측"""
        return self.predictor.predict_market(market)

    # ============================================================
    # 조회
    # ============================================================

    def get_models(self, market: str = None) -> list[dict]:
        """모델 목록"""
        with database.session() as session:
            repo = MLRepository(session)
            models = repo.get_all_models(market)
            return [self._model_to_dict(m) for m in models]

    def get_model_detail(self, model_id: int) -> dict | None:
        """모델 상세"""
        with database.session() as session:
            repo = MLRepository(session)
            model = repo.get_model(model_id)
            if not model:
                return None
            result = self._model_to_dict(model)
            # 학습 이력 추가
            logs = repo.get_training_logs(model_id=model_id, limit=5)
            result["training_logs"] = [self._training_log_to_dict(log) for log in logs]
            return result

    def delete_model(self, model_id: int) -> bool:
        """모델 삭제"""
        with database.session() as session:
            repo = MLRepository(session)
            return repo.delete_model(model_id)

    def get_predictions(self, market: str = None, code: str = None, limit: int = 100) -> list[dict]:
        """예측 결과 조회"""
        with database.session() as session:
            repo = MLRepository(session)
            predictions = repo.get_predictions(market=market, code=code, limit=limit)
            return [self._prediction_to_dict(p) for p in predictions]

    def get_feature_importance(self, model_id: int) -> dict | None:
        """피처 중요도 조회"""
        with database.session() as session:
            repo = MLRepository(session)
            logs = repo.get_training_logs(model_id=model_id, limit=1)
            if not logs or not logs[0].feature_importance_json:
                return None
            return json.loads(logs[0].feature_importance_json)

    # ============================================================
    # 변환 헬퍼
    # ============================================================

    @staticmethod
    def _model_to_dict(m) -> dict:
        return {
            "id": m.id,
            "model_name": m.model_name,
            "model_type": m.model_type,
            "algorithm": m.algorithm,
            "market": m.market,
            "target_column": m.target_column,
            "train_start_date": str(m.train_start_date) if m.train_start_date else None,
            "train_end_date": str(m.train_end_date) if m.train_end_date else None,
            "train_sample_count": m.train_sample_count,
            "accuracy": float(m.accuracy) if m.accuracy else None,
            "precision_score": float(m.precision_score) if m.precision_score else None,
            "recall": float(m.recall) if m.recall else None,
            "f1_score": float(m.f1_score) if m.f1_score else None,
            "auc_roc": float(m.auc_roc) if m.auc_roc else None,
            "is_active": m.is_active,
            "version": m.version,
            "created_at": m.created_at.strftime("%Y-%m-%d %H:%M:%S") if m.created_at else None,
        }

    @staticmethod
    def _prediction_to_dict(p) -> dict:
        return {
            "id": p.id,
            "model_id": p.model_id,
            "market": p.market,
            "code": p.code,
            "prediction_date": str(p.prediction_date) if p.prediction_date else None,
            "target_date": str(p.target_date) if p.target_date else None,
            "predicted_class": p.predicted_class,
            "probability_up": float(p.probability_up) if p.probability_up else None,
            "probability_down": float(p.probability_down) if p.probability_down else None,
            "signal": p.signal,
            "confidence": float(p.confidence) if p.confidence else None,
            "created_at": p.created_at.strftime("%Y-%m-%d %H:%M:%S") if p.created_at else None,
        }

    @staticmethod
    def _training_log_to_dict(log) -> dict:
        return {
            "id": log.id,
            "algorithm": log.algorithm,
            "status": log.status,
            "train_samples": log.train_samples,
            "val_samples": log.val_samples,
            "feature_count": log.feature_count,
            "optuna_trials": log.optuna_trials,
            "best_trial_value": float(log.best_trial_value) if log.best_trial_value else None,
            "started_at": log.started_at.strftime("%Y-%m-%d %H:%M:%S") if log.started_at else None,
            "finished_at": log.finished_at.strftime("%Y-%m-%d %H:%M:%S") if log.finished_at else None,
            "metrics": json.loads(log.metrics_json) if log.metrics_json else None,
        }

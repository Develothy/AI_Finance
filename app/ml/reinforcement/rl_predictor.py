"""
강화학습 예측기
===============

.zip 모델 로드 → 최신 피처 조회 → 추론 → 시그널 생성
"""

from datetime import timedelta

import joblib
import numpy as np
import torch
from stable_baselines3 import DQN, PPO

from core import get_logger
from db import database
from models import MLModel
from repositories import MLRepository

logger = get_logger("rl_predictor")

# SB3 모델 매핑
_SB3_MODELS = {
    "dqn": DQN,
    "ppo": PPO,
}

# 액션 → 시그널 매핑
_ACTION_SIGNAL = {0: "HOLD", 1: "BUY", 2: "SELL"}


class RLPredictor:
    """강화학습 모델 예측기."""

    def predict_single(
        self,
        ml_model: MLModel,
        market: str,
        code: str,
    ) -> dict | None:
        """
        단일 종목 RL 예측.

        최신 피처 1행을 로드하여 관찰 벡터 구성 후 액션 추론.

        Returns:
            Predictor._run_prediction()과 동일한 dict 형식
        """
        algorithm = ml_model.algorithm
        model_path = ml_model.model_path  # .zip 경로

        if algorithm not in _SB3_MODELS:
            logger.error(
                f"지원하지 않는 RL 알고리즘: {algorithm}",
                "predict_single",
            )
            return None

        # 1. SB3 모델 로드
        model_cls = _SB3_MODELS[algorithm]
        sb3_model = model_cls.load(model_path.replace(".zip", ""))

        # 2. 메타데이터 로드
        meta_path = model_path.replace(".zip", "_meta.joblib")
        meta = joblib.load(meta_path)
        scaler = meta["scaler"]
        imputer = meta.get("imputer")
        feature_columns = meta["feature_columns"]

        # 3. 최신 피처 조회
        with database.session() as session:
            repo = MLRepository(session)
            latest = repo.get_latest_features(market, code)

        if not latest:
            logger.warning(f"피처 없음: {market}:{code}", "predict_single")
            return None

        # 3.5 외부 피처 로드 (거시지표 + 시장센티먼트)
        from ml.feature_loader import EXTERNAL_FEATURE_NAMES, get_external_features_for_date
        with database.session() as ext_session:
            external_data = get_external_features_for_date(
                ext_session, latest.date, market,
            )

        # 4. 피처 추출
        feature_values = []
        for col in feature_columns:
            if col in EXTERNAL_FEATURE_NAMES:
                val = external_data.get(col)
            else:
                val = getattr(latest, col, None)
            feature_values.append(float(val) if val is not None else np.nan)

        X = np.array([feature_values], dtype=np.float32)

        # 5. NaN imputation
        if np.any(np.isnan(X)):
            nan_count = int(np.isnan(X).sum())
            if imputer:
                logger.warning(
                    f"NaN {nan_count}개 발견, imputer 적용: {market}:{code}",
                    "predict_single",
                )
                X = imputer.transform(X)
            else:
                logger.error(
                    f"NaN {nan_count}개 발견, imputer 없음 — 0 대체: {market}:{code}",
                    "predict_single",
                )
                X = np.nan_to_num(X, nan=0.0)

        # 6. 스케일링
        X = scaler.transform(X)

        # 7. 포지션 정보 추가 (신규 예측 = 현금 100%)
        cash_ratio = np.float32(1.0)
        stock_ratio = np.float32(0.0)
        obs = np.concatenate([X[0], [cash_ratio, stock_ratio]]).astype(np.float32)

        # 8. 추론
        action, _states = sb3_model.predict(obs, deterministic=True)
        action = int(action)

        # 9. confidence 계산
        confidence = self._compute_confidence(sb3_model, obs, algorithm)

        # 10. 시그널 변환
        signal = _ACTION_SIGNAL.get(action, "HOLD")

        # probability_up/down 변환 (기존 API 호환)
        if signal == "BUY":
            probability_up = confidence
            probability_down = 1.0 - confidence
        elif signal == "SELL":
            probability_up = 1.0 - confidence
            probability_down = confidence
        else:
            probability_up = 0.5
            probability_down = 0.5

        # 11. 타겟 날짜 계산
        prediction_date = latest.date
        target_column = ml_model.target_column or "target_class_1d"
        days_ahead = 1 if "1d" in target_column else 5

        try:
            from core.market_calendar import next_trading_day
            current = prediction_date
            for _ in range(days_ahead):
                current = next_trading_day(market, current)
            target_date = current
        except Exception:
            target_date = prediction_date + timedelta(days=days_ahead)

        return {
            "model_id": ml_model.id,
            "model_name": ml_model.model_name,
            "algorithm": ml_model.algorithm,
            "prediction_date": str(prediction_date),
            "target_date": str(target_date),
            "predicted_class": 1 if action == 1 else 0,  # BUY=1, 나머지=0
            "probability_up": round(probability_up, 4),
            "probability_down": round(probability_down, 4),
            "signal": signal,
            "confidence": round(confidence, 4),
        }

    def _compute_confidence(self, model, obs: np.ndarray, algorithm: str) -> float:
        """액션 확신도 계산."""
        try:
            if algorithm == "ppo":
                # PPO: 정책 분포에서 확률 추출
                with torch.no_grad():
                    obs_th, _ = model.policy.obs_to_tensor(obs)
                    dist = model.policy.get_distribution(obs_th)
                    probs = dist.distribution.probs[0].cpu().numpy()
                return float(np.max(probs))

            elif algorithm == "dqn":
                # DQN: Q-value softmax
                with torch.no_grad():
                    obs_th, _ = model.policy.obs_to_tensor(obs)
                    q_values = model.q_net(obs_th)
                    probs = torch.softmax(q_values, dim=1)[0].cpu().numpy()
                return float(np.max(probs))

        except Exception as e:
            logger.warning(
                f"confidence 계산 실패, 기본값 사용: {e}",
                "_compute_confidence",
            )
            return 0.5

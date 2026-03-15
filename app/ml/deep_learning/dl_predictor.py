"""
딥러닝 예측기
=============

.pt 모델 로드 → 시퀀스 구성 → 추론 → 시그널 생성
"""

from datetime import timedelta

import numpy as np
import torch

from core import get_logger
from db import database
from models import MLModel
from repositories import MLRepository

from ..signal_generator import generate_signal
from .architectures import LSTMClassifier, TransformerClassifier

logger = get_logger("dl_predictor")


class DeepLearningPredictor:
    """딥러닝 모델 예측기."""

    def predict_single(
        self,
        ml_model: MLModel,
        market: str,
        code: str,
    ) -> dict | None:
        """
        단일 종목 DL 예측.

        seq_len개의 최근 피처 행을 로드하여 시퀀스 텐서 구성 후 추론.

        Returns:
            Predictor._run_prediction()과 동일한 dict 형식
        """
        # 1. 모델 아티팩트 로드
        saved = torch.load(ml_model.model_path, map_location="cpu", weights_only=False)
        algorithm = saved["algorithm"]
        model_params = saved["model_params"]
        scaler = saved["scaler"]
        imputer = saved.get("imputer")
        feature_columns = saved["feature_columns"]
        n_features = saved["n_features"]
        seq_len = saved["seq_len"]
        target_column = saved.get("target_column", ml_model.target_column)

        # 2. 최근 피처 행 조회 (seq_len개 필요)
        with database.session() as session:
            repo = MLRepository(session)
            rows = repo.get_features(market, code)  # date ASC

        if not rows or len(rows) < seq_len:
            logger.warning(
                f"피처 부족: {market}:{code} "
                f"({len(rows) if rows else 0}행 < seq_len={seq_len})",
                "predict_single",
            )
            return None

        recent_rows = rows[-seq_len:]

        # 3. 피처 배열 구성
        feature_array = []
        for row in recent_rows:
            row_vals = []
            for col in feature_columns:
                val = getattr(row, col, None)
                row_vals.append(float(val) if val is not None else np.nan)
            feature_array.append(row_vals)

        feature_array = np.array(feature_array, dtype=np.float32)  # (seq_len, n_features)

        # 4. NaN imputation
        if np.any(np.isnan(feature_array)):
            nan_count = np.isnan(feature_array).sum()
            if imputer:
                logger.warning(
                    f"NaN {nan_count}개 발견, imputer 적용: {market}:{code}",
                    "predict_single",
                )
                feature_array = imputer.transform(feature_array)
            else:
                logger.error(
                    f"NaN {nan_count}개 발견, imputer 없음 — 0 대체: {market}:{code}",
                    "predict_single",
                )
                feature_array = np.nan_to_num(feature_array, nan=0.0)

        # 5. 스케일링
        feature_array = scaler.transform(feature_array)  # (seq_len, n_features)

        # 6. 텐서 구성
        X = torch.tensor(feature_array, dtype=torch.float32).unsqueeze(0)  # (1, seq_len, n_features)

        # 7. 모델 재구성 + weights 로드
        model = self._build_model(algorithm, n_features, model_params)
        model.load_state_dict(saved["model_state_dict"])
        model.eval()

        # 8. 추론
        with torch.no_grad():
            logits = model(X)                     # (1, 2)
            probs = torch.softmax(logits, dim=1)  # (1, 2)
            predicted_class = int(logits.argmax(dim=1).item())
            probability_up = float(probs[0, 1].item())
            probability_down = float(probs[0, 0].item())

        # 9. 시그널 생성
        signal, confidence = generate_signal(probability_up)

        # 10. 타겟 날짜 계산
        latest_row = recent_rows[-1]
        prediction_date = latest_row.date
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
            "predicted_class": predicted_class,
            "probability_up": round(probability_up, 4),
            "probability_down": round(probability_down, 4),
            "signal": signal,
            "confidence": confidence,
        }

    def _build_model(self, algorithm: str, n_features: int, params: dict):
        """알고리즘별 모델 재구성."""
        if algorithm == "lstm":
            return LSTMClassifier(
                n_features=n_features,
                hidden_size=params["hidden_size"],
                num_layers=params["num_layers"],
                dropout=params["dropout"],
                bidirectional=params.get("bidirectional", True),
            )
        elif algorithm == "transformer":
            return TransformerClassifier(
                n_features=n_features,
                d_model=params["d_model"],
                nhead=params["nhead"],
                num_layers=params["num_layers"],
                dim_feedforward=params["dim_feedforward"],
                dropout=params["dropout"],
            )
        raise ValueError(f"지원하지 않는 DL 알고리즘: {algorithm}")

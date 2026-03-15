"""
예측 파이프라인
===============

활성 모델 로드 → 최신 피처 조회 → 예측 → 시그널 생성 → DB 저장
"""

import json
from datetime import datetime, timedelta

import joblib
import numpy as np
import pandas as pd

from core import get_logger
from db import database
from models import FeatureStore, MLModel
from repositories import MLRepository

from .feature_engineer import PHASE1_FEATURE_COLUMNS
from .signal_generator import generate_signal

logger = get_logger("predictor")


class Predictor:
    """ML 예측기"""

    def predict_single(
        self,
        code: str,
        market: str,
        model_id: int = None,
    ) -> list[dict]:
        """
        단일 종목 예측

        Args:
            code: 종목코드
            market: 마켓
            model_id: 특정 모델 ID (None이면 활성 모델 사용)

        Returns:
            [{"model_id": ..., "signal": ..., "confidence": ..., ...}]
        """
        with database.session() as session:
            repo = MLRepository(session)

            # 1. 모델 조회
            if model_id:
                models = [repo.get_model(model_id)]
                models = [m for m in models if m is not None]
            else:
                models = repo.get_active_models(market=market, model_type="classification")

            if not models:
                logger.warning(f"활성 모델 없음: {market}", "predict_single")
                return []

            # 2. 최신 피처 조회
            latest = repo.get_latest_features(market, code)
            if not latest:
                logger.warning(f"피처 없음: {market}:{code}", "predict_single")
                return []

            results = []
            for ml_model in models:
                try:
                    result = self._run_prediction(ml_model, latest, market, code)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(
                        f"예측 실패: {market}:{code} model={ml_model.model_name} - {e}",
                        "predict_single",
                    )

            # 3. DB 저장
            if results:
                prediction_records = []
                for r in results:
                    prediction_records.append({
                        "model_id": r["model_id"],
                        "market": market,
                        "code": code,
                        "prediction_date": r["prediction_date"],
                        "target_date": r["target_date"],
                        "predicted_class": r["predicted_class"],
                        "probability_up": r["probability_up"],
                        "probability_down": r["probability_down"],
                        "signal": r["signal"],
                        "confidence": r["confidence"],
                    })
                repo.upsert_predictions(prediction_records)

            return results

    def predict_market(self, market: str) -> dict:
        """
        마켓 전체 종목 예측

        Returns:
            {"total": N, "predicted": N, "failed": N, "signals": {"BUY": N, "SELL": N, "HOLD": N}}
        """
        from repositories import StockRepository

        with database.session() as session:
            codes = StockRepository(session).get_codes_by_market(market)

        if not codes:
            return {"total": 0, "predicted": 0, "failed": 0, "signals": {}}

        total = len(codes)
        predicted = 0
        failed = 0
        signal_counts = {"BUY": 0, "SELL": 0, "HOLD": 0}

        for code in codes:
            try:
                results = self.predict_single(code, market)
                if results:
                    predicted += 1
                    for r in results:
                        sig = r.get("signal", "HOLD")
                        signal_counts[sig] = signal_counts.get(sig, 0) + 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f"예측 실패: {market}:{code} - {e}", "predict_market")
                failed += 1

        logger.info(
            f"마켓 예측 완료: {market} (total={total}, predicted={predicted}, signals={signal_counts})",
            "predict_market",
        )

        return {
            "total": total,
            "predicted": predicted,
            "failed": failed,
            "signals": signal_counts,
        }

    def _run_prediction(
        self,
        ml_model: MLModel,
        feature_row: FeatureStore,
        market: str,
        code: str,
    ) -> dict | None:
        """단일 모델로 예측 실행"""
        # DL 모델(.pt)은 DeepLearningPredictor로 위임
        if ml_model.model_path and ml_model.model_path.endswith(".pt"):
            from .deep_learning import DeepLearningPredictor
            return DeepLearningPredictor().predict_single(ml_model, market, code)

        # 1. 모델 파일 로드
        saved = joblib.load(ml_model.model_path)
        model = saved["model"]
        scaler = saved["scaler"]
        feature_columns = saved["feature_columns"]
        target_column = saved.get("target_column", ml_model.target_column)

        # 2. 피처 추출
        feature_values = []
        for col in feature_columns:
            val = getattr(feature_row, col, None)
            if val is None:
                feature_values.append(np.nan)
            else:
                feature_values.append(float(val))

        X = np.array([feature_values])

        # NaN 처리 — imputer가 있으면 사용, 없으면 0 대체
        if np.any(np.isnan(X)):
            nan_cols = [feature_columns[i] for i in range(len(feature_columns)) if np.isnan(X[0][i])]
            logger.warning(
                f"NaN 피처 발견: {market}:{code} ({nan_cols[:5]}...)",
                "_run_prediction",
            )
            imputer = saved.get("imputer")
            if imputer:
                X = imputer.transform(X)
            else:
                logger.error(
                    f"imputer 없음 — NaN을 0으로 대체 (예측 부정확 가능): {market}:{code}",
                    "_run_prediction",
                )
                X = np.nan_to_num(X, nan=0.0)

        # 3. 스케일링
        X_scaled = scaler.transform(X)

        # 4. 예측
        predicted_class = int(model.predict(X_scaled)[0])
        proba = model.predict_proba(X_scaled)[0] if hasattr(model, "predict_proba") else None

        probability_up = float(proba[1]) if proba is not None else (1.0 if predicted_class == 1 else 0.0)
        probability_down = float(proba[0]) if proba is not None else (1.0 if predicted_class == 0 else 0.0)

        # 5. 시그널 생성
        signal, confidence = generate_signal(probability_up)

        # 6. 타겟 날짜 계산 (영업일 기준)
        prediction_date = feature_row.date
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

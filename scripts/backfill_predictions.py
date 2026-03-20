"""
과거 예측 소급 생성 스크립트
=============================

feature_store의 과거 피처를 이용해 활성 모델로 소급 예측을 생성합니다.
모델은 한 번만 로드하고, 배치로 처리하여 속도를 최적화합니다.
"""

import sys
import os
import argparse
from datetime import date, timedelta

import joblib
import numpy as np

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from db.connection import database
from repositories.ml_repository import MLRepository
from models import FeatureStore, MLModel
from ml.predictor import generate_signal


def backfill_predictions(
    market: str,
    codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = 500,
):
    """과거 예측 소급 생성"""

    with database.session() as session:
        ml_repo = MLRepository(session)

        # 1. 활성 모델 로드
        models = ml_repo.get_active_models(market=market, model_type="classification")
        if not models:
            print("활성 모델이 없습니다")
            return

        print(f"활성 모델 {len(models)}개 로드 중...")
        loaded_models = {}
        for m in models:
            try:
                saved = joblib.load(m.model_path)
                loaded_models[m.id] = {
                    "ml_model": m,
                    "model": saved["model"],
                    "scaler": saved["scaler"],
                    "feature_columns": saved["feature_columns"],
                    "target_column": saved.get("target_column", m.target_column),
                    "imputer": saved.get("imputer"),
                }
                print(f"  ✓ {m.model_name} ({m.algorithm})")
            except Exception as e:
                print(f"  ✗ {m.model_name} 로드 실패: {e}")

        if not loaded_models:
            print("로드된 모델이 없습니다")
            return

        # 2. 피처 데이터 조회
        query = session.query(FeatureStore).filter(FeatureStore.market == market)
        if codes:
            query = query.filter(FeatureStore.code.in_(codes))
        if start_date:
            query = query.filter(FeatureStore.date >= start_date)
        if end_date:
            query = query.filter(FeatureStore.date <= end_date)
        query = query.order_by(FeatureStore.date, FeatureStore.code)

        features = query.all()
        print(f"\n피처 {len(features)}건 로드 완료")

        if not features:
            print("피처 데이터가 없습니다")
            return

        # 3. 예측 생성
        predictions = []
        total = len(features) * len(loaded_models)
        done = 0
        errors = 0

        for feat in features:
            for model_id, saved in loaded_models.items():
                done += 1
                try:
                    result = _predict_one(feat, saved)
                    if result:
                        predictions.append({
                            "model_id": model_id,
                            "market": market,
                            "code": feat.code,
                            "prediction_date": feat.date,
                            "target_date": result["target_date"],
                            "predicted_class": result["predicted_class"],
                            "probability_up": result["probability_up"],
                            "probability_down": result["probability_down"],
                            "signal": result["signal"],
                            "confidence": result["confidence"],
                        })
                except Exception:
                    errors += 1

                # 배치 저장
                if len(predictions) >= batch_size:
                    ml_repo.upsert_predictions(predictions)
                    predictions.clear()

                if done % 1000 == 0:
                    pct = done / total * 100
                    print(f"  진행: {done}/{total} ({pct:.1f}%) — 에러: {errors}")

        # 잔여분 저장
        if predictions:
            ml_repo.upsert_predictions(predictions)

        print(f"\n완료: {done}건 처리, {errors}건 에러")


def _predict_one(feat: FeatureStore, saved: dict) -> dict | None:
    """단일 피처 행에 대해 예측 실행"""
    model = saved["model"]
    scaler = saved["scaler"]
    feature_columns = saved["feature_columns"]
    target_column = saved["target_column"]
    imputer = saved["imputer"]

    # 피처 추출
    feature_values = []
    for col in feature_columns:
        val = getattr(feat, col, None)
        if val is None:
            feature_values.append(np.nan)
        else:
            feature_values.append(float(val))

    X = np.array([feature_values])

    # NaN 처리
    if np.any(np.isnan(X)):
        if imputer:
            X = imputer.transform(X)
        else:
            X = np.nan_to_num(X, nan=0.0)

    # 스케일링 + 예측
    X_scaled = scaler.transform(X)
    predicted_class = int(model.predict(X_scaled)[0])
    proba = model.predict_proba(X_scaled)[0] if hasattr(model, "predict_proba") else None

    probability_up = float(proba[1]) if proba is not None else (1.0 if predicted_class == 1 else 0.0)
    probability_down = float(proba[0]) if proba is not None else (1.0 if predicted_class == 0 else 0.0)

    signal, confidence = generate_signal(probability_up)

    # 타겟 날짜 계산
    days_ahead = 1 if "1d" in target_column else 5
    target_date = feat.date + timedelta(days=days_ahead)

    return {
        "predicted_class": predicted_class,
        "probability_up": round(probability_up, 4),
        "probability_down": round(probability_down, 4),
        "signal": signal,
        "confidence": round(confidence, 4),
        "target_date": target_date,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="과거 예측 소급 생성")
    parser.add_argument("--market", default="KOSPI")
    parser.add_argument("--codes", nargs="*", help="종목 코드 (미지정시 전체)")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    backfill_predictions(
        market=args.market,
        codes=args.codes,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
    )

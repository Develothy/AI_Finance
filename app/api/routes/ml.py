"""
ML API 엔드포인트
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    MLFeatureComputeRequest,
    MLFeatureComputeResponse,
    MLTrainRequest,
    MLTrainResponse,
    MLModelResponse,
    MLModelDetailResponse,
    MLPredictResponse,
    MLPredictionItem,
    MLFeatureImportanceResponse,
)
from services import ml_service

router = APIRouter(prefix="/ml", tags=["ML"])


# ============================================================
# 피처
# ============================================================

@router.post("/features/compute", response_model=MLFeatureComputeResponse)
def compute_features(req: MLFeatureComputeRequest):
    """피처 계산 실행"""
    result = ml_service.compute_features(
        market=req.market,
        code=req.code,
        start_date=req.start_date,
        end_date=req.end_date,
    )
    return MLFeatureComputeResponse(**result)


# ============================================================
# 학습
# ============================================================

@router.post("/train", response_model=MLTrainResponse)
def train_model(req: MLTrainRequest):
    """모델 학습 시작"""
    try:
        result = ml_service.train_model(
            market=req.market,
            algorithm=req.algorithm,
            target_column=req.target_column,
            optuna_trials=req.optuna_trials,
        )
        return MLTrainResponse(
            success=True,
            model_id=result["model_id"],
            model_name=result["model_name"],
            metrics=result["metrics"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"학습 실패: {e}")


# ============================================================
# 모델 조회/삭제
# ============================================================

@router.get("/models", response_model=list[MLModelResponse])
def list_models(market: Optional[str] = Query(default=None)):
    """모델 목록"""
    models = ml_service.get_models(market)
    return [MLModelResponse(**m) for m in models]


@router.get("/models/{model_id}", response_model=MLModelDetailResponse)
def get_model(model_id: int):
    """모델 상세"""
    result = ml_service.get_model_detail(model_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"모델 없음: id={model_id}")
    return MLModelDetailResponse(**result)


@router.delete("/models/{model_id}")
def delete_model(model_id: int):
    """모델 삭제"""
    deleted = ml_service.delete_model(model_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"모델 없음: id={model_id}")
    return {"deleted": True, "id": model_id}


# ============================================================
# 예측
# ============================================================

@router.post("/predict/{code}", response_model=MLPredictResponse)
def predict(
    code: str,
    market: str = Query(default="KOSPI"),
    model_id: Optional[int] = Query(default=None),
):
    """특정 종목 예측"""
    results = ml_service.predict(code, market, model_id)
    if not results:
        raise HTTPException(status_code=404, detail=f"예측 불가: {market}:{code} (활성 모델 또는 피처 없음)")
    for r in results:
        if hasattr(r.get("prediction_date"), "strftime"):
            r["prediction_date"] = str(r["prediction_date"])
        if hasattr(r.get("target_date"), "strftime"):
            r["target_date"] = str(r["target_date"])
    return MLPredictResponse(
        code=code,
        market=market,
        predictions=[MLPredictionItem(**r) for r in results],
    )


@router.get("/predictions", response_model=list[MLPredictionItem])
def list_predictions(
    market: Optional[str] = Query(default=None),
    code: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
):
    """예측 결과 조회"""
    predictions = ml_service.get_predictions(market=market, code=code, limit=limit)
    return [MLPredictionItem(**p) for p in predictions]


@router.get("/predictions/{code}", response_model=list[MLPredictionItem])
def get_predictions_by_code(
    code: str,
    market: str = Query(default="KOSPI"),
    limit: int = Query(default=50, le=200),
):
    """종목별 예측 이력"""
    predictions = ml_service.get_predictions(market=market, code=code, limit=limit)
    return [MLPredictionItem(**p) for p in predictions]


# ============================================================
# 피처 중요도
# ============================================================

@router.get("/feature-importance/{model_id}", response_model=MLFeatureImportanceResponse)
def get_feature_importance(model_id: int):
    """피처 중요도"""
    importance = ml_service.get_feature_importance(model_id)
    if importance is None:
        raise HTTPException(status_code=404, detail=f"피처 중요도 없음: model_id={model_id}")
    return MLFeatureImportanceResponse(model_id=model_id, features=importance)

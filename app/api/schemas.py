"""
API Request/Response 스키마
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, model_validator

from models import StockPrice, StockInfo


class Market(str, Enum):
    """지원 마켓"""
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"
    NYSE = "NYSE"
    NASDAQ = "NASDAQ"


# ============================================================
# Request
# ============================================================

class CollectRequest(BaseModel):
    codes: Optional[list[str]] = None
    market: Optional[str] = None
    sector: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: int = 30


# ============================================================
# Response
# ============================================================

class CollectResponse(BaseModel):
    success: bool
    message: str
    total_codes: int
    success_count: int
    failed_count: int
    db_saved_count: int
    elapsed_seconds: float


class StockPriceResponse(BaseModel):
    market: str
    code: str
    date: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]

    @classmethod
    def from_model(cls, p: StockPrice) -> "StockPriceResponse":
        return cls(
            market=p.market,
            code=p.code,
            date=p.date.strftime('%Y-%m-%d'),
            open=float(p.open) if p.open else None,
            high=float(p.high) if p.high else None,
            low=float(p.low) if p.low else None,
            close=float(p.close) if p.close else None,
            volume=int(p.volume) if p.volume else None,
        )


class StockInfoResponse(BaseModel):
    market: str
    code: str
    name: Optional[str]
    sector: Optional[str]
    industry: Optional[str]

    @classmethod
    def from_model(cls, s: StockInfo) -> "StockInfoResponse":
        return cls(
            market=s.market,
            code=s.code,
            name=s.name,
            sector=s.sector,
            industry=s.industry,
        )


# ============================================================
# 기술적 지표 Response
# ============================================================

class SMAResponse(BaseModel):
    date: str
    close: float
    sma: Optional[float] = None


class EMAResponse(BaseModel):
    date: str
    close: float
    ema: Optional[float] = None


class RSIResponse(BaseModel):
    date: str
    close: float
    rsi: Optional[float] = None


class MACDResponse(BaseModel):
    date: str
    close: float
    macd: Optional[float] = None
    signal: Optional[float] = None
    histogram: Optional[float] = None


class BollingerResponse(BaseModel):
    date: str
    close: float
    upper: Optional[float] = None
    middle: Optional[float] = None
    lower: Optional[float] = None


class OBVResponse(BaseModel):
    date: str
    close: float
    volume: int
    obv: Optional[float] = None


class IndicatorSummaryResponse(BaseModel):
    code: str
    market: str
    period: str
    sma_20: list[SMAResponse]
    ema_20: list[EMAResponse]
    rsi_14: list[RSIResponse]
    macd: list[MACDResponse]
    bollinger: list[BollingerResponse]
    obv: list[OBVResponse]


# ============================================================
# 어드민 스키마
# ============================================================

class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float
    started_at: str
    version: str
    python_version: str
    db_type: str


class TableStats(BaseModel):
    row_count: int
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None
    markets: list[str] = []
    code_count: Optional[int] = None
    sector_count: Optional[int] = None


class DBResponse(BaseModel):
    connected: bool
    db_type: str
    error: Optional[str] = None
    tables: Optional[dict[str, TableStats]] = None


class LogEntry(BaseModel):
    time: str
    level: str
    module: str
    function: str
    message: str


class LogResponse(BaseModel):
    file: str
    total: int
    entries: list[LogEntry]


class ConfigGroup(BaseModel):
    items: dict[str, str]


class ConfigResponse(BaseModel):
    warnings: list[str]
    groups: dict[str, ConfigGroup]


class ScheduleJobRequest(BaseModel):
    # --- 공통 필드 ---
    job_name: str
    job_type: str = "data_collect"
    market: str
    sector: Optional[str] = None
    cron_expr: str  # 크론식: "0 18 * * *" (5필드) 또는 "0 0 18 * * *" (6필드, 초 포함)
    days_back: int = 7
    enabled: bool = True
    description: Optional[str] = None

    # --- ML 학습 전용 필드 (job_type="ml_train"일 때만 사용) ---
    ml_markets: list[str] = ["KOSPI", "KOSDAQ"]
    ml_algorithms: list[str] = ["random_forest", "xgboost", "lightgbm"]
    ml_target_days: list[int] = [1, 5]
    ml_include_feature_compute: bool = True
    ml_optuna_trials: int = 50

    @model_validator(mode="after")
    def validate_cron(self):
        parts = self.cron_expr.strip().split()
        if len(parts) not in (5, 6):
            raise ValueError(
                "cron_expr은 5필드(분 시 일 월 요일) 또는 6필드(초 분 시 일 월 요일) 형식이어야 합니다. "
                "예: '0 18 * * *', '0 0 18 * * *', '*/10 * * * *'"
            )
        return self

    @model_validator(mode="after")
    def validate_job_type(self):
        if self.job_type not in ("data_collect", "ml_train"):
            raise ValueError("job_type은 'data_collect' 또는 'ml_train'이어야 합니다.")
        return self


class ScheduleJobResponse(BaseModel):
    id: int
    job_name: str
    job_type: str = "data_collect"
    market: str
    sector: Optional[str]
    cron_expr: str
    days_back: int
    enabled: bool
    description: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
    next_run_time: Optional[str] = None
    # ML 학습 전용 (job_type="ml_train")
    ml_markets: Optional[list[str]] = None
    ml_algorithms: Optional[list[str]] = None
    ml_target_days: Optional[list[int]] = None
    ml_include_feature_compute: Optional[bool] = None
    ml_optuna_trials: Optional[int] = None

    @classmethod
    def from_model(cls, job, next_run: Optional[str] = None, ml_config=None):
        data = dict(
            id=job.id,
            job_name=job.job_name,
            job_type=getattr(job, "job_type", "data_collect"),
            market=job.market,
            sector=job.sector,
            cron_expr=job.cron_expr,
            days_back=job.days_back,
            enabled=job.enabled,
            description=job.description,
            created_at=job.created_at.strftime("%Y-%m-%d %H:%M:%S") if job.created_at else None,
            updated_at=job.updated_at.strftime("%Y-%m-%d %H:%M:%S") if job.updated_at else None,
            next_run_time=next_run,
        )
        if ml_config:
            data["ml_markets"] = ml_config.get_markets()
            data["ml_algorithms"] = ml_config.get_algorithms()
            data["ml_target_days"] = ml_config.get_target_days()
            data["ml_include_feature_compute"] = ml_config.include_feature_compute
            data["ml_optuna_trials"] = ml_config.optuna_trials
        return cls(**data)


class ScheduleLogResponse(BaseModel):
    id: int
    job_id: int
    job_name: Optional[str] = None
    started_at: str
    finished_at: Optional[str]
    status: str
    total_codes: int
    success_count: int
    failed_count: int
    db_saved_count: int
    trigger_by: str
    message: Optional[str]


# ============================================================
# ML 스키마
# ============================================================

class MLFeatureComputeRequest(BaseModel):
    market: str = "KOSPI"
    code: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class MLFeatureComputeResponse(BaseModel):
    market: str
    code: Optional[str] = None
    saved_count: Optional[int] = None
    total: Optional[int] = None
    success: Optional[int] = None
    failed: Optional[int] = None


class MLTrainRequest(BaseModel):
    market: str = "KOSPI"
    algorithm: str = "random_forest"  # random_forest / xgboost / lightgbm
    target_column: str = "target_class_1d"  # target_class_1d / target_class_5d
    optuna_trials: int = 50

    @model_validator(mode="after")
    def validate_algorithm(self):
        valid_algorithms = {"random_forest", "xgboost", "lightgbm"}
        if self.algorithm not in valid_algorithms:
            raise ValueError(f"algorithm은 {valid_algorithms} 중 하나여야 합니다")
        valid_targets = {"target_class_1d", "target_class_5d"}
        if self.target_column not in valid_targets:
            raise ValueError(f"target_column은 {valid_targets} 중 하나여야 합니다")
        return self


class MLTrainResponse(BaseModel):
    success: bool
    model_id: int
    model_name: str
    metrics: dict


class MLModelResponse(BaseModel):
    id: int
    model_name: str
    model_type: str
    algorithm: str
    market: str
    target_column: str
    train_start_date: Optional[str] = None
    train_end_date: Optional[str] = None
    train_sample_count: Optional[int] = None
    accuracy: Optional[float] = None
    precision_score: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    auc_roc: Optional[float] = None
    is_active: bool
    version: int
    created_at: Optional[str] = None


class MLTrainingLogItem(BaseModel):
    id: int
    algorithm: str
    status: str
    train_samples: Optional[int] = None
    val_samples: Optional[int] = None
    feature_count: Optional[int] = None
    optuna_trials: Optional[int] = None
    best_trial_value: Optional[float] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    metrics: Optional[dict] = None


class MLModelDetailResponse(MLModelResponse):
    training_logs: list[MLTrainingLogItem] = []


class MLPredictionItem(BaseModel):
    id: Optional[int] = None
    model_id: int
    model_name: Optional[str] = None
    algorithm: Optional[str] = None
    market: Optional[str] = None
    code: Optional[str] = None
    prediction_date: Optional[str] = None
    target_date: Optional[str] = None
    predicted_class: Optional[int] = None
    probability_up: Optional[float] = None
    probability_down: Optional[float] = None
    signal: Optional[str] = None
    confidence: Optional[float] = None
    created_at: Optional[str] = None


class MLPredictResponse(BaseModel):
    code: str
    market: str
    predictions: list[MLPredictionItem]


class MLFeatureImportanceResponse(BaseModel):
    model_id: int
    features: dict[str, float]

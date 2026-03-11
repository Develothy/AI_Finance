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
    period_count: Optional[int] = None
    active_count: Optional[int] = None
    phase6_count: Optional[int] = None
    phase6_code_count: Optional[int] = None


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


class JobStepRequest(BaseModel):
    step_type: str
    step_order: int
    enabled: bool = True
    config: Optional[dict] = None

    @model_validator(mode="after")
    def validate_step_type(self):
        valid = {"price", "fundamental", "market_investor", "macro", "news",
                 "disclosure", "supply", "alternative", "feature", "ml"}
        if self.step_type not in valid:
            raise ValueError(f"step_type은 {valid} 중 하나여야 합니다")
        return self


class JobStepResponse(BaseModel):
    id: int
    step_type: str
    step_order: int
    enabled: bool
    config: Optional[dict] = None

    @classmethod
    def from_model(cls, step) -> "JobStepResponse":
        return cls(
            id=step.id,
            step_type=step.step_type,
            step_order=step.step_order,
            enabled=step.enabled,
            config=step.get_config(),
        )


class ScheduleJobRequest(BaseModel):
    job_name: str
    market: str
    sector: Optional[str] = None
    cron_expr: str
    days_back: int = 7
    enabled: bool = True
    description: Optional[str] = None
    steps: list[JobStepRequest] = []

    @model_validator(mode="after")
    def validate_cron(self):
        parts = self.cron_expr.strip().split()
        if len(parts) not in (5, 6):
            raise ValueError(
                "cron_expr은 5필드(분 시 일 월 요일) 또는 6필드(초 분 시 일 월 요일) 형식이어야 합니다"
            )
        return self


class ScheduleJobResponse(BaseModel):
    id: int
    job_name: str
    market: str
    sector: Optional[str]
    cron_expr: str
    days_back: int
    enabled: bool
    description: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
    next_run_time: Optional[str] = None
    steps: list[JobStepResponse] = []

    @classmethod
    def from_model(cls, job, next_run: Optional[str] = None, steps: list = None):
        return cls(
            id=job.id,
            job_name=job.job_name,
            market=job.market,
            sector=job.sector,
            cron_expr=job.cron_expr,
            days_back=job.days_back,
            enabled=job.enabled,
            description=job.description,
            created_at=job.created_at.strftime("%Y-%m-%d %H:%M:%S") if job.created_at else None,
            updated_at=job.updated_at.strftime("%Y-%m-%d %H:%M:%S") if job.updated_at else None,
            next_run_time=next_run,
            steps=[JobStepResponse.from_model(s) for s in (steps or [])],
        )


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


# ============================================================
# 재무 데이터 스키마 (Phase 2)
# ============================================================

class FundamentalCollectRequest(BaseModel):
    market: str = "KOSPI"
    codes: Optional[list[str]] = None
    date: Optional[str] = None


class FundamentalCollectResponse(BaseModel):
    market: str
    total: Optional[int] = None
    success: Optional[int] = None
    failed: Optional[int] = None
    saved: int = 0
    skipped: bool = False
    message: str = ""


class FinancialCollectRequest(BaseModel):
    market: str = "KOSPI"
    codes: Optional[list[str]] = None
    year: Optional[int] = None
    quarter: Optional[str] = None

    @model_validator(mode="after")
    def validate_quarter(self):
        if self.quarter and self.quarter not in ("Q1", "Q2", "Q3", "A"):
            raise ValueError("quarter는 'Q1', 'Q2', 'Q3', 'A' 중 하나여야 합니다")
        return self


class FinancialCollectResponse(BaseModel):
    market: str
    year: Optional[int] = None
    quarter: Optional[str] = None
    total: Optional[int] = None
    success: Optional[int] = None
    failed: Optional[int] = None
    saved: int = 0
    skipped: bool = False
    message: str = ""


class StockFundamentalResponse(BaseModel):
    market: str
    code: str
    date: Optional[str] = None
    per: Optional[float] = None
    pbr: Optional[float] = None
    eps: Optional[float] = None
    bps: Optional[float] = None
    market_cap: Optional[int] = None
    div_yield: Optional[float] = None
    foreign_ratio: Optional[float] = None
    inst_net_buy: Optional[int] = None
    foreign_net_buy: Optional[int] = None
    individual_net_buy: Optional[int] = None
    # Phase 5.5: 거래대금 + 매수/매도 거래량
    inst_net_buy_amount: Optional[int] = None
    foreign_net_buy_amount: Optional[int] = None
    individual_net_buy_amount: Optional[int] = None
    inst_buy_vol: Optional[int] = None
    foreign_buy_vol: Optional[int] = None
    individual_buy_vol: Optional[int] = None
    inst_sell_vol: Optional[int] = None
    foreign_sell_vol: Optional[int] = None
    individual_sell_vol: Optional[int] = None


class FinancialStatementResponse(BaseModel):
    market: str
    code: str
    period: str
    period_date: Optional[str] = None
    revenue: Optional[int] = None
    operating_profit: Optional[int] = None
    net_income: Optional[int] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_ratio: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    source: Optional[str] = None


class FundamentalSummaryResponse(BaseModel):
    market: str
    code: str
    fundamental: Optional[StockFundamentalResponse] = None
    financial_statement: Optional[FinancialStatementResponse] = None


# ============================================================
# 시장 투자자매매동향 스키마 (Phase 5.5)
# ============================================================

class MarketInvestorCollectRequest(BaseModel):
    markets: Optional[list[str]] = None  # ["KOSPI", "KOSDAQ"] 기본: 둘 다
    date: Optional[str] = None  # YYYYMMDD, 기본: 직전 거래일


class MarketInvestorCollectResponse(BaseModel):
    markets: list[dict] = []
    saved: int = 0


class MarketInvestorTradingResponse(BaseModel):
    market: str
    date: Optional[str] = None
    foreign_net_buy_qty: Optional[int] = None
    inst_net_buy_qty: Optional[int] = None
    individual_net_buy_qty: Optional[int] = None
    foreign_net_buy_amount: Optional[int] = None
    inst_net_buy_amount: Optional[int] = None
    individual_net_buy_amount: Optional[int] = None


# ============================================================
# 거시경제 지표 스키마 (Phase 3)
# ============================================================

class MacroCollectRequest(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days_back: int = 30


class MacroCollectResponse(BaseModel):
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    saved: int = 0
    message: str = ""


class MacroIndicatorResponse(BaseModel):
    indicator_name: str
    date: Optional[str] = None
    value: Optional[float] = None
    change_pct: Optional[float] = None
    source: Optional[str] = None


# ============================================================
# 뉴스 센티먼트 스키마 (Phase 4)
# ============================================================

class NewsCollectRequest(BaseModel):
    market: str = "KR"
    codes: Optional[list[list[str]]] = None  # [["005930", "삼성전자"], ...]
    include_market_news: bool = True
    max_items_per_code: int = 50


class NewsCollectResponse(BaseModel):
    total_codes: int = 0
    stock_success: int = 0
    stock_failed: int = 0
    market_news: int = 0
    saved: int = 0
    message: str = ""


class NewsArticleResponse(BaseModel):
    id: Optional[int] = None
    date: Optional[str] = None
    market: Optional[str] = None
    code: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    source: Optional[str] = None
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None


class NewsSentimentSummaryResponse(BaseModel):
    date: Optional[str] = None
    sentiment: Optional[float] = None
    volume: Optional[int] = None


# ============================================================
# 공시 + 수급 스키마 (Phase 5)
# ============================================================

class DisclosureCollectRequest(BaseModel):
    market: str = "KOSPI"
    codes: Optional[list[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: int = 60
    analyze_sentiment: bool = True


class DisclosureCollectResponse(BaseModel):
    market: str
    total: Optional[int] = None
    success: Optional[int] = None
    failed: Optional[int] = None
    saved: int = 0
    message: str = ""


class SupplyDemandCollectRequest(BaseModel):
    market: str = "KOSPI"
    codes: Optional[list[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: int = 60


class SupplyDemandCollectResponse(BaseModel):
    market: str
    total: Optional[int] = None
    success: Optional[int] = None
    failed: Optional[int] = None
    saved: int = 0
    message: str = ""


class DisclosureResponse(BaseModel):
    id: Optional[int] = None
    date: Optional[str] = None
    market: Optional[str] = None
    code: Optional[str] = None
    corp_name: Optional[str] = None
    report_nm: Optional[str] = None
    rcept_no: Optional[str] = None
    flr_nm: Optional[str] = None
    rcept_dt: Optional[str] = None
    report_type: Optional[str] = None
    type_score: Optional[float] = None
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None


class SupplyDemandResponse(BaseModel):
    id: Optional[int] = None
    date: Optional[str] = None
    market: Optional[str] = None
    code: Optional[str] = None
    short_selling_volume: Optional[int] = None
    short_selling_ratio: Optional[float] = None
    program_buy_volume: Optional[int] = None
    program_sell_volume: Optional[int] = None
    program_net_volume: Optional[int] = None
    source: Optional[str] = None


# ============================================================
# 즉시실행 Request
# ============================================================

class RunJobRequest(BaseModel):
    # 스케줄 즉시실행 시
    base_date: Optional[str] = None  # "YYYY-MM-DD" 또는 None(=오늘)

    @model_validator(mode="after")
    def validate_base_date(self):
        if self.base_date:
            from datetime import datetime
            try:
                datetime.strptime(self.base_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError("base_date는 'YYYY-MM-DD' 형식이어야 합니다")
        return self

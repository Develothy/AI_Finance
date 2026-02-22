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
    job_name: str
    market: str
    sector: Optional[str] = None
    cron_expr: str  # 크론식: "0 18 * * *" (5필드) 또는 "0 0 18 * * *" (6필드, 초 포함)
    days_back: int = 7
    enabled: bool = True
    description: Optional[str] = None

    @model_validator(mode="after")
    def validate_cron(self):
        parts = self.cron_expr.strip().split()
        if len(parts) not in (5, 6):
            raise ValueError(
                "cron_expr은 5필드(분 시 일 월 요일) 또는 6필드(초 분 시 일 월 요일) 형식이어야 합니다. "
                "예: '0 18 * * *', '0 0 18 * * *', '*/10 * * * *'"
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

    @classmethod
    def from_model(cls, job, next_run: Optional[str] = None):
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

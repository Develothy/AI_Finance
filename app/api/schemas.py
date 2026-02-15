"""
API Request/Response 스키마
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel

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

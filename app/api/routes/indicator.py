"""
기술적 지표 엔드포인트
"""

from typing import Optional

from fastapi import APIRouter, Query

from api.schemas import (
    SMAResponse,
    EMAResponse,
    RSIResponse,
    MACDResponse,
    BollingerResponse,
    OBVResponse,
    IndicatorSummaryResponse,
)
from services import indicator_service

router = APIRouter(prefix="/indicators", tags=["기술적 지표"])


@router.get("/sma/{code}", response_model=list[SMAResponse])
def get_sma(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    period: int = Query(default=20, ge=2, description="이동평균 기간"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    단순 이동평균 (SMA)

    - period: 이동평균 기간 (default 20)
    """
    return indicator_service.get_sma(code, market, period, start_date, end_date)


@router.get("/ema/{code}", response_model=list[EMAResponse])
def get_ema(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    period: int = Query(default=20, ge=2, description="이동평균 기간"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    지수 이동평균 (EMA)

    - period: 이동평균 기간 (default 20)
    """
    return indicator_service.get_ema(code, market, period, start_date, end_date)


@router.get("/rsi/{code}", response_model=list[RSIResponse])
def get_rsi(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    period: int = Query(default=14, ge=2, description="RSI 기간"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    상대강도지수 (RSI)

    - period: RSI 기간 (default 14)
    - 70 이상: 과매수, 30 이하: 과매도
    """
    return indicator_service.get_rsi(code, market, period, start_date, end_date)


@router.get("/macd/{code}", response_model=list[MACDResponse])
def get_macd(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    fast: int = Query(default=12, ge=2, description="단기 EMA 기간"),
    slow: int = Query(default=26, ge=2, description="장기 EMA 기간"),
    signal: int = Query(default=9, ge=2, description="시그널 라인 기간"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    MACD (Moving Average Convergence Divergence)

    - macd: 단기 EMA - 장기 EMA
    - signal: MACD의 EMA
    - histogram: MACD - Signal (양수: 상승 모멘텀)
    """
    return indicator_service.get_macd(code, market, fast, slow, signal, start_date, end_date)


@router.get("/bollinger/{code}", response_model=list[BollingerResponse])
def get_bollinger(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    period: int = Query(default=20, ge=2, description="이동평균 기간"),
    num_std: float = Query(default=2.0, gt=0, description="표준편차 배수"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    볼린저밴드 (Bollinger Bands)

    - upper: 상단밴드 (중심선 + 표준편차 * num_std)
    - middle: 중심선 (SMA)
    - lower: 하단밴드 (중심선 - 표준편차 * num_std)
    """
    return indicator_service.get_bollinger(code, market, period, num_std, start_date, end_date)


@router.get("/obv/{code}", response_model=list[OBVResponse])
def get_obv(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    OBV (On Balance Volume)

    가격 상승 시 거래량 누적, 하락 시 차감.
    OBV 상승 추세: 매집, 하락 추세: 분산.
    """
    return indicator_service.get_obv(code, market, start_date, end_date)


@router.get("/summary/{code}", response_model=IndicatorSummaryResponse)
def get_summary(
    code: str,
    market: str = Query(default="KOSPI", description="KOSPI, KOSDAQ, NYSE, NASDAQ"),
    start_date: Optional[str] = Query(default=None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="종료일 (YYYY-MM-DD)"),
):
    """
    전체 기술적 지표 요약

    SMA(20), EMA(20), RSI(14), MACD(12,26,9), 볼린저밴드(20,2), OBV를 한번에 조회.
    """
    return indicator_service.get_summary(code, market, start_date, end_date)

"""
기술적 지표 서비스
"""

from typing import Optional

import pandas as pd
import numpy as np
from fastapi import HTTPException

from db import database
from repositories import StockRepository
from core import get_logger, log_execution
from indicators import (
    calc_sma,
    calc_ema,
    calc_rsi,
    calc_macd,
    calc_bollinger_bands,
    calc_obv,
)
from api.schemas import (
    SMAResponse,
    EMAResponse,
    RSIResponse,
    MACDResponse,
    BollingerResponse,
    OBVResponse,
    IndicatorSummaryResponse,
)

logger = get_logger("indicator_service")


def _to_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame을 JSON-safe dict 리스트로 변환"""
    result = df.copy()

    # date 컬럼 문자열 변환
    if 'date' in result.columns:
        result['date'] = result['date'].astype(str)

    # NaN → None 변환
    return result.replace({np.nan: None}).to_dict(orient='records')


class IndicatorService:

    def __init__(self, db=None):
        self.database = db or database

    def _get_price_df(
        self,
        code: str,
        market: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """DB에서 주가 데이터를 조회하여 DataFrame으로 반환"""
        with self.database.session() as session:
            repo = StockRepository(session)
            prices = repo.get_prices(code, market, start_date, end_date)

            if not prices:
                raise HTTPException(
                    status_code=404,
                    detail=f"주가 데이터 없음: {market}:{code}"
                )

            records = []
            for p in prices:
                records.append({
                    'date': p.date,
                    'open': float(p.open) if p.open else 0.0,
                    'high': float(p.high) if p.high else 0.0,
                    'low': float(p.low) if p.low else 0.0,
                    'close': float(p.close) if p.close else 0.0,
                    'volume': int(p.volume) if p.volume else 0,
                })

        return pd.DataFrame(records)

    # ── SMA ────────────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_sma(
        self,
        code: str,
        market: str,
        period: int,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> list[SMAResponse]:
        df = self._get_price_df(code, market, start_date, end_date)
        result = calc_sma(df, period)
        return [SMAResponse(**r) for r in _to_records(result)]

    # ── EMA ────────────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_ema(
        self,
        code: str,
        market: str,
        period: int,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> list[EMAResponse]:
        df = self._get_price_df(code, market, start_date, end_date)
        result = calc_ema(df, period)
        return [EMAResponse(**r) for r in _to_records(result)]

    # ── RSI ────────────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_rsi(
        self,
        code: str,
        market: str,
        period: int,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> list[RSIResponse]:
        df = self._get_price_df(code, market, start_date, end_date)
        result = calc_rsi(df, period)
        return [RSIResponse(**r) for r in _to_records(result)]

    # ── MACD ───────────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_macd(
        self,
        code: str,
        market: str,
        fast: int,
        slow: int,
        signal: int,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> list[MACDResponse]:
        df = self._get_price_df(code, market, start_date, end_date)
        result = calc_macd(df, fast, slow, signal)
        return [MACDResponse(**r) for r in _to_records(result)]

    # ── 볼린저밴드 ─────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_bollinger(
        self,
        code: str,
        market: str,
        period: int,
        num_std: float,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> list[BollingerResponse]:
        df = self._get_price_df(code, market, start_date, end_date)
        result = calc_bollinger_bands(df, period, num_std)
        return [BollingerResponse(**r) for r in _to_records(result)]

    # ── OBV ────────────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_obv(
        self,
        code: str,
        market: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> list[OBVResponse]:
        df = self._get_price_df(code, market, start_date, end_date)
        result = calc_obv(df)
        return [OBVResponse(**r) for r in _to_records(result)]

    # ── 전체 요약 ──────────────────────────────────────────

    @log_execution(module="indicator_service")
    def get_summary(
        self,
        code: str,
        market: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> IndicatorSummaryResponse:
        df = self._get_price_df(code, market, start_date, end_date)

        dates = df['date']
        period_str = f"{dates.iloc[0]} ~ {dates.iloc[-1]}"

        return IndicatorSummaryResponse(
            code=code,
            market=market,
            period=period_str,
            sma_20=[SMAResponse(**r) for r in _to_records(calc_sma(df, 20))],
            ema_20=[EMAResponse(**r) for r in _to_records(calc_ema(df, 20))],
            rsi_14=[RSIResponse(**r) for r in _to_records(calc_rsi(df, 14))],
            macd=[MACDResponse(**r) for r in _to_records(calc_macd(df))],
            bollinger=[BollingerResponse(**r) for r in _to_records(calc_bollinger_bands(df))],
            obv=[OBVResponse(**r) for r in _to_records(calc_obv(df))],
        )

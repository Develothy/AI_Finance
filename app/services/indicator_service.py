"""
기술적 지표 서비스
"""

from typing import Optional
from datetime import datetime

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


def _validate_date(date_str: Optional[str], field_name: str = "날짜") -> Optional[str]:
    """날짜 문자열 형식(YYYY-MM-DD) 검증"""
    if date_str is None:
        return None
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"잘못된 {field_name} 형식: '{date_str}' (YYYY-MM-DD 형식 필요)",
        )


def _to_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame을 JSON-safe dict 리스트로 변환"""
    result = df.copy()

    # date 컬럼 문자열 변환
    if 'date' in result.columns:
        result['date'] = result['date'].astype(str)

    # NaN → None 변환
    return result.replace({np.nan: None}).to_dict(orient='records')


def _check_data_sufficiency(df: pd.DataFrame, min_rows: int, indicator_name: str):
    """지표 계산에 필요한 최소 데이터 수 검증"""
    if len(df) < min_rows:
        raise HTTPException(
            status_code=400,
            detail=f"{indicator_name} 계산에 데이터 부족: {len(df)}개 (최소 {min_rows}개 필요)",
        )


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
        _validate_date(start_date, "시작일")
        _validate_date(end_date, "종료일")

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
                    'open': float(p.open) if p.open is not None else None,
                    'high': float(p.high) if p.high is not None else None,
                    'low': float(p.low) if p.low is not None else None,
                    'close': float(p.close) if p.close is not None else None,
                    'volume': int(p.volume) if p.volume is not None else 0,
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
        _check_data_sufficiency(df, period, "SMA")
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
        _check_data_sufficiency(df, period, "EMA")
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
        _check_data_sufficiency(df, period + 1, "RSI")
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
        _check_data_sufficiency(df, slow + signal, "MACD")
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
        _check_data_sufficiency(df, period, "볼린저밴드")
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
        limit: Optional[int] = None,
    ) -> IndicatorSummaryResponse:
        df = self._get_price_df(code, market, start_date, end_date)

        dates = df['date']
        period_str = f"{dates.iloc[0]} ~ {dates.iloc[-1]}"

        def _build(indicator_df: pd.DataFrame, model_cls):
            records = _to_records(indicator_df)
            if limit is not None:
                records = records[-limit:]
            return [model_cls(**r) for r in records]

        return IndicatorSummaryResponse(
            code=code,
            market=market,
            period=period_str,
            sma_20=_build(calc_sma(df, 20), SMAResponse),
            ema_20=_build(calc_ema(df, 20), EMAResponse),
            rsi_14=_build(calc_rsi(df, 14), RSIResponse),
            macd=_build(calc_macd(df), MACDResponse),
            bollinger=_build(calc_bollinger_bands(df), BollingerResponse),
            obv=_build(calc_obv(df), OBVResponse),
        )

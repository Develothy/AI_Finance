"""
이동평균 지표 (SMA, EMA)
"""

import pandas as pd


def calc_sma(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """
    단순 이동평균 (Simple Moving Average)

    Args:
        df: OHLCV DataFrame (date, close 필수)
        period: 이동평균 기간

    Returns:
        DataFrame with columns: date, close, sma
    """
    result = df[['date', 'close']].copy()
    result['sma'] = df['close'].rolling(window=period).mean()
    return result


def calc_ema(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """
    지수 이동평균 (Exponential Moving Average)

    Args:
        df: OHLCV DataFrame (date, close 필수)
        period: 이동평균 기간

    Returns:
        DataFrame with columns: date, close, ema
    """
    result = df[['date', 'close']].copy()
    result['ema'] = df['close'].ewm(span=period, adjust=False).mean()
    return result

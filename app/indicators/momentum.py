"""
모멘텀 지표 (RSI, MACD)
"""

import pandas as pd
import numpy as np


def calc_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    상대강도지수 (Relative Strength Index) - Wilder's smoothing

    Args:
        df: OHLCV DataFrame (date, close 필수)
        period: RSI 기간 (default 14)

    Returns:
        DataFrame with columns: date, close, rsi
    """
    result = df[['date', 'close']].copy()

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    # Wilder's smoothing (EWM with alpha=1/period)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    # avg_loss=0 (연속 상승) → RSI=100, avg_gain=0 (연속 하락) → RSI=0
    # 둘 다 0 (변동 없음) → RSI=50
    both_zero = (avg_gain == 0) & (avg_loss == 0)
    only_loss_zero = (avg_gain > 0) & (avg_loss == 0)
    only_gain_zero = (avg_gain == 0) & (avg_loss > 0)

    rsi = rsi.where(~only_loss_zero, 100.0)
    rsi = rsi.where(~only_gain_zero, 0.0)
    rsi = rsi.where(~both_zero, 50.0)

    result['rsi'] = rsi

    return result


def calc_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    MACD (Moving Average Convergence Divergence)

    Args:
        df: OHLCV DataFrame (date, close 필수)
        fast: 단기 EMA 기간 (default 12)
        slow: 장기 EMA 기간 (default 26)
        signal: 시그널 라인 기간 (default 9)

    Returns:
        DataFrame with columns: date, close, macd, signal, histogram
    """
    result = df[['date', 'close']].copy()

    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    result['macd'] = ema_fast - ema_slow
    result['signal'] = result['macd'].ewm(span=signal, adjust=False).mean()
    result['histogram'] = result['macd'] - result['signal']

    return result

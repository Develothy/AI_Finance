"""
변동성 지표 (볼린저밴드)
"""

import pandas as pd


def calc_bollinger_bands(
    df: pd.DataFrame,
    period: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    """
    볼린저밴드 (Bollinger Bands)

    Args:
        df: OHLCV DataFrame (date, close 필수)
        period: 이동평균 기간 (default 20)
        num_std: 표준편차 배수 (default 2.0)

    Returns:
        DataFrame with columns: date, close, upper, middle, lower
    """
    result = df[['date', 'close']].copy()

    result['middle'] = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()
    result['upper'] = result['middle'] + (rolling_std * num_std)
    result['lower'] = result['middle'] - (rolling_std * num_std)

    return result

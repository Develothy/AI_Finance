"""
거래량 지표 (OBV)
"""

import pandas as pd
import numpy as np


def calc_obv(df: pd.DataFrame) -> pd.DataFrame:
    """
    OBV (On Balance Volume)

    가격 상승일에는 거래량을 더하고, 하락일에는 빼서 누적.

    Args:
        df: OHLCV DataFrame (date, close, volume 필수)

    Returns:
        DataFrame with columns: date, close, volume, obv
    """
    result = df[['date', 'close', 'volume']].copy()

    direction = np.sign(df['close'].diff())
    result['obv'] = (direction * df['volume']).fillna(0).cumsum()

    return result

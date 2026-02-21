"""상수 정의"""

MARKETS = ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"]

MARKET_LABELS = {
    "KOSPI": "코스피 (KOSPI)",
    "KOSDAQ": "코스닥 (KOSDAQ)",
    "NYSE": "뉴욕증권거래소 (NYSE)",
    "NASDAQ": "나스닥 (NASDAQ)",
}

INDICATOR_COLORS = {
    "sma": "#FF6B6B",
    "ema": "#4ECDC4",
    "bollinger_upper": "#95E1D3",
    "bollinger_lower": "#95E1D3",
    "macd": "#2196F3",
    "signal": "#FF9800",
    "histogram_pos": "#4CAF50",
    "histogram_neg": "#F44336",
    "rsi": "#9C27B0",
    "obv": "#607D8B",
}

# 한국 컨벤션: 빨강=상승, 파랑=하락
CANDLESTICK_COLORS = {
    "increasing": "#FF3B3B",
    "decreasing": "#1261C4",
}

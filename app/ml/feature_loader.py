"""
외부 피처 로딩 유틸리티
=======================

feature_store 정규화 후, 거시지표(macro_indicator)와 시장센티먼트(news_sentiment)는
feature_store에 저장하지 않고 학습/예측 시 원본 테이블에서 로드하여 병합한다.
"""

from datetime import timedelta

import pandas as pd

from core import get_logger

logger = get_logger("feature_loader")

# ============================================================
# 외부 피처 이름 상수
# ============================================================

MACRO_FEATURE_NAMES = [
    "krw_usd", "vix", "kospi_index",
    "us_10y", "kr_3y", "sp500", "wti", "gold",
    "fed_rate", "usd_index", "us_cpi",
]

MARKET_SENTIMENT_FEATURE_NAMES = [
    "market_sentiment", "market_news_volume",
]

EXTERNAL_FEATURE_NAMES = MACRO_FEATURE_NAMES + MARKET_SENTIMENT_FEATURE_NAMES

# indicator_name → 피처명 매핑
_MACRO_COLUMN_MAP = {
    "KRW_USD": "krw_usd",
    "VIX": "vix",
    "KOSPI": "kospi_index",
    "SP500": "sp500",
    "US_10Y": "us_10y",
    "KR_3Y": "kr_3y",
    "WTI": "wti",
    "GOLD": "gold",
    "FED_RATE": "fed_rate",
    "USD_INDEX": "usd_index",
    "US_CPI": "us_cpi",
}

# 월별/분기별 지표 — forward-fill 대상
_MACRO_FFILL_COLUMNS = {"us_cpi"}


# ============================================================
# DataFrame 로딩 (학습용 — 날짜 범위)
# ============================================================

def load_macro_df(session, start_date, end_date) -> pd.DataFrame:
    """macro_indicator에서 피벗된 거시지표 DataFrame 반환.

    Returns:
        columns: [date, krw_usd, vix, kospi_index, ...]
    """
    from repositories import MacroRepository

    rows = MacroRepository(session).get_all_by_date_range(
        start_date=str(start_date), end_date=str(end_date),
    )

    if not rows:
        return pd.DataFrame(columns=["date"] + MACRO_FEATURE_NAMES)

    macro_df = pd.DataFrame([{
        "date": r.date,
        "indicator_name": r.indicator_name,
        "value": float(r.value) if r.value else None,
    } for r in rows])

    pivot = macro_df.pivot_table(
        index="date", columns="indicator_name",
        values="value", aggfunc="first",
    )

    rename = {k: v for k, v in _MACRO_COLUMN_MAP.items() if k in pivot.columns}
    pivot = pivot.rename(columns=rename)
    pivot = pivot.reset_index()

    # 월별 지표 forward-fill
    for col in _MACRO_FFILL_COLUMNS:
        if col in pivot.columns:
            pivot[col] = pivot[col].ffill()

    # 매핑된 컬럼만 유지
    keep = ["date"] + [v for v in _MACRO_COLUMN_MAP.values() if v in pivot.columns]
    pivot = pivot[[c for c in keep if c in pivot.columns]]

    return pivot


def load_market_sentiment_df(session, start_date, end_date, market: str) -> pd.DataFrame:
    """news_sentiment에서 시장 센티먼트 DataFrame 반환.

    Returns:
        columns: [date, market_sentiment, market_news_volume]
    """
    from repositories.news_repository import NewsRepository

    rows = NewsRepository(session).get_daily_market_sentiment(
        start_date, end_date, market,
    )

    if not rows:
        return pd.DataFrame(columns=["date"] + MARKET_SENTIMENT_FEATURE_NAMES)

    return pd.DataFrame(rows)


def merge_external_features(
    df: pd.DataFrame,
    session,
    market: str,
    feature_columns: list[str],
) -> pd.DataFrame:
    """DataFrame에 외부 피처(거시지표 + 시장센티먼트)를 병합.

    feature_columns에 포함된 외부 피처만 병합한다.
    """
    needed_macro = [c for c in MACRO_FEATURE_NAMES if c in feature_columns]
    needed_market = [c for c in MARKET_SENTIMENT_FEATURE_NAMES if c in feature_columns]

    if not needed_macro and not needed_market:
        return df

    if df.empty:
        return df

    min_date = df["date"].min()
    max_date = df["date"].max()

    if needed_macro:
        macro_df = load_macro_df(session, min_date, max_date)
        if not macro_df.empty:
            df = df.merge(macro_df, on="date", how="left")
            # merge 후 월별 지표 forward-fill
            for col in _MACRO_FFILL_COLUMNS:
                if col in df.columns:
                    df[col] = df[col].ffill()

    if needed_market:
        extended_min = min_date - timedelta(days=3)
        market_df = load_market_sentiment_df(session, extended_min, max_date, market)
        if not market_df.empty:
            from .feature_engineer import FeatureEngineer
            market_df = FeatureEngineer._snap_to_trading_day(
                market_df, market,
                ["market_sentiment", "market_news_volume"],
            )
            df = df.merge(market_df, on="date", how="left")

    # 컬럼 보장
    for col in needed_macro + needed_market:
        if col not in df.columns:
            df[col] = None

    return df


# ============================================================
# 단일 날짜 로딩 (예측용)
# ============================================================

def get_external_features_for_date(session, target_date, market: str) -> dict:
    """단일 날짜의 외부 피처 값을 dict로 반환 (예측용).

    최대 7일 이내의 가장 최근 값을 사용 (주말/공휴일 대응).
    """
    result = {}
    lookback_start = target_date - timedelta(days=7)

    # 거시지표
    macro_df = load_macro_df(session, lookback_start, target_date)
    if not macro_df.empty:
        latest = macro_df.iloc[-1]
        for col in MACRO_FEATURE_NAMES:
            if col in latest.index and pd.notna(latest[col]):
                result[col] = float(latest[col])

    # 시장 센티먼트
    market_df = load_market_sentiment_df(session, lookback_start, target_date, market)
    if not market_df.empty:
        # 주말 뉴스를 거래일로 매핑
        from .feature_engineer import FeatureEngineer
        market_df = FeatureEngineer._snap_to_trading_day(
            market_df, market,
            ["market_sentiment", "market_news_volume"],
        )
        if not market_df.empty:
            latest = market_df.iloc[-1]
            if pd.notna(latest.get("market_sentiment")):
                result["market_sentiment"] = float(latest["market_sentiment"])
            if pd.notna(latest.get("market_news_volume")):
                result["market_news_volume"] = float(latest["market_news_volume"])

    return result

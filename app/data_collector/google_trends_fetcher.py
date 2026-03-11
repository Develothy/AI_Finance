"""
Google Trends 데이터 수집 (Phase 7A)
====================================

pytrends로 종목명 기반 Google Trends 검색량을 수집한다.
주간 데이터 → 일별 선형 보간.
"""

import time
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd

from core import get_logger
from core.decorators import retry

logger = get_logger("google_trends_fetcher")

_RATE_LIMIT_DELAY = 2.0


@dataclass
class GoogleTrendsFetchResult:
    records: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    message: str = ""


class GoogleTrendsFetcher:
    """Google Trends 수집기 (pytrends)"""

    def __init__(self):
        self.available = True
        try:
            from pytrends.request import TrendReq
            self._TrendReq = TrendReq
        except ImportError:
            self.available = False
            logger.warning("pytrends 미설치 — Google Trends 수집 비활성화", "__init__")

    @retry(max_attempts=3, delay=3.0, backoff=2.0, module="google_trends")
    def _fetch_trend(self, keyword: str, start_date: str, end_date: str) -> pd.DataFrame:
        """단일 키워드 Google Trends 수집 (주간 DataFrame)"""
        pytrends = self._TrendReq(hl="ko", tz=540)
        timeframe = f"{start_date} {end_date}"

        pytrends.build_payload(kw_list=[keyword], cat=0, timeframe=timeframe, geo="KR")
        df = pytrends.interest_over_time()
        time.sleep(_RATE_LIMIT_DELAY)

        if df.empty:
            return pd.DataFrame(columns=["date", "value"])

        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        df = df.reset_index()
        df = df.rename(columns={"date": "date", keyword: "value"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df[["date", "value"]]

    def _interpolate_to_daily(
        self, weekly_df: pd.DataFrame, start_dt: date, end_dt: date,
    ) -> pd.DataFrame:
        """주간 → 일별 선형 보간"""
        if weekly_df.empty:
            return pd.DataFrame(columns=["date", "google_trend_value", "google_trend_interpolated"])

        wdf = weekly_df.copy()
        wdf["date"] = pd.to_datetime(wdf["date"])
        wdf = wdf.set_index("date")

        daily_idx = pd.date_range(start=start_dt, end=end_dt, freq="D")
        daily_df = wdf.reindex(daily_idx)
        daily_df["value"] = daily_df["value"].interpolate(method="linear")

        result = daily_df.reset_index().rename(columns={"index": "date"})
        result["date"] = result["date"].dt.date

        weekly_dates = set(wdf.index.date)
        result["google_trend_value"] = result.apply(
            lambda r: r["value"] if r["date"] in weekly_dates else None, axis=1,
        )
        result["google_trend_interpolated"] = result["value"]
        return result[["date", "google_trend_value", "google_trend_interpolated"]]

    def fetch_stock_trend(
        self, code: str, stock_name: str, market: str = "KR",
        start_date: str = None, end_date: str = None, days: int = 90,
    ) -> list[dict]:
        """단일 종목 Google Trends 수집 + 일별 보간"""
        if not self.available:
            return []

        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")

        try:
            weekly_df = self._fetch_trend(stock_name, start_date, end_date)
        except Exception as e:
            logger.warning(f"Trends 수집 실패: {stock_name} ({code}) - {e}", "fetch_stock_trend")
            return []

        if weekly_df.empty:
            return []

        daily_df = self._interpolate_to_daily(
            weekly_df, date.fromisoformat(start_date), date.fromisoformat(end_date),
        )

        records = []
        for _, row in daily_df.iterrows():
            records.append({
                "date": row["date"],
                "market": market,
                "code": code,
                "google_trend_value": (
                    round(float(row["google_trend_value"]), 2)
                    if row["google_trend_value"] is not None and pd.notna(row["google_trend_value"])
                    else None
                ),
                "google_trend_interpolated": (
                    round(float(row["google_trend_interpolated"]), 2)
                    if pd.notna(row["google_trend_interpolated"])
                    else None
                ),
            })
        return records

    def fetch_all(
        self, codes_with_names: list[tuple[str, str]], market: str,
        start_date: str = None, end_date: str = None, days: int = 90,
    ) -> GoogleTrendsFetchResult:
        """전체 종목 Google Trends 수집"""
        result = GoogleTrendsFetchResult(total_count=len(codes_with_names))

        if not self.available:
            result.message = "pytrends 미설치 — 건너뜀"
            return result

        logger.info(f"Google Trends 수집 시작: {market} ({len(codes_with_names)}종목)", "fetch_all")

        for code, name in codes_with_names:
            try:
                records = self.fetch_stock_trend(code, name, market, start_date, end_date, days)
                result.records.extend(records)
                result.success_count += 1
            except Exception as e:
                result.failed_count += 1
                logger.warning(f"Trends 실패: {code} ({name}) - {e}", "fetch_all")

        result.message = f"Trends 완료: {result.success_count}/{result.total_count} 성공, {len(result.records)}건"
        logger.info(result.message, "fetch_all")
        return result

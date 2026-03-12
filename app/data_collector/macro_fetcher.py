"""
거시경제 지표 수집 (Phase 3)
===========================

yfinance: KRW/USD, VIX, KOSPI, S&P500, WTI, Gold, US 10Y (7개)
FinanceDataReader: KR 3Y 국고채 (1개)
FRED: 기준금리(DFF), 달러인덱스(DTWEXBGS), CPI(CPIAUCSL) (3개)
"""

from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass, field

import pandas as pd

from config import settings
from core import get_logger
from core.decorators import retry

logger = get_logger("macro_fetcher")

# indicator_name → feature_store 컬럼 매핑
INDICATOR_TO_COLUMN = {
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


@dataclass
class MacroFetchResult:
    """거시지표 수집 결과"""
    records: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    skipped_indicators: list[str] = field(default_factory=list)
    message: str = ""


class MacroFetcher:
    """거시경제 지표 수집기"""

    # yfinance 심볼 → indicator_name (개별 다운로드)
    YFINANCE_SYMBOLS = {
        "KRW=X": "KRW_USD",
        "^VIX": "VIX",
        "^KS11": "KOSPI",
        "^GSPC": "SP500",
        "CL=F": "WTI",
        "GC=F": "GOLD",
        "^TNX": "US_10Y",       # FRED DGS10 대체 (동일 데이터, SSL 이슈 없음)
    }

    # FinanceDataReader 심볼 (INVESTING: 프리픽스로 Investing.com 강제)
    FDR_SYMBOLS = {"INVESTING:KR3YT=RR": "KR_3Y"}

    # FRED series_id → indicator_name
    FRED_SERIES = {
        "DFF": "FED_RATE",          # 미국 기준금리 (일별)
        "DTWEXBGS": "USD_INDEX",    # 달러 인덱스 (일별)
        "CPIAUCSL": "US_CPI",       # 미국 CPI (월별 → forward-fill)
    }

    def __init__(self):
        self.fred_available = bool(settings.FRED_API_KEY) and len(self.FRED_SERIES) > 0
        if not self.fred_available and len(self.FRED_SERIES) > 0:
            logger.warning(
                "FRED API 키 미설정 — FED_RATE, USD_INDEX, US_CPI 수집 비활성화",
                "__init__",
            )

    def fetch_all(
        self,
        start_date: str,
        end_date: str = None,
    ) -> MacroFetchResult:
        """
        전체 거시지표 수집

        Args:
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (기본: 오늘)

        Returns:
            MacroFetchResult
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        all_records: list[dict] = []
        success_count = 0
        failed_count = 0
        skipped: list[str] = []
        total = len(self.YFINANCE_SYMBOLS) + len(self.FDR_SYMBOLS) + len(self.FRED_SERIES)

        # 1) yfinance (7개 — 개별 다운로드로 SQLite 잠금 회피)
        yf_ok, yf_fail, yf_records = self._fetch_yfinance(start_date, end_date)
        all_records.extend(yf_records)
        success_count += yf_ok
        failed_count += yf_fail
        logger.info(f"yfinance 수집 완료: {len(yf_records)}건 (성공 {yf_ok}, 실패 {yf_fail})", "fetch_all")

        # 2) FDR (1개)
        try:
            fdr_records = self._fetch_fdr(start_date, end_date)
            all_records.extend(fdr_records)
            success_count += len(self.FDR_SYMBOLS)
            logger.info(
                f"FDR 수집 완료: {len(fdr_records)}건",
                "fetch_all",
            )
        except Exception as e:
            failed_count += len(self.FDR_SYMBOLS)
            logger.error(f"FDR 수집 실패: {e}", "fetch_all")

        # 3) FRED (3개: 기준금리, 달러인덱스, CPI)
        if self.fred_available:
            try:
                fred_records = self._fetch_fred(start_date, end_date)
                all_records.extend(fred_records)
                success_count += len(self.FRED_SERIES)
                logger.info(
                    f"FRED 수집 완료: {len(fred_records)}건",
                    "fetch_all",
                )
            except Exception as e:
                failed_count += len(self.FRED_SERIES)
                logger.error(f"FRED 수집 실패: {e}", "fetch_all")

        msg_parts = [f"총 {len(all_records)}건 수집"]
        if skipped:
            msg_parts.append(f"skipped: {', '.join(skipped)}")

        return MacroFetchResult(
            records=all_records,
            total_count=total,
            success_count=success_count,
            failed_count=failed_count,
            skipped_indicators=skipped,
            message=" | ".join(msg_parts),
        )

    def _fetch_yfinance(self, start_date: str, end_date: str) -> tuple[int, int, list[dict]]:
        """
        yfinance로 7개 지표 개별 수집 (배치 다운로드 시 SQLite 잠금 이슈 회피)

        Returns:
            (success_count, failed_count, records)
        """
        import yfinance as yf

        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        end_adj = end_dt.strftime("%Y-%m-%d")

        records = []
        ok = 0
        fail = 0

        for symbol, indicator_name in self.YFINANCE_SYMBOLS.items():
            try:
                df = yf.download(
                    symbol,
                    start=start_date,
                    end=end_adj,
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )

                if df is None or df.empty:
                    logger.warning(f"yfinance {symbol}({indicator_name}) 데이터 없음", "_fetch_yfinance")
                    fail += 1
                    continue

                # Close 컬럼 추출
                close = df["Close"].dropna()
                if close.empty:
                    fail += 1
                    continue

                # MultiIndex / DataFrame → Series 변환
                if isinstance(close.columns, pd.MultiIndex):
                    close = close.droplevel("Ticker", axis=1)
                if hasattr(close, "columns"):
                    close = close.iloc[:, 0] if len(close.columns) == 1 else close

                # 스칼라인 경우 (데이터 1건) Series로 변환
                if not isinstance(close, pd.Series):
                    logger.warning(f"yfinance {symbol}({indicator_name}) 수집 실패: 단일 스칼라", "_fetch_yfinance")
                    fail += 1
                    continue

                pct = close.pct_change()

                for dt, val in close.items():
                    rec_date = pd.Timestamp(dt).date()
                    change = pct.get(dt)
                    records.append({
                        "date": rec_date,
                        "indicator_name": indicator_name,
                        "value": round(float(val), 4),
                        "change_pct": round(float(change), 6) if pd.notna(change) else None,
                        "source": "yfinance",
                    })
                ok += 1
            except Exception as e:
                logger.warning(
                    f"yfinance {symbol}({indicator_name}) 수집 실패: {e}",
                    "_fetch_yfinance",
                )
                fail += 1

        return ok, fail, records

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="macro_fetcher")
    def _fetch_fdr(self, start_date: str, end_date: str) -> list[dict]:
        """FinanceDataReader로 한국 국고채 3년 수집 (INVESTING: 프리픽스)"""
        import FinanceDataReader as fdr

        records = []
        for symbol, indicator_name in self.FDR_SYMBOLS.items():
            try:
                df = fdr.DataReader(symbol, start_date, end_date)
                if df is None or df.empty:
                    logger.warning(f"FDR {symbol} 데이터 없음", "_fetch_fdr")
                    continue

                # Close 컬럼 (금리는 Close가 금리값)
                col = "Close" if "Close" in df.columns else df.columns[0]
                close = df[col].dropna()
                pct = close.pct_change()

                for dt, val in close.items():
                    rec_date = pd.Timestamp(dt).date()
                    change = pct.get(dt)
                    records.append({
                        "date": rec_date,
                        "indicator_name": indicator_name,
                        "value": round(float(val), 4),
                        "change_pct": round(float(change), 6) if pd.notna(change) else None,
                        "source": "fdr",
                    })
            except Exception as e:
                logger.warning(f"FDR {symbol} 수집 실패: {e}", "_fetch_fdr")

        return records

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="macro_fetcher")
    def _fetch_fred(self, start_date: str, end_date: str) -> list[dict]:
        """FRED API 수집"""
        import os
        import certifi
        os.environ.setdefault("SSL_CERT_FILE", certifi.where())

        from fredapi import Fred

        fred = Fred(api_key=settings.FRED_API_KEY)
        records = []

        for series_id, indicator_name in self.FRED_SERIES.items():
            try:
                series = fred.get_series(
                    series_id,
                    observation_start=start_date,
                    observation_end=end_date,
                )
                if series is None or series.empty:
                    logger.warning(f"FRED {series_id} 데이터 없음", "_fetch_fred")
                    continue

                series = series.dropna()
                pct = series.pct_change()

                for dt, val in series.items():
                    rec_date = pd.Timestamp(dt).date()
                    change = pct.get(dt)
                    records.append({
                        "date": rec_date,
                        "indicator_name": indicator_name,
                        "value": round(float(val), 4),
                        "change_pct": round(float(change), 6) if pd.notna(change) else None,
                        "source": "fred",
                    })
            except Exception as e:
                logger.warning(f"FRED {series_id} 수집 실패: {e}", "_fetch_fred")

        return records

"""
미국 주식 데이터 수집
==================

yfinance 일괄 수집
"""

import time
from dataclasses import dataclass, field

import pandas as pd
import yfinance as yf

from core import get_logger, retry

logger = get_logger("us_fetcher")


@dataclass
class FetchResult:
    """수집 결과"""
    success: dict[str, pd.DataFrame] = field(default_factory=dict)
    failed: list[str] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    elapsed_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.success_count / self.total_count * 100


@retry(max_attempts=3, delay=2.0, backoff=2.0, module="us_fetcher")
def _download_with_retry(tickers, start_date, end_date, show_progress):
    """yfinance 다운로드 (재시도는 @retry 데코레이터가 처리)"""
    return yf.download(
        tickers,
        start=start_date,
        end=end_date,
        group_by='ticker',
        progress=show_progress,
        threads=True
    )


def fetch_us_stocks(
        tickers: list[str],
        start_date: str,
        end_date: str,
        show_progress: bool = True
) -> FetchResult:
    """
    미국 주식 데이터 일괄 수집

    Args:
        tickers: 종목 코드 리스트 ['AAPL', 'MSFT', ...]
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        show_progress: 진행률 표시

    Returns:
        FetchResult 객체
    """
    result = FetchResult(total_count=len(tickers))
    start_time = time.perf_counter()

    logger.info(
        "미국 주식 데이터 수집 시작",
        "fetch_us_stocks",
        {
            "tickers_count": len(tickers),
            "start_date": start_date,
            "end_date": end_date
        }
    )

    try:
        # yfinance 일괄 다운로드 (내부적으로 병렬 처리)
        raw_data = _download_with_retry(tickers, start_date, end_date, show_progress)

        # 단일 종목인 경우 처리
        if len(tickers) == 1:
            ticker = tickers[0]
            if not raw_data.empty:
                df = raw_data.reset_index()
                df.columns = [c.lower() for c in df.columns]
                result.success[ticker] = df
                result.success_count = 1
            else:
                result.failed.append(ticker)
                result.failed_count = 1

        else:
            # 멀티 종목: ticker별로 분리
            for ticker in tickers:
                try:
                    if ticker in raw_data.columns.get_level_values(0):
                        df = raw_data[ticker].copy()
                        df = df.dropna(how='all')

                        if not df.empty:
                            df = df.reset_index()
                            df.columns = [c.lower() for c in df.columns]
                            result.success[ticker] = df
                            result.success_count += 1
                        else:
                            result.failed.append(ticker)
                            result.failed_count += 1
                    else:
                        result.failed.append(ticker)
                        result.failed_count += 1

                except Exception as e:
                    result.failed.append(ticker)
                    result.failed_count += 1
                    logger.warning(
                        f"종목 파싱 실패",
                        "fetch_us_stocks",
                        {"ticker": ticker, "error": str(e)}
                    )

    except Exception as e:
        logger.error(
            f"yfinance 다운로드 실패",
            "fetch_us_stocks",
            {"error": str(e)}
        )
        result.failed = tickers
        result.failed_count = len(tickers)

    result.elapsed_seconds = time.perf_counter() - start_time

    logger.info(
        "미국 주식 데이터 수집 완료",
        "fetch_us_stocks",
        {
            "total": result.total_count,
            "success": result.success_count,
            "failed": result.failed_count,
            "success_rate": f"{result.success_rate:.1f}%",
            "elapsed_sec": round(result.elapsed_seconds, 2)
        }
    )

    # 실패율 50% 초과 경고
    if result.success_rate < 50 and result.total_count > 0:
        logger.critical(
            "수집 실패율 50% 초과",
            "fetch_us_stocks",
            {
                "success_rate": f"{result.success_rate:.1f}%",
                "failed_tickers": result.failed[:10]
            }
        )

    return result
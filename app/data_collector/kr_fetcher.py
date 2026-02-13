"""
한국 주식 데이터 수집
==================

FinanceDataReader + ThreadPoolExecutor 병렬 처리
"""

import time
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pandas as pd
import FinanceDataReader as fdr
from tqdm import tqdm

from core import get_logger, retry, DataFetchError

logger = get_logger("kr_fetcher")


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


@retry(max_attempts=3, delay=1.0, backoff=2.0, module="kr_fetcher")
def _fetch_with_retry(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """FDR 호출 (재시도는 @retry 데코레이터가 처리)"""
    df = fdr.DataReader(code, start_date, end_date)
    if df is None or df.empty:
        raise DataFetchError(f"데이터 없음: {code}")
    return df


def fetch_single_stock(
        code: str,
        start_date: str,
        end_date: str,
) -> tuple[str, Optional[pd.DataFrame]]:
    """
    단일 종목 데이터 수집 (재시도 포함)

    Args:
        code: 종목 코드
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)

    Returns:
        (종목코드, DataFrame 또는 None)
    """
    try:
        df = _fetch_with_retry(code, start_date, end_date)

        # 컬럼 정리
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]

        # 필요한 컬럼만
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                # 대체 컬럼명 시도
                alt_names = {
                    'date': ['Date', 'index'],
                    'volume': ['Volume', 'vol']
                }
                for alt in alt_names.get(col, []):
                    if alt in df.columns:
                        df = df.rename(columns={alt: col})
                        break

        return code, df

    except Exception as e:
        logger.warning(
            f"수집 실패",
            "fetch_single_stock",
            {"code": code, "error": str(e)}
        )
        return code, None


def fetch_kr_stocks(
        codes: list[str],
        start_date: str,
        end_date: str,
        max_workers: int = 8,
        show_progress: bool = True
) -> FetchResult:
    """
    한국 주식 데이터 병렬 수집

    Args:
        codes: 종목 코드 리스트
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        max_workers: 동시 작업 수
        show_progress: 진행률 표시

    Returns:
        FetchResult 객체
    """
    result = FetchResult(total_count=len(codes))
    start_time = time.perf_counter()

    logger.info(
        "한국 주식 데이터 수집 시작",
        "fetch_kr_stocks",
        {
            "codes_count": len(codes),
            "start_date": start_date,
            "end_date": end_date,
            "max_workers": max_workers
        }
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 작업 제출
        futures = {
            executor.submit(
                fetch_single_stock, code, start_date, end_date
            ): code
            for code in codes
        }

        # 결과 수집
        iterator = as_completed(futures)
        if show_progress:
            iterator = tqdm(iterator, total=len(codes), desc="KR Stocks")

        for future in iterator:
            code = futures[future]
            try:
                _, df = future.result()
                if df is not None and not df.empty:
                    result.success[code] = df
                    result.success_count += 1
                else:
                    result.failed.append(code)
                    result.failed_count += 1
            except Exception as e:
                result.failed.append(code)
                result.failed_count += 1
                logger.error(
                    f"예외 발생",
                    "fetch_kr_stocks",
                    {"code": code, "error": str(e)}
                )

    result.elapsed_seconds = time.perf_counter() - start_time

    logger.info(
        "한국 주식 데이터 수집 완료",
        "fetch_kr_stocks",
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
            "fetch_kr_stocks",
            {
                "success_rate": f"{result.success_rate:.1f}%",
                "failed_codes": result.failed[:10]  # 처음 10개만
            }
        )

    return result
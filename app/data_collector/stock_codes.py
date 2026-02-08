"""
종목 코드 조회
============

마켓/섹터 기준 종목 코드 리스트 조회
"""

from typing import Optional
import pandas as pd

import FinanceDataReader as fdr

import sys
sys.path.append('..')
from core import get_logger, retry, DataFetchError

logger = get_logger("stock_code")


# ============================================================
# 한국 주식 종목 코드 조회
# ============================================================

@retry(max_attempts=3, delay=1)
def get_kr_stock_list(market: Optional[str] = None) -> pd.DataFrame:
    """
    한국 주식 종목 리스트 조회

    Args:
        market: 'KOSPI', 'KOSDAQ', None(전체)

    Returns:
        DataFrame with columns: Code, Name, Market, Sector, Industry
    """
    try:
        if market and market.upper() in ['KOSPI', 'KOSDAQ']:
            df = fdr.StockListing(market.upper())
        else:
            # 전체 조회
            kospi = fdr.StockListing('KOSPI')
            kosdaq = fdr.StockListing('KOSDAQ')
            df = pd.concat([kospi, kosdaq], ignore_index=True)

        # 컬럼 정리
        df = df.rename(columns={
            'Code': 'code',
            'Name': 'name',
            'Market': 'market',
            'Sector': 'sector',
            'Industry': 'industry'
        })

        # 필요한 컬럼만 선택
        cols = ['code', 'name', 'market', 'sector', 'industry']
        existing_cols = [c for c in cols if c in df.columns]
        df = df[existing_cols]

        logger.info(
            f"한국 종목 리스트 조회 완료",
            "get_kr_stock_list",
            {"market": market, "count": len(df)}
        )

        return df

    except Exception as e:
        logger.error(f"한국 종목 리스트 조회 실패: {e}", "get_kr_stock_list")
        raise DataFetchError(f"한국 종목 리스트 조회 실패: {e}")


def filter_kr_stocks_by_sector(
        df: pd.DataFrame,
        sector: Optional[str] = None
) -> pd.DataFrame:
    """섹터로 필터링"""
    if sector is None:
        return df

    # 섹터명 부분 매칭
    mask = df['sector'].str.contains(sector, case=False, na=False)
    filtered = df[mask]

    logger.info(
        f"섹터 필터링 완료",
        "filter_kr_stocks_by_sector",
        {"sector": sector, "before": len(df), "after": len(filtered)}
    )

    return filtered


def get_kr_codes(
        market: Optional[str] = None,
        sector: Optional[str] = None
) -> list[str]:
    """
    한국 종목 코드 리스트 조회

    Args:
        market: 'KOSPI', 'KOSDAQ', None(전체)
        sector: 섹터명 (부분 매칭)

    Returns:
        종목 코드 리스트 ['005930', '000660', ...]
    """
    df = get_kr_stock_list(market)
    df = filter_kr_stocks_by_sector(df, sector)
    return df['code'].tolist()


# ============================================================
# 미국 주식 종목 코드 조회
# ============================================================

@retry(max_attempts=3, delay=1)
def get_us_stock_list(market: Optional[str] = None) -> pd.DataFrame:
    """
    미국 주식 종목 리스트 조회

    Args:
        market: 'NYSE', 'NASDAQ', 'S&P500', None(S&P500 기본)

    Returns:
        DataFrame with columns: code, name, sector, industry
    """
    try:
        if market and market.upper() == 'NYSE':
            df = fdr.StockListing('NYSE')
        elif market and market.upper() == 'NASDAQ':
            df = fdr.StockListing('NASDAQ')
        else:
            # 기본: S&P500
            df = fdr.StockListing('S&P500')

        # 컬럼 정리
        df = df.rename(columns={
            'Symbol': 'code',
            'Name': 'name',
            'Sector': 'sector',
            'Industry': 'industry'
        })

        # code 컬럼이 없으면 다른 이름으로 시도
        if 'code' not in df.columns:
            for col in ['Code', 'Ticker', 'symbol']:
                if col in df.columns:
                    df = df.rename(columns={col: 'code'})
                    break

        cols = ['code', 'name', 'sector', 'industry']
        existing_cols = [c for c in cols if c in df.columns]
        df = df[existing_cols]

        # market 컬럼 추가
        df['market'] = market.upper() if market else 'S&P500'

        logger.info(
            f"미국 종목 리스트 조회 완료",
            "get_us_stock_list",
            {"market": market, "count": len(df)}
        )

        return df

    except Exception as e:
        logger.error(f"미국 종목 리스트 조회 실패: {e}", "get_us_stock_list")
        raise DataFetchError(f"미국 종목 리스트 조회 실패: {e}")


def filter_us_stocks_by_sector(
        df: pd.DataFrame,
        sector: Optional[str] = None
) -> pd.DataFrame:
    """섹터로 필터링"""
    if sector is None or 'sector' not in df.columns:
        return df

    mask = df['sector'].str.contains(sector, case=False, na=False)
    filtered = df[mask]

    logger.info(
        f"섹터 필터링 완료",
        "filter_us_stocks_by_sector",
        {"sector": sector, "before": len(df), "after": len(filtered)}
    )

    return filtered


def get_us_codes(
        market: Optional[str] = None,
        sector: Optional[str] = None
) -> list[str]:
    """
    미국 종목 코드 리스트 조회

    Args:
        market: 'NYSE', 'NASDAQ', 'S&P500', None(S&P500 기본)
        sector: 섹터명 (부분 매칭)

    Returns:
        종목 코드 리스트 ['AAPL', 'MSFT', ...]
    """
    df = get_us_stock_list(market)
    df = filter_us_stocks_by_sector(df, sector)
    return df['code'].tolist()


# ============================================================
# 통합 함수
# ============================================================

def get_stock_codes(
        market: Optional[str] = None,
        sector: Optional[str] = None,
        region: str = 'KR'
) -> list[str]:
    """
    종목 코드 리스트 조회 (통합)

    Args:
        market: 마켓명
        sector: 섹터명
        region: 'KR' 또는 'US'

    Returns:
        종목 코드 리스트
    """
    if region.upper() == 'KR':
        return get_kr_codes(market, sector)
    else:
        return get_us_codes(market, sector)


def is_korean_market(market: Optional[str]) -> bool:
    """한국 마켓인지 확인"""
    if market is None:
        return True  # 기본값은 한국

    kr_markets = ['KOSPI', 'KOSDAQ', 'KR', 'KOREA']
    return market.upper() in kr_markets


def is_us_market(market: Optional[str]) -> bool:
    """미국 마켓인지 확인"""
    if market is None:
        return False

    us_markets = ['NYSE', 'NASDAQ', 'S&P500', 'SP500', 'US', 'USA']
    return market.upper() in us_markets
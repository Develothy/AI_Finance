"""
영업일 캘린더 유틸리티
=====================

exchange_calendars 라이브러리 기반.
시장 코드(KOSPI, KOSDAQ 등) → 거래소 코드(XKRX 등) 매핑 후
영업일 판별/조회 기능 제공.
"""

from datetime import date, datetime, timedelta
from typing import Union
from functools import lru_cache

import pandas as pd
import exchange_calendars as xcals

from core import get_logger

logger = get_logger("market_calendar")

# 시장 → 거래소 코드 매핑
MARKET_TO_EXCHANGE = {
    "KOSPI": "XKRX",
    "KOSDAQ": "XKRX",
    "NYSE": "XNYS",
    "NASDAQ": "XNAS",
    "S&P500": "XNYS",
}

DateLike = Union[date, datetime, str]


def _to_date(d: DateLike) -> date:
    # str / datetime / date → date 변환
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d").date()
    return d


@lru_cache(maxsize=8)
def _get_calendar(exchange: str) -> xcals.ExchangeCalendar:
    # 거래소 캘린더 인스턴스 (캐시)
    return xcals.get_calendar(exchange)


def _resolve_exchange(market: str) -> str:
    # 시장 코드 → 거래소 코드
    exchange = MARKET_TO_EXCHANGE.get(market)
    if not exchange:
        raise ValueError(
            f"지원하지 않는 시장: {market}. "
            f"지원 목록: {list(MARKET_TO_EXCHANGE.keys())}"
        )
    return exchange


def is_trading_day(market: str, d: DateLike = None) -> bool:
    # 해당 날짜가 영업일(거래일)인지 확인 (기본: 오늘)
    cal = _get_calendar(_resolve_exchange(market))
    target = _to_date(d) if d else date.today()
    return cal.is_session(pd.Timestamp(target))


def next_trading_day(market: str, d: DateLike = None) -> date:
    # d 이후 첫 영업일 반환 (d가 영업일이면 그 다음 영업일)
    cal = _get_calendar(_resolve_exchange(market))
    target = _to_date(d) if d else date.today()
    ts = pd.Timestamp(target)
    # d를 포함한 가장 가까운 세션 찾기
    session = cal.date_to_session(ts, direction="next")
    # d 자체가 영업일이면 그 다음으로 이동
    if session.date() == target:
        session = cal.next_session(session)
    return session.date()


def previous_trading_day(market: str, d: DateLike = None) -> date:
    # d 이전(또는 당일) 가장 가까운 영업일 반환 (d가 영업일이면 d 자체)
    cal = _get_calendar(_resolve_exchange(market))
    target = _to_date(d) if d else date.today()
    session = cal.date_to_session(pd.Timestamp(target), direction="previous")
    return session.date()


def get_trading_days(
    market: str,
    start: DateLike,
    end: DateLike,
) -> list[date]:
    # start~end 범위의 영업일 목록 반환 (양쪽 포함)
    cal = _get_calendar(_resolve_exchange(market))
    sessions = cal.sessions_in_range(
        pd.Timestamp(_to_date(start)),
        pd.Timestamp(_to_date(end)),
    )
    return [s.date() for s in sessions]
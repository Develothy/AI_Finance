"""공통 사이드바 위젯"""

from datetime import datetime, timedelta

import streamlit as st

from dashboard.utils.constants import MARKETS, MARKET_LABELS
from dashboard.config import DEFAULT_DAYS


def market_selector(key: str = "market") -> str:
    return st.sidebar.selectbox(
        "마켓",
        MARKETS,
        format_func=lambda m: MARKET_LABELS[m],
        key=key,
    )


def date_range_selector(key_prefix: str = "") -> tuple[str, str]:
    col1, col2 = st.sidebar.columns(2)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DEFAULT_DAYS)

    with col1:
        start = st.date_input("시작일", value=start_date, key=f"{key_prefix}_start")
    with col2:
        end = st.date_input("종료일", value=end_date, key=f"{key_prefix}_end")

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def stock_code_input(key: str = "code") -> str:
    return st.sidebar.text_input(
        "종목 코드",
        value="005930",
        key=key,
        help="예: 005930 (삼성전자), AAPL (Apple)",
    )

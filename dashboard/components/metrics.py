"""메트릭 카드 컴포넌트"""

import streamlit as st

from dashboard.utils.formatters import format_price, format_volume


def price_metrics(prices: list[dict], market: str):
    """최근 주가 메트릭 카드"""
    if not prices or len(prices) < 2:
        st.warning("데이터가 부족합니다.")
        return

    latest = prices[-1]
    prev = prices[-2]

    close = latest["close"] or 0
    prev_close = prev["close"] or 0
    change = close - prev_close
    pct = (change / prev_close * 100) if prev_close else 0

    # 한국 컨벤션: 상승=빨강, 하락=파랑
    color = "#e74c3c" if change >= 0 else "#3478f6"
    arrow = "▲" if change > 0 else "▼" if change < 0 else ""

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("종가", format_price(close, market))
    col1.markdown(
        f'<p style="color:{color};font-size:0.9rem;margin-top:-15px;">'
        f'{arrow} {change:+,.0f} ({pct:+.2f}%)</p>',
        unsafe_allow_html=True,
    )
    col2.metric("시가", format_price(latest.get("open"), market))
    col3.metric("고가", format_price(latest.get("high"), market))
    col4.metric("거래량", format_volume(latest.get("volume")))

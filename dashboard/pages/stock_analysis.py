"""종목 분석 페이지"""

import pandas as pd
import streamlit as st

from dashboard.api.client import api_client
from dashboard.components.charts import (
    add_bollinger_overlay,
    add_ema_overlay,
    add_sma_overlay,
    create_candlestick_chart,
    create_macd_chart,
    create_obv_chart,
    create_rsi_chart,
)
from dashboard.components.metrics import price_metrics
from dashboard.components.sidebar import (
    date_range_selector,
    market_selector,
    stock_code_input,
)


def render():
    st.header("종목 분석")

    # ── 사이드바 ───────────────────────────────────────
    market = market_selector(key="analysis_market")
    code = stock_code_input(key="analysis_code")
    start_date, end_date = date_range_selector(key_prefix="analysis")

    st.sidebar.markdown("---")
    st.sidebar.subheader("기술적 지표")
    show_sma = st.sidebar.checkbox("SMA (20)", value=True, key="show_sma")
    show_ema = st.sidebar.checkbox("EMA (20)", value=False, key="show_ema")
    show_bollinger = st.sidebar.checkbox("볼린저밴드", value=True, key="show_bollinger")
    show_rsi = st.sidebar.checkbox("RSI (14)", value=True, key="show_rsi")
    show_macd = st.sidebar.checkbox("MACD", value=True, key="show_macd")
    show_obv = st.sidebar.checkbox("OBV", value=False, key="show_obv")

    if not code:
        st.info("사이드바에서 종목 코드를 입력하세요.")
        return

    # ── 주가 데이터 조회 ───────────────────────────────
    with st.spinner(f"{code} 데이터 조회 중..."):
        try:
            prices = api_client.get_prices_by_code(
                code, market, start_date, end_date, limit=500
            )
        except Exception as e:
            st.error(f"주가 데이터 조회 실패: {e}")
            return

    if not prices:
        st.warning("해당 조건의 데이터가 없습니다. 먼저 데이터를 수집해주세요.")
        return

    df_prices = pd.DataFrame(prices)

    # ── 종목명 조회 ──────────────────────────────────────
    stock_info = api_client.get_stock_info(code, market)
    stock_name = stock_info["name"] if stock_info and stock_info.get("name") else code
    display_title = f"{stock_name} ({code})" if stock_name != code else code

    st.subheader(display_title)

    # ── 메트릭 카드 ────────────────────────────────────
    price_metrics(prices, market)
    st.markdown("---")

    # ── 캔들스틱 차트 + 오버레이 ───────────────────────
    fig = create_candlestick_chart(df_prices, title=f"{display_title} 주가 차트")

    params = {
        "code": code,
        "market": market,
        "start_date": start_date,
        "end_date": end_date,
    }

    try:
        if show_sma:
            sma_data = api_client.get_sma(**params)
            if sma_data:
                add_sma_overlay(fig, pd.DataFrame(sma_data))

        if show_ema:
            ema_data = api_client.get_ema(**params)
            if ema_data:
                add_ema_overlay(fig, pd.DataFrame(ema_data))

        if show_bollinger:
            bb_data = api_client.get_bollinger(**params)
            if bb_data:
                add_bollinger_overlay(fig, pd.DataFrame(bb_data))
    except Exception as e:
        st.warning(f"지표 오버레이 로드 실패: {e}")

    st.plotly_chart(fig, use_container_width=True)

    # ── 보조 지표 차트 ─────────────────────────────────
    try:
        if show_rsi:
            rsi_data = api_client.get_rsi(**params)
            if rsi_data:
                st.plotly_chart(
                    create_rsi_chart(pd.DataFrame(rsi_data)),
                    use_container_width=True,
                )

        if show_macd:
            macd_data = api_client.get_macd(**params)
            if macd_data:
                st.plotly_chart(
                    create_macd_chart(pd.DataFrame(macd_data)),
                    use_container_width=True,
                )

        if show_obv:
            obv_data = api_client.get_obv(**params)
            if obv_data:
                st.plotly_chart(
                    create_obv_chart(pd.DataFrame(obv_data)),
                    use_container_width=True,
                )
    except Exception as e:
        st.warning(f"보조 지표 로드 실패: {e}")

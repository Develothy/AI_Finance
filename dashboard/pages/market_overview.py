"""시장 개요 페이지"""

import pandas as pd
import streamlit as st

from dashboard.api.client import api_client
from dashboard.components.charts import create_candlestick_chart
from dashboard.components.sidebar import date_range_selector, market_selector


def render():
    st.header("시장 개요")

    # ── 사이드바 ───────────────────────────────────────
    market = market_selector(key="overview_market")
    start_date, end_date = date_range_selector(key_prefix="overview")
    sector = st.sidebar.text_input(
        "섹터",
        value="반도체",
        key="overview_sector",
        help="예: 반도체, IT, 금융, Technology",
    )

    if not sector:
        st.info("사이드바에서 섹터를 입력하세요.")
        return

    # ── 종목 목록 조회 ─────────────────────────────────
    with st.spinner("종목 목록 조회 중..."):
        try:
            stocks = api_client.get_stocks_by_sector(sector, market)
        except Exception as e:
            st.error(f"종목 조회 실패: {e}")
            return

    if not stocks:
        st.warning(f"'{sector}' 섹터에 종목이 없습니다.")
        return

    st.subheader(f"{sector} 섹터 ({len(stocks)}개 종목)")

    # ── 종목 테이블 ────────────────────────────────────
    df_stocks = pd.DataFrame(stocks)
    display_cols = {"code": "종목코드", "name": "종목명", "sector": "섹터", "industry": "산업"}
    df_display = df_stocks.rename(columns=display_cols)
    st.dataframe(
        df_display[[c for c in display_cols.values() if c in df_display.columns]],
        use_container_width=True,
        hide_index=True,
    )

    # ── 주요 종목 차트 ─────────────────────────────────
    st.subheader("주요 종목 차트")
    display_count = min(6, len(stocks))
    cols = st.columns(2)

    for i, stock in enumerate(stocks[:display_count]):
        with cols[i % 2]:
            try:
                prices = api_client.get_prices_by_code(
                    stock["code"], market, start_date, end_date, limit=100
                )
                if prices:
                    df = pd.DataFrame(prices)
                    name = stock.get("name", stock["code"])
                    fig = create_candlestick_chart(
                        df, title=f"{name} ({stock['code']})", show_volume=False
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.caption(f"{stock['code']}: 데이터 없음")
            except Exception as e:
                st.caption(f"{stock['code']}: 로드 실패")

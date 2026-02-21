"""섹터 분석 페이지"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.api.client import api_client
from dashboard.components.sidebar import date_range_selector, market_selector


def render():
    st.header("섹터 분석")

    # ── 사이드바 ───────────────────────────────────────
    market = market_selector(key="sector_market")
    start_date, end_date = date_range_selector(key_prefix="sector")
    sector = st.sidebar.text_input(
        "섹터",
        value="반도체",
        key="sector_input",
        help="예: 반도체, IT, 금융, Technology",
    )

    if not sector:
        st.info("사이드바에서 섹터를 입력하세요.")
        return

    # ── 종목 목록 조회 ─────────────────────────────────
    with st.spinner("종목 조회 중..."):
        try:
            stocks = api_client.get_stocks_by_sector(sector, market)
        except Exception as e:
            st.error(f"종목 조회 실패: {e}")
            return

    if not stocks:
        st.warning(f"'{sector}' 섹터에 종목이 없습니다.")
        return

    # ── 종목별 수익률 수집 ─────────────────────────────
    st.subheader(f"{sector} 섹터 수익률 비교")

    compare_count = min(10, len(stocks))
    stock_data = {}

    with st.spinner("주가 데이터 조회 중..."):
        for stock in stocks[:compare_count]:
            try:
                prices = api_client.get_prices_by_code(
                    stock["code"], market, start_date, end_date, limit=500
                )
                if prices and len(prices) >= 2:
                    stock_data[stock.get("name", stock["code"])] = prices
            except Exception:
                continue

    if not stock_data:
        st.warning("조회된 주가 데이터가 없습니다.")
        return

    # ── 수익률 비교 차트 (정규화) ──────────────────────
    fig = go.Figure()
    summary_rows = []

    for name, prices in stock_data.items():
        df = pd.DataFrame(prices)
        first_close = df["close"].dropna().iloc[0] if not df["close"].dropna().empty else None
        if not first_close:
            continue

        normalized = (df["close"] / first_close - 1) * 100
        fig.add_trace(
            go.Scatter(x=df["date"], y=normalized, mode="lines", name=name)
        )

        last_close = df["close"].iloc[-1] or 0
        change_pct = (last_close / first_close - 1) * 100
        last_volume = df["volume"].iloc[-1] if "volume" in df.columns else 0
        summary_rows.append({
            "종목": name,
            "시작가": f"{first_close:,.0f}",
            "현재가": f"{last_close:,.0f}",
            "등락률(%)": round(change_pct, 2),
            "최근거래량": f"{last_volume:,}" if last_volume else "-",
        })

    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.update_layout(
        title=f"{sector} 섹터 수익률 비교 (%)",
        yaxis_title="수익률 (%)",
        height=500,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── 요약 테이블 ────────────────────────────────────
    if summary_rows:
        st.subheader("종목별 등락 현황")
        df_summary = pd.DataFrame(summary_rows)
        df_summary = df_summary.sort_values("등락률(%)", ascending=False)
        st.dataframe(df_summary, use_container_width=True, hide_index=True)

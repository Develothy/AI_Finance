"""뉴스 센티먼트 분석 페이지"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.api.client import api_client
from dashboard.components.sidebar import (
    date_range_selector,
    market_selector,
    stock_code_input,
)

SENTIMENT_COLORS = {
    "positive": "#4CAF50",
    "neutral": "#FF9800",
    "negative": "#f44336",
}


def render():
    st.header("뉴스 센티먼트 분석")

    # ── 사이드바 ───────────────────────────────────────
    market = market_selector(key="news_market")
    code = stock_code_input(key="news_code")
    start_date, end_date = date_range_selector(key_prefix="news")

    if not code:
        st.info("사이드바에서 종목 코드를 입력하세요.")
        return

    # ── 데이터 조회 ────────────────────────────────────
    with st.spinner(f"{code} 뉴스 센티먼트 조회 중..."):
        try:
            articles = api_client.get_news_articles(
                code=code, start_date=start_date, end_date=end_date, limit=500,
            )
        except Exception as e:
            st.error(f"뉴스 데이터 조회 실패: {e}")
            return

    if not articles:
        st.warning("해당 조건의 뉴스가 없습니다. 먼저 뉴스 데이터를 수집해주세요.")
        return

    df = pd.DataFrame(articles)
    df["date"] = pd.to_datetime(df["date"])

    # ── 종목명 조회 ────────────────────────────────────
    stock_info = api_client.get_stock_info(code, market)
    stock_name = stock_info["name"] if stock_info and stock_info.get("name") else code
    display_title = f"{stock_name} ({code})" if stock_name != code else code

    st.subheader(display_title)

    # ── 센티먼트 요약 메트릭 ───────────────────────────
    summary = api_client.get_sentiment_summary(code)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("총 기사 수", f"{len(df):,}건")

    if summary and summary.get("sentiment") is not None:
        score = summary["sentiment"]
        m2.metric("최근 평균 센티먼트", f"{score:+.3f}")
        m3.metric("최근 기사 수", f"{summary.get('volume', 0)}건")
        m4.metric("최근 날짜", summary.get("date", "-"))
    else:
        pos_count = len(df[df["sentiment_label"] == "positive"])
        neg_count = len(df[df["sentiment_label"] == "negative"])
        avg_score = df["sentiment_score"].mean() if "sentiment_score" in df.columns else 0
        m2.metric("평균 센티먼트", f"{avg_score:+.3f}")
        m3.metric("긍정 기사", f"{pos_count}건")
        m4.metric("부정 기사", f"{neg_count}건")

    st.markdown("---")

    # ── 센티먼트 추이 차트 ─────────────────────────────
    st.subheader("센티먼트 추이")

    daily = (
        df.groupby(df["date"].dt.date)
        .agg(
            avg_sentiment=("sentiment_score", "mean"),
            count=("sentiment_score", "count"),
        )
        .reset_index()
    )
    daily.columns = ["date", "avg_sentiment", "count"]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=daily["date"],
        y=daily["avg_sentiment"],
        mode="lines+markers",
        name="평균 센티먼트",
        line=dict(color="#1976D2", width=2),
        marker=dict(size=5),
    ))
    fig_trend.add_hline(y=0, line_dash="dash", line_color="#999", opacity=0.5)
    fig_trend.update_layout(
        height=350,
        yaxis_title="센티먼트 점수",
        xaxis_title="날짜",
        hovermode="x unified",
        margin=dict(t=20, b=40),
    )

    # 양/음 영역 색상
    fig_trend.add_hrect(y0=0, y1=1, fillcolor="#4CAF50", opacity=0.05, line_width=0)
    fig_trend.add_hrect(y0=-1, y1=0, fillcolor="#f44336", opacity=0.05, line_width=0)

    st.plotly_chart(fig_trend, use_container_width=True)

    # ── 기사 수 추이 (바 차트) ─────────────────────────
    fig_vol = px.bar(
        daily, x="date", y="count",
        labels={"date": "날짜", "count": "기사 수"},
        color_discrete_sequence=["#90CAF9"],
    )
    fig_vol.update_layout(
        height=200,
        margin=dict(t=10, b=40),
        showlegend=False,
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("---")

    # ── 센티먼트 분포 ──────────────────────────────────
    col_pie, col_bar = st.columns(2)

    with col_pie:
        st.subheader("센티먼트 분포")
        label_counts = df["sentiment_label"].value_counts()
        fig_pie = px.pie(
            values=label_counts.values,
            names=label_counts.index,
            color=label_counts.index,
            color_discrete_map=SENTIMENT_COLORS,
        )
        fig_pie.update_layout(height=300, margin=dict(t=20, b=20))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_bar:
        st.subheader("점수 분포")
        fig_hist = px.histogram(
            df, x="sentiment_score", nbins=30,
            labels={"sentiment_score": "센티먼트 점수", "count": "기사 수"},
            color_discrete_sequence=["#64B5F6"],
        )
        fig_hist.update_layout(height=300, margin=dict(t=20, b=20))
        st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown("---")

    # ── 뉴스 기사 목록 ────────────────────────────────
    st.subheader("뉴스 기사 목록")

    df_display = df[["date", "title", "sentiment_label", "sentiment_score", "source"]].copy()
    df_display["date"] = df_display["date"].dt.strftime("%Y-%m-%d")
    df_display["sentiment_score"] = df_display["sentiment_score"].apply(
        lambda x: f"{x:+.3f}" if pd.notna(x) else "-"
    )
    df_display.columns = ["날짜", "제목", "센티먼트", "점수", "출처"]

    st.dataframe(df_display, use_container_width=True, hide_index=True, height=400)

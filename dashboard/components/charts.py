"""Plotly 차트 빌더"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.utils.constants import CANDLESTICK_COLORS, INDICATOR_COLORS


def create_candlestick_chart(
    df: pd.DataFrame,
    title: str = "",
    show_volume: bool = True,
) -> go.Figure:
    """캔들스틱 + 거래량 차트"""
    rows = 2 if show_volume else 1
    row_heights = [0.7, 0.3] if show_volume else [1.0]

    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
    )

    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            increasing_line_color=CANDLESTICK_COLORS["increasing"],
            decreasing_line_color=CANDLESTICK_COLORS["decreasing"],
            name="주가",
        ),
        row=1,
        col=1,
    )

    if show_volume and "volume" in df.columns:
        colors = [
            CANDLESTICK_COLORS["increasing"]
            if (c or 0) >= (o or 0)
            else CANDLESTICK_COLORS["decreasing"]
            for c, o in zip(df["close"], df["open"])
        ]
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["volume"],
                marker_color=colors,
                name="거래량",
                opacity=0.5,
            ),
            row=2,
            col=1,
        )

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        height=600,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


# ── 오버레이 ───────────────────────────────────────────


def add_sma_overlay(fig: go.Figure, df: pd.DataFrame) -> go.Figure:
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["sma"],
            mode="lines",
            name="SMA(20)",
            line=dict(color=INDICATOR_COLORS["sma"], width=1),
        ),
        row=1,
        col=1,
    )
    return fig


def add_ema_overlay(fig: go.Figure, df: pd.DataFrame) -> go.Figure:
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["ema"],
            mode="lines",
            name="EMA(20)",
            line=dict(color=INDICATOR_COLORS["ema"], width=1),
        ),
        row=1,
        col=1,
    )
    return fig


def add_bollinger_overlay(fig: go.Figure, df: pd.DataFrame) -> go.Figure:
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["upper"],
            mode="lines",
            name="BB 상단",
            line=dict(
                color=INDICATOR_COLORS["bollinger_upper"], dash="dot", width=1
            ),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["lower"],
            mode="lines",
            name="BB 하단",
            line=dict(
                color=INDICATOR_COLORS["bollinger_lower"], dash="dot", width=1
            ),
            fill="tonexty",
            fillcolor="rgba(149,225,211,0.1)",
        ),
        row=1,
        col=1,
    )
    return fig


# ── 보조 지표 차트 ─────────────────────────────────────


def create_rsi_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["rsi"],
            mode="lines",
            name="RSI(14)",
            line=dict(color=INDICATOR_COLORS["rsi"]),
        )
    )
    fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="과매수 (70)")
    fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="과매도 (30)")
    fig.update_layout(
        title="RSI (14)",
        yaxis=dict(range=[0, 100]),
        height=250,
        template="plotly_white",
    )
    return fig


def create_macd_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    colors = [
        INDICATOR_COLORS["histogram_pos"]
        if v and v >= 0
        else INDICATOR_COLORS["histogram_neg"]
        for v in df["histogram"]
    ]
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=df["histogram"],
            name="히스토그램",
            marker_color=colors,
            opacity=0.5,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["macd"],
            mode="lines",
            name="MACD",
            line=dict(color=INDICATOR_COLORS["macd"]),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["signal"],
            mode="lines",
            name="시그널",
            line=dict(color=INDICATOR_COLORS["signal"]),
        )
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.update_layout(
        title="MACD (12, 26, 9)",
        height=250,
        template="plotly_white",
    )
    return fig


def create_obv_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["obv"],
            mode="lines",
            name="OBV",
            line=dict(color=INDICATOR_COLORS["obv"]),
        )
    )
    fig.update_layout(
        title="OBV (On Balance Volume)",
        height=250,
        template="plotly_white",
    )
    return fig

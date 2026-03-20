"""레이스 페이지 — 모델 레이스 + 종목 레이스"""

from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from admin.api.client import admin_client
from admin.pages.components import inject_custom_css, pct

# ── 이모지 매핑 ──────────────────────────────────────
MODEL_EMOJIS = {
    "random_forest": "\U0001f98a",
    "xgboost": "\U0001f680",
    "lightgbm": "\u26a1",
    "lstm": "\U0001f9e0",
    "transformer": "\U0001f916",
    "dqn": "\U0001f3ae",
    "ppo": "\U0001f3af",
}

STOCK_EMOJIS = ["\U0001f3ce\ufe0f", "\U0001f699", "\U0001f695", "\U0001f6fb", "\U0001f3cd\ufe0f", "\U0001f6b2", "\U0001f6f4"]

RACE_COLORS = {
    "random_forest": "#2e7d32",
    "xgboost": "#1565c0",
    "lightgbm": "#6a1b9a",
    "lstm": "#e65100",
    "transformer": "#00695c",
    "dqn": "#c62828",
    "ppo": "#4527a0",
}

STOCK_COLORS = ["#1565c0", "#2e7d32", "#e65100", "#6a1b9a", "#00695c", "#c62828", "#4527a0"]

PERIOD_PRESETS = {
    "1주일": 7,
    "30일": 30,
    "60일": 60,
    "100일": 100,
    "1년": 365,
}


def render():
    inject_custom_css()
    st.header("레이스")

    tab_model, tab_stock = st.tabs(["모델 레이스", "종목 레이스"])

    with tab_model:
        _render_model_race()

    with tab_stock:
        _render_stock_race()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 1 — 모델 레이스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_model_race():
    # 활성 모델 로드
    try:
        all_models = admin_client.get_ml_models()
        active_models = [m for m in all_models if m.get("is_active")]
    except Exception:
        active_models = []

    if not active_models:
        st.info("활성 모델이 없습니다. ML 모델을 먼저 학습하세요.")
        return

    # 모델 선택 옵션
    model_options = {}
    for m in active_models:
        algo = m.get("algorithm", "?")
        label = f"[{m['id']}] {m['model_name']} ({algo})"
        model_options[label] = m["id"]

    with st.container(border=True):
        st.markdown("**레이스 설정**")
        c1, c2 = st.columns([1, 1])
        market = c1.selectbox("마켓", ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"], key="race_model_market")
        period_label = c2.selectbox("기간", list(PERIOD_PRESETS.keys()), index=1, key="race_model_period")

        selected_labels = st.multiselect(
            "참가 모델",
            options=list(model_options.keys()),
            default=list(model_options.keys()),
            key="race_model_select",
        )

        c3, c4 = st.columns([1, 1])
        initial_capital = c3.number_input(
            "초기 자본금", value=10_000_000, step=1_000_000,
            min_value=1_000_000, key="race_model_capital",
        )

        run_btn = st.button("레이스 시작", key="run_model_race", type="primary")

    if run_btn:
        if not selected_labels:
            st.error("모델을 1개 이상 선택하세요.")
            return

        selected_ids = [model_options[lbl] for lbl in selected_labels]
        period_days = PERIOD_PRESETS[period_label]

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=period_days)

        data = {
            "market": market,
            "model_ids": selected_ids,
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "initial_capital": initial_capital,
        }

        with st.spinner("모델 레이스 실행 중..."):
            try:
                result = admin_client.run_model_race(data)
            except Exception as e:
                st.error(f"레이스 실패: {e}")
                return

        st.session_state["model_race_result"] = result

    # 결과 렌더링
    result = st.session_state.get("model_race_result")
    if result:
        _render_model_race_result(result)


def _render_model_race_result(result: dict):
    summary = result.get("summary", {})
    participants = result.get("participants", [])

    # 서머리 카드
    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("참가 모델", summary.get("total_models", 0))
    sc2.metric("성공", summary.get("success_count", 0))
    sc3.metric("1위", f"{summary.get('best_model', '-')} ({pct(summary.get('best_return'))})")
    sc4.metric("꼴찌", f"{summary.get('worst_model', '-')} ({pct(summary.get('worst_return'))})")

    st.markdown("---")

    # 레이싱 애니메이션
    st.subheader("레이싱")
    _render_racing_bars(participants, race_type="model")

    st.markdown("---")

    # 에쿼티 커브 차트
    st.subheader("에쿼티 커브")
    _render_model_equity_chart(participants)

    st.markdown("---")

    # 성과 지표 테이블
    st.subheader("성과 지표")
    _render_model_metrics_table(participants)


def _render_model_equity_chart(participants: list[dict]):
    fig = go.Figure()
    for p in participants:
        if p.get("status") != "success" or not p.get("equity_curve"):
            continue
        algo = p.get("algorithm", "unknown")
        color = RACE_COLORS.get(algo, "#888")
        emoji = MODEL_EMOJIS.get(algo, "")
        dates = [e["date"] for e in p["equity_curve"]]
        returns = [(e.get("cumulative_return") or 0) * 100 for e in p["equity_curve"]]
        fig.add_trace(go.Scatter(
            x=dates, y=returns,
            name=f"{emoji} {p.get('model_name', algo)}",
            mode="lines",
            line=dict(color=color, width=2),
        ))
    fig.update_layout(
        yaxis_title="누적 수익률 (%)",
        xaxis_title="날짜",
        hovermode="x unified",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_model_metrics_table(participants: list[dict]):
    rows = []
    for p in participants:
        metrics = p.get("metrics", {})
        algo = p.get("algorithm", "-")
        emoji = MODEL_EMOJIS.get(algo, "")
        rows.append({
            "": emoji,
            "모델": p.get("model_name", "-"),
            "알고리즘": algo,
            "상태": p.get("status", "-"),
            "수익률": pct(metrics.get("total_return")),
            "샤프": f"{metrics.get('sharpe_ratio', 0) or 0:.2f}",
            "MDD": pct(metrics.get("max_drawdown")),
            "승률": pct(metrics.get("win_rate")),
            "거래수": metrics.get("total_trades", 0) or 0,
        })
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 2 — 종목 레이스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_stock_race():
    with st.container(border=True):
        st.markdown("**레이스 설정**")
        c1, c2 = st.columns([1, 1])
        market = c1.selectbox("마켓", ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"], key="race_stock_market")
        period_label = c2.selectbox("기간", list(PERIOD_PRESETS.keys()), index=1, key="race_stock_period")

        codes_input = st.text_input(
            "종목 코드 (쉼표 구분)",
            placeholder="005930, 000660, 035720",
            key="race_stock_codes",
        )

        run_btn = st.button("레이스 시작", key="run_stock_race", type="primary")

    if run_btn:
        codes = [c.strip() for c in codes_input.split(",") if c.strip()]
        if len(codes) < 2:
            st.error("종목을 2개 이상 입력하세요.")
            return

        period_days = PERIOD_PRESETS[period_label]
        data = {
            "market": market,
            "codes": codes,
            "period_days": period_days,
        }

        with st.spinner("종목 레이스 실행 중..."):
            try:
                result = admin_client.run_stock_race(data)
            except Exception as e:
                st.error(f"레이스 실패: {e}")
                return

        st.session_state["stock_race_result"] = result

    result = st.session_state.get("stock_race_result")
    if result:
        _render_stock_race_result(result)


def _render_stock_race_result(result: dict):
    summary = result.get("summary", {})
    participants = result.get("participants", [])

    # 서머리 카드
    sc1, sc2, sc3 = st.columns(3)
    sc1.metric("참가 종목", summary.get("total_stocks", 0))
    sc2.metric("1위", f"{summary.get('best_stock', '-')} ({pct(summary.get('best_return'))})")
    sc3.metric("꼴찌", f"{summary.get('worst_stock', '-')} ({pct(summary.get('worst_return'))})")

    st.markdown("---")

    # 레이싱 애니메이션
    st.subheader("레이싱")
    _render_racing_bars(participants, race_type="stock")

    st.markdown("---")

    # 종가 수익률 차트
    st.subheader("종가 수익률")
    _render_stock_equity_chart(participants)

    st.markdown("---")

    # 최종 수익률 테이블
    st.subheader("최종 수익률")
    _render_stock_results_table(participants)


def _render_stock_equity_chart(participants: list[dict]):
    fig = go.Figure()
    for i, p in enumerate(participants):
        if not p.get("equity_curve"):
            continue
        color = STOCK_COLORS[i % len(STOCK_COLORS)]
        emoji = STOCK_EMOJIS[i % len(STOCK_EMOJIS)]
        name = p.get("name") or p.get("code", "?")
        dates = [e["date"] for e in p["equity_curve"]]
        returns = [(e.get("cumulative_return") or 0) * 100 for e in p["equity_curve"]]
        fig.add_trace(go.Scatter(
            x=dates, y=returns,
            name=f"{emoji} {name}",
            mode="lines",
            line=dict(color=color, width=2),
        ))
    fig.update_layout(
        yaxis_title="누적 수익률 (%)",
        xaxis_title="날짜",
        hovermode="x unified",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_stock_results_table(participants: list[dict]):
    rows = []
    for i, p in enumerate(participants):
        emoji = STOCK_EMOJIS[i % len(STOCK_EMOJIS)]
        rows.append({
            "": emoji,
            "종목코드": p.get("code", "-"),
            "종목명": p.get("name", "-"),
            "수익률": pct(p.get("total_return")),
            "오류": p.get("error_message") or "",
        })
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 공통 — 레이싱 바 애니메이션
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_racing_bars(participants: list[dict], race_type: str = "model"):
    """수익률 기반 레이싱 바를 HTML로 렌더링"""

    # 수익률 수집
    returns = []
    for i, p in enumerate(participants):
        if race_type == "model":
            ret = (p.get("metrics") or {}).get("total_return")
            name = p.get("model_name") or p.get("algorithm", "?")
            algo = p.get("algorithm", "unknown")
            emoji = MODEL_EMOJIS.get(algo, "\U0001f3c3")
            color = RACE_COLORS.get(algo, "#888")
        else:
            ret = p.get("total_return")
            name = p.get("name") or p.get("code", "?")
            emoji = STOCK_EMOJIS[i % len(STOCK_EMOJIS)]
            color = STOCK_COLORS[i % len(STOCK_COLORS)]

        returns.append({
            "name": name,
            "return": ret if ret is not None else 0,
            "emoji": emoji,
            "color": color,
            "failed": (race_type == "model" and p.get("status") != "success")
                      or (race_type == "stock" and p.get("error_message")),
        })

    if not returns:
        st.info("레이스 결과가 없습니다.")
        return

    # 정규화: min→5%, max→95%
    vals = [r["return"] for r in returns]
    min_val = min(vals)
    max_val = max(vals)
    val_range = max_val - min_val if max_val != min_val else 1

    html_parts = [
        '<div style="padding:12px 0;">',
    ]

    # 수익률 내림차순 정렬
    sorted_returns = sorted(returns, key=lambda r: r["return"], reverse=True)

    for r in sorted_returns:
        if r["failed"]:
            bar_pct = 0
            ret_display = "FAIL"
            opacity = "0.4"
        else:
            bar_pct = 5 + ((r["return"] - min_val) / val_range) * 90
            ret_display = f"{r['return'] * 100:+.1f}%"
            opacity = "1"

        html_parts.append(f'''
        <div style="margin:6px 0;opacity:{opacity};">
          <div style="display:flex;align-items:center;gap:8px;">
            <span style="width:140px;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{r["name"]}</span>
            <div style="flex:1;background:#f0f0f0;border-radius:8px;height:36px;position:relative;overflow:hidden;">
              <div style="width:{bar_pct:.1f}%;background:linear-gradient(90deg,{r["color"]}cc,{r["color"]});height:100%;border-radius:8px;transition:width 1.5s ease-out;"></div>
              <span style="position:absolute;left:{bar_pct:.1f}%;top:50%;font-size:24px;transform:translate(-50%,-50%) scaleX(-1);filter:drop-shadow(0 1px 2px rgba(0,0,0,0.3));">{r["emoji"]}</span>
            </div>
            <span style="width:80px;text-align:right;font-size:13px;font-weight:600;">{ret_display}</span>
          </div>
        </div>
        ''')

    html_parts.append('</div>')
    st.markdown("".join(html_parts), unsafe_allow_html=True)

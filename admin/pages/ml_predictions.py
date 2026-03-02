"""ML 예측 테스트 페이지 — 탭 기반 UX"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.pages.components import (
    ALGO_LABELS, SIGNAL_COLORS, inject_custom_css, signal_badge, algo_badge, pct,
)

SIGNAL_ICONS = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}


def render():
    inject_custom_css()
    st.header("ML 예측 테스트")

    if st.button("새로고침", key="refresh_ml_pred"):
        st.cache_data.clear()

    tab_run, tab_history = st.tabs(["예측 실행", "예측 이력"])

    with tab_run:
        _render_prediction_run()

    with tab_history:
        _render_prediction_history()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 1 — 예측 실행
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_prediction_run():
    # 활성 모델 목록 로드 (셀렉트박스용)
    try:
        all_models = admin_client.get_ml_models()
        active_models = [m for m in all_models if m.get("is_active")]
    except Exception:
        active_models = []

    # 모델 선택 옵션
    model_choices = {"전체 활성 모델": None}
    for m in active_models:
        label = f"[{m['id']}] {m['model_name']} ({ALGO_LABELS.get(m['algorithm'], m['algorithm'])})"
        model_choices[label] = m["id"]

    # 입력 영역
    with st.container(border=True):
        st.markdown("**예측 설정**")
        c1, c2, c3 = st.columns([2, 1, 2])
        code = c1.text_input("종목코드", placeholder="005930", key="pred_code")
        market = c2.selectbox("마켓", ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"], key="pred_market")
        selected_model = c3.selectbox("모델", list(model_choices.keys()), key="pred_model_select")

        run_btn = st.button("예측 실행", key="run_prediction", type="primary")

    if run_btn:
        if not code.strip():
            st.error("종목코드를 입력하세요.")
        else:
            model_id = model_choices[selected_model]
            with st.spinner("예측 중..."):
                try:
                    result = admin_client.run_prediction(
                        code=code.strip(), market=market, model_id=model_id,
                    )
                    predictions = result.get("predictions", [])
                    if not predictions:
                        st.warning("예측 결과가 없습니다. (활성 모델 또는 피처 데이터 없음)")
                    else:
                        st.success(f"{code.strip()} 예측 완료 — {len(predictions)}개 모델")
                        _render_prediction_cards(predictions)
                except Exception as e:
                    st.error(f"예측 실패: {e}")


def _render_prediction_cards(predictions: list[dict]):
    for pred in predictions:
        signal = pred.get("signal", "HOLD")
        icon = SIGNAL_ICONS.get(signal, "⚪")
        fg, _, _ = SIGNAL_COLORS.get(signal, ("#666", "#eee", signal))
        prob_up = pred.get("probability_up") or 0
        prob_down = pred.get("probability_down") or 0
        confidence = pred.get("confidence") or 0
        model_name = pred.get("model_name", f"model_{pred.get('model_id', '?')}")
        algorithm = pred.get("algorithm", "-")
        target_date = pred.get("target_date", "-")

        with st.container(border=True):
            # 시그널 + 모델명
            header_html = (
                f'<span style="font-size:1.4rem;">{icon}</span> '
                f'<span style="font-size:1.2rem;font-weight:700;color:{fg};">{signal}</span>'
                f'&nbsp;&nbsp;'
                f'{algo_badge(algorithm)} '
                f'<span style="font-size:0.95rem;">{model_name}</span>'
            )
            st.markdown(header_html, unsafe_allow_html=True)

            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("상승확률", pct(prob_up))
            mc2.metric("하락확률", pct(prob_down))
            mc3.metric("편향도", pct(confidence))
            mc4.metric("목표일", target_date)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 2 — 예측 이력
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_prediction_history():
    # 인라인 필터
    fc1, fc2, _ = st.columns([1, 1, 2])
    filter_market = fc1.selectbox(
        "마켓 필터", ["전체", "KOSPI", "KOSDAQ", "NYSE", "NASDAQ"],
        key="pred_filter_market",
    )
    filter_code = fc2.text_input("종목코드 필터", key="pred_filter_code")

    try:
        mkt = None if filter_market == "전체" else filter_market
        cd = filter_code.strip() if filter_code.strip() else None
        predictions = admin_client.get_predictions(market=mkt, code=cd, limit=50)
    except Exception as e:
        st.error(f"예측 결과 조회 실패: {e}")
        predictions = []

    if not predictions:
        st.info("예측 결과가 없습니다.")
        return

    rows = []
    for p in predictions:
        signal = p.get("signal", "-")
        rows.append({
            "종목": p.get("code", "-"),
            "마켓": p.get("market", "-"),
            "모델": p.get("model_name", f"id:{p.get('model_id', '-')}"),
            "알고리즘": ALGO_LABELS.get(p.get("algorithm", ""), p.get("algorithm", "-")),
            "시그널": signal,
            "상승확률": pct(p.get("probability_up")),
            "편향도": pct(p.get("confidence")),
            "예측일": p.get("prediction_date", "-"),
            "목표일": p.get("target_date", "-"),
        })

    df = pd.DataFrame(rows)

    # 시그널 컬럼 색상
    def _style_signal(val):
        colors = {"BUY": "#4CAF50", "SELL": "#f44336", "HOLD": "#FF9800"}
        color = colors.get(val, "#888")
        return f"color: {color}; font-weight: 600;"

    styled = df.style.map(_style_signal, subset=["시그널"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

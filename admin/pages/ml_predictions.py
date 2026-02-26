"""ML 예측 테스트 페이지"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client

SIGNAL_STYLE = {
    "BUY": ("🟢", "#4CAF50"),
    "SELL": ("🔴", "#f44336"),
    "HOLD": ("🟡", "#FF9800"),
}


def render():
    st.header("ML 예측 테스트")

    if st.button("새로고침", key="refresh_ml_pred"):
        st.cache_data.clear()

    # ── 예측 실행 ─────────────────────────────────────
    st.subheader("예측 실행")

    pc1, pc2, pc3 = st.columns([2, 1, 1])
    code = pc1.text_input("종목코드", placeholder="005930", key="pred_code")
    market = pc2.selectbox("마켓", ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"], key="pred_market")
    model_id_input = pc3.text_input("모델 ID (비워두면 활성 모델 전체)", placeholder="", key="pred_model_id")

    if st.button("예측 실행", key="run_prediction", type="primary"):
        if not code.strip():
            st.error("종목코드를 입력하세요.")
        else:
            model_id = int(model_id_input) if model_id_input.strip() else None
            with st.spinner("예측 중..."):
                try:
                    result = admin_client.run_prediction(
                        code=code.strip(),
                        market=market,
                        model_id=model_id,
                    )
                    predictions = result.get("predictions", [])
                    if not predictions:
                        st.warning("예측 결과가 없습니다. (활성 모델 또는 피처 데이터 없음)")
                    else:
                        st.success(f"{code.strip()} 예측 완료 — {len(predictions)}개 모델")
                        _render_prediction_cards(predictions)
                except Exception as e:
                    st.error(f"예측 실패: {e}")

    # ── 최근 예측 결과 ────────────────────────────────
    st.markdown("---")
    st.subheader("최근 예측 결과")

    filter_market = st.sidebar.selectbox(
        "마켓 필터", ["전체", "KOSPI", "KOSDAQ", "NYSE", "NASDAQ"],
        key="pred_filter_market",
    )
    filter_code = st.sidebar.text_input("종목코드 필터", key="pred_filter_code")

    try:
        mkt = None if filter_market == "전체" else filter_market
        cd = filter_code.strip() if filter_code.strip() else None
        predictions = admin_client.get_predictions(market=mkt, code=cd, limit=50)
    except Exception as e:
        st.error(f"예측 결과 조회 실패: {e}")
        predictions = []

    if predictions:
        rows = []
        for p in predictions:
            signal = p.get("signal", "-")
            icon, _ = SIGNAL_STYLE.get(signal, ("⚪", "#999"))
            rows.append({
                "종목": p.get("code", "-"),
                "마켓": p.get("market", "-"),
                "모델": p.get("model_name", f"id:{p.get('model_id', '-')}"),
                "알고리즘": p.get("algorithm", "-"),
                "시그널": f"{icon} {signal}",
                "상승확률": f"{(p.get('probability_up') or 0) * 100:.1f}%",
                "신뢰도": f"{(p.get('confidence') or 0) * 100:.1f}%",
                "예측일": p.get("prediction_date", "-"),
                "목표일": p.get("target_date", "-"),
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("예측 결과가 없습니다.")


def _render_prediction_cards(predictions: list[dict]):
    #예측 결과를 카드 형태로 표시
    for pred in predictions:
        signal = pred.get("signal", "HOLD")
        icon, color = SIGNAL_STYLE.get(signal, ("⚪", "#999"))
        prob_up = pred.get("probability_up") or 0
        prob_down = pred.get("probability_down") or 0
        confidence = pred.get("confidence") or 0

        model_name = pred.get("model_name", f"model_{pred.get('model_id', '?')}")
        algorithm = pred.get("algorithm", "-")
        target_date = pred.get("target_date", "-")

        st.markdown(
            f"""<div style="border:1px solid {color};border-radius:8px;padding:12px;margin:8px 0;">
            <div style="display:flex;align-items:center;gap:16px;">
                <span style="font-size:2rem;">{icon}</span>
                <div>
                    <span style="font-size:1.3rem;font-weight:bold;color:{color};">{signal}</span>
                    <span style="color:#888;margin-left:8px;">{model_name} ({algorithm})</span>
                </div>
            </div>
            <div style="display:flex;gap:24px;margin-top:8px;">
                <span>상승확률: <b>{prob_up * 100:.1f}%</b></span>
                <span>하락확률: <b>{prob_down * 100:.1f}%</b></span>
                <span>신뢰도: <b>{confidence * 100:.1f}%</b></span>
                <span>목표일: <b>{target_date}</b></span>
            </div>
            </div>""",
            unsafe_allow_html=True,
        )
"""ML 모델 결과 페이지"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst

ALGO_LABELS = {
    "random_forest": "Random Forest",
    "xgboost": "XGBoost",
    "lightgbm": "LightGBM",
}


def _fmt_pct(val):
    # float → 퍼센트 문자열
    if val is None:
        return "-"
    return f"{val * 100:.1f}%"


def _fmt_float(val, digits=4):
    if val is None:
        return "-"
    return f"{val:.{digits}f}"


def render():
    st.header("ML 모델 결과")

    if st.button("새로고침", key="refresh_ml_models"):
        st.cache_data.clear()

    # ── 사이드바 필터 ─────────────────────────────────
    market_filter = st.sidebar.selectbox(
        "마켓 필터", ["전체", "KOSPI", "KOSDAQ", "NYSE", "NASDAQ"],
        key="ml_model_market",
    )

    # ── 모델 목록 ─────────────────────────────────────
    st.subheader("학습된 모델 목록")

    try:
        market_param = None if market_filter == "전체" else market_filter
        models = admin_client.get_ml_models(market=market_param)
    except Exception as e:
        st.error(f"모델 목록 조회 실패: {e}")
        return

    if not models:
        st.info("학습된 모델이 없습니다.")
        return

    # 테이블 표시
    rows = []
    for m in models:
        rows.append({
            "ID": m["id"],
            "모델명": m["model_name"],
            "알고리즘": ALGO_LABELS.get(m["algorithm"], m["algorithm"]),
            "마켓": m["market"],
            "타겟": m["target_column"],
            "Accuracy": _fmt_pct(m.get("accuracy")),
            "F1": _fmt_pct(m.get("f1_score")),
            "AUC-ROC": _fmt_float(m.get("auc_roc")),
            "활성": "✅" if m.get("is_active") else "❌",
            "학습기간": f"{m.get('train_start_date', '-')} ~ {m.get('train_end_date', '-')}",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # ── 모델 상세 ─────────────────────────────────────
    st.markdown("---")
    st.subheader("모델 상세")

    model_options = {f"[{m['id']}] {m['model_name']}": m["id"] for m in models}
    selected = st.selectbox("모델 선택", list(model_options.keys()), key="ml_model_select")

    if not selected:
        return

    model_id = model_options[selected]

    try:
        detail = admin_client.get_ml_model_detail(model_id)
    except Exception as e:
        st.error(f"모델 상세 조회 실패: {e}")
        return

    # 메트릭 카드
    st.markdown("##### 성능 지표")
    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("Accuracy", _fmt_pct(detail.get("accuracy")))
    mc2.metric("Precision", _fmt_pct(detail.get("precision_score")))
    mc3.metric("Recall", _fmt_pct(detail.get("recall")))
    mc4.metric("F1 Score", _fmt_pct(detail.get("f1_score")))
    mc5.metric("AUC-ROC", _fmt_float(detail.get("auc_roc")))

    # 학습 정보
    st.markdown("##### 학습 정보")
    ic1, ic2, ic3, ic4 = st.columns(4)
    ic1.metric("알고리즘", ALGO_LABELS.get(detail["algorithm"], detail["algorithm"]))
    ic2.metric("타겟", detail["target_column"])
    ic3.metric("학습 기간", f"{detail.get('train_start_date', '-')} ~ {detail.get('train_end_date', '-')}")
    ic4.metric("학습 샘플수", f"{detail.get('train_sample_count', 0):,}")

    # ── 피처 중요도 ───────────────────────────────────
    st.markdown("---")
    st.markdown("##### 피처 중요도 (Top 20)")

    try:
        fi_data = admin_client.get_feature_importance(model_id)
        features = fi_data.get("features", {})
    except Exception:
        features = {}

    if features:
        # 상위 20개 정렬
        sorted_features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:20]
        fi_df = pd.DataFrame(sorted_features, columns=["피처", "중요도"])
        fi_df = fi_df.sort_values("중요도", ascending=True)  # 차트용 오름차순
        st.bar_chart(fi_df.set_index("피처"), horizontal=True)
    else:
        st.info("피처 중요도 데이터가 없습니다.")

    # ── 학습 이력 ─────────────────────────────────────
    st.markdown("---")
    st.markdown("##### 학습 이력")

    training_logs = detail.get("training_logs", [])
    if training_logs:
        log_rows = []
        for log in training_logs:
            metrics = log.get("metrics") or {}
            log_rows.append({
                "시작": utc_to_kst(log.get("started_at")),
                "종료": utc_to_kst(log.get("finished_at")),
                "상태": log["status"],
                "알고리즘": log["algorithm"],
                "학습샘플": log.get("train_samples", 0),
                "검증샘플": log.get("val_samples", 0),
                "피처수": log.get("feature_count", 0),
                "Optuna": log.get("optuna_trials", 0),
                "최적F1": _fmt_float(log.get("best_trial_value")),
                "Accuracy": _fmt_pct(metrics.get("accuracy")),
                "F1": _fmt_pct(metrics.get("f1_score")),
            })
        log_df = pd.DataFrame(log_rows)
        st.dataframe(log_df, use_container_width=True, hide_index=True)
    else:
        st.info("학습 이력이 없습니다.")

    # ── 모델 삭제 ─────────────────────────────────────
    st.markdown("---")
    with st.expander("모델 삭제", expanded=False):
        st.warning(f"모델 **{selected}** 을(를) 삭제합니다. 이 작업은 되돌릴 수 없습니다.")
        if st.button("삭제 확인", key="ml_model_delete"):
            try:
                admin_client.delete_ml_model(model_id)
                st.success("모델 삭제 완료")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"삭제 실패: {e}")
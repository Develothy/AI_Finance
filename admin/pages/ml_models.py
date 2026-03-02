"""ML 모델 결과 페이지 — 탭 기반 UX"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst
from admin.pages.components import (
    ALGO_LABELS, inject_custom_css,
    algo_badge, pct, fmt,
)

PHASE1_FEATURES = {
    "return_1d", "return_5d", "return_20d", "volatility_20d", "volume_ratio",
    "sma_5", "sma_20", "sma_60", "ema_12", "ema_26",
    "rsi_14", "macd", "macd_signal", "macd_histogram",
    "bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_pctb",
    "obv", "price_to_sma20", "price_to_sma60", "golden_cross", "rsi_zone",
}

PHASE2_ONLY_FEATURES = {
    "per", "pbr", "eps", "market_cap",
    "foreign_ratio", "inst_net_buy", "foreign_net_buy",
    "roe", "debt_ratio",
}

TARGET_LABELS = {
    "target_return_1d": "1일",
    "target_return_5d": "5일",
    "target_return_20d": "20일",
}


def render():
    inject_custom_css()
    st.header("ML 모델 결과")

    if st.button("새로고침", key="refresh_ml_models"):
        st.cache_data.clear()

    # 사이드바 마켓 필터
    market_filter = st.sidebar.selectbox(
        "마켓 필터", ["전체", "KOSPI", "KOSDAQ", "NYSE", "NASDAQ"],
        key="ml_model_market",
    )

    # 데이터 로드
    try:
        market_param = None if market_filter == "전체" else market_filter
        models = admin_client.get_ml_models(market=market_param)
    except Exception as e:
        st.error(f"모델 목록 조회 실패: {e}")
        return

    if not models:
        st.info("학습된 모델이 없습니다.")
        return

    active_models = [m for m in models if m.get("is_active")]

    # ── 탭 ──────────────────────────────────────────────
    tab_active, tab_list, tab_detail = st.tabs([
        f"활성 모델 ({len(active_models)})",
        "전체 목록",
        "모델 상세",
    ])

    # ── 탭 1: 활성 모델 대시보드 ────────────────────────
    with tab_active:
        _render_active_dashboard(active_models, models)

    # ── 탭 2: 전체 목록 ────────────────────────────────
    with tab_list:
        _render_full_list(models)

    # ── 탭 3: 모델 상세 ────────────────────────────────
    with tab_detail:
        _render_model_detail(models)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 1 — 활성 모델 대시보드
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_active_dashboard(active_models: list, all_models: list):
    if not active_models:
        st.info("활성 모델이 없습니다.")
        return

    # 히어로 메트릭
    best_f1 = max((m.get("f1_score") or 0) for m in active_models)
    best_acc = max((m.get("accuracy") or 0) for m in active_models)

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("활성 모델", f"{len(active_models)}개")
    mc2.metric("최고 F1", pct(best_f1))
    mc3.metric("최고 Accuracy", pct(best_acc))

    st.markdown("")

    # 모델 카드들
    for m in active_models:
        algo = m.get("algorithm", "")
        target = TARGET_LABELS.get(m.get("target_column", ""), m.get("target_column", ""))
        version = m.get("version", "")

        with st.container(border=True):
            # 헤더 행: 뱃지 + 모델명
            header_html = (
                f'{algo_badge(algo)} '
                f'<span style="font-size:1.05rem;font-weight:600;">{m["model_name"]}</span>'
                f'&nbsp;<span style="color:#888;font-size:0.85rem;">v{version}</span>'
            )
            st.markdown(header_html, unsafe_allow_html=True)

            # 정보 행
            info_cols = st.columns([1, 1, 1, 1, 1])
            info_cols[0].caption(f"마켓: **{m.get('market', '-')}**")
            info_cols[1].caption(f"타겟: **{target}**")
            info_cols[2].caption(f"Acc **{pct(m.get('accuracy'))}**")
            info_cols[3].caption(f"F1 **{pct(m.get('f1_score'))}**")
            info_cols[4].caption(f"AUC **{fmt(m.get('auc_roc'), 3)}**")

            # 학습 기간
            period = f"{m.get('train_start_date', '-')} ~ {m.get('train_end_date', '-')}"
            st.caption(f"학습기간: {period}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 2 — 전체 목록
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_full_list(models: list):
    # 정렬: 활성 모델 우선 → ID 내림차순
    sorted_models = sorted(
        models,
        key=lambda m: (not m.get("is_active"), -m["id"]),
    )

    rows = []
    for m in sorted_models:
        target = TARGET_LABELS.get(m.get("target_column", ""), m.get("target_column", ""))
        rows.append({
            "ID": m["id"],
            "모델명": m["model_name"],
            "알고리즘": ALGO_LABELS.get(m["algorithm"], m["algorithm"]),
            "마켓": m["market"],
            "타겟": target,
            "피처수": m.get("feature_count", "-"),
            "Accuracy": pct(m.get("accuracy")),
            "F1": pct(m.get("f1_score")),
            "AUC-ROC": fmt(m.get("auc_roc")),
            "활성": "✅" if m.get("is_active") else "❌",
            "학습기간": f"{m.get('train_start_date', '-')} ~ {m.get('train_end_date', '-')}",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 3 — 모델 상세
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_model_detail(models: list):
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

    # ── 성능 지표 카드 ──────────────────────────────────
    with st.container(border=True):
        st.markdown("**성능 지표**")
        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric("Accuracy", pct(detail.get("accuracy")))
        mc2.metric("Precision", pct(detail.get("precision_score")))
        mc3.metric("Recall", pct(detail.get("recall")))
        mc4.metric("F1 Score", pct(detail.get("f1_score")))
        mc5.metric("AUC-ROC", fmt(detail.get("auc_roc")))

    # ── 학습 정보 카드 ──────────────────────────────────
    with st.container(border=True):
        st.markdown("**학습 정보**")
        ic1, ic2, ic3, ic4 = st.columns(4)
        ic1.metric("알고리즘", ALGO_LABELS.get(detail["algorithm"], detail["algorithm"]))
        target = TARGET_LABELS.get(detail.get("target_column", ""), detail.get("target_column", ""))
        ic2.metric("타겟", target)
        ic3.metric("학습 기간", f"{detail.get('train_start_date', '-')} ~ {detail.get('train_end_date', '-')}")
        ic4.metric("학습 샘플수", f"{detail.get('train_sample_count', 0):,}")

    # ── 피처 분석 카드 ──────────────────────────────────
    with st.container(border=True):
        st.markdown("**피처 분석**")

        try:
            fi_data = admin_client.get_feature_importance(model_id)
            features = fi_data.get("features", {})
        except Exception:
            features = {}

        if features:
            # 상위 15개 바 차트
            sorted_features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:15]
            fi_df = pd.DataFrame(sorted_features, columns=["피처", "중요도"])
            fi_df = fi_df.sort_values("중요도", ascending=True)
            st.bar_chart(fi_df.set_index("피처"), horizontal=True)

            # Phase 분류 메트릭
            used = set(features.keys())
            p1_used = used & PHASE1_FEATURES
            p2_used = used & PHASE2_ONLY_FEATURES
            p2_excluded = PHASE2_ONLY_FEATURES - used
            p1_excluded = PHASE1_FEATURES - used

            pc1, pc2, pc3 = st.columns(3)
            pc1.metric("전체 사용 피처", f"{len(used)}개")
            pc2.metric("Phase 1 (기술지표)", f"{len(p1_used)}/{len(PHASE1_FEATURES)}")
            pc3.metric("Phase 2 (재무지표)", f"{len(p2_used)}/{len(PHASE2_ONLY_FEATURES)}")

            if p2_excluded:
                st.caption(f"Phase 2 미사용 (NaN 제외): {', '.join(sorted(p2_excluded))}")
            if p1_excluded:
                st.caption(f"Phase 1 미사용: {', '.join(sorted(p1_excluded))}")
        else:
            st.info("피처 중요도 데이터가 없습니다.")

    # ── 학습 이력 (expander) ────────────────────────────
    with st.expander("학습 이력", expanded=False):
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
                    "최적F1": fmt(log.get("best_trial_value")),
                    "Accuracy": pct(metrics.get("accuracy")),
                    "F1": pct(metrics.get("f1_score")),
                })
            log_df = pd.DataFrame(log_rows)
            st.dataframe(log_df, use_container_width=True, hide_index=True)
        else:
            st.info("학습 이력이 없습니다.")

    # ── 모델 삭제 (expander) ────────────────────────────
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

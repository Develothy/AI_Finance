"""ML 학습 관리 페이지"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst

MARKETS = ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"]
ALGORITHMS = ["random_forest", "xgboost", "lightgbm"]
TARGET_DAYS = [1, 5]


def render():
    st.header("ML 학습 관리")

    if st.button("새로고침", key="refresh_ml_train"):
        st.cache_data.clear()

    # ── 등록된 ML 학습 스케줄 ──────────────────────────
    st.subheader("등록된 ML 학습 스케줄")

    try:
        all_jobs = admin_client.get_schedule_jobs()
    except Exception as e:
        st.error(f"스케줄 목록 조회 실패: {e}")
        all_jobs = []

    ml_jobs = [j for j in all_jobs if j.get("job_type") == "ml_train"]

    if ml_jobs:
        for job in ml_jobs:
            col1, col2, col3, col4, col5 = st.columns([2.5, 2, 2, 1, 1])

            # 잡 이름 + 설명
            col1.markdown(
                f"**{job['job_name']}**"
                f"<br><span style='color:#888;font-size:0.8rem;'>"
                f"{job.get('description', '') or ''}</span>",
                unsafe_allow_html=True,
            )

            # ML 설정 요약
            markets = job.get("ml_markets") or [job.get("market", "-")]
            algos = job.get("ml_algorithms") or ["-"]
            targets = job.get("ml_target_days") or ["-"]
            optuna = job.get("ml_optuna_trials") or "-"
            col2.markdown(
                f"<span style='font-size:0.85rem;'>"
                f"마켓: {', '.join(str(m) for m in markets)}<br>"
                f"알고리즘: {', '.join(algos)}<br>"
                f"타겟: {', '.join(str(d) for d in targets)}일"
                f"</span>",
                unsafe_allow_html=True,
            )

            # 크론식 + optuna
            col3.markdown(
                f"<span style='font-size:0.85rem;'>"
                f"크론: {job['cron_expr']}<br>"
                f"Optuna: {optuna}회<br>"
                f"피처재계산: {'✅' if job.get('ml_include_feature_compute', True) else '❌'}"
                f"</span>",
                unsafe_allow_html=True,
            )

            # 상태
            status_text = "활성" if job["enabled"] else "비활성"
            status_color = "#4CAF50" if job["enabled"] else "#999"
            col4.markdown(
                f"<span style='color:{status_color};font-weight:bold;'>{status_text}</span>",
                unsafe_allow_html=True,
            )

            # 버튼
            btn_col = col5
            if btn_col.button("즉시실행", key=f"ml_run_{job['id']}"):
                try:
                    result = admin_client.run_schedule_job(job["id"])
                    st.info(
                        f"🚀 {result.get('message', '백그라운드 실행 시작')} "
                        f"— 아래 실행 이력에서 진행상황을 확인하세요."
                    )
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"실행 실패: {e}")

            if btn_col.button("삭제", key=f"ml_del_{job['id']}"):
                try:
                    admin_client.delete_schedule_job(job["id"])
                    st.success(f"삭제 완료: {job['job_name']}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 실패: {e}")

            st.markdown("<hr style='margin:4px 0;border-color:#333;'>", unsafe_allow_html=True)
    else:
        st.info("등록된 ML 학습 스케줄이 없습니다.")

    # ── ML 학습 스케줄 추가 ────────────────────────────
    st.markdown("---")
    st.subheader("ML 학습 스케줄 추가")

    CRON_EXAMPLES = (
        "예시: `0 2 * * *` (매일 02시) · `0 2 * * 0` (매주 일요일 02시) · "
        "`0 2 1 * *` (매월 1일 02시)"
    )

    with st.form("add_ml_schedule"):
        fc1, fc2 = st.columns([1, 2])
        job_name = fc1.text_input("Job 이름", placeholder="ml_daily_train")
        description = fc2.text_input("설명", placeholder="KOSPI/KOSDAQ 일일 ML 학습")

        fc3, fc4 = st.columns(2)
        cron_expr = fc3.text_input(
            "크론식 (분 시 일 월 요일)", value="0 2 * * *", help=CRON_EXAMPLES
        )
        optuna_trials = fc4.number_input(
            "Optuna 시행 횟수", min_value=1, max_value=500, value=50
        )

        fc5, fc6, fc7 = st.columns(3)
        ml_markets = fc5.multiselect("대상 마켓", MARKETS, default=["KOSPI", "KOSDAQ"])
        ml_algorithms = fc6.multiselect("알고리즘", ALGORITHMS, default=ALGORITHMS)
        ml_target_days = fc7.multiselect(
            "예측 기간 (일)", TARGET_DAYS, default=TARGET_DAYS,
            format_func=lambda x: f"{x}일 후"
        )

        include_feature = st.checkbox("피처 재계산 포함", value=True)

        submitted = st.form_submit_button("스케줄 추가")
        if submitted:
            if not job_name:
                st.error("Job 이름은 필수입니다.")
            elif not cron_expr.strip():
                st.error("크론식은 필수입니다.")
            elif not ml_markets:
                st.error("마켓을 1개 이상 선택하세요.")
            elif not ml_algorithms:
                st.error("알고리즘을 1개 이상 선택하세요.")
            elif not ml_target_days:
                st.error("예측 기간을 1개 이상 선택하세요.")
            else:
                try:
                    data = {
                        "job_name": job_name,
                        "job_type": "ml_train",
                        "market": ml_markets[0],
                        "cron_expr": cron_expr.strip(),
                        "days_back": 365,
                        "description": description or None,
                        "ml_markets": ml_markets,
                        "ml_algorithms": ml_algorithms,
                        "ml_target_days": ml_target_days,
                        "ml_include_feature_compute": include_feature,
                        "ml_optuna_trials": optuna_trials,
                    }
                    result = admin_client.create_schedule_job(data)
                    st.success(f"ML 학습 스케줄 추가 완료: {result.get('job_name', '')}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"추가 실패: {e}")

    # ── 최근 ML 실행 이력 ─────────────────────────────
    st.markdown("---")
    st.subheader("최근 ML 학습 실행 이력")

    # ML 학습 잡 ID 목록
    ml_job_ids = {j["id"] for j in ml_jobs}

    try:
        logs = admin_client.get_schedule_logs(limit=30)
    except Exception as e:
        st.error(f"실행 이력 조회 실패: {e}")
        logs = []

    ml_logs = [log for log in logs if log.get("job_id") in ml_job_ids]

    if ml_logs:
        rows = []
        for log in ml_logs:
            rows.append({
                "시작시각": utc_to_kst(log.get("started_at")),
                "종료시각": utc_to_kst(log.get("finished_at")),
                "Job": log.get("job_name", f"id:{log['job_id']}"),
                "상태": log["status"],
                "성공": log.get("success_count", 0),
                "실패": log.get("failed_count", 0),
                "실행주체": log.get("trigger_by", "manual"),
                "메시지": log.get("message", "") or "",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("ML 학습 실행 이력이 없습니다.")
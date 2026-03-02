"""ML 학습 관리 페이지 — 탭 기반 UX"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst
from admin.pages.components import inject_custom_css, status_dot

MARKETS = ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"]
ALGORITHMS = ["random_forest", "xgboost", "lightgbm"]
TARGET_DAYS = [1, 5]

CRON_EXAMPLES = (
    "예시: `0 2 * * *` (매일 02시) · `0 2 * * 0` (매주 일요일 02시) · "
    "`0 2 1 * *` (매월 1일 02시)"
)

STATUS_COLORS = {
    "success": "#4CAF50",
    "completed": "#4CAF50",
    "failed": "#f44336",
    "error": "#f44336",
    "running": "#1565c0",
    "pending": "#FF9800",
}


def render():
    inject_custom_css()
    st.header("ML 학습 관리")

    if st.button("새로고침", key="refresh_ml_train"):
        st.cache_data.clear()

    # 데이터 로드
    try:
        all_jobs = admin_client.get_schedule_jobs()
    except Exception as e:
        st.error(f"스케줄 목록 조회 실패: {e}")
        all_jobs = []

    ml_jobs = [j for j in all_jobs if j.get("job_type") == "ml_train"]

    # ── 탭 ──────────────────────────────────────────────
    tab_list, tab_add, tab_history = st.tabs([
        f"스케줄 목록 ({len(ml_jobs)})",
        "스케줄 추가",
        "실행 이력",
    ])

    with tab_list:
        _render_schedule_list(ml_jobs)

    with tab_add:
        _render_add_schedule()

    with tab_history:
        _render_execution_history(ml_jobs)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 1 — 스케줄 목록
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_schedule_list(ml_jobs: list):
    if not ml_jobs:
        st.info("등록된 ML 학습 스케줄이 없습니다.")
        return

    for job in ml_jobs:
        with st.container(border=True):
            # 헤더: 상태 점 + 잡 이름
            header_html = (
                f'{status_dot(job["enabled"])} '
                f'&nbsp;<span style="font-size:1.05rem;font-weight:600;">'
                f'{job["job_name"]}</span>'
            )
            if job.get("description"):
                header_html += (
                    f'&nbsp;<span style="color:#888;font-size:0.85rem;">'
                    f'— {job["description"]}</span>'
                )
            st.markdown(header_html, unsafe_allow_html=True)

            # 정보 행
            markets = job.get("ml_markets") or [job.get("market", "-")]
            algos = job.get("ml_algorithms") or ["-"]
            targets = job.get("ml_target_days") or ["-"]
            optuna = job.get("ml_optuna_trials") or "-"
            # 파이프라인 단계 표시
            steps = []
            if job.get("ml_include_price_collect"):
                steps.append("가격")
            if job.get("ml_include_kis_collect"):
                steps.append("KIS")
            if job.get("ml_include_dart_collect"):
                steps.append("DART")
            if job.get("ml_include_feature_compute", True):
                steps.append("피처")
            steps.append("학습")
            pipeline = "→".join(steps)

            c1, c2 = st.columns(2)
            c1.caption(
                f"마켓: **{', '.join(str(m) for m in markets)}** · "
                f"알고리즘: **{', '.join(algos)}**"
            )
            c2.caption(
                f"타겟: **{', '.join(str(d) for d in targets)}일** · "
                f"Optuna: **{optuna}회**"
            )

            c3, c4 = st.columns(2)
            c3.caption(f"크론: `{job['cron_expr']}` · 파이프라인: {pipeline}")

            # 버튼
            btn1, btn2, _ = c4.columns([1, 1, 2])
            if btn1.button("즉시실행", key=f"ml_run_{job['id']}"):
                try:
                    result = admin_client.run_schedule_job(job["id"])
                    st.info(
                        f"🚀 {result.get('message', '백그라운드 실행 시작')} "
                        f"— 실행 이력 탭에서 진행상황을 확인하세요."
                    )
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"실행 실패: {e}")

            if btn2.button("삭제", key=f"ml_del_{job['id']}"):
                try:
                    admin_client.delete_schedule_job(job["id"])
                    st.success(f"삭제 완료: {job['job_name']}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 2 — 스케줄 추가
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_add_schedule():
    with st.form("add_ml_schedule"):
        # 행 1: 기본 정보
        st.markdown("**기본 정보**")
        r1c1, r1c2, r1c3 = st.columns([2, 2, 3])
        job_name = r1c1.text_input("Job 이름", placeholder="ml_daily_train")
        cron_expr = r1c2.text_input(
            "크론식 (분 시 일 월 요일)", value="0 2 * * *", help=CRON_EXAMPLES,
        )
        description = r1c3.text_input("설명", placeholder="KOSPI/KOSDAQ 일일 ML 학습")

        st.markdown("")

        # 행 2: ML 설정
        st.markdown("**ML 설정**")
        r2c1, r2c2 = st.columns(2)
        ml_markets = r2c1.multiselect("대상 마켓", MARKETS, default=["KOSPI", "KOSDAQ"])
        ml_algorithms = r2c2.multiselect("알고리즘", ALGORITHMS, default=ALGORITHMS)

        r3c1, r3c2 = st.columns(2)
        ml_target_days = r3c1.multiselect(
            "예측 기간 (일)", TARGET_DAYS, default=TARGET_DAYS,
            format_func=lambda x: f"{x}일 후",
        )
        optuna_trials = r3c2.number_input(
            "Optuna 시행 횟수", min_value=1, max_value=500, value=50,
        )

        st.markdown("")

        # 행 3: 파이프라인 단계
        st.markdown("**파이프라인 단계**")
        p1, p2, p3, p4 = st.columns(4)
        include_price = p1.checkbox("Step1: 가격 수집", value=False)
        include_kis = p2.checkbox("Step2: KIS 수집", value=False)
        include_dart = p3.checkbox("Step3: DART 수집", value=False)
        include_feature = p4.checkbox("Step4: 피처 계산", value=True)

        submitted = st.form_submit_button("스케줄 추가", type="primary")
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
                        "ml_include_price_collect": include_price,
                        "ml_include_kis_collect": include_kis,
                        "ml_include_dart_collect": include_dart,
                        "ml_include_feature_compute": include_feature,
                        "ml_optuna_trials": optuna_trials,
                    }
                    result = admin_client.create_schedule_job(data)
                    st.success(f"ML 학습 스케줄 추가 완료: {result.get('job_name', '')}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"추가 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 3 — 실행 이력
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_execution_history(ml_jobs: list):
    ml_job_ids = {j["id"] for j in ml_jobs}

    try:
        logs = admin_client.get_schedule_logs(limit=30)
    except Exception as e:
        st.error(f"실행 이력 조회 실패: {e}")
        logs = []

    ml_logs = [log for log in logs if log.get("job_id") in ml_job_ids]

    if not ml_logs:
        st.info("ML 학습 실행 이력이 없습니다.")
        return

    rows = []
    for log in ml_logs:
        status = log["status"]
        color = STATUS_COLORS.get(status, "#888")
        rows.append({
            "시작시각": utc_to_kst(log.get("started_at")),
            "종료시각": utc_to_kst(log.get("finished_at")),
            "Job": log.get("job_name", f"id:{log['job_id']}"),
            "상태": status,
            "성공": log.get("success_count", 0),
            "실패": log.get("failed_count", 0),
            "실행주체": log.get("trigger_by", "manual"),
            "메시지": log.get("message", "") or "",
        })

    df = pd.DataFrame(rows)

    # 상태 컬럼 색상 스타일링
    def _style_status(val):
        color = STATUS_COLORS.get(val, "#888")
        return f"color: {color}; font-weight: 600;"

    styled = df.style.map(_style_status, subset=["상태"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

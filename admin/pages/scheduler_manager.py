"""스케줄러 관리 페이지"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst

MARKETS = ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"]


def render():
    st.header("스케줄러 관리")

    if st.button("새로고침", key="refresh_scheduler"):
        st.cache_data.clear()

    # ── 등록된 스케줄 ─────────────────────────────────
    st.subheader("등록된 스케줄")

    try:
        jobs = admin_client.get_schedule_jobs()
    except Exception as e:
        st.error(f"스케줄 목록 조회 실패: {e}")
        jobs = []

    if jobs:
        for job in jobs:
            col1, col2, col3, col4, col5, col6 = st.columns([2, 1.5, 1.5, 1.5, 1, 1])

            col1.markdown(
                f"**{job['job_name']}**"
                f"<br><span style='color:#888;font-size:0.8rem;'>{job.get('description', '') or ''}</span>",
                unsafe_allow_html=True,
            )
            col2.text(f"{job['market']}" + (f" / {job['sector']}" if job.get("sector") else ""))
            col3.text(f"{job['cron_expr']} ({job['days_back']}일)")

            status_text = "활성" if job["enabled"] else "비활성"
            status_color = "#4CAF50" if job["enabled"] else "#999"
            col4.markdown(
                f"<span style='color:{status_color};font-weight:bold;'>{status_text}</span>",
                unsafe_allow_html=True,
            )

            if col5.button("즉시실행", key=f"run_{job['id']}"):
                try:
                    result = admin_client.run_schedule_job(job["id"])
                    st.info(f"🚀 {result.get('message', '백그라운드 실행 시작')} — 아래 실행 이력에서 진행상황을 확인하세요.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"실행 실패: {e}")

            if col6.button("삭제", key=f"del_{job['id']}"):
                try:
                    admin_client.delete_schedule_job(job["id"])
                    st.success(f"삭제 완료: {job['job_name']}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 실패: {e}")
    else:
        st.info("등록된 스케줄이 없습니다.")

    # ── 스케줄 추가 폼 ────────────────────────────────
    st.markdown("---")
    st.subheader("스케줄 추가")

    CRON_EXAMPLES = (
        "예시: `0 18 * * *` (매일 18시) · `*/10 * * * *` (10분마다) · "
        "`0 */6 * * *` (6시간마다) · `0 18 */7 * *` (7일마다 18시) · "
        "`0 9 * * 1` (매주 월요일 9시)"
    )

    with st.form("add_schedule"):
        fc1, fc2, fc3 = st.columns(3)
        job_name = fc1.text_input("Job 이름", placeholder="kr_daily_kospi")
        market = fc2.selectbox("마켓", MARKETS)
        sector = fc3.text_input("섹터 (선택)", placeholder="반도체")

        fc4, fc5, fc6 = st.columns([2, 1, 2])
        cron_expr = fc4.text_input("크론식 (분 시 일 월 요일)", value="0 18 * * *", help=CRON_EXAMPLES)
        days_back = fc5.number_input("수집 기간 (일)", min_value=1, max_value=365, value=7)
        description = fc6.text_input("설명", placeholder="KOSPI 일일 수집")

        submitted = st.form_submit_button("추가")
        if submitted:
            if not job_name:
                st.error("Job 이름은 필수입니다.")
            elif not cron_expr.strip():
                st.error("크론식은 필수입니다.")
            else:
                try:
                    data = {
                        "job_name": job_name,
                        "market": market,
                        "sector": sector or None,
                        "cron_expr": cron_expr.strip(),
                        "days_back": days_back,
                        "description": description or None,
                    }
                    result = admin_client.create_schedule_job(data)
                    st.success(f"스케줄 추가 완료: {result.get('job_name', '')}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"추가 실패: {e}")

    # ── 최근 실행 이력 ────────────────────────────────
    st.markdown("---")
    st.subheader("최근 실행 이력")

    try:
        logs = admin_client.get_schedule_logs(limit=20)
    except Exception as e:
        st.error(f"실행 이력 조회 실패: {e}")
        logs = []

    if logs:
        rows = []
        for log in logs:
            status = log["status"]
            rows.append({
                "시작시각": utc_to_kst(log.get("started_at")),
                "종료시각": utc_to_kst(log.get("finished_at")),
                "Job": log.get("job_name", f"id:{log['job_id']}"),
                "상태": status,
                "종목수": log["total_codes"],
                "성공": log["success_count"],
                "저장": log["db_saved_count"],
                "실행주체": log.get("trigger_by", "manual"),
                "메시지": log.get("message", "") or "",
            })

        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("실행 이력이 없습니다.")

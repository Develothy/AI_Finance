"""스케줄러 관리 페이지"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst
from admin.pages.components import inject_custom_css, status_dot

MARKETS = ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"]

# 파이프라인 단계 정의 (순서대로)
STEP_TYPES = [
    ("price", "📊 가격"),
    ("fundamental", "💰 재무"),
    ("market_investor", "🏦 시장수급"),
    ("macro", "🌍 거시지표"),
    ("news", "📰 뉴스"),
    ("disclosure", "📋 공시"),
    ("supply", "📈 수급"),
    ("alternative", "🔮 대안"),
    ("feature", "⚙️ 피처"),
    ("ml", "🤖 ML학습"),
]

STEP_ORDER = {st: i + 1 for i, (st, _) in enumerate(STEP_TYPES)}
STEP_LABELS = {st: label for st, label in STEP_TYPES}


def _get_step_badges(job: dict) -> str:
    """활성화된 단계를 뱃지로 표시"""
    steps = job.get("steps", [])
    if not steps:
        return ""
    enabled = {s["step_type"] for s in steps if s.get("enabled", True)}
    return "".join(STEP_LABELS.get(st, "").split(" ")[0] for st in dict(STEP_TYPES) if st in enabled)


def _get_ml_config_summary(job: dict) -> str | None:
    """ML step config 요약"""
    for s in job.get("steps", []):
        if s["step_type"] == "ml" and s.get("enabled", True):
            cfg = s.get("config") or {}
            parts = []
            if cfg.get("markets"):
                parts.append(f"마켓: {', '.join(cfg['markets'])}")
            if cfg.get("algorithms"):
                parts.append(f"알고: {', '.join(cfg['algorithms'])}")
            if cfg.get("target_days"):
                parts.append(f"타겟: {', '.join(str(d) + '일' for d in cfg['target_days'])}")
            if cfg.get("optuna_trials"):
                parts.append(f"Optuna: {cfg['optuna_trials']}회")
            return " · ".join(parts) if parts else None
    return None


def render():
    inject_custom_css()
    st.header("스케줄러 관리")

    if st.button("🔄 새로고침", key="refresh_scheduler"):
        st.cache_data.clear()

    try:
        jobs = admin_client.get_schedule_jobs()
    except Exception as e:
        st.error(f"스케줄 목록 조회 실패: {e}")
        jobs = []

    # ── 탭 ──
    tab_list, tab_add, tab_history = st.tabs([
        f"스케줄 목록 ({len(jobs)})",
        "스케줄 추가",
        "실행 이력",
    ])

    with tab_list:
        _render_schedule_list(jobs)

    with tab_add:
        _render_add_schedule()

    with tab_history:
        _render_execution_history()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 1 — 스케줄 목록
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_schedule_list(jobs: list):
    if not jobs:
        st.info("등록된 스케줄이 없습니다.")
        return

    for job in jobs:
        with st.container(border=True):
            # ── 헤더 행: 상태 + 이름 + 뱃지 ──
            top1, top2 = st.columns([3, 1])
            badges = _get_step_badges(job)
            steps = job.get("steps", [])
            enabled_count = sum(1 for s in steps if s.get("enabled", True))

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
            top1.markdown(header_html, unsafe_allow_html=True)

            # 뱃지 행
            top1.markdown(
                f'<span style="font-size:0.8rem;background:#262730;padding:2px 8px;'
                f'border-radius:4px;letter-spacing:1px;">{badges}</span>'
                f'&nbsp;<span style="color:#888;font-size:0.8rem;">{enabled_count}단계</span>',
                unsafe_allow_html=True,
            )

            # 버튼
            b1, b2 = top2.columns(2)
            if b1.button("▶ 실행", key=f"run_{job['id']}", type="primary"):
                try:
                    result = admin_client.run_schedule_job(job["id"])
                    st.info(f"🚀 {result.get('message', '백그라운드 실행 시작')}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"실행 실패: {e}")

            if b2.button("🗑 삭제", key=f"del_{job['id']}"):
                try:
                    admin_client.delete_schedule_job(job["id"])
                    st.success(f"삭제 완료: {job['job_name']}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 실패: {e}")

            # ── 정보 행 ──
            c1, c2, c3 = st.columns(3)
            c1.caption(f"마켓: **{job['market']}**" + (f" / {job['sector']}" if job.get("sector") else ""))
            c2.caption(f"크론: `{job['cron_expr']}`")
            c3.caption(f"수집기간: **{job['days_back']}일**")

            # ── Step 상세 (펼치기) ──
            ml_summary = _get_ml_config_summary(job)
            with st.expander("단계 상세 보기", expanded=False):
                step_rows = []
                for s in sorted(steps, key=lambda x: x.get("step_order", 0)):
                    label = STEP_LABELS.get(s["step_type"], s["step_type"])
                    status = "✅" if s.get("enabled", True) else "⬜"
                    cfg = s.get("config")
                    cfg_str = ""
                    if cfg:
                        cfg_parts = []
                        for k, v in cfg.items():
                            if isinstance(v, list):
                                cfg_parts.append(f"{k}: {', '.join(str(x) for x in v)}")
                            else:
                                cfg_parts.append(f"{k}: {v}")
                        cfg_str = " · ".join(cfg_parts)
                    step_rows.append({
                        "순서": s.get("step_order", 0),
                        "상태": status,
                        "단계": label,
                        "설정": cfg_str,
                    })

                if step_rows:
                    st.dataframe(
                        pd.DataFrame(step_rows),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "순서": st.column_config.NumberColumn(width="small"),
                            "상태": st.column_config.TextColumn(width="small"),
                            "단계": st.column_config.TextColumn(width="medium"),
                            "설정": st.column_config.TextColumn(width="large"),
                        },
                    )

            # ML config 요약 (expander 바깥에 간략히)
            if ml_summary:
                st.caption(f"🤖 ML: {ml_summary}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 2 — 스케줄 추가
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_add_schedule():
    CRON_EXAMPLES = (
        "예시: `0 18 * * *` (매일 18시) · `*/10 * * * *` (10분마다) · "
        "`0 */6 * * *` (6시간마다) · `0 18 */7 * *` (7일마다 18시) · "
        "`0 9 * * 1` (매주 월요일 9시)"
    )

    with st.form("add_schedule"):
        # ── 파이프라인 단계 선택 ──
        st.markdown("**파이프라인 단계 선택**")
        select_all = st.checkbox("🔄 전체 선택 (풀 파이프라인)", value=True, key="select_all")

        cols = st.columns(len(STEP_TYPES))
        step_flags = {}
        for i, (step_type, label) in enumerate(STEP_TYPES):
            checked = cols[i].checkbox(
                label,
                value=select_all,
                key=f"step_{step_type}",
                disabled=select_all,
            )
            if select_all:
                step_flags[step_type] = True
            else:
                step_flags[step_type] = checked

        st.markdown("---")

        # ── 기본 정보 ──
        st.markdown("**기본 정보**")
        fc1, fc2, fc3 = st.columns([2, 1.5, 1.5])
        job_name = fc1.text_input("Job 이름", placeholder="kr_daily_kospi")
        market = fc2.selectbox("마켓", MARKETS)
        sector = fc3.text_input("섹터 (선택)", placeholder="반도체")

        fc4, fc5, fc6 = st.columns([2, 1, 2])
        cron_expr = fc4.text_input("크론식 (분 시 일 월 요일)", value="0 19 * * *", help=CRON_EXAMPLES)
        days_back = fc5.number_input("수집 기간 (일)", min_value=1, max_value=365, value=7)
        description = fc6.text_input("설명", placeholder="KOSPI 일일 수집")

        # ── ML 설정 (ML 체크 시) ──
        if step_flags.get("ml"):
            st.markdown("---")
            st.markdown("**🤖 ML 학습 설정**")
            ml1, ml2 = st.columns(2)
            ml_markets = ml1.multiselect("대상 마켓", MARKETS, default=["KOSPI", "KOSDAQ"], key="ml_markets")
            ml_algorithms = ml2.multiselect(
                "알고리즘", ["random_forest", "xgboost", "lightgbm"],
                default=["random_forest", "xgboost", "lightgbm"], key="ml_algos",
            )
            ml3, ml4 = st.columns(2)
            ml_target_days = ml3.multiselect(
                "예측 기간", [1, 5], default=[1, 5],
                format_func=lambda x: f"{x}일 후", key="ml_targets",
            )
            ml_optuna_trials = ml4.number_input(
                "Optuna 시행 횟수", min_value=1, max_value=500, value=50, key="ml_optuna",
            )

        submitted = st.form_submit_button("스케줄 추가", type="primary")
        if submitted:
            if not job_name:
                st.error("Job 이름은 필수입니다.")
            elif not cron_expr.strip():
                st.error("크론식은 필수입니다.")
            elif not any(step_flags.values()):
                st.error("최소 1개 단계를 선택하세요.")
            else:
                try:
                    steps = []
                    for step_type, _label in STEP_TYPES:
                        if not step_flags.get(step_type):
                            continue
                        step_data = {
                            "step_type": step_type,
                            "step_order": STEP_ORDER[step_type],
                            "enabled": True,
                        }
                        if step_type == "ml":
                            step_data["config"] = {
                                "markets": ml_markets,
                                "algorithms": ml_algorithms,
                                "target_days": ml_target_days,
                                "optuna_trials": ml_optuna_trials,
                            }
                        steps.append(step_data)

                    data = {
                        "job_name": job_name,
                        "market": market,
                        "sector": sector or None,
                        "cron_expr": cron_expr.strip(),
                        "days_back": days_back,
                        "description": description or "데이터 수집",
                        "steps": steps,
                    }
                    result = admin_client.create_schedule_job(data)
                    st.success(f"스케줄 추가 완료: {result.get('job_name', '')}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"추가 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 3 — 실행 이력
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STATUS_COLORS = {
    "success": "#4CAF50",
    "completed": "#4CAF50",
    "partial": "#FF9800",
    "failed": "#f44336",
    "error": "#f44336",
    "running": "#1565c0",
    "pending": "#FF9800",
}


def _render_execution_history():
    try:
        logs = admin_client.get_schedule_logs(limit=30)
    except Exception as e:
        st.error(f"실행 이력 조회 실패: {e}")
        logs = []

    if not logs:
        st.info("실행 이력이 없습니다.")
        return

    rows = []
    for log in logs:
        status = log["status"]
        rows.append({
            "시작시각": utc_to_kst(log.get("started_at")),
            "종료시각": utc_to_kst(log.get("finished_at")),
            "Job": log.get("job_name", f"id:{log['job_id']}"),
            "상태": status,
            "성공": log.get("success_count", 0),
            "실패": log.get("failed_count", 0),
            "저장": log.get("db_saved_count", 0),
            "실행주체": log.get("trigger_by", "manual"),
            "메시지": log.get("message", "") or "",
        })

    df = pd.DataFrame(rows)

    def _style_status(val):
        color = STATUS_COLORS.get(val, "#888")
        return f"color: {color}; font-weight: 600;"

    styled = df.style.map(_style_status, subset=["상태"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

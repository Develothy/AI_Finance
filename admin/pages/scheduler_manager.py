"""스케줄러 관리 페이지"""

import pandas as pd
import streamlit as st

from admin.api.client import admin_client
from admin.config import utc_to_kst
from admin.pages.components import inject_custom_css, metric_card, status_dot

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
    ("predict", "🎯 예측"),
]

STEP_ORDER = {st: i + 1 for i, (st, _) in enumerate(STEP_TYPES)}
STEP_LABELS = {st: label for st, label in STEP_TYPES}

# 스텝 의존성 매핑
STEP_DEPENDENCIES = {
    "price":           [],
    "fundamental":     ["price"],
    "market_investor": [],
    "macro":           [],
    "news":            ["price"],
    "disclosure":      ["price"],
    "supply":          ["price"],
    "alternative":     ["price"],
    "feature":         ["price", "fundamental", "macro", "news", "disclosure", "supply", "alternative"],
    "ml":              ["feature"],
    "predict":         ["ml"],
}

STATUS_COLORS = {
    "success": "#4CAF50",
    "completed": "#4CAF50",
    "partial": "#FF9800",
    "failed": "#f44336",
    "error": "#f44336",
    "running": "#1565c0",
    "pending": "#888",
    "skipped": "#555",
}

STATUS_ICONS = {
    "success": "✅",
    "completed": "✅",
    "partial": "⚠️",
    "failed": "❌",
    "error": "❌",
    "running": "🔄",
    "pending": "⏳",
    "skipped": "⏭️",
}


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
        _render_execution_history(jobs)


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
            tcs = job.get("target_codes", [])
            if tcs:
                preview = ", ".join(f"{tc['code']}({tc.get('name', '')})" for tc in tcs[:3])
                if len(tcs) > 3:
                    preview += f" 외 {len(tcs) - 3}종목"
                c1.caption(f"마켓: **{job['market']}** · 대상: {preview}")
            else:
                c1.caption(f"마켓: **{job['market']}**")
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

    # session_state 초기화
    if "selected_stocks" not in st.session_state:
        st.session_state["selected_stocks"] = {}  # {code: name}

    # ── 종목 검색 영역 (form 바깥) ──
    st.markdown("**대상 종목 선택**")

    sc1, sc2 = st.columns(2)
    with sc1:
        st.caption("종목 검색 (코드 또는 이름)")
        kw = st.text_input("검색어", placeholder="삼성전자, 005930", key="stock_search_kw")
        if st.button("검색", key="btn_stock_search") and kw:
            try:
                results = admin_client.search_stocks(kw, limit=30)
                st.session_state["stock_search_results"] = results
            except Exception as e:
                st.error(f"검색 실패: {e}")

    with sc2:
        st.caption("섹터/산업 일괄 추가")
        sector_kw = st.text_input("섹터 키워드", placeholder="반도체", key="sector_search_kw")
        if st.button("섹터 검색", key="btn_sector_search") and sector_kw:
            try:
                results = admin_client.search_stocks_by_sector(sector_kw)
                st.session_state["sector_search_results"] = results
            except Exception as e:
                st.error(f"검색 실패: {e}")

    # 종목 검색 결과
    if st.session_state.get("stock_search_results"):
        results = st.session_state["stock_search_results"]
        st.caption(f"검색 결과: {len(results)}건")
        for i, s in enumerate(results[:20]):
            lbl = f"{s['code']} {s.get('name', '')} ({s.get('market', '')})"
            if s["code"] not in st.session_state["selected_stocks"]:
                if st.button(f"+ {lbl}", key=f"add_stock_{i}"):
                    st.session_state["selected_stocks"][s["code"]] = s.get("name", "")
                    st.rerun()
            else:
                st.caption(f"  ✅ {lbl}")

    # 섹터 검색 결과
    if st.session_state.get("sector_search_results"):
        results = st.session_state["sector_search_results"]
        new_count = sum(1 for s in results if s["code"] not in st.session_state["selected_stocks"])
        st.caption(f"섹터 검색 결과: {len(results)}건 (신규 {new_count}건)")
        if new_count > 0 and st.button(f"전체 {new_count}종목 추가", key="btn_add_all_sector"):
            for s in results:
                if s["code"] not in st.session_state["selected_stocks"]:
                    st.session_state["selected_stocks"][s["code"]] = s.get("name", "")
            st.rerun()

    # 선택된 종목 목록
    selected = st.session_state["selected_stocks"]
    if selected:
        st.markdown(f"**선택된 종목 ({len(selected)}개)**")
        remove_codes = []
        cols_per_row = 4
        items = list(selected.items())
        for row_start in range(0, len(items), cols_per_row):
            row_items = items[row_start:row_start + cols_per_row]
            cols = st.columns(cols_per_row)
            for ci, (code, name) in enumerate(row_items):
                if cols[ci].button(f"❌ {code}({name})", key=f"rm_{code}"):
                    remove_codes.append(code)
        if remove_codes:
            for c in remove_codes:
                del st.session_state["selected_stocks"][c]
            st.rerun()
    else:
        st.warning("종목을 1개 이상 선택하세요.")

    st.markdown("---")

    # ── 파이프라인 단계 선택 (form 밖 — 즉시 반응) ──
    st.markdown("**파이프라인 단계 선택**")
    if "step_select_all" not in st.session_state:
        st.session_state["step_select_all"] = True
        for st_type, _ in STEP_TYPES:
            st.session_state[f"step_{st_type}"] = True

    def _on_select_all_change():
        val = st.session_state["step_select_all"]
        for st_type, _ in STEP_TYPES:
            st.session_state[f"step_{st_type}"] = val

    st.checkbox(
        "전체 선택 (풀 파이프라인)", key="step_select_all",
        on_change=_on_select_all_change,
    )

    cols = st.columns(len(STEP_TYPES))
    for i, (step_type, label) in enumerate(STEP_TYPES):
        cols[i].checkbox(label, key=f"step_{step_type}")

    step_flags = {st_type: st.session_state.get(f"step_{st_type}", True) for st_type, _ in STEP_TYPES}

    st.markdown("---")

    with st.form("add_schedule"):

        # ── 기본 정보 ──
        st.markdown("**기본 정보**")
        fc1, fc2 = st.columns([2, 1.5])
        job_name = fc1.text_input("Job 이름", placeholder="kr_daily_kospi")
        market = fc2.selectbox("마켓", MARKETS)

        fc4, fc5, fc6 = st.columns([2, 1, 2])
        cron_expr = fc4.text_input("크론식 (분 시 일 월 요일)", value="0 19 * * *", help=CRON_EXAMPLES)
        days_back = fc5.number_input("수집 기간 (일)", min_value=1, max_value=365, value=7)
        description = fc6.text_input("설명", placeholder="KOSPI 일일 수집")

        # ── ML 설정 (ML 체크 시) ──
        if step_flags.get("ml"):
            st.markdown("---")
            st.markdown("**ML 학습 설정**")
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
            target_codes = [
                {"code": code, "name": name}
                for code, name in st.session_state.get("selected_stocks", {}).items()
            ]
            if not job_name:
                st.error("Job 이름은 필수입니다.")
            elif not cron_expr.strip():
                st.error("크론식은 필수입니다.")
            elif not any(step_flags.values()):
                st.error("최소 1개 단계를 선택하세요.")
            elif not target_codes:
                st.error("종목을 1개 이상 선택하세요.")
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
                        "cron_expr": cron_expr.strip(),
                        "days_back": days_back,
                        "description": description or "데이터 수집",
                        "steps": steps,
                        "target_codes": target_codes,
                    }
                    result = admin_client.create_schedule_job(data)
                    st.success(f"스케줄 추가 완료: {result.get('job_name', '')}")
                    st.session_state["selected_stocks"] = {}
                    st.session_state.pop("stock_search_results", None)
                    st.session_state.pop("sector_search_results", None)
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"추가 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 3 — 실행 이력 (파이프라인 뷰)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _format_duration(sec: int | None) -> str:
    if sec is None:
        return "-"
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    return f"{m}m{s}s"


def _calc_duration(started: str, finished: str | None) -> str:
    if not started or not finished:
        return ""
    try:
        from datetime import datetime
        s = datetime.strptime(started, "%Y-%m-%d %H:%M:%S")
        f = datetime.strptime(finished, "%Y-%m-%d %H:%M:%S")
        return _format_duration(int((f - s).total_seconds()))
    except Exception:
        return ""


def _render_execution_history(jobs: list):
    try:
        logs = admin_client.get_schedule_logs(limit=30)
    except Exception as e:
        st.error(f"실행 이력 조회 실패: {e}")
        logs = []

    if not logs:
        st.info("실행 이력이 없습니다.")
        return

    for log in logs:
        _render_log_card(log, jobs)


def _render_log_card(log: dict, jobs: list):
    """실행 이력 1건을 카드형으로 렌더링"""
    log_id = log["id"]
    status = log["status"]
    icon = STATUS_ICONS.get(status, "●")
    color = STATUS_COLORS.get(status, "#888")
    job_name = log.get("job_name") or f"id:{log['job_id']}"
    trigger = log.get("trigger_by", "manual")
    started = utc_to_kst(log.get("started_at"))
    duration_str = _calc_duration(log.get("started_at", ""), log.get("finished_at"))
    sc = log.get("success_count", 0)
    fc = log.get("failed_count", 0)
    saved = log.get("db_saved_count", 0)
    total = log.get("total_codes", 0)

    with st.container(border=True):
        # ── 헤더: 아이콘 + 이름 + 부가정보 ──
        st.markdown(
            f'<div class="log-card-header">'
            f'<span style="font-size:1.3rem;">{icon}</span>'
            f'<span class="log-title">#{log_id} {job_name}</span>'
            f'<span class="log-trigger">{trigger}</span>'
            f'<span class="log-sub">{started}</span>'
            f'<span class="log-sub" style="color:{color};font-weight:600;">'
            f'{duration_str or "진행중"}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── 메트릭 행 ──
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.markdown(metric_card("성공", f"{sc}", color="#4CAF50"), unsafe_allow_html=True)
        mc2.markdown(metric_card("실패", f"{fc}", color="#f44336" if fc > 0 else "#888"), unsafe_allow_html=True)
        mc3.markdown(metric_card("저장", f"{saved}"), unsafe_allow_html=True)
        mc4.markdown(metric_card("종목", f"{total}"), unsafe_allow_html=True)

        # ── 파이프라인 상세 (펼치기) ──
        with st.expander("파이프라인 상세", expanded=False):
            try:
                step_logs = admin_client.get_step_logs(log_id)
            except Exception as e:
                st.warning(f"스텝 로그 조회 실패: {e}")
                step_logs = []

            if step_logs:
                # 파이프라인 바 (시각 개요)
                _render_pipeline_bar(step_logs)

                # 탭으로 스텝 상세 (클릭하면 해당 스텝만 표시)
                tab_labels = [
                    STEP_LABELS.get(sl['step_type'], sl['step_type']).split(" ", 1)[-1]
                    for sl in step_logs
                ]
                tabs = st.tabs(tab_labels)
                for tab, sl in zip(tabs, step_logs):
                    with tab:
                        _render_step_detail(sl, step_logs, log)
            else:
                st.caption("스텝 로그 없음 (이전 방식 실행 기록)")
                if log.get("message"):
                    st.code(log["message"])


def _render_pipeline_bar(step_logs: list):
    """파이프라인 노드를 HTML flex 플로우차트로 렌더링"""
    nodes_html = []
    for i, sl in enumerate(step_logs):
        stype = sl["step_type"]
        status = sl["status"]
        st_icon = STATUS_ICONS.get(status, "●")
        label = STEP_LABELS.get(stype, stype)
        parts = label.split(" ", 1)
        emoji = parts[0] if len(parts) > 1 else ""
        name = parts[1] if len(parts) > 1 else label
        dur = _format_duration(sl.get("duration_sec"))
        saved = sl.get("saved_count", 0)

        meta_parts = []
        if dur != "-":
            meta_parts.append(dur)
        if saved > 0:
            meta_parts.append(f"{saved}건")
        meta = " · ".join(meta_parts) if meta_parts else ""

        # 실패/에러만 아이콘 표시, 성공은 테두리 색으로 충분
        icon_prefix = f"{st_icon} " if status in ("failed", "error", "skipped") else ""

        node = (
            f'<div class="pipeline-node st-{status}">'
            f'<div class="node-emoji">{emoji}</div>'
            f'<div class="node-name">{name}</div>'
            f'<div class="node-meta">{icon_prefix}{meta}</div>'
            f'</div>'
        )
        nodes_html.append(node)
        if i < len(step_logs) - 1:
            nodes_html.append('<span class="pipeline-arrow">→</span>')

    st.markdown(
        f'<div class="pipeline-flow">{"".join(nodes_html)}</div>',
        unsafe_allow_html=True,
    )


def _render_step_detail(step_data: dict, all_step_logs: list, log: dict):
    """스텝 1건의 상세 (탭 안에서 직접 렌더링)"""
    stype = step_data["step_type"]
    status = step_data["status"]
    icon = STATUS_ICONS.get(status, "●")
    color = STATUS_COLORS.get(status, "#888")
    dur = _format_duration(step_data.get("duration_sec"))
    summary = step_data.get("summary") or ""
    saved = step_data.get("saved_count", 0)
    is_error = status in ("failed", "error")

    # ── 상태 + 요약 헤더 ──
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("상태", f"{icon} {status}", color=color), unsafe_allow_html=True)
    c2.markdown(metric_card("소요", dur), unsafe_allow_html=True)
    c3.markdown(metric_card("저장", f"{saved}건"), unsafe_allow_html=True)
    c4.markdown(
        metric_card("시작", (step_data.get("started_at") or "-").split(" ")[-1]),
        unsafe_allow_html=True,
    )

    if is_error and step_data.get("error_message"):
        st.error(step_data["error_message"])

    if summary:
        st.caption(f"결과: {summary}")

    # 의존 스텝 (실패한 것만 경고)
    deps = STEP_DEPENDENCIES.get(stype, [])
    if deps:
        failed_deps = [
            d for d in deps
            if any(sl["step_type"] == d and sl["status"] in ("failed", "error") for sl in all_step_logs)
        ]
        if failed_deps:
            st.warning(f"선행 스텝 실패: {', '.join(STEP_LABELS.get(d, d) for d in failed_deps)}")

    # 재실행 버튼 + 로그
    job_id = log.get("job_id")
    if job_id:
        # 원래 실행 날짜 추출 (YYYY-MM-DD)
        orig_date = (log.get("started_at") or "")[:10] or None

        bc1, bc2, _ = st.columns([1, 1, 2])
        if bc1.button("▶ 이 스텝만", key=f"rerun_step_{log['id']}_{stype}"):
            try:
                result = admin_client.run_single_step(job_id, stype, base_date=orig_date)
                st.info(f"실행 시작 (기준일: {orig_date}): {result.get('message', '')}")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"실행 실패: {e}")

        if bc2.button("▶▶ 여기서부터", key=f"rerun_from_{log['id']}_{stype}"):
            try:
                result = admin_client.run_from_step(job_id, stype, base_date=orig_date)
                st.info(f"실행 시작 (기준일: {orig_date}): {result.get('message', '')}")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"실행 실패: {e}")

    if step_data.get("log_text"):
        st.code(step_data["log_text"], language="log")

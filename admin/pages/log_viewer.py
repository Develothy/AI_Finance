"""로그 조회 페이지"""

import streamlit as st

from admin.api.client import admin_client

LEVEL_COLORS = {
    "DEBUG": "#888888",
    "INFO": "#2196F3",
    "WARNING": "#FF9800",
    "ERROR": "#f44336",
    "CRITICAL": "#B71C1C",
}


def render():
    st.header("로그 조회")

    # 사이드바 필터
    log_file = st.sidebar.selectbox(
        "로그 파일",
        ["app", "error", "trade"],
        key="log_file",
    )
    lines = st.sidebar.slider("줄 수", 10, 500, 100, key="log_lines")
    level = st.sidebar.selectbox(
        "레벨 필터",
        ["전체", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        key="log_level",
    )
    search = st.sidebar.text_input("텍스트 검색", key="log_search")

    if st.button("새로고침", key="refresh_logs"):
        st.cache_data.clear()

    level_param = None if level == "전체" else level
    search_param = search if search else None

    try:
        data = admin_client.get_logs(
            file=log_file,
            lines=lines,
            level=level_param,
            search=search_param,
        )
    except Exception as e:
        st.error(f"로그 조회 실패: {e}")
        return

    entries = data.get("entries", [])
    st.caption(f"{data.get('file', '')} — {data.get('total', 0)}건")

    if not entries:
        st.info("로그가 없습니다.")
        return

    for entry in entries:
        lvl = entry.get("level", "")
        color = LEVEL_COLORS.get(lvl, "#888888")
        time_str = entry.get("time", "")
        module = entry.get("module", "")
        func = entry.get("function", "")
        msg = entry.get("message", "")

        loc = f"{module}:{func}" if module else ""

        st.markdown(
            f'<div style="font-family:monospace;font-size:0.85rem;margin-bottom:2px;">'
            f'<span style="color:#999;">{time_str}</span> '
            f'<span style="color:{color};font-weight:bold;">[{lvl}]</span> '
            f'<span style="color:#6a9fb5;">{loc}</span> '
            f'{msg}</div>',
            unsafe_allow_html=True,
        )

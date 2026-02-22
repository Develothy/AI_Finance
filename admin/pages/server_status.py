"""서버 상태 페이지"""

import streamlit as st

from admin.api.client import admin_client


def render():
    st.header("API 서버 상태")

    if st.button("새로고침", key="refresh_health"):
        st.cache_data.clear()

    try:
        data = admin_client.get_health()
    except Exception as e:
        st.error(f"서버 연결 실패: {e}")
        return

    status = data.get("status", "unknown")
    color = "🟢" if status == "ok" else "🔴"

    col1, col2, col3 = st.columns(3)
    col1.metric("상태", f"{color} {status.upper()}")
    col2.metric("Uptime", f"{data.get('uptime_seconds', 0):,.0f}초")
    col3.metric("시작 시각", data.get("started_at", "-"))

    col4, col5, col6 = st.columns(3)
    col4.metric("API 버전", data.get("version", "-"))
    col5.metric("Python", data.get("python_version", "-"))
    col6.metric("DB 타입", data.get("db_type", "-"))

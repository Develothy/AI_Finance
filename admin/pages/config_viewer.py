"""설정 확인 페이지"""

import streamlit as st

from admin.api.client import admin_client


def render():
    st.header("설정 확인")

    if st.button("새로고침", key="refresh_config"):
        st.cache_data.clear()

    try:
        data = admin_client.get_config()
    except Exception as e:
        st.error(f"설정 조회 실패: {e}")
        return

    # 경고
    warnings = data.get("warnings", [])
    if warnings:
        for w in warnings:
            st.warning(w)
    else:
        st.success("설정 검증 통과")

    # 그룹별 표시
    groups = data.get("groups", {})
    group_labels = {
        "app": "앱 설정",
        "logging": "로깅",
        "database": "데이터베이스",
        "scheduler": "스케줄러",
        "slack": "슬랙",
        "kis": "한국투자증권",
        "alpaca": "Alpaca",
        "openai": "OpenAI",
    }

    for key, group in groups.items():
        label = group_labels.get(key, key)
        with st.expander(label, expanded=(key in ("app", "database", "scheduler"))):
            items = group.get("items", {})
            for k, v in items.items():
                display = v if v else "(미설정)"
                if "MASKED" in display:
                    st.text(f"{k}: ●●●●●●")
                else:
                    st.text(f"{k}: {display}")

"""DB 상태 페이지"""

import streamlit as st

from admin.api.client import admin_client


def render():
    st.header("DB 상태")

    if st.button("새로고침", key="refresh_db"):
        st.cache_data.clear()

    try:
        data = admin_client.get_db_status()
    except Exception as e:
        st.error(f"DB 상태 조회 실패: {e}")
        return

    connected = data.get("connected", False)
    if connected:
        st.success(f"연결 정상 ({data.get('db_type', '-')})")
    else:
        st.error(f"연결 실패: {data.get('error', 'unknown')}")
        return

    tables = data.get("tables", {})

    # stock_price
    sp = tables.get("stock_price", {})
    st.subheader("stock_price (주가 데이터)")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("레코드 수", f"{sp.get('row_count', 0):,}")
    col2.metric("종목 수", f"{sp.get('code_count', 0):,}")
    col3.metric("최초 날짜", sp.get("earliest_date", "-") or "-")
    col4.metric("최근 날짜", sp.get("latest_date", "-") or "-")
    st.caption(f"마켓: {', '.join(sp.get('markets', []))}")

    st.markdown("---")

    # stock_info
    si = tables.get("stock_info", {})
    st.subheader("stock_info (종목 정보)")
    col5, col6, col7 = st.columns(3)
    col5.metric("레코드 수", f"{si.get('row_count', 0):,}")
    col6.metric("섹터 수", f"{si.get('sector_count', 0):,}")
    col7.metric("마켓", ", ".join(si.get("markets", [])) or "-")

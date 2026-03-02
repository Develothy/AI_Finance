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

    st.markdown("---")

    # stock_fundamental (KIS)
    sf = tables.get("stock_fundamental", {})
    st.subheader("stock_fundamental (KIS 기초정보)")
    fc1, fc2, fc3, fc4 = st.columns(4)
    fc1.metric("레코드 수", f"{sf.get('row_count', 0):,}")
    fc2.metric("종목 수", f"{sf.get('code_count', 0):,}")
    fc3.metric("최초 날짜", sf.get("earliest_date", "-") or "-")
    fc4.metric("최근 날짜", sf.get("latest_date", "-") or "-")
    st.caption(f"마켓: {', '.join(sf.get('markets', []))}")

    st.markdown("---")

    # financial_statement (DART)
    fs = tables.get("financial_statement", {})
    st.subheader("financial_statement (DART 재무제표)")
    dc1, dc2, dc3 = st.columns(3)
    dc1.metric("레코드 수", f"{fs.get('row_count', 0):,}")
    dc2.metric("종목 수", f"{fs.get('code_count', 0):,}")
    dc3.metric("분기 수", f"{fs.get('period_count', 0):,}")
    st.caption(f"마켓: {', '.join(fs.get('markets', []))}")

    st.markdown("---")

    # feature_store
    ft = tables.get("feature_store", {})
    st.subheader("feature_store (ML 피처)")
    ft1, ft2, ft3, ft4 = st.columns(4)
    ft1.metric("레코드 수", f"{ft.get('row_count', 0):,}")
    ft2.metric("종목 수", f"{ft.get('code_count', 0):,}")
    ft3.metric("최초 날짜", ft.get("earliest_date", "-") or "-")
    ft4.metric("최근 날짜", ft.get("latest_date", "-") or "-")
    st.caption(f"마켓: {', '.join(ft.get('markets', []))}")

    st.markdown("---")

    # ml_model / ml_prediction
    mm = tables.get("ml_model", {})
    mp = tables.get("ml_prediction", {})
    st.subheader("ML 모델 & 예측")
    ml1, ml2, ml3, ml4 = st.columns(4)
    ml1.metric("모델 수", f"{mm.get('row_count', 0):,}")
    ml2.metric("활성 모델", f"{mm.get('active_count', 0):,}")
    ml3.metric("예측 레코드", f"{mp.get('row_count', 0):,}")
    ml4.metric("예측 종목 수", f"{mp.get('code_count', 0):,}")

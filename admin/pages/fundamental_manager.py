"""재무 데이터 관리 페이지"""

from datetime import datetime

import pandas as pd
import streamlit as st

from admin.api.client import admin_client

MARKETS = ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"]
QUARTERS = ["Q1", "Q2", "Q3", "A"]


def render():
    st.header("재무 데이터 관리")

    if st.button("새로고침", key="refresh_fund"):
        st.cache_data.clear()

    tab1, tab2, tab3, tab4 = st.tabs([
        "데이터 수집", "기초정보 (KIS)", "재무제표 (DART)", "커버리지",
    ])

    # ── 탭 1: 데이터 수집 ─────────────────────────────────
    with tab1:
        st.subheader("KIS 기초정보 수집")
        with st.form("collect_kis"):
            c1, c2, c3 = st.columns(3)
            kis_market = c1.selectbox("마켓", MARKETS, key="kis_m")
            kis_date = c2.date_input("수집 날짜", value=datetime.now().date(), key="kis_d")
            kis_code = c3.text_input("종목코드 (선택)", placeholder="비워두면 전체", key="kis_c")

            if st.form_submit_button("KIS 수집 실행"):
                try:
                    codes = [c.strip() for c in kis_code.split(",") if c.strip()] or None
                    with st.spinner("KIS 기초정보 수집 중..."):
                        result = admin_client.collect_fundamentals(
                            market=kis_market,
                            codes=codes,
                            date=kis_date.strftime("%Y-%m-%d"),
                        )
                    if result.get("skipped"):
                        st.warning(f"건너뜀: {result.get('message', '')}")
                    else:
                        st.success(
                            f"수집 완료: {result.get('success', 0)}/{result.get('total', 0)} 성공, "
                            f"저장 {result.get('saved', 0)}건"
                        )
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"수집 실패: {e}")

        st.markdown("---")
        st.subheader("DART 재무제표 수집")
        with st.form("collect_dart"):
            c4, c5, c6, c7 = st.columns(4)
            dart_market = c4.selectbox("마켓", MARKETS, key="dart_m")
            dart_year = c5.selectbox("연도", list(range(datetime.now().year, datetime.now().year - 5, -1)), key="dart_y")
            dart_quarter = c6.selectbox("분기", QUARTERS, key="dart_q")
            dart_code = c7.text_input("종목코드 (선택)", placeholder="비워두면 전체", key="dart_c")

            if st.form_submit_button("DART 수집 실행"):
                try:
                    codes = [c.strip() for c in dart_code.split(",") if c.strip()] or None
                    with st.spinner(f"DART 재무제표 수집 중 ({dart_year}{dart_quarter})..."):
                        result = admin_client.collect_financial_statements(
                            market=dart_market,
                            codes=codes,
                            year=dart_year,
                            quarter=dart_quarter,
                        )
                    if result.get("skipped"):
                        st.warning(f"건너뜀: {result.get('message', '')}")
                    else:
                        st.success(
                            f"수집 완료: {result.get('success', 0)}/{result.get('total', 0)} 성공, "
                            f"저장 {result.get('saved', 0)}건"
                        )
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"수집 실패: {e}")

    # ── 탭 2: KIS 기초정보 조회 ───────────────────────────
    with tab2:
        st.subheader("종목 기초정보 조회")
        c8, c9 = st.columns(2)
        q_market = c8.selectbox("마켓", MARKETS, key="q_m")
        q_code = c9.text_input("종목코드", placeholder="005930", key="q_c")

        if q_code.strip():
            try:
                data = admin_client.get_fundamentals(code=q_code.strip(), market=q_market)
                if data:
                    df = pd.DataFrame(data)
                    if len(df) > 0:
                        latest = df.iloc[-1]
                        m1, m2, m3, m4, m5 = st.columns(5)
                        m1.metric("날짜", str(latest.get("date", "-")))
                        m2.metric("PER", f"{latest['per']:.2f}" if latest.get("per") else "-")
                        m3.metric("PBR", f"{latest['pbr']:.2f}" if latest.get("pbr") else "-")
                        m4.metric("EPS", f"{latest['eps']:,.0f}" if latest.get("eps") else "-")
                        m5.metric("시가총액", f"{latest['market_cap']:,.0f}" if latest.get("market_cap") else "-")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("데이터 없음")
            except Exception as e:
                st.error(f"조회 실패: {e}")

    # ── 탭 3: DART 재무제표 조회 ──────────────────────────
    with tab3:
        st.subheader("종목 재무제표 조회")
        c10, c11 = st.columns(2)
        fs_market = c10.selectbox("마켓", MARKETS, key="fs_m")
        fs_code = c11.text_input("종목코드", placeholder="005930", key="fs_c")

        if fs_code.strip():
            try:
                data = admin_client.get_financial_statements(
                    code=fs_code.strip(), market=fs_market, limit=20,
                )
                if data:
                    df = pd.DataFrame(data)
                    if len(df) > 0:
                        latest = df.iloc[-1]
                        m6, m7, m8, m9, m10 = st.columns(5)
                        m6.metric("분기", str(latest.get("period", "-")))
                        m7.metric("매출", f"{latest['revenue']:,.0f}" if latest.get("revenue") else "-")
                        m8.metric("영업이익", f"{latest['operating_profit']:,.0f}" if latest.get("operating_profit") else "-")
                        m9.metric("ROE", f"{latest['roe']:.2f}%" if latest.get("roe") else "-")
                        m10.metric("부채비율", f"{latest['debt_ratio']:.2f}%" if latest.get("debt_ratio") else "-")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("데이터 없음")
            except Exception as e:
                st.error(f"조회 실패: {e}")

    # ── 탭 4: 데이터 커버리지 ─────────────────────────────
    with tab4:
        st.subheader("데이터 커버리지 현황")
        try:
            db_data = admin_client.get_db_status()
            tables = db_data.get("tables", {})

            si = tables.get("stock_info", {})
            sf = tables.get("stock_fundamental", {})
            fs = tables.get("financial_statement", {})

            total = si.get("row_count", 0)
            kis_pct = (sf.get("code_count", 0) / total * 100) if total > 0 else 0
            dart_pct = (fs.get("code_count", 0) / total * 100) if total > 0 else 0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("전체 등록 종목", f"{total:,}")
            c2.metric("KIS 수집 종목", f"{sf.get('code_count', 0):,}")
            c3.metric("DART 수집 종목", f"{fs.get('code_count', 0):,}")
            c4.metric("KIS 커버리지", f"{kis_pct:.1f}%")

            st.markdown("---")
            summary = pd.DataFrame({
                "테이블": ["stock_fundamental (KIS)", "financial_statement (DART)"],
                "레코드 수": [f"{sf.get('row_count', 0):,}", f"{fs.get('row_count', 0):,}"],
                "종목 수": [f"{sf.get('code_count', 0):,}", f"{fs.get('code_count', 0):,}"],
                "날짜/기간": [
                    f"{sf.get('earliest_date', '-')} ~ {sf.get('latest_date', '-')}",
                    f"{fs.get('period_count', 0)}개 분기",
                ],
                "마켓": [
                    ", ".join(sf.get("markets", [])) or "-",
                    ", ".join(fs.get("markets", [])) or "-",
                ],
            })
            st.dataframe(summary, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"커버리지 조회 실패: {e}")

"""공시/수급 관리 페이지 (Phase 5)"""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from admin.api.client import admin_client

MARKETS = ["KOSPI", "KOSDAQ"]


def render():
    st.header("공시/수급 관리")

    if st.button("새로고침", key="refresh_disclosure"):
        st.cache_data.clear()

    tab1, tab2, tab3, tab4 = st.tabs(["공시 수집", "공시 조회", "수급 수집/조회", "수집 현황"])

    # ── 탭 1: 공시 수집 ─────────────────────────────────
    with tab1:
        st.subheader("DART 공시 수집")
        with st.form("collect_disclosure"):
            c1, c2, c3 = st.columns(3)
            disc_market = c1.selectbox("마켓", MARKETS, key="disc_m")
            disc_code = c2.text_input(
                "종목코드 (콤마 구분)",
                placeholder="005930,000660 (비워두면 전체)",
                key="disc_c",
            )
            disc_days = c3.number_input(
                "수집 일수", min_value=7, max_value=365, value=60, key="disc_d",
            )
            disc_sentiment = st.checkbox(
                "센티먼트 분석 포함",
                value=True,
                key="disc_sent",
                help="공시 제목에 대해 KR-FinBert-SC 센티먼트 분석 수행",
            )

            if st.form_submit_button("공시 수집 실행"):
                try:
                    codes = None
                    if disc_code.strip():
                        codes = [c.strip() for c in disc_code.split(",") if c.strip()]

                    with st.spinner("DART 공시 수집 중... (수 분 소요될 수 있습니다)"):
                        result = admin_client.collect_disclosures(
                            market=disc_market,
                            codes=codes,
                            days=disc_days,
                            analyze_sentiment=disc_sentiment,
                        )

                    st.success(
                        f"수집 완료: {result.get('success', 0)}/{result.get('total', 0)} 성공, "
                        f"저장 {result.get('saved', 0)}건"
                    )
                    if result.get("failed", 0) > 0:
                        st.warning(f"실패 종목: {result['failed']}개")
                    if result.get("message"):
                        st.info(result["message"])
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"수집 실패: {e}")

    # ── 탭 2: 공시 조회 ─────────────────────────────────
    with tab2:
        st.subheader("공시 목록 조회")

        c4, c5, c6, c7 = st.columns(4)
        q_code = c4.text_input("종목코드", placeholder="005930", key="q_disc_c")
        q_start = c5.date_input(
            "시작일", value=datetime.now().date() - timedelta(days=90), key="q_disc_s",
        )
        q_end = c6.date_input("종료일", value=datetime.now().date(), key="q_disc_e")
        q_limit = c7.number_input("조회 건수", min_value=10, max_value=500, value=100, key="q_disc_l")

        if not q_code.strip():
            st.info("종목코드를 입력해주세요.")
        else:
            try:
                disclosures = admin_client.get_disclosures(
                    code=q_code.strip(),
                    start_date=q_start.strftime("%Y-%m-%d"),
                    end_date=q_end.strftime("%Y-%m-%d"),
                    limit=q_limit,
                )

                if disclosures:
                    df = pd.DataFrame(disclosures)

                    # 공시유형 분포 메트릭
                    total = len(df)
                    type_counts = df["report_type"].value_counts() if "report_type" in df.columns else pd.Series()

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("전체 공시", f"{total}건")
                    top_types = type_counts.head(3)
                    for i, (col, (rtype, cnt)) in enumerate(zip([m2, m3, m4], top_types.items())):
                        col.metric(rtype or "기타", f"{cnt}건")

                    st.markdown("---")

                    # 공시 목록 테이블
                    display_cols = ["date", "code", "report_nm", "report_type", "sentiment_score", "type_score", "flr_nm"]
                    available = [c for c in display_cols if c in df.columns]
                    df_display = df[available].copy()
                    if "sentiment_score" in df_display.columns:
                        df_display["sentiment_score"] = df_display["sentiment_score"].apply(
                            lambda x: f"{x:+.3f}" if pd.notna(x) else "-"
                        )
                    if "type_score" in df_display.columns:
                        df_display["type_score"] = df_display["type_score"].apply(
                            lambda x: f"{x:.2f}" if pd.notna(x) else "-"
                        )
                    col_map = {
                        "date": "날짜", "code": "종목코드", "report_nm": "보고서명",
                        "report_type": "유형", "sentiment_score": "센티먼트",
                        "type_score": "유형점수", "flr_nm": "공시자",
                    }
                    df_display.rename(columns={k: v for k, v in col_map.items() if k in df_display.columns}, inplace=True)
                    st.dataframe(df_display, use_container_width=True, hide_index=True, height=400)
                else:
                    st.info("해당 조건의 공시가 없습니다.")
            except Exception as e:
                st.error(f"조회 실패: {e}")

    # ── 탭 3: 수급 수집/조회 ─────────────────────────────
    with tab3:
        st.subheader("KRX 수급 데이터")

        # 수집 폼
        with st.form("collect_supply"):
            sc1, sc2, sc3 = st.columns(3)
            sup_market = sc1.selectbox("마켓", MARKETS, key="sup_m")
            sup_code = sc2.text_input(
                "종목코드 (콤마 구분)",
                placeholder="005930,000660 (비워두면 전체)",
                key="sup_c",
            )
            sup_days = sc3.number_input(
                "수집 일수", min_value=7, max_value=365, value=60, key="sup_d",
            )

            if st.form_submit_button("수급 수집 실행"):
                try:
                    codes = None
                    if sup_code.strip():
                        codes = [c.strip() for c in sup_code.split(",") if c.strip()]

                    with st.spinner("KRX 수급 데이터 수집 중..."):
                        result = admin_client.collect_supply_demand(
                            market=sup_market,
                            codes=codes,
                            days=sup_days,
                        )

                    st.success(
                        f"수집 완료: {result.get('success', 0)}/{result.get('total', 0)} 성공, "
                        f"저장 {result.get('saved', 0)}건"
                    )
                    if result.get("failed", 0) > 0:
                        st.warning(f"실패 종목: {result['failed']}개")
                    if result.get("message"):
                        st.info(result["message"])
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"수집 실패: {e}")

        st.markdown("---")

        # 조회
        st.subheader("수급 데이터 조회")
        qc1, qc2, qc3, qc4, qc5 = st.columns(5)
        sq_market = qc1.selectbox("마켓", MARKETS, key="sq_m")
        sq_code = qc2.text_input("종목코드", placeholder="005930", key="sq_c")
        sq_start = qc3.date_input(
            "시작일", value=datetime.now().date() - timedelta(days=60), key="sq_s",
        )
        sq_end = qc4.date_input("종료일", value=datetime.now().date(), key="sq_e")
        sq_limit = qc5.number_input("조회 건수", min_value=10, max_value=500, value=100, key="sq_l")

        if not sq_code.strip():
            st.info("종목코드를 입력해주세요.")
        else:
            try:
                supply_data = admin_client.get_supply_demand(
                    market=sq_market,
                    code=sq_code.strip(),
                    start_date=sq_start.strftime("%Y-%m-%d"),
                    end_date=sq_end.strftime("%Y-%m-%d"),
                    limit=sq_limit,
                )

                if supply_data:
                    df = pd.DataFrame(supply_data)

                    # 메트릭
                    sm1, sm2, sm3 = st.columns(3)
                    avg_short = df["short_selling_ratio"].mean() if "short_selling_ratio" in df.columns else 0
                    sm1.metric("평균 공매도 비율", f"{avg_short:.2f}%")
                    if "program_net_volume" in df.columns:
                        net_sum = df["program_net_volume"].sum()
                        sm2.metric("프로그램 순매수 합계", f"{net_sum:,.0f}")
                    if "short_selling_volume" in df.columns:
                        avg_short_vol = df["short_selling_volume"].mean()
                        sm3.metric("평균 공매도 거래량", f"{avg_short_vol:,.0f}")

                    st.markdown("---")

                    # 테이블
                    display_cols = ["date", "short_selling_volume", "short_selling_ratio",
                                    "program_buy_volume", "program_sell_volume", "program_net_volume"]
                    available = [c for c in display_cols if c in df.columns]
                    df_display = df[available].copy()
                    col_map = {
                        "date": "날짜", "short_selling_volume": "공매도 거래량",
                        "short_selling_ratio": "공매도 비율(%)",
                        "program_buy_volume": "프로그램 매수",
                        "program_sell_volume": "프로그램 매도",
                        "program_net_volume": "프로그램 순매수",
                    }
                    df_display.rename(columns={k: v for k, v in col_map.items() if k in df_display.columns}, inplace=True)
                    st.dataframe(df_display, use_container_width=True, hide_index=True, height=400)
                else:
                    st.info("해당 조건의 수급 데이터가 없습니다.")
            except Exception as e:
                st.error(f"조회 실패: {e}")

    # ── 탭 4: 수집 현황 ─────────────────────────────────
    with tab4:
        st.subheader("공시/수급 수집 현황")

        try:
            db_data = admin_client.get_db_status()
            tables = db_data.get("tables", {})

            # DART 공시 현황
            dd = tables.get("dart_disclosure", {})
            st.markdown("#### DART 공시")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("총 공시 수", f"{dd.get('row_count', 0):,}")
            d2.metric("종목 수", f"{dd.get('code_count', 0):,}")
            d3.metric("시작일", dd.get("earliest_date", "-") or "-")
            d4.metric("종료일", dd.get("latest_date", "-") or "-")

            st.markdown("---")

            # KRX 수급 현황
            ks = tables.get("krx_supply_demand", {})
            st.markdown("#### KRX 수급")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("총 레코드 수", f"{ks.get('row_count', 0):,}")
            k2.metric("종목 수", f"{ks.get('code_count', 0):,}")
            k3.metric("시작일", ks.get("earliest_date", "-") or "-")
            k4.metric("종료일", ks.get("latest_date", "-") or "-")

        except Exception as e:
            st.error(f"현황 조회 실패: {e}")

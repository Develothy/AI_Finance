"""뉴스 센티먼트 관리 페이지"""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from admin.api.client import admin_client

MARKETS = ["KR"]


def render():
    st.header("뉴스 센티먼트 관리")

    if st.button("새로고침", key="refresh_news"):
        st.cache_data.clear()

    tab1, tab2, tab3 = st.tabs(["뉴스 수집", "기사 조회", "수집 현황"])

    # ── 탭 1: 뉴스 수집 ─────────────────────────────────
    with tab1:
        st.subheader("뉴스 수집 실행")
        with st.form("collect_news"):
            c1, c2, c3 = st.columns(3)
            news_market = c1.selectbox("마켓", MARKETS, key="news_m")
            news_code = c2.text_input(
                "종목코드 (선택)",
                placeholder="비워두면 전체 종목",
                key="news_c",
            )
            max_items = c3.number_input(
                "종목당 최대 기사 수", min_value=10, max_value=200, value=50, key="news_max",
            )

            include_market = st.checkbox(
                "시장 전체 뉴스 포함",
                value=False,
                key="news_incl",
                help="전체 종목 수집 시에만 체크 (개별 종목 수집 시 불필요)",
            )

            if st.form_submit_button("뉴스 수집 실행"):
                try:
                    codes = None
                    if news_code.strip():
                        # [["code", "name"]] 형식 — name 없이 code만 전달
                        codes = [[c.strip(), ""] for c in news_code.split(",") if c.strip()]

                    with st.spinner("뉴스 수집 + 센티먼트 분석 중... (수 분 소요될 수 있습니다)"):
                        result = admin_client.collect_news(
                            market=news_market,
                            codes=codes,
                            include_market_news=include_market,
                            max_items_per_code=max_items,
                        )

                    st.success(
                        f"수집 완료: 종목 {result.get('stock_success', 0)}/{result.get('total_codes', 0)} 성공, "
                        f"시장뉴스 {result.get('market_news', 0)}건, "
                        f"저장 {result.get('saved', 0)}건"
                    )
                    if result.get("stock_failed", 0) > 0:
                        st.warning(f"실패 종목: {result['stock_failed']}개")
                    if result.get("message"):
                        st.info(result["message"])
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"수집 실패: {e}")

    # ── 탭 2: 기사 조회 ─────────────────────────────────
    with tab2:
        st.subheader("뉴스 기사 조회")

        c4, c5, c6, c7 = st.columns(4)
        q_code = c4.text_input("종목코드", placeholder="005930 (비워두면 전체)", key="q_news_c")
        q_start = c5.date_input(
            "시작일", value=datetime.now().date() - timedelta(days=30), key="q_news_s",
        )
        q_end = c6.date_input("종료일", value=datetime.now().date(), key="q_news_e")
        q_limit = c7.number_input("조회 건수", min_value=10, max_value=500, value=100, key="q_news_l")

        try:
            articles = admin_client.get_news_articles(
                code=q_code.strip() or None,
                start_date=q_start.strftime("%Y-%m-%d"),
                end_date=q_end.strftime("%Y-%m-%d"),
                limit=q_limit,
            )

            if articles:
                df = pd.DataFrame(articles)

                # 센티먼트 분포 메트릭
                total = len(df)
                pos = len(df[df["sentiment_label"] == "positive"])
                neg = len(df[df["sentiment_label"] == "negative"])
                neu = len(df[df["sentiment_label"] == "neutral"])

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("전체 기사", f"{total}건")
                m2.metric("긍정", f"{pos}건 ({pos/total*100:.1f}%)" if total > 0 else "0건")
                m3.metric("중립", f"{neu}건 ({neu/total*100:.1f}%)" if total > 0 else "0건")
                m4.metric("부정", f"{neg}건 ({neg/total*100:.1f}%)" if total > 0 else "0건")

                st.markdown("---")

                # 기사 목록
                df_display = df[["date", "code", "title", "sentiment_label", "sentiment_score", "source"]].copy()
                df_display["sentiment_score"] = df_display["sentiment_score"].apply(
                    lambda x: f"{x:+.3f}" if pd.notna(x) else "-"
                )
                df_display.columns = ["날짜", "종목코드", "제목", "센티먼트", "점수", "출처"]
                st.dataframe(df_display, use_container_width=True, hide_index=True, height=400)
            else:
                st.info("해당 조건의 뉴스가 없습니다.")
        except Exception as e:
            st.error(f"조회 실패: {e}")

    # ── 탭 3: 수집 현황 ─────────────────────────────────
    with tab3:
        st.subheader("뉴스 수집 현황")

        try:
            db_data = admin_client.get_db_status()
            tables = db_data.get("tables", {})
            ns = tables.get("news_sentiment", {})

            c8, c9, c10, c11 = st.columns(4)
            c8.metric("총 기사 수", f"{ns.get('row_count', 0):,}")
            c9.metric("수집 종목 수", f"{ns.get('code_count', 0):,}")
            c10.metric("시작일", ns.get("earliest_date", "-"))
            c11.metric("종료일", ns.get("latest_date", "-"))

            st.markdown("---")

            # 최근 수집 기사로 센티먼트 분포 확인
            recent = admin_client.get_news_articles(limit=500)
            if recent:
                df_recent = pd.DataFrame(recent)
                label_counts = df_recent["sentiment_label"].value_counts()

                st.subheader("최근 기사 센티먼트 분포")
                summary = pd.DataFrame({
                    "센티먼트": label_counts.index,
                    "기사 수": label_counts.values,
                    "비율": [f"{v / len(df_recent) * 100:.1f}%" for v in label_counts.values],
                })
                st.dataframe(summary, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"현황 조회 실패: {e}")

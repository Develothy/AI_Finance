"""
퀀트 플랫폼 대시보드
실행: streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

st.set_page_config(
    page_title="퀀트 플랫폼",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from dashboard.pages import market_overview, sector_view, stock_analysis

PAGES = {
    "종목 분석": stock_analysis,
    "시장 개요": market_overview,
    "섹터 분석": sector_view,
}

st.sidebar.title("퀀트 플랫폼")
selection = st.sidebar.radio("페이지", list(PAGES.keys()))
st.sidebar.markdown("---")

PAGES[selection].render()

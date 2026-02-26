"""
퀀트 플랫폼 관리자 대시보드
실행: streamlit run admin/app.py --server.port 8502
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

st.set_page_config(
    page_title="퀀트 관리자",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from admin.pages import (
    server_status, db_status, log_viewer, config_viewer, scheduler_manager,
    ml_train_manager, ml_models, ml_predictions,
)

PAGES = {
    "서버 상태": server_status,
    "DB 상태": db_status,
    "로그 조회": log_viewer,
    "설정 확인": config_viewer,
    "스케줄러 관리": scheduler_manager,
    "ML 학습 관리": ml_train_manager,
    "ML 모델 결과": ml_models,
    "ML 예측 테스트": ml_predictions,
}

st.sidebar.title("퀀트 관리자")
selection = st.sidebar.radio("페이지", list(PAGES.keys()))
st.sidebar.markdown("---")

PAGES[selection].render()

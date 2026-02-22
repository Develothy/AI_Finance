"""
관리자 대시보드 설정
"""

import os

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT = 30
PAGE_TITLE = "퀀트 관리자"

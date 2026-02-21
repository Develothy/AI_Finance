"""
대시보드 설정
"""

import os


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT = 30
DEFAULT_MARKET = "KOSPI"
DEFAULT_DAYS = 90
PAGE_TITLE = "퀀트 플랫폼"

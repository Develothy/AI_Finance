"""
관리자 대시보드 설정
"""

import os
from datetime import datetime, timedelta, timezone

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT = 30
PAGE_TITLE = "퀀트 관리자"

KST = timezone(timedelta(hours=9))


def utc_to_kst(dt_str: str | None) -> str:
    if not dt_str or dt_str == "-":
        return "-"
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        dt_kst = dt.replace(tzinfo=timezone.utc).astimezone(KST)
        return dt_kst.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return dt_str

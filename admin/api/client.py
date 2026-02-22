"""
어드민 API 클라이언트
"""

import requests
import streamlit as st

from admin.config import API_BASE_URL, REQUEST_TIMEOUT


class AdminAPIClient:

    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.timeout = REQUEST_TIMEOUT

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, json=json, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, json: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.put(url, json=json, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.delete(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ── 모니터링 ────────────────────────────────────────

    @st.cache_data(ttl=10, show_spinner=False)
    def get_health(_self) -> dict:
        return _self._get("/admin/health")

    @st.cache_data(ttl=10, show_spinner=False)
    def get_db_status(_self) -> dict:
        return _self._get("/admin/db")

    @st.cache_data(ttl=5, show_spinner=False)
    def get_logs(_self, file: str = "app", lines: int = 100, level: str = None, search: str = None) -> dict:
        params = {"file": file, "lines": lines}
        if level:
            params["level"] = level
        if search:
            params["search"] = search
        return _self._get("/admin/logs", params)

    @st.cache_data(ttl=30, show_spinner=False)
    def get_config(_self) -> dict:
        return _self._get("/admin/config")

    # ── 스케줄러 ────────────────────────────────────────

    @st.cache_data(ttl=5, show_spinner=False)
    def get_schedule_jobs(_self) -> list[dict]:
        return _self._get("/admin/scheduler/jobs")

    def create_schedule_job(_self, data: dict) -> dict:
        return _self._post("/admin/scheduler/jobs", json=data)

    def update_schedule_job(_self, job_id: int, data: dict) -> dict:
        return _self._put(f"/admin/scheduler/jobs/{job_id}", json=data)

    def delete_schedule_job(_self, job_id: int) -> dict:
        return _self._delete(f"/admin/scheduler/jobs/{job_id}")

    def run_schedule_job(_self, job_id: int) -> dict:
        return _self._post(f"/admin/scheduler/jobs/{job_id}/run")

    @st.cache_data(ttl=5, show_spinner=False)
    def get_schedule_logs(_self, job_id: int = None, limit: int = 20) -> list[dict]:
        params = {"limit": limit}
        if job_id:
            params["job_id"] = job_id
        return _self._get("/admin/scheduler/logs", params)


admin_client = AdminAPIClient()

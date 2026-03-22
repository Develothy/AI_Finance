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

    def get_step_logs(_self, log_id: int) -> list[dict]:
        return _self._get(f"/admin/scheduler/logs/{log_id}/steps")

    def get_step_log_text(_self, log_id: int, step_type: str) -> dict:
        return _self._get(f"/admin/scheduler/logs/{log_id}/steps/{step_type}/log")

    def run_single_step(_self, job_id: int, step_type: str) -> dict:
        return _self._post(f"/admin/scheduler/jobs/{job_id}/run-step", json={"step_type": step_type})

    def run_from_step(_self, job_id: int, from_step: str) -> dict:
        return _self._post(f"/admin/scheduler/jobs/{job_id}/run-from", json={"from_step": from_step})

    # ── 종목 검색 ────────────────────────────────────────

    def search_stocks(_self, keyword: str, market: str = None, limit: int = 50) -> list[dict]:
        params = {"keyword": keyword, "limit": limit}
        if market:
            params["market"] = market
        return _self._get("/stocks/search", params)

    def search_stocks_by_sector(_self, keyword: str, market: str = None) -> list[dict]:
        params = {"keyword": keyword}
        if market:
            params["market"] = market
        return _self._get("/stocks/search/sector", params)

    # ── ML 모델 ────────────────────────────────────────

    @st.cache_data(ttl=10, show_spinner=False)
    def get_ml_models(_self, market: str = None) -> list[dict]:
        params = {}
        if market:
            params["market"] = market
        return _self._get("/ml/models", params)

    @st.cache_data(ttl=10, show_spinner=False)
    def get_ml_model_detail(_self, model_id: int) -> dict:
        return _self._get(f"/ml/models/{model_id}")

    def delete_ml_model(_self, model_id: int) -> dict:
        return _self._delete(f"/ml/models/{model_id}")

    @st.cache_data(ttl=30, show_spinner=False)
    def get_feature_importance(_self, model_id: int) -> dict:
        return _self._get(f"/ml/feature-importance/{model_id}")

    # ── ML 예측 ────────────────────────────────────────

    def run_prediction(_self, code: str, market: str = "KOSPI", model_id: int = None) -> dict:
        params = {"market": market}
        if model_id:
            params["model_id"] = model_id
        url = f"{_self.base_url}/ml/predict/{code}"
        resp = requests.post(url, params=params, timeout=_self.timeout)
        resp.raise_for_status()
        return resp.json()

    @st.cache_data(ttl=5, show_spinner=False)
    def get_predictions(_self, market: str = None, code: str = None, limit: int = 50) -> list[dict]:
        params = {"limit": limit}
        if market:
            params["market"] = market
        if code:
            params["code"] = code
        return _self._get("/ml/predictions", params)


    # ── 뉴스 ─────────────────────────────────────────

    def collect_news(
        _self,
        market: str = "KR",
        codes: list[list[str]] = None,
        include_market_news: bool = True,
        max_items_per_code: int = 50,
    ) -> dict:
        json_data = {
            "market": market,
            "include_market_news": include_market_news,
            "max_items_per_code": max_items_per_code,
        }
        if codes:
            json_data["codes"] = codes
        url = f"{_self.base_url}/news/collect"
        resp = requests.post(url, json=json_data, timeout=600)
        resp.raise_for_status()
        return resp.json()

    @st.cache_data(ttl=30, show_spinner=False)
    def get_news_articles(
        _self,
        code: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100,
    ) -> list[dict]:
        params = {"limit": limit}
        if code:
            params["code"] = code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get("/news/articles", params)

    @st.cache_data(ttl=30, show_spinner=False)
    def get_news_sentiment_summary(_self, code: str) -> dict | None:
        try:
            return _self._get(f"/news/sentiment/{code}")
        except Exception:
            return None

    # ── 재무 데이터 ────────────────────────────────────

    def collect_fundamentals(_self, market: str, codes: list[str] = None, date: str = None) -> dict:
        json_data = {"market": market}
        if codes:
            json_data["codes"] = codes
        if date:
            json_data["date"] = date
        return _self._post("/fundamental/collect", json=json_data)

    def collect_financial_statements(
        _self, market: str, codes: list[str] = None, year: int = None, quarter: str = None,
    ) -> dict:
        json_data = {"market": market}
        if codes:
            json_data["codes"] = codes
        if year:
            json_data["year"] = year
        if quarter:
            json_data["quarter"] = quarter
        return _self._post("/fundamental/collect/financial", json=json_data)

    @st.cache_data(ttl=30, show_spinner=False)
    def get_fundamental_summary(_self, code: str, market: str = "KOSPI") -> dict:
        return _self._get(f"/fundamental/summary/{code}", params={"market": market})

    @st.cache_data(ttl=30, show_spinner=False)
    def get_fundamentals(_self, code: str, market: str = "KOSPI") -> list:
        return _self._get(f"/fundamental/{code}", params={"market": market})

    @st.cache_data(ttl=30, show_spinner=False)
    def get_financial_statements(_self, code: str, market: str = "KOSPI", limit: int = 20) -> list:
        return _self._get(f"/fundamental/{code}/financial", params={"market": market, "limit": limit})

    # ── 공시/수급 ────────────────────────────────────

    def collect_disclosures(
        _self,
        market: str = "KOSPI",
        codes: list[str] = None,
        days: int = 60,
        analyze_sentiment: bool = True,
    ) -> dict:
        json_data = {"market": market, "days": days, "analyze_sentiment": analyze_sentiment}
        if codes:
            json_data["codes"] = codes
        url = f"{_self.base_url}/disclosure/collect"
        resp = requests.post(url, json=json_data, timeout=600)
        resp.raise_for_status()
        return resp.json()

    @st.cache_data(ttl=30, show_spinner=False)
    def get_disclosures(
        _self,
        code: str,
        market: str = "KOSPI",
        start_date: str = None,
        end_date: str = None,
        limit: int = 100,
    ) -> list:
        params = {"market": market, "code": code, "limit": limit}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get("/disclosure/list", params)

    def collect_supply_demand(
        _self,
        market: str = "KOSPI",
        codes: list[str] = None,
        days: int = 60,
    ) -> dict:
        json_data = {"market": market, "days": days}
        if codes:
            json_data["codes"] = codes
        url = f"{_self.base_url}/disclosure/supply/collect"
        resp = requests.post(url, json=json_data, timeout=600)
        resp.raise_for_status()
        return resp.json()

    @st.cache_data(ttl=30, show_spinner=False)
    def get_supply_demand(
        _self,
        market: str,
        code: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100,
    ) -> list:
        params = {"limit": limit}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/disclosure/supply/{market}/{code}", params)


    # ── 레이스 ────────────────────────────────────────

    def run_model_race(_self, data: dict) -> dict:
        url = f"{_self.base_url}/backtest/race/model"
        resp = requests.post(url, json=data, timeout=300)
        resp.raise_for_status()
        return resp.json()

    def run_stock_race(_self, data: dict) -> dict:
        url = f"{_self.base_url}/backtest/race/stock"
        resp = requests.post(url, json=data, timeout=300)
        resp.raise_for_status()
        return resp.json()

    @st.cache_data(ttl=30, show_spinner=False)
    def get_race_results(_self, race_group: str) -> dict:
        return _self._get(f"/backtest/race/{race_group}")


admin_client = AdminAPIClient()

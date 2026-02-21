"""
FastAPI 백엔드 HTTP 클라이언트
"""

from typing import Optional

import requests
import streamlit as st

from dashboard.config import API_BASE_URL, REQUEST_TIMEOUT


class APIClient:

    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.timeout = REQUEST_TIMEOUT

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ── Stock ──────────────────────────────────────────

    @st.cache_data(ttl=60, show_spinner=False)
    def get_prices_by_code(
        _self,
        code: str,
        market: str = "KOSPI",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 300,
    ) -> list[dict]:
        params = {"market": market, "limit": limit}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/stocks/prices/code/{code}", params)

    @st.cache_data(ttl=300, show_spinner=False)
    def get_stock_info(_self, code: str, market: str = "KOSPI") -> dict | None:
        try:
            return _self._get(f"/stocks/info/{code}", {"market": market})
        except Exception:
            return None

    @st.cache_data(ttl=60, show_spinner=False)
    def get_stocks_by_sector(
        _self, sector: str, market: Optional[str] = None
    ) -> list[dict]:
        params = {}
        if market:
            params["market"] = market
        return _self._get(f"/stocks/stocks/sector/{sector}", params)

    # ── Indicators ─────────────────────────────────────

    @st.cache_data(ttl=60, show_spinner=False)
    def get_sma(
        _self,
        code: str,
        market: str = "KOSPI",
        period: int = 20,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        params = {"market": market, "period": period}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/indicators/sma/{code}", params)

    @st.cache_data(ttl=60, show_spinner=False)
    def get_ema(
        _self,
        code: str,
        market: str = "KOSPI",
        period: int = 20,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        params = {"market": market, "period": period}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/indicators/ema/{code}", params)

    @st.cache_data(ttl=60, show_spinner=False)
    def get_rsi(
        _self,
        code: str,
        market: str = "KOSPI",
        period: int = 14,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        params = {"market": market, "period": period}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/indicators/rsi/{code}", params)

    @st.cache_data(ttl=60, show_spinner=False)
    def get_macd(
        _self,
        code: str,
        market: str = "KOSPI",
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        params = {"market": market, "fast": fast, "slow": slow, "signal": signal}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/indicators/macd/{code}", params)

    @st.cache_data(ttl=60, show_spinner=False)
    def get_bollinger(
        _self,
        code: str,
        market: str = "KOSPI",
        period: int = 20,
        num_std: float = 2.0,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        params = {"market": market, "period": period, "num_std": num_std}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/indicators/bollinger/{code}", params)

    @st.cache_data(ttl=60, show_spinner=False)
    def get_obv(
        _self,
        code: str,
        market: str = "KOSPI",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        params = {"market": market}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return _self._get(f"/indicators/obv/{code}", params)


api_client = APIClient()

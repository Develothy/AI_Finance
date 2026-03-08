"""
수급 데이터 수집 (Phase 5B)
===========================

KIS API를 사용하여 공매도/프로그램매매 일별 데이터를 수집한다.
(pykrx → KIS API 전환: KRX가 자동화 요청을 차단하여 pykrx 사용 불가)
"""

import time
from datetime import date, timedelta, datetime
from dataclasses import dataclass, field
from typing import Optional

import requests

from config import settings
from core import get_logger, retry

logger = get_logger("supply_fetcher")

# KIS API
_BASE_URL_REAL = "https://openapi.koreainvestment.com:9443"
_BASE_URL_MOCK = "https://openapivts.koreainvestment.com:29443"
_REQUEST_INTERVAL = 0.12  # 초당 요청 제한


@dataclass
class KRXFetchResult:
    """수급 수집 결과"""
    records: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    message: str = ""


class KRXSupplyFetcher:
    """수급 데이터 수집기 (KIS API)"""

    def __init__(self):
        self.mock_mode = settings.KIS_MOCK_MODE

        if self.mock_mode:
            self.app_key = settings.KIS_MOCK_APP_KEY
            self.app_secret = settings.KIS_MOCK_APP_SECRET
        else:
            self.app_key = settings.KIS_APP_KEY
            self.app_secret = settings.KIS_APP_SECRET

        self.available = bool(self.app_key and self.app_secret)
        self.base_url = _BASE_URL_MOCK if self.mock_mode else _BASE_URL_REAL

        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

        if not self.available:
            logger.warning("KIS API 키 미설정 — 수급 수집 비활성화", "__init__")

    # ── OAuth ──────────────────────────────────────────

    def _get_access_token(self) -> str:
        now = datetime.now()
        if self._access_token and self._token_expires_at and now < self._token_expires_at:
            return self._access_token

        url = f"{self.base_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        resp = requests.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        self._token_expires_at = now + timedelta(hours=23)
        logger.info("KIS OAuth 토큰 발급 (supply_fetcher)", "_get_access_token")
        return self._access_token

    def _make_headers(self, tr_id: str) -> dict:
        token = self._get_access_token()
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    # ── 공매도 일별 ──────────────────────────────────

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="supply_fetcher")
    def _fetch_short_selling(self, code: str, start: str, end: str) -> list[dict]:
        """
        KIS API 공매도 일별추이 (TR: FHPST04830000)

        Returns:
            [{date, short_selling_volume, short_selling_ratio}, ...]
        """
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/daily-short-sale"
        headers = self._make_headers("FHPST04830000")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
        }

        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") != "0":
            logger.warning(
                f"공매도 조회 실패: {data.get('msg1', '')}",
                "_fetch_short_selling",
                {"code": code},
            )
            return []

        items = data.get("output2", [])
        records = []
        for item in items:
            dt_str = item.get("stck_bsop_date", "")
            if not dt_str:
                continue
            records.append({
                "date": date(int(dt_str[:4]), int(dt_str[4:6]), int(dt_str[6:])),
                "short_selling_volume": _safe_int(item.get("ssts_cntg_qty")),
                "short_selling_ratio": _safe_float(item.get("ssts_vol_rlim")),
            })
        return records

    # ── 프로그램매매 일별 ──────────────────────────────

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="supply_fetcher")
    def _fetch_program_trading(self, code: str, end: str) -> list[dict]:
        """
        KIS API 프로그램매매 종목별 일별 (TR: FHPPG04650201)
        FID_INPUT_DATE_1 = 종료일 기준으로 과거 30영업일 데이터 반환

        Returns:
            [{date, program_buy_volume, program_sell_volume}, ...]
        """
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/program-trade-by-stock-daily"
        headers = self._make_headers("FHPPG04650201")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": end,
        }

        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") != "0":
            logger.warning(
                f"프로그램매매 조회 실패: {data.get('msg1', '')}",
                "_fetch_program_trading",
                {"code": code},
            )
            return []

        items = data.get("output", [])
        records = []
        for item in items:
            dt_str = item.get("stck_bsop_date", "")
            if not dt_str:
                continue
            records.append({
                "date": date(int(dt_str[:4]), int(dt_str[4:6]), int(dt_str[6:])),
                "program_buy_volume": _safe_int(item.get("whol_smtn_shnu_vol")),
                "program_sell_volume": _safe_int(item.get("whol_smtn_seln_vol")),
            })
        return records

    # ── 분할 호출 (API 반환 제한 대응) ─────────────────

    def _fetch_chunked_short(self, code: str, start: str, end: str) -> list[dict]:
        """공매도: ~40영업일(56일) 단위로 분할 호출"""
        _CHUNK_DAYS = 56
        all_records = []
        seen_dates = set()
        start_dt = date(int(start[:4]), int(start[4:6]), int(start[6:]))
        end_dt = date(int(end[:4]), int(end[4:6]), int(end[6:]))

        cursor = start_dt
        while cursor <= end_dt:
            chunk_end = min(cursor + timedelta(days=_CHUNK_DAYS - 1), end_dt)
            records = self._fetch_short_selling(
                code, cursor.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"),
            )
            time.sleep(_REQUEST_INTERVAL)
            for r in records:
                if r["date"] not in seen_dates:
                    all_records.append(r)
                    seen_dates.add(r["date"])
            cursor = chunk_end + timedelta(days=1)

        return all_records

    def _fetch_chunked_program(self, code: str, start: str, end: str) -> list[dict]:
        """프로그램매매: 30영업일(45일) 단위로 분할 호출 (end_date 기준 과거)"""
        _CHUNK_DAYS = 45
        all_records = []
        seen_dates = set()
        start_dt = date(int(start[:4]), int(start[4:6]), int(start[6:]))
        end_dt = date(int(end[:4]), int(end[4:6]), int(end[6:]))

        cursor = end_dt
        while cursor >= start_dt:
            records = self._fetch_program_trading(code, cursor.strftime("%Y%m%d"))
            time.sleep(_REQUEST_INTERVAL)
            oldest = cursor
            for r in records:
                if r["date"] not in seen_dates and r["date"] >= start_dt:
                    all_records.append(r)
                    seen_dates.add(r["date"])
                if r["date"] < oldest:
                    oldest = r["date"]
            # 다음 청크: 가장 오래된 날짜 - 1일을 종료일로
            cursor = oldest - timedelta(days=1)

        return all_records

    # ── 단일 종목 수집 ────────────────────────────────

    def fetch_supply_demand(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        days: int = 60,
    ) -> list[dict]:
        """
        단일 종목의 공매도 + 프로그램매매 수집

        Args:
            code: 종목코드 (6자리)
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
            days: lookback 일수
        """
        if not self.available:
            return []

        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            start_dt = date.today() - timedelta(days=days)
            start_date = start_dt.strftime("%Y-%m-%d")

        start_fmt = start_date.replace("-", "")
        end_fmt = end_date.replace("-", "")

        # 1) 공매도 일별 — 40영업일 제한이므로 구간 분할
        short_records = self._fetch_chunked_short(code, start_fmt, end_fmt)

        # 2) 프로그램매매 일별 — 30영업일 제한이므로 구간 분할
        program_records = self._fetch_chunked_program(code, start_fmt, end_fmt)

        # 3) 날짜 기준 병합
        short_by_date = {r["date"]: r for r in short_records}
        program_by_date = {r["date"]: r for r in program_records}

        all_dates = sorted(set(short_by_date.keys()) | set(program_by_date.keys()))

        records = []
        for dt in all_dates:
            short = short_by_date.get(dt, {})
            prog = program_by_date.get(dt, {})
            records.append({
                "code": code,
                "date": dt,
                "short_selling_volume": short.get("short_selling_volume"),
                "short_selling_ratio": short.get("short_selling_ratio"),
                "program_buy_volume": prog.get("program_buy_volume"),
                "program_sell_volume": prog.get("program_sell_volume"),
                "source": "kis",
            })

        return records

    # ── 전체 종목 수집 ────────────────────────────────

    def fetch_all(
        self,
        codes: list[str],
        market: str,
        start_date: str = None,
        end_date: str = None,
        days: int = 60,
    ) -> KRXFetchResult:
        """전체 종목의 수급 데이터 수집"""
        result = KRXFetchResult(total_count=len(codes))

        if not self.available:
            result.message = "KIS API 키 미설정 — 수급 수집 건너뜀"
            logger.warning(result.message, "fetch_all")
            return result

        logger.info(
            f"수급 수집 시작: {market} ({len(codes)}종목)",
            "fetch_all",
        )

        for code in codes:
            try:
                records = self.fetch_supply_demand(
                    code, start_date, end_date, days,
                )
                for rec in records:
                    rec["market"] = market
                result.records.extend(records)
                result.success_count += 1
            except Exception as e:
                result.failed_count += 1
                logger.warning(
                    f"수급 수집 실패: {code} - {e}",
                    "fetch_all",
                )

        result.message = (
            f"수급 수집 완료: {result.success_count}/{result.total_count} 성공, "
            f"{len(result.records)}건"
        )
        logger.info(result.message, "fetch_all")
        return result


# ── 유틸리티 ─────────────────────────────────────────────────

def _safe_int(val) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    if val is None or val == "":
        return None
    try:
        return round(float(val), 4)
    except (ValueError, TypeError):
        return None

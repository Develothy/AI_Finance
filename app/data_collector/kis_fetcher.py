"""
KIS API (한국투자증권) 데이터 수집
================================

기업 기초정보(PER/PBR/EPS/시가총액) + 투자자별 매매동향(기관/외인/개인 순매수)

API 키가 설정되지 않으면 graceful skip.
"""

import time
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field

import requests

from config import settings
from core import get_logger, retry, APIConnectionError

logger = get_logger("kis_fetcher")

# KIS API Base URL
_BASE_URL_REAL = "https://openapi.koreainvestment.com:9443"
_BASE_URL_MOCK = "https://openapivts.koreainvestment.com:29443"

# 초당 요청 제한 (20/sec → 0.05초 간격, 여유 있게 0.1초)
_REQUEST_INTERVAL = 0.1


@dataclass
class KISFetchResult:
    """KIS API 수집 결과"""
    fundamentals: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    skipped: bool = False
    message: str = ""


class KISClient:
    """한국투자증권 Open API 클라이언트"""

    def __init__(self):
        self.app_key = settings.KIS_APP_KEY
        self.app_secret = settings.KIS_APP_SECRET
        self.account_no = settings.KIS_ACCOUNT_NO
        self.mock_mode = settings.KIS_MOCK_MODE

        self.available = bool(self.app_key and self.app_secret)
        self.base_url = _BASE_URL_MOCK if self.mock_mode else _BASE_URL_REAL

        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

        if not self.available:
            logger.warning(
                "KIS API 키 미설정 — 기초정보 수집 비활성화",
                "__init__",
            )

    # ============================================================
    # OAuth
    # ============================================================

    def _get_access_token(self) -> str:
        """OAuth2 액세스 토큰 발급 (캐시)"""
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
        # 토큰 유효기간: 보통 24시간, 안전하게 23시간으로 설정
        from datetime import timedelta
        self._token_expires_at = now + timedelta(hours=23)

        logger.info("KIS OAuth 토큰 발급 완료", "_get_access_token")
        return self._access_token

    def _make_headers(self, tr_id: str) -> dict:
        """API 요청 헤더 생성"""
        token = self._get_access_token()
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    # ============================================================
    # 주식현재가 시세 (PER, PBR, EPS, BPS, 시가총액, 외국인보유)
    # ============================================================

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="kis_fetcher")
    def fetch_stock_info(self, code: str) -> Optional[dict]:
        """
        주식현재가 시세 조회 → 밸류에이션 + 외국인보유

        Returns:
            {"per": float, "pbr": float, "eps": float, "bps": float,
             "market_cap": int, "div_yield": float, "foreign_ratio": float}
        """
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._make_headers("FHKST01010100")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
        }

        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") != "0":
            logger.warning(
                f"KIS inquire-price 오류: {data.get('msg1', '')}",
                "fetch_stock_info",
                {"code": code},
            )
            return None

        output = data.get("output", {})

        return {
            "per": _safe_float(output.get("per")),
            "pbr": _safe_float(output.get("pbr")),
            "eps": _safe_float(output.get("eps")),
            "bps": _safe_float(output.get("bps")),
            "market_cap": _safe_int(output.get("hts_avls")),  # 시가총액 (억원)
            "div_yield": _safe_float(output.get("stck_dryy")),  # 배당수익률
            "foreign_ratio": _safe_float(output.get("hts_frgn_ehrt")),  # 외국인 보유비율
        }

    # ============================================================
    # 투자자별 매매동향
    # ============================================================

    @retry(max_attempts=3, delay=1.0, backoff=2.0, module="kis_fetcher")
    def fetch_investor_trading(self, code: str) -> Optional[dict]:
        """
        투자자별 매매동향 조회 → 기관/외인/개인 순매수

        Returns:
            {"inst_net_buy": int, "foreign_net_buy": int, "individual_net_buy": int}
        """
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-investor"
        headers = self._make_headers("FHKST01010900")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
        }

        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") != "0":
            logger.warning(
                f"KIS inquire-investor 오류: {data.get('msg1', '')}",
                "fetch_investor_trading",
                {"code": code},
            )
            return None

        # output1은 요약, output2는 일별 상세 — 최신 1건(당일) 사용
        items = data.get("output", [])
        if not items:
            return None

        latest = items[0] if isinstance(items, list) else items

        return {
            "inst_net_buy": _safe_int(latest.get("orgn_ntby_qty")),       # 기관 순매수
            "foreign_net_buy": _safe_int(latest.get("frgn_ntby_qty")),    # 외국인 순매수
            "individual_net_buy": _safe_int(latest.get("prsn_ntby_qty")), # 개인 순매수
        }

    # ============================================================
    # 전체 종목 수집
    # ============================================================

    def fetch_all(
        self,
        codes: list[str],
        market: str,
        date: Optional[str] = None,
    ) -> KISFetchResult:
        """
        전체 종목의 기초정보 + 투자자별매매 수집

        Args:
            codes: 종목 코드 리스트
            market: KOSPI, KOSDAQ 등
            date: 수집 날짜 (기본: 오늘)
        """
        result = KISFetchResult(total_count=len(codes))

        if not self.available:
            result.skipped = True
            result.message = "KIS API 키 미설정 — 수집 건너뜀"
            logger.warning(result.message, "fetch_all")
            return result

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"KIS 기초정보 수집 시작: {market} ({len(codes)}종목)",
            "fetch_all",
        )

        for code in codes:
            try:
                # 1) 주식 기본 시세 (PER, PBR, EPS, 시총, 외인보유)
                stock_info = self.fetch_stock_info(code)
                time.sleep(_REQUEST_INTERVAL)

                # 2) 투자자별 매매동향
                investor = self.fetch_investor_trading(code)
                time.sleep(_REQUEST_INTERVAL)

                if stock_info is None and investor is None:
                    result.failed_count += 1
                    continue

                record = {
                    "market": market,
                    "code": code,
                    "date": date,
                }
                if stock_info:
                    record.update(stock_info)
                if investor:
                    record.update(investor)

                result.fundamentals.append(record)
                result.success_count += 1

            except Exception as e:
                result.failed_count += 1
                logger.warning(
                    f"KIS 수집 실패: {code} - {e}",
                    "fetch_all",
                    {"code": code},
                )

        result.message = (
            f"KIS 수집 완료: {result.success_count}/{result.total_count} 성공"
        )
        logger.info(result.message, "fetch_all")
        return result


# ============================================================
# 유틸
# ============================================================

def _safe_float(val) -> Optional[float]:
    """안전한 float 변환 (빈 문자열, None 처리)"""
    if val is None or val == "" or val == "0":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """안전한 int 변환"""
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

"""
DART API 재무제표 수집
====================

OpenDartReader를 사용하여 분기별 재무제표 데이터 수집.
API 키가 설정되지 않으면 graceful skip.
"""

from datetime import date
from typing import Optional
from dataclasses import dataclass, field

from config import settings
from core import get_logger

logger = get_logger("dart_fetcher")

# reprt_code: 보고서 코드
_REPRT_CODES = {
    "Q1": "11013",  # 1분기
    "Q2": "11012",  # 반기
    "Q3": "11014",  # 3분기
    "A": "11011",   # 사업보고서(연간)
}

# 분기 말일 (MMDD)
_QUARTER_END_DATES = {
    "Q1": "0331",
    "Q2": "0630",
    "Q3": "0930",
    "A": "1231",
}


@dataclass
class DARTFetchResult:
    """DART 수집 결과"""
    statements: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    skipped: bool = False
    message: str = ""


class DARTClient:
    """DART 전자공시 API 클라이언트"""

    def __init__(self):
        self.api_key = settings.DART_API_KEY
        self.available = bool(self.api_key)
        self._dart = None

        if not self.available:
            logger.warning(
                "DART API 키 미설정 — 재무제표 수집 비활성화",
                "__init__",
            )

    def _get_dart(self):
        """OpenDartReader 인스턴스 (lazy init)"""
        if self._dart is None:
            try:
                import OpenDartReader
                self._dart = OpenDartReader(self.api_key)
            except ImportError:
                logger.error(
                    "OpenDartReader 미설치 — pip install opendartreader",
                    "_get_dart",
                )
                self.available = False
                return None
        return self._dart

    # ============================================================
    # 단일 종목 재무제표 수집
    # ============================================================

    def fetch_financial_statement(
        self,
        code: str,
        year: int,
        quarter: str = "A",
    ) -> Optional[dict]:
        """
        분기별 재무제표 조회

        Args:
            code: 종목코드 (6자리)
            year: 사업연도
            quarter: Q1, Q2, Q3, A(연간)

        Returns:
            재무제표 dict 또는 None
        """
        dart = self._get_dart()
        if dart is None:
            return None

        reprt_code = _REPRT_CODES.get(quarter)
        if not reprt_code:
            logger.warning(f"잘못된 분기: {quarter}", "fetch_financial_statement")
            return None

        try:
            df = dart.finstate(code, year, reprt_code=reprt_code)

            if df is None or df.empty:
                logger.info(
                    f"재무제표 없음: {code} {year}{quarter}",
                    "fetch_financial_statement",
                )
                return None

            # 연결재무제표(CFS) 우선, 없으면 개별(OFS)
            if "fs_div" in df.columns:
                cfs = df[df["fs_div"] == "CFS"]
                if not cfs.empty:
                    df = cfs
                else:
                    df = df[df["fs_div"] == "OFS"]

            return self._parse_financial_data(df, code, year, quarter)

        except Exception as e:
            logger.warning(
                f"재무제표 조회 실패: {code} {year}{quarter} - {e}",
                "fetch_financial_statement",
            )
            return None

    def _parse_financial_data(
        self,
        df,
        code: str,
        year: int,
        quarter: str,
    ) -> Optional[dict]:
        """DataFrame에서 주요 재무 항목 추출"""
        # account_nm으로 항목 매칭
        def find_amount(keywords: list[str]) -> Optional[int]:
            """account_nm에서 키워드로 금액 찾기"""
            for _, row in df.iterrows():
                account = str(row.get("account_nm", ""))
                for kw in keywords:
                    if kw in account:
                        val = row.get("thstrm_amount")
                        if val is None:
                            val = row.get("thstrm_add_amount")
                        return _safe_bigint(val)
            return None

        revenue = find_amount(["매출액", "매출", "수익(매출액)", "영업수익"])
        operating_profit = find_amount(["영업이익", "영업손익"])
        net_income = find_amount(["당기순이익", "당기순손익", "분기순이익", "반기순이익"])
        total_equity = find_amount(["자본총계"])
        total_liabilities = find_amount(["부채총계"])

        # 파생 지표 계산
        roe = None
        if net_income is not None and total_equity and total_equity != 0:
            roe = round(net_income / total_equity * 100, 2)

        debt_ratio = None
        if total_liabilities is not None and total_equity and total_equity != 0:
            debt_ratio = round(total_liabilities / total_equity * 100, 2)

        operating_margin = None
        if operating_profit is not None and revenue and revenue != 0:
            operating_margin = round(operating_profit / revenue * 100, 2)

        net_margin = None
        if net_income is not None and revenue and revenue != 0:
            net_margin = round(net_income / revenue * 100, 2)

        # ROA
        total_assets = find_amount(["자산총계"])
        roa = None
        if net_income is not None and total_assets and total_assets != 0:
            roa = round(net_income / total_assets * 100, 2)

        # 분기 말일 계산
        end_mmdd = _QUARTER_END_DATES[quarter]
        period_date = f"{year}-{end_mmdd[:2]}-{end_mmdd[2:]}"

        return {
            "code": code,
            "period": f"{year}{quarter}",
            "period_date": period_date,
            "revenue": revenue,
            "operating_profit": operating_profit,
            "net_income": net_income,
            "roe": roe,
            "roa": roa,
            "debt_ratio": debt_ratio,
            "operating_margin": operating_margin,
            "net_margin": net_margin,
            "source": "dart",
        }

    # ============================================================
    # 전체 종목 수집
    # ============================================================

    def fetch_all(
        self,
        codes: list[str],
        market: str,
        year: int,
        quarter: str = "A",
    ) -> DARTFetchResult:
        """
        전체 종목의 분기별 재무제표 수집

        Args:
            codes: 종목 코드 리스트
            market: KOSPI, KOSDAQ 등
            year: 사업연도
            quarter: Q1, Q2, Q3, A
        """
        result = DARTFetchResult(total_count=len(codes))

        if not self.available:
            result.skipped = True
            result.message = "DART API 키 미설정 — 수집 건너뜀"
            logger.warning(result.message, "fetch_all")
            return result

        logger.info(
            f"DART 재무제표 수집 시작: {market} {year}{quarter} ({len(codes)}종목)",
            "fetch_all",
        )

        for code in codes:
            try:
                stmt = self.fetch_financial_statement(code, year, quarter)
                if stmt:
                    stmt["market"] = market
                    result.statements.append(stmt)
                    result.success_count += 1
                else:
                    result.failed_count += 1
            except Exception as e:
                result.failed_count += 1
                logger.warning(
                    f"DART 수집 실패: {code} - {e}",
                    "fetch_all",
                    {"code": code},
                )

        result.message = (
            f"DART 수집 완료: {result.success_count}/{result.total_count} 성공 "
            f"({year}{quarter})"
        )
        logger.info(result.message, "fetch_all")
        return result


# ============================================================
# 유틸
# ============================================================

def _safe_bigint(val) -> Optional[int]:
    """안전한 BigInteger 변환 (문자열 쉼표 제거 포함)"""
    if val is None or val == "":
        return None
    try:
        if isinstance(val, str):
            val = val.replace(",", "").strip()
        return int(float(val))
    except (ValueError, TypeError):
        return None


def get_current_quarter() -> tuple[int, str]:
    """현재 날짜 기준 최신 확정 분기 반환 (공시 지연 고려)

    Returns:
        (year, quarter): 예) (2025, "Q3")
    """
    today = date.today()
    year = today.year
    month = today.month

    # 공시 지연 약 45일 감안:
    # 1~4월 → 전년 Q3 (11월에 3분기 공시)
    # 5~7월 → 전년 A  (3월에 사업보고서 공시)
    # 8~10월 → 금년 Q1 (5월에 1분기 공시)
    # 11~12월 → 금년 Q2 (8월에 반기 공시)
    if month <= 4:
        return year - 1, "Q3"
    elif month <= 7:
        return year - 1, "A"
    elif month <= 10:
        return year, "Q1"
    else:
        return year, "Q2"

"""
DART 공시 목록 수집 (Phase 5A)
============================

OpenDartReader.list()를 사용하여 종목별 공시 목록을 수집한다.
기존 DARTClient(재무제표)와 별개 — 공시 목록(제목, 유형, 접수번호) 수집 전용.
"""

from datetime import date, timedelta
from typing import Optional
from dataclasses import dataclass, field

from config import settings
from core import get_logger

logger = get_logger("disclosure_fetcher")

# 공시 제목 키워드 → report_type + type_score
_TYPE_RULES = [
    # (키워드 리스트, report_type, type_score)
    (["영업(잠정)실적", "매출액또는손익구조", "실적"], "실적", 1.0),
    (["주요사항보고서", "소송"], "주요사항", 0.8),
    (["지분", "임원ㆍ주요주주", "주식등의대량보유", "공개매수"], "지분", 0.7),
    (["유상증자", "무상증자", "전환사채", "신주인수권"], "자본변동", 0.6),
    (["사업보고서", "분기보고서", "반기보고서"], "정기보고", 0.5),
    (["합병", "분할", "영업양수도"], "구조변경", 0.9),
]


def _classify_report(report_nm: str) -> tuple[str, float]:
    """공시 제목에서 유형과 가중치 판별"""
    for keywords, rtype, score in _TYPE_RULES:
        for kw in keywords:
            if kw in report_nm:
                return rtype, score
    return "기타", 0.2


@dataclass
class DisclosureFetchResult:
    """공시 수집 결과"""
    records: list[dict] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    message: str = ""


class DisclosureFetcher:
    """DART 공시 목록 수집기"""

    def __init__(self):
        self.api_key = settings.DART_API_KEY
        self.available = bool(self.api_key)
        self._dart = None

        if not self.available:
            logger.warning(
                "DART API 키 미설정 — 공시 수집 비활성화",
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

    def fetch_disclosures(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        days: int = 60,
    ) -> list[dict]:
        """
        단일 종목의 공시 목록 수집

        Args:
            code: 종목코드 (6자리)
            start_date: 시작일 (YYYY-MM-DD). None이면 end_date - days
            end_date: 종료일 (YYYY-MM-DD). None이면 오늘
            days: start_date가 None일 때 lookback 일수
        """
        dart = self._get_dart()
        if dart is None:
            return []

        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            start_dt = date.today() - timedelta(days=days)
            start_date = start_dt.strftime("%Y-%m-%d")

        # YYYYMMDD 형식으로 변환
        start_yyyymmdd = start_date.replace("-", "")
        end_yyyymmdd = end_date.replace("-", "")

        try:
            df = dart.list(code, start=start_yyyymmdd, end=end_yyyymmdd)

            if df is None or (hasattr(df, 'empty') and df.empty):
                return []

            # DataFrame이 아닌 경우 (에러 응답)
            if not hasattr(df, 'iterrows'):
                return []

            records = []
            for _, row in df.iterrows():
                report_nm = str(row.get("report_nm", ""))
                rcept_no = str(row.get("rcept_no", ""))
                rcept_dt = str(row.get("rcept_dt", ""))
                corp_name = str(row.get("corp_name", ""))
                flr_nm = str(row.get("flr_nm", ""))

                if not report_nm or not rcept_no:
                    continue

                report_type, type_score = _classify_report(report_nm)

                # rcept_dt (YYYYMMDD) → date
                try:
                    rec_date = date(
                        int(rcept_dt[:4]),
                        int(rcept_dt[4:6]),
                        int(rcept_dt[6:8]),
                    )
                except (ValueError, IndexError):
                    rec_date = date.today()

                records.append({
                    "date": rec_date,
                    "code": code,
                    "corp_name": corp_name,
                    "report_nm": report_nm,
                    "rcept_no": rcept_no,
                    "flr_nm": flr_nm,
                    "rcept_dt": rcept_dt,
                    "report_type": report_type,
                    "type_score": type_score,
                })

            return records

        except Exception as e:
            logger.warning(
                f"공시 목록 조회 실패: {code} - {e}",
                "fetch_disclosures",
            )
            return []

    def fetch_all(
        self,
        codes: list[str],
        market: str,
        start_date: str = None,
        end_date: str = None,
        days: int = 60,
    ) -> DisclosureFetchResult:
        """
        전체 종목의 공시 목록 수집

        Args:
            codes: 종목 코드 리스트
            market: KOSPI, KOSDAQ 등
            start_date: 시작일
            end_date: 종료일
            days: lookback 일수
        """
        result = DisclosureFetchResult(total_count=len(codes))

        if not self.available:
            result.message = "DART API 키 미설정 — 수집 건너뜀"
            logger.warning(result.message, "fetch_all")
            return result

        logger.info(
            f"DART 공시 수집 시작: {market} ({len(codes)}종목)",
            "fetch_all",
        )

        for code in codes:
            try:
                records = self.fetch_disclosures(
                    code, start_date, end_date, days,
                )
                for rec in records:
                    rec["market"] = market
                result.records.extend(records)
                result.success_count += 1
            except Exception as e:
                result.failed_count += 1
                logger.warning(
                    f"공시 수집 실패: {code} - {e}",
                    "fetch_all",
                )

        result.message = (
            f"DART 공시 수집 완료: {result.success_count}/{result.total_count} 성공, "
            f"{len(result.records)}건"
        )
        logger.info(result.message, "fetch_all")
        return result

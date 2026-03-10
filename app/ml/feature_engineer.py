"""
피처 엔지니어링 파이프라인
==========================

StockPrice → 가격/기술지표/파생 피처 계산 → feature_store 저장
Phase 2: + StockFundamental/FinancialStatement → 재무 피처 병합
"""

from datetime import timedelta

import numpy as np
import pandas as pd

from core import get_logger
from db import database
from indicators import calc_sma, calc_ema, calc_rsi, calc_macd, calc_bollinger_bands, calc_obv
from models import StockPrice, StockFundamental, FinancialStatement, MacroIndicator, StockInfo
from repositories import MLRepository
from repositories.stock_repository import StockRepository
from repositories.news_repository import NewsRepository
from repositories.disclosure_repository import DisclosureRepository

# SMA60이 가장 긴 lookback → 캘린더일 120일 ≈ 영업일 80일
_LOOKBACK_CALENDAR_DAYS = 120

logger = get_logger("feature_engineer")

# Phase 1 피처 컬럼 목록 (학습에 사용)
PHASE1_FEATURE_COLUMNS = [
    "return_1d", "return_5d", "return_20d", "volatility_20d", "volume_ratio",
    "sma_5", "sma_20", "sma_60", "ema_12", "ema_26",
    "rsi_14", "macd", "macd_signal", "macd_histogram",
    "bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_pctb",
    "obv",
    "price_to_sma20", "price_to_sma60", "golden_cross", "rsi_zone",
]

# Phase 2 피처 컬럼 (Phase 1 + 재무 피처)
PHASE2_FEATURE_COLUMNS = PHASE1_FEATURE_COLUMNS + [
    "per", "pbr", "eps", "market_cap",
    "foreign_ratio", "inst_net_buy", "foreign_net_buy",
    "roe", "debt_ratio",
]

# Phase 3 피처 컬럼 (Phase 2 + 거시 피처)
PHASE3_FEATURE_COLUMNS = PHASE2_FEATURE_COLUMNS + [
    "krw_usd", "vix", "kospi_index",
    "us_10y", "kr_3y", "sp500", "wti", "gold",
    "fed_rate", "usd_index", "us_cpi",
]

# Phase 4 피처 컬럼 (Phase 3 + 뉴스 센티먼트 피처)
PHASE4_FEATURE_COLUMNS = PHASE3_FEATURE_COLUMNS + [
    "news_sentiment", "news_volume", "news_sentiment_std",
    "market_sentiment", "market_news_volume",
]

# Phase 5 피처 컬럼 (Phase 4 + 공시 + 수급 피처)
PHASE5_FEATURE_COLUMNS = PHASE4_FEATURE_COLUMNS + [
    "disclosure_count_30d", "days_since_disclosure",
    "disclosure_sentiment", "disclosure_type_score", "disclosure_volume_change",
    "short_selling_volume", "short_selling_ratio",
    "program_buy_volume", "program_sell_volume", "program_net_volume",
]

# Phase 6 피처 컬럼 (Phase 5 + 섹터/상대강도 + 뉴스 정제)
PHASE6_FEATURE_COLUMNS = PHASE5_FEATURE_COLUMNS + [
    "sector_return_1d", "sector_return_5d",
    "relative_strength_1d", "relative_strength_5d", "relative_strength_20d",
    "sector_momentum_rank", "sector_breadth",
    "news_relevance_ratio", "news_sentiment_filtered", "sector_news_sentiment",
]

# Phase 6A 섹터 피처 내부 상수
_SECTOR_FEATURE_COLUMNS = [
    "sector_return_1d", "sector_return_5d",
    "relative_strength_1d", "relative_strength_5d", "relative_strength_20d",
    "sector_momentum_rank", "sector_breadth",
]

# Phase 6B 뉴스 정제 피처 내부 상수
_NEWS_REFINED_FEATURE_COLUMNS = [
    "news_relevance_ratio", "news_sentiment_filtered", "sector_news_sentiment",
]

# indicator_name → feature_store 컬럼명 매핑
_MACRO_COLUMN_MAP = {
    "KRW_USD": "krw_usd",
    "VIX": "vix",
    "KOSPI": "kospi_index",
    "SP500": "sp500",
    "US_10Y": "us_10y",
    "KR_3Y": "kr_3y",
    "WTI": "wti",
    "GOLD": "gold",
    "FED_RATE": "fed_rate",
    "USD_INDEX": "usd_index",
    "US_CPI": "us_cpi",
}

# 월별/분기별 지표 — forward-fill 대상 (일별 지표는 left join만으로 충분)
_MACRO_FFILL_COLUMNS = {"us_cpi"}

# Phase 4 뉴스 센티먼트 피처 컬럼
_NEWS_FEATURE_COLUMNS = [
    "news_sentiment", "news_volume", "news_sentiment_std",
    "market_sentiment", "market_news_volume",
]

TARGET_COLUMNS = [
    "target_class_1d", "target_class_5d",
    "target_return_1d", "target_return_5d",
]


class FeatureEngineer:
    """피처 계산 및 저장"""

    def __init__(self):
        pass

    def compute_features(
        self,
        market: str,
        code: str,
        start_date: str = None,
        end_date: str = None,
        target_days: list[int] = None,
        incremental: bool = True,
        include_phase6: bool = True,
    ) -> int:
        """
        단일 종목의 피처를 계산하여 feature_store에 저장

        Args:
            market: 마켓 (KOSPI 등)
            code: 종목코드
            start_date: 시작일 (없으면 전체)
            end_date: 종료일
            target_days: 타겟 일수 목록
            incremental: True면 마지막 계산일 이후 신규분만 계산

        Returns:
            저장된 레코드 수
        """
        with database.session() as session:
            repo = MLRepository(session)
            last_date = None

            # 증분 모드: 마지막 계산일 확인
            if incremental:
                last_feature = repo.get_latest_features(market, code)
                if last_feature:
                    last_date = last_feature.date

            # 1. StockPrice 조회
            query = session.query(StockPrice).filter(
                StockPrice.market == market,
                StockPrice.code == code,
            )

            if last_date:
                # 증분: lookback 구간부터 조회
                lookback_start = last_date - timedelta(days=_LOOKBACK_CALENDAR_DAYS)
                query = query.filter(StockPrice.date >= lookback_start)
            elif start_date:
                query = query.filter(StockPrice.date >= start_date)

            if end_date:
                query = query.filter(StockPrice.date <= end_date)

            rows = query.order_by(StockPrice.date).all()

            if len(rows) < 60:
                logger.warning(
                    f"데이터 부족: {market}:{code} ({len(rows)}행, 최소 60행 필요)",
                    "compute_features",
                )
                return 0

            # 증분: 새 데이터 없으면 스킵
            if last_date:
                max_price_date = rows[-1].date
                if max_price_date <= last_date:
                    return 0

            # 2. DataFrame 변환
            df = pd.DataFrame([{
                "date": r.date,
                "open": float(r.open) if r.open else None,
                "high": float(r.high) if r.high else None,
                "low": float(r.low) if r.low else None,
                "close": float(r.close) if r.close else None,
                "volume": int(r.volume) if r.volume else 0,
            } for r in rows])

            df = df.dropna(subset=["close"])
            if len(df) < 60:
                return 0

            # 3. 피처 계산 (lookback 포함 전체 구간)
            features_df = self._compute_all_features(df, target_days=target_days)

            # 3.5 재무 피처 병합 (Phase 2)
            features_df = self._merge_fundamental_features(
                features_df, market, code, session,
            )

            # 3.6 거시 피처 병합 (Phase 3)
            features_df = self._merge_macro_features(features_df, session)

            # 3.7 뉴스 센티먼트 피처 병합 (Phase 4)
            features_df = self._merge_news_features(
                features_df, market, code, session,
            )

            # 3.8 공시 피처 병합 (Phase 5A)
            features_df = self._merge_disclosure_features(
                features_df, market, code, session,
            )

            # 3.9 수급 피처 병합 (Phase 5B)
            features_df = self._merge_supply_demand_features(
                features_df, market, code, session,
            )

            # 3.10 섹터/상대강도 피처 병합 (Phase 6A)
            # 3.11 뉴스 정제 피처 병합 (Phase 6B)
            # compute_all()에서는 include_phase6=False로 Pass 1 수행 후
            # Pass 2에서 _compute_phase6_only()로 별도 처리
            if include_phase6:
                features_df = self._merge_sector_features(
                    features_df, market, code, session,
                )
                features_df = self._merge_news_refined_features(
                    features_df, market, code, session,
                )

            # 증분: 신규분만 필터링
            if last_date:
                features_df = features_df[features_df["date"] > last_date]
                if features_df.empty:
                    return 0

            # 4. feature_store 레코드 생성
            records = []
            for _, row in features_df.iterrows():
                record = {
                    "market": market,
                    "code": code,
                    "date": row["date"],
                }
                for col in features_df.columns:
                    if col == "date":
                        continue
                    val = row[col]
                    if pd.isna(val):
                        record[col] = None
                    elif isinstance(val, (np.integer,)):
                        record[col] = int(val)
                    elif isinstance(val, (np.floating,)):
                        record[col] = float(val)
                    else:
                        record[col] = val
                records.append(record)

            # 5. DB 저장
            saved = repo.upsert_features(records)
            mode = "증분" if last_date else "전체"
            logger.info(
                f"피처 저장 완료 ({mode}): {market}:{code} ({saved}행)",
                "compute_features",
            )
            return saved

    def compute_all(
        self,
        market: str,
        start_date: str = None,
        end_date: str = None,
        target_days: list[int] = None,
        incremental: bool = True,
    ) -> dict:
        """
        마켓 전체 종목의 피처를 계산 (2-pass)

        Pass 1: Phase 1~5 피처 계산 → feature_store 저장
        Pass 2: Phase 6 섹터/뉴스정제 피처 계산 → feature_store 업데이트
        (Phase 6은 동일 섹터 피어의 Phase 1~5 데이터가 필요하므로 분리)

        Args:
            incremental: True면 종목별 마지막 계산일 이후 신규분만 계산

        Returns:
            {"total": N, "success": N, "failed": N, "skipped": N}
        """
        from models import StockInfo

        with database.session() as session:
            codes = [
                r[0] for r in
                session.query(StockInfo.code)
                .filter(StockInfo.market == market)
                .all()
            ]

        if not codes:
            logger.warning(f"종목 없음: {market}", "compute_all")
            return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

        total = len(codes)
        success = 0
        failed = 0
        skipped = 0
        phase1_updated_codes = set()  # Pass 1에서 신규 데이터 처리된 종목

        # Pass 1: Phase 1~5 피처 계산 (Phase 6 제외)
        logger.info(f"Pass 1/2: Phase 1~5 피처 계산 ({total}종목)", "compute_all")
        for code in codes:
            try:
                count = self.compute_features(
                    market, code, start_date, end_date,
                    target_days=target_days,
                    incremental=incremental,
                    include_phase6=False,
                )
                if count > 0:
                    success += 1
                    phase1_updated_codes.add(code)
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"피처 계산 실패: {market}:{code} - {e}", "compute_all")
                failed += 1

        # Pass 2: Phase 6 섹터/뉴스정제 피처 계산
        # 증분 모드: Pass 1에서 신규 데이터가 있던 종목 + Phase 6 미계산 종목만 처리
        phase6_targets = self._get_phase6_targets(
            market, codes, phase1_updated_codes, incremental,
        )
        logger.info(
            f"Pass 2/2: Phase 6 피처 계산 ({len(phase6_targets)}/{total}종목)",
            "compute_all",
        )
        phase6_success = 0
        phase6_failed = 0
        for code in phase6_targets:
            try:
                count = self._compute_phase6_only(
                    market, code, start_date, end_date,
                )
                if count > 0:
                    phase6_success += 1
            except Exception as e:
                logger.warning(
                    f"Phase 6 피처 계산 실패: {market}:{code} - {e}",
                    "compute_all",
                )
                phase6_failed += 1

        logger.info(
            f"Phase 6 업데이트: {phase6_success}/{len(phase6_targets)} (실패: {phase6_failed})",
            "compute_all",
        )

        mode = "증분" if incremental else "전체"
        logger.info(
            f"피처 계산 완료 ({mode}): {market} (total={total}, success={success}, skipped={skipped}, failed={failed})",
            "compute_all",
        )
        return {"total": total, "success": success, "failed": failed, "skipped": skipped}

    def _get_phase6_targets(
        self,
        market: str,
        all_codes: list[str],
        updated_codes: set[str],
        incremental: bool,
    ) -> list[str]:
        """
        Pass 2에서 처리할 종목 목록 결정

        - 전체 모드: 전종목
        - 증분 모드: Pass 1에서 갱신된 종목 + Phase 6 미계산 종목
        """
        if not incremental:
            return all_codes

        # Pass 1에서 갱신된 종목은 무조건 포함
        targets = set(updated_codes)

        # Phase 6 미계산 종목 추가 (sector_return_1d가 NULL인 종목)
        with database.session() as session:
            from models import FeatureStore

            # Phase 6 데이터가 하나도 없는 종목코드 조회
            codes_with_phase6 = set(
                r[0] for r in
                session.query(FeatureStore.code)
                .filter(
                    FeatureStore.market == market,
                    FeatureStore.code.in_(all_codes),
                    FeatureStore.sector_return_1d.isnot(None),
                )
                .distinct()
                .all()
            )

            for code in all_codes:
                if code not in codes_with_phase6:
                    targets.add(code)

        return [c for c in all_codes if c in targets]  # 순서 유지

    def _compute_phase6_only(
        self,
        market: str,
        code: str,
        start_date: str = None,
        end_date: str = None,
    ) -> int:
        """
        Phase 6 피처만 계산하여 feature_store 업데이트

        compute_all()의 Pass 2에서 호출.
        feature_store에 이미 저장된 Phase 1~5 데이터를 읽어
        섹터/상대강도 + 뉴스 정제 피처를 계산한다.
        """
        with database.session() as session:
            ml_repo = MLRepository(session)

            features = ml_repo.get_features(market, code, start_date, end_date)
            if not features:
                return 0

            # Phase 6 미계산 레코드만 처리 (sector_return_1d가 NULL)
            features = [f for f in features if f.sector_return_1d is None]
            if not features:
                return 0

            # Phase 6 계산에 필요한 최소 컬럼만 로드
            df = pd.DataFrame([{
                "date": f.date,
                "return_1d": float(f.return_1d) if f.return_1d is not None else None,
                "return_5d": float(f.return_5d) if f.return_5d is not None else None,
                "return_20d": float(f.return_20d) if f.return_20d is not None else None,
                "news_sentiment": float(f.news_sentiment) if f.news_sentiment is not None else None,
            } for f in features])

            if df.empty:
                return 0

            # Phase 6A: 섹터/상대강도
            df = self._merge_sector_features(df, market, code, session)

            # Phase 6B: 뉴스 정제
            df = self._merge_news_refined_features(df, market, code, session)

            # Phase 6 컬럼만 추출하여 upsert
            phase6_cols = _SECTOR_FEATURE_COLUMNS + _NEWS_REFINED_FEATURE_COLUMNS
            records = []
            for _, row in df.iterrows():
                record = {"market": market, "code": code, "date": row["date"]}
                for col in phase6_cols:
                    val = row.get(col)
                    if pd.isna(val):
                        record[col] = None
                    elif isinstance(val, (np.integer,)):
                        record[col] = int(val)
                    elif isinstance(val, (np.floating,)):
                        record[col] = float(val)
                    else:
                        record[col] = val
                records.append(record)

            saved = ml_repo.upsert_features(records)
            logger.info(
                f"Phase 6 피처 업데이트: {market}:{code} ({saved}행)",
                "_compute_phase6_only",
            )
            return saved

    def _merge_fundamental_features(
        self,
        df: pd.DataFrame,
        market: str,
        code: str,
        session,
    ) -> pd.DataFrame:
        """
        재무 피처를 기존 피처 DataFrame에 병합 (Phase 2)

        1) StockFundamental (일별): date 기준 left join → per, pbr, eps, market_cap, foreign_ratio, inst_net_buy, foreign_net_buy
        2) FinancialStatement (분기별): period_date 기준 forward-fill → roe, debt_ratio
        3) 데이터 없으면 NULL 유지 (graceful)
        """
        # --- 1) KIS 기초정보 병합 ---
        fund_rows = (
            session.query(StockFundamental)
            .filter(
                StockFundamental.market == market,
                StockFundamental.code == code,
            )
            .order_by(StockFundamental.date)
            .all()
        )

        if fund_rows:
            fund_df = pd.DataFrame([{
                "date": r.date,
                "per": float(r.per) if r.per else None,
                "pbr": float(r.pbr) if r.pbr else None,
                "eps": float(r.eps) if r.eps else None,
                "market_cap": int(r.market_cap) if r.market_cap else None,
                "foreign_ratio": float(r.foreign_ratio) if r.foreign_ratio else None,
                "inst_net_buy": int(r.inst_net_buy) if r.inst_net_buy else None,
                "foreign_net_buy": int(r.foreign_net_buy) if r.foreign_net_buy else None,
            } for r in fund_rows])

            df = df.merge(fund_df, on="date", how="left")
        else:
            # 컬럼만 추가 (NULL)
            for col in ["per", "pbr", "eps", "market_cap", "foreign_ratio", "inst_net_buy", "foreign_net_buy"]:
                if col not in df.columns:
                    df[col] = None

        # --- 2) DART 재무제표 병합 (forward-fill) ---
        stmt_rows = (
            session.query(FinancialStatement)
            .filter(
                FinancialStatement.market == market,
                FinancialStatement.code == code,
            )
            .order_by(FinancialStatement.period_date)
            .all()
        )

        if stmt_rows:
            stmt_df = pd.DataFrame([{
                "period_date": r.period_date,
                "roe": float(r.roe) if r.roe else None,
                "debt_ratio": float(r.debt_ratio) if r.debt_ratio else None,
            } for r in stmt_rows])

            # forward-fill: 각 날짜에 대해 가장 최근 분기 데이터 적용
            df["roe"] = None
            df["debt_ratio"] = None

            for _, stmt_row in stmt_df.iterrows():
                mask = df["date"] >= stmt_row["period_date"]
                if stmt_row["roe"] is not None:
                    df.loc[mask, "roe"] = stmt_row["roe"]
                if stmt_row["debt_ratio"] is not None:
                    df.loc[mask, "debt_ratio"] = stmt_row["debt_ratio"]
        else:
            for col in ["roe", "debt_ratio"]:
                if col not in df.columns:
                    df[col] = None

        return df

    def _merge_macro_features(
        self,
        df: pd.DataFrame,
        session,
    ) -> pd.DataFrame:
        """
        거시 피처를 기존 피처 DataFrame에 병합 (Phase 3)

        macro_indicator 테이블에서 날짜 기준으로 피벗 후 left join.
        데이터 없으면 NULL 유지 (graceful).
        """
        if df.empty:
            return df

        min_date = df["date"].min()
        max_date = df["date"].max()

        macro_rows = (
            session.query(MacroIndicator)
            .filter(
                MacroIndicator.date >= min_date,
                MacroIndicator.date <= max_date,
            )
            .all()
        )

        if macro_rows:
            macro_df = pd.DataFrame([{
                "date": r.date,
                "indicator_name": r.indicator_name,
                "value": float(r.value) if r.value else None,
            } for r in macro_rows])

            # 피벗: (date) × (indicator_name) → value
            pivot = macro_df.pivot_table(
                index="date",
                columns="indicator_name",
                values="value",
                aggfunc="first",
            )

            # indicator_name → feature_store 컬럼명 매핑
            rename = {k: v for k, v in _MACRO_COLUMN_MAP.items() if k in pivot.columns}
            pivot = pivot.rename(columns=rename)
            pivot = pivot.reset_index()

            # 월별 지표 forward-fill (CPI 등: 발표일 이후 다음 발표까지 동일 값 유지)
            for col in _MACRO_FFILL_COLUMNS:
                if col in pivot.columns:
                    pivot[col] = pivot[col].ffill()

            # 필요 없는 컬럼 제거 (매핑에 없는 지표)
            keep_cols = ["date"] + [v for v in _MACRO_COLUMN_MAP.values() if v in pivot.columns]
            pivot = pivot[keep_cols]

            df = df.merge(pivot, on="date", how="left")

            # merge 후에도 월별 지표는 forward-fill (feature_store 날짜에 빈 날 채우기)
            for col in _MACRO_FFILL_COLUMNS:
                if col in df.columns:
                    df[col] = df[col].ffill()
        else:
            logger.info("거시지표 데이터 없음 — NULL 유지", "_merge_macro_features")

        # 컬럼 보장 (데이터 없어도 컬럼은 존재해야 함)
        for col in _MACRO_COLUMN_MAP.values():
            if col not in df.columns:
                df[col] = None

        return df

    def _merge_news_features(
        self,
        df: pd.DataFrame,
        market: str,
        code: str,
        session,
    ) -> pd.DataFrame:
        """
        뉴스 센티먼트 피처를 기존 피처 DataFrame에 병합 (Phase 4)

        1) 종목별 일별 센티먼트: news_sentiment, news_volume, news_sentiment_std
        2) 시장 전체 일별 센티먼트: market_sentiment, market_news_volume
        3) 데이터 없으면 NULL 유지 (graceful)
        """
        if df.empty:
            return df

        min_date = df["date"].min()
        max_date = df["date"].max()

        repo = NewsRepository(session)

        # 1) 종목별 센티먼트
        stock_sent = repo.get_daily_sentiment(code, min_date, max_date)
        if stock_sent:
            sent_df = pd.DataFrame(stock_sent)
            df = df.merge(sent_df, on="date", how="left")

        # 2) 시장 전체 센티먼트
        market_sent = repo.get_daily_market_sentiment(min_date, max_date, market)
        if market_sent:
            msent_df = pd.DataFrame(market_sent)
            df = df.merge(msent_df, on="date", how="left")

        # 컬럼 보장 (데이터 없어도 컬럼은 존재해야 함)
        for col in _NEWS_FEATURE_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df

    def _merge_disclosure_features(
        self,
        df: pd.DataFrame,
        market: str,
        code: str,
        session,
    ) -> pd.DataFrame:
        """
        공시 피처를 기존 피처 DataFrame에 병합 (Phase 5A)

        disclosure_count_30d: 최근 30일 공시 건수
        days_since_disclosure: 마지막 공시 이후 일수
        disclosure_sentiment: 30일 공시 제목 평균 센티먼트
        disclosure_type_score: 30일 공시 유형 가중치 평균
        disclosure_volume_change: 공시 빈도 변화율 (30d vs 이전 30d)
        """
        _DISC_COLS = [
            "disclosure_count_30d", "days_since_disclosure",
            "disclosure_sentiment", "disclosure_type_score",
            "disclosure_volume_change",
        ]

        if df.empty:
            for col in _DISC_COLS:
                df[col] = None
            return df

        min_date = df["date"].min()
        max_date = df["date"].max()

        # 60일 lookback (30d + 이전 30d 비교용)
        lookback_start = min_date - timedelta(days=60)

        repo = DisclosureRepository(session)
        disc_rows = repo.get_disclosures_for_features(
            market, code, lookback_start, max_date,
        )

        if not disc_rows:
            for col in _DISC_COLS:
                df[col] = None
            return df

        # 공시 DataFrame 생성
        disc_df = pd.DataFrame([{
            "date": d.date,
            "type_score": float(d.type_score) if d.type_score else 0.2,
            "sentiment_score": float(d.sentiment_score) if d.sentiment_score else None,
        } for d in disc_rows])

        # 날짜별 피처 계산
        disc_features = []
        for _, row in df.iterrows():
            feat_date = row["date"]
            d30_start = feat_date - timedelta(days=30)
            d60_start = feat_date - timedelta(days=60)

            # 30일 윈도우
            mask_30d = (disc_df["date"] >= d30_start) & (disc_df["date"] <= feat_date)
            # 이전 30일 윈도우
            mask_prev30d = (disc_df["date"] >= d60_start) & (disc_df["date"] < d30_start)

            window_30d = disc_df[mask_30d]
            window_prev30d = disc_df[mask_prev30d]

            count_30d = len(window_30d)
            count_prev30d = len(window_prev30d)

            # days_since_disclosure
            recent = disc_df[disc_df["date"] <= feat_date]
            if not recent.empty:
                last_disc_date = recent["date"].max()
                days_since = (feat_date - last_disc_date).days
            else:
                days_since = None

            # 센티먼트 / type_score 평균
            if count_30d > 0:
                sent_vals = window_30d["sentiment_score"].dropna()
                avg_sent = float(sent_vals.mean()) if not sent_vals.empty else None
                avg_type = float(window_30d["type_score"].mean())
            else:
                avg_sent = None
                avg_type = None

            # 빈도 변화율
            if count_prev30d > 0:
                vol_change = (count_30d - count_prev30d) / count_prev30d
            elif count_30d > 0:
                vol_change = 1.0  # 이전 기간 0건 → 100% 증가
            else:
                vol_change = None

            disc_features.append({
                "date": feat_date,
                "disclosure_count_30d": count_30d,
                "days_since_disclosure": days_since,
                "disclosure_sentiment": round(avg_sent, 4) if avg_sent is not None else None,
                "disclosure_type_score": round(avg_type, 2) if avg_type is not None else None,
                "disclosure_volume_change": round(vol_change, 4) if vol_change is not None else None,
            })

        disc_feat_df = pd.DataFrame(disc_features)
        df = df.merge(disc_feat_df, on="date", how="left")

        for col in _DISC_COLS:
            if col not in df.columns:
                df[col] = None

        return df

    def _merge_supply_demand_features(
        self,
        df: pd.DataFrame,
        market: str,
        code: str,
        session,
    ) -> pd.DataFrame:
        """
        수급 피처를 기존 피처 DataFrame에 병합 (Phase 5B)

        short_selling_volume, short_selling_ratio: 공매도
        program_buy_volume, program_sell_volume, program_net_volume: 프로그램매매
        """
        _SUPPLY_COLS = [
            "short_selling_volume", "short_selling_ratio",
            "program_buy_volume", "program_sell_volume", "program_net_volume",
        ]

        if df.empty:
            for col in _SUPPLY_COLS:
                df[col] = None
            return df

        min_date = df["date"].min()
        max_date = df["date"].max()

        repo = DisclosureRepository(session)
        supply_rows = repo.get_supply_demand_for_features(
            market, code, min_date, max_date,
        )

        if supply_rows:
            supply_df = pd.DataFrame([{
                "date": s.date,
                "short_selling_volume": int(s.short_selling_volume) if s.short_selling_volume else None,
                "short_selling_ratio": float(s.short_selling_ratio) if s.short_selling_ratio else None,
                "program_buy_volume": int(s.program_buy_volume) if s.program_buy_volume else None,
                "program_sell_volume": int(s.program_sell_volume) if s.program_sell_volume else None,
            } for s in supply_rows])

            # program_net_volume 파생
            supply_df["program_net_volume"] = (
                supply_df["program_buy_volume"].fillna(0) -
                supply_df["program_sell_volume"].fillna(0)
            )
            # buy/sell 모두 None인 경우 net도 None
            both_null = supply_df["program_buy_volume"].isna() & supply_df["program_sell_volume"].isna()
            supply_df.loc[both_null, "program_net_volume"] = None

            df = df.merge(supply_df, on="date", how="left")

        # 컬럼 보장
        for col in _SUPPLY_COLS:
            if col not in df.columns:
                df[col] = None

        return df

    # ============================================================
    # Phase 6A: 섹터/상대강도 피처
    # ============================================================

    def _merge_sector_features(
        self,
        df: pd.DataFrame,
        market: str,
        code: str,
        session,
    ) -> pd.DataFrame:
        """
        섹터/상대강도 피처를 기존 피처 DataFrame에 병합 (Phase 6A)

        feature_store에서 동일 섹터 종목들의 수익률을 가져와
        섹터 평균, 상대강도, 모멘텀 순위, 시장폭을 계산한다.
        """
        if df.empty:
            for col in _SECTOR_FEATURE_COLUMNS:
                df[col] = None
            return df

        stock_repo = StockRepository(session)
        ml_repo = MLRepository(session)

        # 1) 섹터 조회
        sector = stock_repo.get_sector_for_code(code, market)
        if not sector:
            for col in _SECTOR_FEATURE_COLUMNS:
                df[col] = None
            return df

        # 2) 동일 섹터 종목코드
        sector_codes = stock_repo.get_codes_in_sector(sector, market)
        if len(sector_codes) < 2:
            for col in _SECTOR_FEATURE_COLUMNS:
                df[col] = None
            return df

        # 3) feature_store에서 벌크 조회
        min_date = df["date"].min()
        max_date = df["date"].max()

        sector_rows = ml_repo.get_sector_features_bulk(
            market, sector_codes, min_date, max_date,
        )

        if not sector_rows:
            for col in _SECTOR_FEATURE_COLUMNS:
                df[col] = None
            return df

        # 4) DataFrame 변환
        sector_df = pd.DataFrame([{
            "code": r.code,
            "date": r.date,
            "return_1d": float(r.return_1d) if r.return_1d is not None else None,
            "return_5d": float(r.return_5d) if r.return_5d is not None else None,
            "return_20d": float(r.return_20d) if r.return_20d is not None else None,
        } for r in sector_rows])

        # 5) 날짜별 섹터 집계
        sector_agg = sector_df.groupby("date").agg(
            sector_return_1d=("return_1d", "mean"),
            sector_return_5d=("return_5d", "mean"),
            _sector_return_20d=("return_20d", "mean"),
            _positive_count=("return_1d", lambda x: (x > 0).sum()),
            _total_count=("return_1d", "count"),
        ).reset_index()

        sector_agg["sector_breadth"] = (
            sector_agg["_positive_count"] / sector_agg["_total_count"]
        )

        # 6) 현재 종목의 수익률 (df에 이미 있음)
        my_cols = ["date"]
        for ret_col in ["return_1d", "return_5d", "return_20d"]:
            if ret_col in df.columns:
                my_cols.append(ret_col)

        my_stock = df[my_cols].copy()

        # 병합: 섹터 집계 + 내 종목
        sector_feat = sector_agg.merge(my_stock, on="date", how="inner")

        # 상대강도 계산
        if "return_1d" in sector_feat.columns:
            sector_feat["relative_strength_1d"] = (
                sector_feat["return_1d"] - sector_feat["sector_return_1d"]
            )
        if "return_5d" in sector_feat.columns:
            sector_feat["relative_strength_5d"] = (
                sector_feat["return_5d"] - sector_feat["sector_return_5d"]
            )
        if "return_20d" in sector_feat.columns:
            sector_feat["relative_strength_20d"] = (
                sector_feat["return_20d"] - sector_feat["_sector_return_20d"]
            )

        # 7) 모멘텀 순위 (벡터화)
        rank_data = []
        for date_val in sector_feat["date"].unique():
            day_returns = sector_df.loc[
                sector_df["date"] == date_val, "return_5d"
            ].dropna()
            my_row = sector_feat.loc[sector_feat["date"] == date_val]
            if my_row.empty or "return_5d" not in my_row.columns:
                rank_data.append({"date": date_val, "sector_momentum_rank": None})
                continue
            my_val = my_row["return_5d"].iloc[0]
            if pd.isna(my_val) or day_returns.empty:
                rank_data.append({"date": date_val, "sector_momentum_rank": None})
            else:
                rank = float((day_returns < my_val).sum()) / len(day_returns)
                rank_data.append({"date": date_val, "sector_momentum_rank": round(rank, 4)})

        rank_df = pd.DataFrame(rank_data)
        sector_feat = sector_feat.merge(rank_df, on="date", how="left")

        # 필요 컬럼만 선택
        merge_cols = ["date"]
        for col in _SECTOR_FEATURE_COLUMNS:
            if col in sector_feat.columns:
                merge_cols.append(col)

        sector_result = sector_feat[merge_cols].copy()

        # 반올림
        for col in _SECTOR_FEATURE_COLUMNS:
            if col in sector_result.columns and col != "date":
                sector_result[col] = sector_result[col].round(6)

        df = df.merge(sector_result, on="date", how="left")

        # 컬럼 보장
        for col in _SECTOR_FEATURE_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df

    # ============================================================
    # Phase 6B: 뉴스 정제 피처
    # ============================================================

    def _merge_news_refined_features(
        self,
        df: pd.DataFrame,
        market: str,
        code: str,
        session,
    ) -> pd.DataFrame:
        """
        뉴스 정제 피처를 기존 피처 DataFrame에 병합 (Phase 6B)

        1) news_relevance_ratio: 제목에 종목명 포함 비율
        2) news_sentiment_filtered: 필터링된 센티먼트
        3) sector_news_sentiment: 섹터 뉴스 센티먼트 (feature_store 기반)
        """
        if df.empty:
            for col in _NEWS_REFINED_FEATURE_COLUMNS:
                df[col] = None
            return df

        min_date = df["date"].min()
        max_date = df["date"].max()

        stock_repo = StockRepository(session)
        stock_info = stock_repo.get_info(code, market)
        stock_name = stock_info.name if stock_info else None

        # --- 1) 필터링된 센티먼트 ---
        if stock_name:
            news_repo = NewsRepository(session)
            filtered_sent = news_repo.get_daily_sentiment_filtered(
                code, stock_name, min_date, max_date,
            )
            if filtered_sent:
                filt_df = pd.DataFrame(filtered_sent)
                df = df.merge(filt_df, on="date", how="left")

        # --- 2) 섹터 뉴스 센티먼트 ---
        sector = stock_info.sector if stock_info else None
        if sector:
            sector_codes = stock_repo.get_codes_in_sector(sector, market)
            if len(sector_codes) >= 2:
                ml_repo = MLRepository(session)
                sector_rows = ml_repo.get_sector_features_bulk(
                    market, sector_codes, min_date, max_date,
                )
                if sector_rows:
                    s_df = pd.DataFrame([{
                        "date": r.date,
                        "news_sentiment": float(r.news_sentiment) if r.news_sentiment is not None else None,
                    } for r in sector_rows])

                    sector_sent = s_df.dropna(subset=["news_sentiment"]).groupby("date").agg(
                        sector_news_sentiment=("news_sentiment", "mean"),
                    ).reset_index()
                    sector_sent["sector_news_sentiment"] = sector_sent["sector_news_sentiment"].round(4)

                    df = df.merge(sector_sent, on="date", how="left")

        # 컬럼 보장
        for col in _NEWS_REFINED_FEATURE_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df

    def _compute_all_features(self, df: pd.DataFrame, target_days: list[int] = None) -> pd.DataFrame:
        """모든 피처를 계산하여 하나의 DataFrame으로 반환"""
        if target_days is None:
            target_days = [1, 5]  # 기본값 (하위 호환)
        result = df[["date", "close"]].copy()

        # === 가격 피처 ===
        result["return_1d"] = df["close"].pct_change(1)
        result["return_5d"] = df["close"].pct_change(5)
        result["return_20d"] = df["close"].pct_change(20)
        result["volatility_20d"] = df["close"].pct_change().rolling(20).std()
        vol_sma20 = df["volume"].rolling(20).mean()
        result["volume_ratio"] = df["volume"] / vol_sma20.replace(0, np.nan)

        # === 기술적 지표 ===
        # SMA
        sma5 = calc_sma(df, 5)["sma"]
        sma20 = calc_sma(df, 20)["sma"]
        sma60 = calc_sma(df, 60)["sma"]
        result["sma_5"] = sma5
        result["sma_20"] = sma20
        result["sma_60"] = sma60

        # EMA
        result["ema_12"] = calc_ema(df, 12)["ema"]
        result["ema_26"] = calc_ema(df, 26)["ema"]

        # RSI
        result["rsi_14"] = calc_rsi(df, 14)["rsi"]

        # MACD
        macd_df = calc_macd(df)
        result["macd"] = macd_df["macd"]
        result["macd_signal"] = macd_df["signal"]
        result["macd_histogram"] = macd_df["histogram"]

        # Bollinger Bands
        bb_df = calc_bollinger_bands(df)
        result["bb_upper"] = bb_df["upper"]
        result["bb_middle"] = bb_df["middle"]
        result["bb_lower"] = bb_df["lower"]
        bb_range = bb_df["upper"] - bb_df["lower"]
        result["bb_width"] = bb_range / bb_df["middle"].replace(0, np.nan)
        result["bb_pctb"] = (df["close"] - bb_df["lower"]) / bb_range.replace(0, np.nan)

        # OBV
        result["obv"] = calc_obv(df)["obv"]

        # === 파생 피처 ===
        result["price_to_sma20"] = df["close"] / sma20.replace(0, np.nan)
        result["price_to_sma60"] = df["close"] / sma60.replace(0, np.nan)
        result["golden_cross"] = (sma5 > sma20).astype(int)
        result["rsi_zone"] = pd.cut(
            result["rsi_14"],
            bins=[-np.inf, 30, 70, np.inf],
            labels=[-1, 0, 1],
        ).astype(float).astype("Int64")

        # === 타겟 변수 (동적 생성) ===
        for days in target_days:
            col_return = f"target_return_{days}d"
            col_class = f"target_class_{days}d"

            result[col_return] = df["close"].shift(-days) / df["close"] - 1
            result[col_class] = (result[col_return] > 0).astype("Int64")
            result.loc[result[col_return].isna(), col_class] = pd.NA

        return result

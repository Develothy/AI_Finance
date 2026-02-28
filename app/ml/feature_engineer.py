"""
피처 엔지니어링 파이프라인
==========================

StockPrice → 가격/기술지표/파생 피처 계산 → feature_store 저장
Phase 2: + StockFundamental/FinancialStatement → 재무 피처 병합
"""

import numpy as np
import pandas as pd

from core import get_logger
from db import database
from indicators import calc_sma, calc_ema, calc_rsi, calc_macd, calc_bollinger_bands, calc_obv
from models import StockPrice, StockFundamental, FinancialStatement
from repositories import MLRepository

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
    ) -> int:
        """
        단일 종목의 피처를 계산하여 feature_store에 저장

        Args:
            market: 마켓 (KOSPI 등)
            code: 종목코드
            start_date: 시작일 (없으면 전체)
            end_date: 종료일

        Returns:
            저장된 레코드 수
        """
        with database.session() as session:
            # 1. StockPrice 조회
            query = session.query(StockPrice).filter(
                StockPrice.market == market,
                StockPrice.code == code,
            )
            if start_date:
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

            # 3. 피처 계산
            features_df = self._compute_all_features(df, target_days=target_days)

            # 3.5 재무 피처 병합 (Phase 2)
            features_df = self._merge_fundamental_features(
                features_df, market, code, session,
            )

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
            repo = MLRepository(session)
            saved = repo.upsert_features(records)
            logger.info(
                f"피처 저장 완료: {market}:{code} ({saved}행)",
                "compute_features",
            )
            return saved

    def compute_all(
        self,
        market: str,
        start_date: str = None,
        end_date: str = None,
        target_days: list[int] = None,
    ) -> dict:
        """
        마켓 전체 종목의 피처를 계산

        Returns:
            {"total": N, "success": N, "failed": N}
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
            return {"total": 0, "success": 0, "failed": 0}

        total = len(codes)
        success = 0
        failed = 0

        for code in codes:
            try:
                count = self.compute_features(market, code, start_date, end_date, target_days=target_days)
                if count > 0:
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f"피처 계산 실패: {market}:{code} - {e}", "compute_all")
                failed += 1

        logger.info(
            f"전체 피처 계산 완료: {market} (total={total}, success={success}, failed={failed})",
            "compute_all",
        )
        return {"total": total, "success": success, "failed": failed}

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

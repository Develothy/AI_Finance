"""
피처 엔지니어링 파이프라인
==========================

StockPrice → 가격/기술지표/파생 피처 계산 → feature_store 저장
"""

import numpy as np
import pandas as pd

from core import get_logger
from db import database
from indicators import calc_sma, calc_ema, calc_rsi, calc_macd, calc_bollinger_bands, calc_obv
from models import StockPrice
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
            features_df = self._compute_all_features(df)

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
                count = self.compute_features(market, code, start_date, end_date)
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

    def _compute_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """모든 피처를 계산하여 하나의 DataFrame으로 반환"""
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

        # === 타겟 변수 (미래 데이터 사용) ===
        result["target_return_1d"] = df["close"].shift(-1) / df["close"] - 1
        result["target_return_5d"] = df["close"].shift(-5) / df["close"] - 1
        result["target_class_1d"] = (result["target_return_1d"] > 0).astype("Int64")
        result["target_class_5d"] = (result["target_return_5d"] > 0).astype("Int64")

        # 미래 데이터 없는 마지막 행들은 타겟 NULL
        result.loc[result["target_return_1d"].isna(), "target_class_1d"] = pd.NA
        result.loc[result["target_return_5d"].isna(), "target_class_5d"] = pd.NA

        return result

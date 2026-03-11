"""
어드민 레포지토리
=================

DB 통계 조회
"""

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import (
    StockPrice, StockInfo, StockFundamental, FinancialStatement,
    FeatureStore, MLModel, MLPrediction,
    NewsSentiment, DartDisclosure, KrxSupplyDemand,
)


class AdminRepository:
    def __init__(self, session: Session):
        self.session = session

    def stock_price_stats(self) -> dict:
        count = self.session.query(func.count(StockPrice.id)).scalar() or 0
        if count == 0:
            return {"row_count": 0}
        earliest = self.session.query(func.min(StockPrice.date)).scalar()
        latest = self.session.query(func.max(StockPrice.date)).scalar()
        markets = [r[0] for r in self.session.query(StockPrice.market).distinct().all()]
        code_count = self.session.query(func.count(func.distinct(StockPrice.code))).scalar() or 0
        return {
            "row_count": count,
            "earliest_date": earliest.strftime("%Y-%m-%d") if earliest else None,
            "latest_date": latest.strftime("%Y-%m-%d") if latest else None,
            "markets": markets,
            "code_count": code_count,
        }

    def stock_info_stats(self) -> dict:
        count = self.session.query(func.count(StockInfo.id)).scalar() or 0
        if count == 0:
            return {"row_count": 0}
        markets = [r[0] for r in self.session.query(StockInfo.market).distinct().all()]
        sector_count = self.session.query(func.count(func.distinct(StockInfo.sector))).scalar() or 0
        return {"row_count": count, "markets": markets, "sector_count": sector_count}

    def fundamental_stats(self) -> dict:
        count = self.session.query(func.count(StockFundamental.id)).scalar() or 0
        if count == 0:
            return {"row_count": 0}
        earliest = self.session.query(func.min(StockFundamental.date)).scalar()
        latest = self.session.query(func.max(StockFundamental.date)).scalar()
        markets = [r[0] for r in self.session.query(StockFundamental.market).distinct().all()]
        code_count = self.session.query(func.count(func.distinct(StockFundamental.code))).scalar() or 0
        return {
            "row_count": count,
            "earliest_date": earliest.strftime("%Y-%m-%d") if earliest else None,
            "latest_date": latest.strftime("%Y-%m-%d") if latest else None,
            "markets": markets,
            "code_count": code_count,
        }

    def financial_stmt_stats(self) -> dict:
        count = self.session.query(func.count(FinancialStatement.id)).scalar() or 0
        if count == 0:
            return {"row_count": 0}
        markets = [r[0] for r in self.session.query(FinancialStatement.market).distinct().all()]
        code_count = self.session.query(func.count(func.distinct(FinancialStatement.code))).scalar() or 0
        period_count = self.session.query(func.count(func.distinct(FinancialStatement.period_date))).scalar() or 0
        return {"row_count": count, "markets": markets, "code_count": code_count, "period_count": period_count}

    def feature_store_stats(self) -> dict:
        count = self.session.query(func.count(FeatureStore.id)).scalar() or 0
        if count == 0:
            return {"row_count": 0}
        earliest = self.session.query(func.min(FeatureStore.date)).scalar()
        latest = self.session.query(func.max(FeatureStore.date)).scalar()
        markets = [r[0] for r in self.session.query(FeatureStore.market).distinct().all()]
        code_count = self.session.query(func.count(func.distinct(FeatureStore.code))).scalar() or 0
        phase6_count = self.session.query(func.count(FeatureStore.id)).filter(
            FeatureStore.sector_return_1d.isnot(None),
        ).scalar() or 0
        phase6_code_count = self.session.query(
            func.count(func.distinct(FeatureStore.code))
        ).filter(
            FeatureStore.sector_return_1d.isnot(None),
        ).scalar() or 0
        return {
            "row_count": count,
            "earliest_date": earliest.strftime("%Y-%m-%d") if earliest else None,
            "latest_date": latest.strftime("%Y-%m-%d") if latest else None,
            "markets": markets,
            "code_count": code_count,
            "phase6_count": phase6_count,
            "phase6_code_count": phase6_code_count,
        }

    def news_stats(self) -> dict:
        count = self.session.query(func.count(NewsSentiment.id)).scalar() or 0
        if count == 0:
            return {"row_count": 0}
        earliest = self.session.query(func.min(NewsSentiment.date)).scalar()
        latest = self.session.query(func.max(NewsSentiment.date)).scalar()
        code_count = self.session.query(
            func.count(func.distinct(NewsSentiment.code))
        ).filter(NewsSentiment.code.isnot(None)).scalar() or 0
        return {
            "row_count": count,
            "earliest_date": earliest.strftime("%Y-%m-%d") if earliest else None,
            "latest_date": latest.strftime("%Y-%m-%d") if latest else None,
            "code_count": code_count,
        }

    def ml_model_stats(self) -> dict:
        count = self.session.query(func.count(MLModel.id)).scalar() or 0
        active = self.session.query(func.count(MLModel.id)).filter(MLModel.is_active.is_(True)).scalar() or 0
        return {"row_count": count, "active_count": active}

    def ml_prediction_stats(self) -> dict:
        count = self.session.query(func.count(MLPrediction.id)).scalar() or 0
        return {"row_count": count}

    def dart_stats(self) -> dict:
        return {
            "row_count": self.session.query(func.count(DartDisclosure.id)).scalar() or 0,
            "code_count": self.session.query(func.count(func.distinct(DartDisclosure.code))).scalar() or 0,
        }

    def krx_stats(self) -> dict:
        return {
            "row_count": self.session.query(func.count(KrxSupplyDemand.id)).scalar() or 0,
            "code_count": self.session.query(func.count(func.distinct(KrxSupplyDemand.code))).scalar() or 0,
        }

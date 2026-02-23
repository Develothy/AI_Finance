"""
ML 모델 Repository
"""

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models import FeatureStore, MLModel, MLTrainingLog, MLPrediction


class MLRepository:
    """ML 관련 DB 작업"""

    def __init__(self, session: Session):
        self.session = session

    # ============================================================
    # Upsert 헬퍼 (StockRepository 패턴 재사용)
    # ============================================================

    def _upsert(self, model, records, constraint, index_elements, update_fields, extra_set=None) -> int:
        if not records:
            return 0

        if settings.DB_TYPE == "postgresql":
            stmt = pg_insert(model).values(records)
            set_ = {col: getattr(stmt.excluded, col) for col in update_fields}
            if extra_set:
                set_.update(extra_set)
            stmt = stmt.on_conflict_do_update(constraint=constraint, set_=set_)
        else:
            stmt = sqlite_insert(model).values(records)
            set_ = {col: getattr(stmt.excluded, col) for col in update_fields}
            if extra_set:
                set_.update(extra_set)
            stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)

        self.session.execute(stmt)
        return len(records)

    # ============================================================
    # FeatureStore
    # ============================================================

    def upsert_features(self, records: list[dict]) -> int:
        """피처 데이터 Upsert"""
        if not records:
            return 0

        # records의 키에서 market, code, date를 제외한 나머지가 update 대상
        sample = records[0]
        update_fields = [k for k in sample.keys() if k not in ("market", "code", "date")]

        return self._upsert(
            FeatureStore, records, "uq_feature_store",
            ["market", "code", "date"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    def get_features(
        self,
        market: str,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[FeatureStore]:
        """종목별 피처 조회"""
        query = self.session.query(FeatureStore).filter(
            FeatureStore.market == market,
            FeatureStore.code == code,
        )
        if start_date:
            query = query.filter(FeatureStore.date >= start_date)
        if end_date:
            query = query.filter(FeatureStore.date <= end_date)
        return query.order_by(FeatureStore.date).all()

    def get_features_by_market(
        self,
        market: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[FeatureStore]:
        """마켓별 전체 피처 조회 (학습용)"""
        query = self.session.query(FeatureStore).filter(
            FeatureStore.market == market,
            FeatureStore.target_class_1d.isnot(None),
        )
        if start_date:
            query = query.filter(FeatureStore.date >= start_date)
        if end_date:
            query = query.filter(FeatureStore.date <= end_date)
        return query.order_by(FeatureStore.date).all()

    def get_latest_features(self, market: str, code: str) -> Optional[FeatureStore]:
        """최신 피처 조회"""
        return self.session.query(FeatureStore).filter(
            FeatureStore.market == market,
            FeatureStore.code == code,
        ).order_by(FeatureStore.date.desc()).first()

    # ============================================================
    # MLModel
    # ============================================================

    def save_model(self, record: dict) -> MLModel:
        """모델 메타 저장"""
        model = MLModel(**record)
        self.session.add(model)
        self.session.flush()
        return model

    def get_model(self, model_id: int) -> Optional[MLModel]:
        """모델 조회"""
        return self.session.query(MLModel).filter(MLModel.id == model_id).first()

    def get_model_by_name(self, model_name: str, version: int = None) -> Optional[MLModel]:
        """이름으로 모델 조회"""
        query = self.session.query(MLModel).filter(MLModel.model_name == model_name)
        if version is not None:
            query = query.filter(MLModel.version == version)
        else:
            query = query.order_by(MLModel.version.desc())
        return query.first()

    def get_active_models(self, market: str = None, model_type: str = None) -> list[MLModel]:
        """활성 모델 조회"""
        query = self.session.query(MLModel).filter(MLModel.is_active == True)
        if market:
            query = query.filter(MLModel.market == market)
        if model_type:
            query = query.filter(MLModel.model_type == model_type)
        return query.all()

    def get_all_models(self, market: str = None) -> list[MLModel]:
        """전체 모델 목록"""
        query = self.session.query(MLModel)
        if market:
            query = query.filter(MLModel.market == market)
        return query.order_by(MLModel.created_at.desc()).all()

    def deactivate_models(self, market: str, model_type: str, target_column: str):
        """동일 조건의 기존 활성 모델 비활성화"""
        self.session.query(MLModel).filter(
            MLModel.market == market,
            MLModel.model_type == model_type,
            MLModel.target_column == target_column,
            MLModel.is_active == True,
        ).update({"is_active": False})

    def delete_model(self, model_id: int) -> bool:
        """모델 삭제"""
        model = self.get_model(model_id)
        if model:
            self.session.delete(model)
            return True
        return False

    # ============================================================
    # MLTrainingLog
    # ============================================================

    def save_training_log(self, record: dict) -> MLTrainingLog:
        """학습 이력 저장"""
        log = MLTrainingLog(**record)
        self.session.add(log)
        self.session.flush()
        return log

    def update_training_log(self, log_id: int, updates: dict):
        """학습 이력 업데이트"""
        self.session.query(MLTrainingLog).filter(
            MLTrainingLog.id == log_id
        ).update(updates)

    def get_training_logs(self, model_id: int = None, limit: int = 20) -> list[MLTrainingLog]:
        """학습 이력 조회"""
        query = self.session.query(MLTrainingLog)
        if model_id:
            query = query.filter(MLTrainingLog.model_id == model_id)
        return query.order_by(MLTrainingLog.started_at.desc()).limit(limit).all()

    # ============================================================
    # MLPrediction
    # ============================================================

    def upsert_predictions(self, records: list[dict]) -> int:
        """예측 결과 Upsert"""
        if not records:
            return 0

        update_fields = [
            "target_date", "predicted_class", "probability_up", "probability_down",
            "predicted_return", "signal", "confidence",
        ]

        return self._upsert(
            MLPrediction, records, "uq_ml_prediction",
            ["model_id", "market", "code", "prediction_date"],
            update_fields,
            extra_set={"created_at": datetime.now()},
        )

    def get_predictions(
        self,
        market: str = None,
        code: str = None,
        prediction_date: str = None,
        signal: str = None,
        limit: int = 100,
    ) -> list[MLPrediction]:
        """예측 결과 조회"""
        query = self.session.query(MLPrediction)
        if market:
            query = query.filter(MLPrediction.market == market)
        if code:
            query = query.filter(MLPrediction.code == code)
        if prediction_date:
            query = query.filter(MLPrediction.prediction_date == prediction_date)
        if signal:
            query = query.filter(MLPrediction.signal == signal)
        return query.order_by(MLPrediction.prediction_date.desc()).limit(limit).all()

    def get_latest_predictions(self, market: str = None, limit: int = 100) -> list[MLPrediction]:
        """최신 예측 결과 조회"""
        # 가장 최근 prediction_date 기준
        subquery = self.session.query(MLPrediction.prediction_date).order_by(
            MLPrediction.prediction_date.desc()
        ).limit(1).scalar_subquery()

        query = self.session.query(MLPrediction).filter(
            MLPrediction.prediction_date == subquery
        )
        if market:
            query = query.filter(MLPrediction.market == market)
        return query.limit(limit).all()

"""
ML 모델 관련 테이블
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)

from db import ModelBase


class FeatureStore(ModelBase):
    """종목별 일별 피처 저장소"""

    __tablename__ = "feature_store"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)

    # 가격 피처
    close = Column(Numeric(15, 2))
    return_1d = Column(Numeric(10, 6))
    return_5d = Column(Numeric(10, 6))
    return_20d = Column(Numeric(10, 6))
    volatility_20d = Column(Numeric(10, 6))
    volume_ratio = Column(Numeric(10, 4))

    # 기술적 지표 피처
    sma_5 = Column(Numeric(15, 4))
    sma_20 = Column(Numeric(15, 4))
    sma_60 = Column(Numeric(15, 4))
    ema_12 = Column(Numeric(15, 4))
    ema_26 = Column(Numeric(15, 4))
    rsi_14 = Column(Numeric(8, 4))
    macd = Column(Numeric(15, 4))
    macd_signal = Column(Numeric(15, 4))
    macd_histogram = Column(Numeric(15, 4))
    bb_upper = Column(Numeric(15, 4))
    bb_middle = Column(Numeric(15, 4))
    bb_lower = Column(Numeric(15, 4))
    bb_width = Column(Numeric(10, 6))
    bb_pctb = Column(Numeric(10, 6))
    obv = Column(Numeric(20, 2))

    # 파생 피처
    price_to_sma20 = Column(Numeric(10, 6))
    price_to_sma60 = Column(Numeric(10, 6))
    golden_cross = Column(Integer)
    rsi_zone = Column(Integer)

    # 재무 피처 (Phase 2)
    per = Column(Numeric(10, 2))
    pbr = Column(Numeric(10, 2))
    eps = Column(Numeric(15, 2))
    market_cap = Column(BigInteger)
    foreign_ratio = Column(Numeric(8, 4))
    inst_net_buy = Column(BigInteger)
    foreign_net_buy = Column(BigInteger)
    roe = Column(Numeric(10, 2))
    debt_ratio = Column(Numeric(10, 2))

    # 거시 피처 (Phase 3)
    krw_usd = Column(Numeric(10, 4))
    vix = Column(Numeric(8, 4))
    kospi_index = Column(Numeric(10, 2))
    us_10y = Column(Numeric(8, 4))
    kr_3y = Column(Numeric(8, 4))
    sp500 = Column(Numeric(10, 2))
    wti = Column(Numeric(10, 2))
    gold = Column(Numeric(10, 2))
    fed_rate = Column(Numeric(8, 4))      # 미국 기준금리 (FRED DFF)
    usd_index = Column(Numeric(10, 4))    # 달러 인덱스 (FRED DTWEXBGS)
    us_cpi = Column(Numeric(10, 4))       # 미국 CPI (FRED CPIAUCSL, 월별 forward-fill)

    # 뉴스 센티먼트 피처 (Phase 4)
    news_sentiment = Column(Numeric(6, 4))       # 종목별 일평균 센티먼트 (-1 ~ +1)
    news_volume = Column(Integer)                 # 종목별 일간 뉴스 건수
    news_sentiment_std = Column(Numeric(6, 4))    # 뉴스 센티먼트 표준편차
    market_sentiment = Column(Numeric(6, 4))      # 시장 전체 일평균 센티먼트
    market_news_volume = Column(Integer)           # 시장 전체 일간 뉴스 건수

    # 공시 피처 (Phase 5A)
    disclosure_count_30d = Column(Integer)                  # 최근 30일 공시 건수
    days_since_disclosure = Column(Integer)                 # 마지막 공시 이후 일수
    disclosure_sentiment = Column(Numeric(6, 4))            # 공시 제목 센티먼트
    disclosure_type_score = Column(Numeric(4, 2))           # 공시 유형 가중치 평균
    disclosure_volume_change = Column(Numeric(10, 4))       # 공시 빈도 변화율

    # 수급 피처 (Phase 5B)
    short_selling_volume = Column(BigInteger)               # 공매도 거래량
    short_selling_ratio = Column(Numeric(8, 4))             # 공매도 비율 (%)
    program_buy_volume = Column(BigInteger)                 # 프로그램 매수
    program_sell_volume = Column(BigInteger)                # 프로그램 매도
    program_net_volume = Column(BigInteger)                 # 프로그램 순매수

    # 섹터/상대강도 피처 (Phase 6A)
    sector_return_1d = Column(Numeric(10, 6))               # 섹터 평균 1일 수익률
    sector_return_5d = Column(Numeric(10, 6))               # 섹터 평균 5일 수익률
    relative_strength_1d = Column(Numeric(10, 6))           # 종목 - 섹터 평균 (1일)
    relative_strength_5d = Column(Numeric(10, 6))           # 종목 - 섹터 평균 (5일)
    relative_strength_20d = Column(Numeric(10, 6))          # 종목 - 섹터 평균 (20일)
    sector_momentum_rank = Column(Numeric(6, 4))            # 섹터 내 모멘텀 백분위
    sector_breadth = Column(Numeric(6, 4))                  # 섹터 상승 종목 비율

    # 뉴스 정제 피처 (Phase 6B)
    news_relevance_ratio = Column(Numeric(6, 4))            # 뉴스 관련성 비율
    news_sentiment_filtered = Column(Numeric(6, 4))         # 필터링된 뉴스 센티먼트
    sector_news_sentiment = Column(Numeric(6, 4))           # 섹터 뉴스 센티먼트

    # 타겟 변수
    target_class_1d = Column(Integer)
    target_class_5d = Column(Integer)
    target_return_1d = Column(Numeric(10, 6))
    target_return_5d = Column(Numeric(10, 6))

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("market", "code", "date", name="uq_feature_store"),
        Index("idx_feature_store_lookup", "market", "code", "date"),
        Index("idx_feature_store_date", "date"),
    )

    def __repr__(self):
        return f"<FeatureStore({self.market}:{self.code} {self.date})>"


class MLModel(ModelBase):
    """학습된 ML 모델 메타데이터"""

    __tablename__ = "ml_model"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_name = Column(String(100), nullable=False)
    model_type = Column(String(30), nullable=False)        # classification / regression
    algorithm = Column(String(30), nullable=False)          # random_forest / xgboost / lightgbm / ensemble
    market = Column(String(10), nullable=False)
    target_column = Column(String(50), nullable=False)      # target_class_1d 등

    # 학습 파라미터
    hyperparameters = Column(String(2000))                  # JSON
    feature_columns = Column(String(2000))                  # JSON
    train_start_date = Column(Date)
    train_end_date = Column(Date)
    train_sample_count = Column(Integer)

    # 분류 성능 지표
    accuracy = Column(Numeric(6, 4))
    precision_score = Column(Numeric(6, 4))
    recall = Column(Numeric(6, 4))
    f1_score = Column(Numeric(6, 4))
    auc_roc = Column(Numeric(6, 4))

    # 회귀 성능 지표
    mse = Column(Numeric(15, 6))
    rmse = Column(Numeric(15, 6))
    mae = Column(Numeric(15, 6))
    r2_score = Column(Numeric(8, 6))

    # 모델 파일
    model_path = Column(String(500))
    is_active = Column(Boolean, default=False)
    version = Column(Integer, default=1)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("model_name", "version", name="uq_ml_model"),
        Index("idx_ml_model_active", "market", "model_type", "is_active"),
    )

    def __repr__(self):
        return f"<MLModel({self.model_name} v{self.version} {self.algorithm})>"


class MLTrainingLog(ModelBase):
    """ML 모델 학습 이력"""

    __tablename__ = "ml_training_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("ml_model.id", ondelete="SET NULL"), nullable=True)
    algorithm = Column(String(30), nullable=False)
    model_type = Column(String(30), nullable=False)
    market = Column(String(10), nullable=False)
    target_column = Column(String(50), nullable=False)

    # 학습 데이터 정보
    train_start_date = Column(Date)
    train_end_date = Column(Date)
    val_start_date = Column(Date)
    val_end_date = Column(Date)
    train_samples = Column(Integer)
    val_samples = Column(Integer)
    feature_count = Column(Integer)

    # 학습 결과
    status = Column(String(20), nullable=False, default="running")  # running / success / failed
    metrics_json = Column(String(3000))
    feature_importance_json = Column(String(5000))
    hyperparameters_json = Column(String(2000))

    # Optuna
    optuna_trials = Column(Integer)
    best_trial_value = Column(Numeric(10, 6))

    started_at = Column(DateTime, nullable=False, default=datetime.now)
    finished_at = Column(DateTime, nullable=True)
    error_message = Column(String(1000))

    __table_args__ = (
        Index("idx_training_log_model", "model_id"),
        Index("idx_training_log_status", "status", "started_at"),
    )

    def __repr__(self):
        return f"<MLTrainingLog(model_id={self.model_id} {self.status} {self.started_at})>"


class MLPrediction(ModelBase):
    """ML 모델 예측 결과"""

    __tablename__ = "ml_prediction"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("ml_model.id", ondelete="CASCADE"), nullable=False)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    prediction_date = Column(Date, nullable=False)
    target_date = Column(Date, nullable=False)

    # 분류 결과
    predicted_class = Column(Integer)
    probability_up = Column(Numeric(6, 4))
    probability_down = Column(Numeric(6, 4))

    # 회귀 결과
    predicted_return = Column(Numeric(10, 6))

    # 시그널
    signal = Column(String(10))         # BUY / SELL / HOLD
    confidence = Column(Numeric(6, 4))

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("model_id", "market", "code", "prediction_date", name="uq_ml_prediction"),
        Index("idx_prediction_lookup", "market", "code", "prediction_date"),
        Index("idx_prediction_signal", "signal", "prediction_date"),
    )

    def __repr__(self):
        return f"<MLPrediction({self.market}:{self.code} {self.prediction_date} {self.signal})>"

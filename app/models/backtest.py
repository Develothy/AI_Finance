"""
백테스트 관련 테이블
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)

from db import ModelBase


class BacktestRun(ModelBase):
    """백테스트 실행 메타데이터"""

    __tablename__ = "backtest_run"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    market = Column(String(10), nullable=False)
    strategy = Column(String(50), nullable=False)          # ml_ensemble / single_model

    # 백테스트 기간
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    # 설정 (JSON)
    config_json = Column(Text)                              # model_ids, aggregation_method 등

    # 포트폴리오 파라미터
    initial_capital = Column(Numeric(15, 2), nullable=False, default=10_000_000)
    transaction_fee = Column(Numeric(8, 6), default=0.00015)
    tax_rate = Column(Numeric(8, 6), default=0.0023)

    # 종목 목록 (JSON)
    codes_json = Column(Text)

    # 성과 지표
    total_return = Column(Numeric(10, 6))
    annualized_return = Column(Numeric(10, 6))
    sharpe_ratio = Column(Numeric(10, 6))
    sortino_ratio = Column(Numeric(10, 6))
    max_drawdown = Column(Numeric(10, 6))
    calmar_ratio = Column(Numeric(10, 6))
    win_rate = Column(Numeric(8, 6))
    profit_factor = Column(Numeric(10, 4))
    total_trades = Column(Integer)

    # 벤치마크
    benchmark_return = Column(Numeric(10, 6))
    alpha = Column(Numeric(10, 6))

    # 모델 레이스
    race_group = Column(String(36), nullable=True)          # UUID — 레이스 그룹 식별자

    # 실행 정보
    status = Column(String(20), default="running")         # running / success / failed
    error_message = Column(String(1000))
    started_at = Column(DateTime, default=datetime.now)
    finished_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index("idx_backtest_run_market", "market", "status"),
        Index("idx_backtest_run_date", "start_date", "end_date"),
        Index("idx_backtest_run_race_group", "race_group"),
    )

    def __repr__(self):
        return f"<BacktestRun({self.name} {self.market} {self.status})>"


class BacktestTrade(ModelBase):
    """백테스트 개별 거래 기록"""

    __tablename__ = "backtest_trade"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False)
    market = Column(String(10), nullable=False)
    code = Column(String(20), nullable=False)
    trade_date = Column(Date, nullable=False)
    action = Column(String(10), nullable=False)            # BUY / SELL
    price = Column(Numeric(15, 2), nullable=False)
    shares = Column(Integer, nullable=False)
    amount = Column(Numeric(15, 2), nullable=False)        # price * shares
    fee = Column(Numeric(15, 4))
    tax = Column(Numeric(15, 4))

    # 시그널 출처
    signal_source = Column(String(100))                    # ensemble / model_id
    signal_confidence = Column(Numeric(6, 4))
    probability_up = Column(Numeric(6, 4))

    # 거래 후 포트폴리오 상태
    cash_after = Column(Numeric(15, 2))
    portfolio_value_after = Column(Numeric(15, 2))

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index("idx_backtest_trade_run", "run_id"),
        Index("idx_backtest_trade_code", "run_id", "code", "trade_date"),
    )

    def __repr__(self):
        return f"<BacktestTrade(run={self.run_id} {self.code} {self.action} {self.trade_date})>"


class BacktestDaily(ModelBase):
    """백테스트 일별 에쿼티 커브"""

    __tablename__ = "backtest_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    portfolio_value = Column(Numeric(15, 2), nullable=False)
    cash = Column(Numeric(15, 2))
    positions_value = Column(Numeric(15, 2))
    daily_return = Column(Numeric(10, 6))
    cumulative_return = Column(Numeric(10, 6))
    drawdown = Column(Numeric(10, 6))
    benchmark_value = Column(Numeric(15, 2))
    benchmark_return = Column(Numeric(10, 6))

    # 보유 현황 (JSON: {"005930": {"shares": 10, "avg_cost": 70000}, ...})
    positions_json = Column(String(5000))

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("run_id", "date", name="uq_backtest_daily"),
        Index("idx_backtest_daily_run", "run_id", "date"),
    )

    def __repr__(self):
        return f"<BacktestDaily(run={self.run_id} {self.date} val={self.portfolio_value})>"

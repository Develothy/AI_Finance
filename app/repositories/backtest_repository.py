"""
백테스트 Repository
"""

from typing import Optional

from sqlalchemy.orm import Session

from models.backtest import BacktestRun, BacktestTrade, BacktestDaily


class BacktestRepository:


    def __init__(self, session: Session):
        self.session = session

    # ============================================================
    # BacktestRun
    # ============================================================

    def create_run(self, record: dict) -> BacktestRun:
        run = BacktestRun(**record)
        self.session.add(run)
        self.session.flush()
        return run

    def update_run(self, run_id: int, updates: dict):
        self.session.query(BacktestRun).filter(
            BacktestRun.id == run_id
        ).update(updates)

    def get_run(self, run_id: int) -> Optional[BacktestRun]:
        return self.session.query(BacktestRun).filter(
            BacktestRun.id == run_id
        ).first()

    def get_runs(self, market: str = None, limit: int = 50) -> list[BacktestRun]:
        query = self.session.query(BacktestRun)
        if market:
            query = query.filter(BacktestRun.market == market)
        return query.order_by(BacktestRun.created_at.desc()).limit(limit).all()

    def get_runs_by_race_group(self, race_group: str) -> list[BacktestRun]:
        """레이스 그룹의 모든 실행 조회"""
        return self.session.query(BacktestRun).filter(
            BacktestRun.race_group == race_group
        ).order_by(BacktestRun.id).all()

    def delete_run(self, run_id: int) -> bool:
        run = self.get_run(run_id)
        if run:
            # CASCADE가 DB 레벨이므로 수동 삭제
            self.session.query(BacktestDaily).filter(BacktestDaily.run_id == run_id).delete()
            self.session.query(BacktestTrade).filter(BacktestTrade.run_id == run_id).delete()
            self.session.delete(run)
            return True
        return False

    # ============================================================
    # BacktestTrade
    # ============================================================

    def bulk_insert_trades(self, records: list[dict]) -> int:
        if not records:
            return 0
        self.session.bulk_insert_mappings(BacktestTrade, records)
        return len(records)

    def get_trades(self, run_id: int, code: str = None, limit: int = 500) -> list[BacktestTrade]:
        query = self.session.query(BacktestTrade).filter(
            BacktestTrade.run_id == run_id
        )
        if code:
            query = query.filter(BacktestTrade.code == code)
        return query.order_by(BacktestTrade.trade_date).limit(limit).all()

    # ============================================================
    # BacktestDaily
    # ============================================================

    def bulk_insert_daily(self, records: list[dict]) -> int:
        if not records:
            return 0
        self.session.bulk_insert_mappings(BacktestDaily, records)
        return len(records)

    def get_daily(self, run_id: int) -> list[BacktestDaily]:
        return self.session.query(BacktestDaily).filter(
            BacktestDaily.run_id == run_id
        ).order_by(BacktestDaily.date).all()

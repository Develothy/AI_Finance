"""
백테스트 서비스
================

데이터 로딩 → 엔진 실행 → 결과 저장 오케스트레이션
"""

import json
import uuid
from datetime import datetime, timedelta

import pandas as pd

from core import get_logger
from db import database
from ml.backtester import BacktestEngine
from repositories import MLRepository
from repositories.backtest_repository import BacktestRepository
from repositories.stock_repository import StockRepository

logger = get_logger("backtest_service")


class BacktestService:

    # ============================================================
    # 실행
    # ============================================================

    def run_backtest(
        self,
        market: str,
        codes: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        model_ids: list[int] | None = None,
        aggregation_method: str = "majority_vote",
        initial_capital: float = 10_000_000,
        transaction_fee: float = 0.00015,
        tax_rate: float = 0.0023,
        max_position_pct: float = 0.2,
        name: str | None = None,
        race_group: str | None = None,
    ) -> dict:
        """백테스트 실행.

        1. BacktestRun 생성 (status=running)
        2. 가격/시그널 로딩
        3. BacktestEngine 실행
        4. 거래/일별 기록 저장
        5. 지표 업데이트 (status=success)
        6. 결과 반환
        """
        # 기본값
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if not name:
            name = f"backtest_{market}_{start_date}_{end_date}"

        with database.session() as session:
            bt_repo = BacktestRepository(session)
            ml_repo = MLRepository(session)
            stock_repo = StockRepository(session)

            # 1. 종목 목록 결정
            if not codes:
                codes = self._get_predicted_codes(ml_repo, market, start_date, end_date, model_ids)
                if not codes:
                    raise ValueError("해당 기간에 예측 데이터가 없습니다")

            # 2. BacktestRun 생성
            config = {
                "model_ids": model_ids,
                "aggregation_method": aggregation_method,
                "max_position_pct": max_position_pct,
            }
            run = bt_repo.create_run({
                "name": name,
                "market": market,
                "strategy": "ml_ensemble" if not model_ids or len(model_ids) != 1 else "single_model",
                "start_date": start_date,
                "end_date": end_date,
                "config_json": json.dumps(config),
                "initial_capital": initial_capital,
                "transaction_fee": transaction_fee,
                "tax_rate": tax_rate,
                "codes_json": json.dumps(codes),
                "race_group": race_group,
                "status": "running",
                "started_at": datetime.now(),
            })
            session.flush()
            run_id = run.id

            try:
                # 3. 가격 데이터 로딩
                prices = self._load_prices(stock_repo, market, codes, start_date, end_date)
                if not prices:
                    raise ValueError("가격 데이터가 없습니다")

                # 4. 시그널 로딩
                signals = self._load_signals(ml_repo, market, codes, start_date, end_date, model_ids)
                if not signals:
                    raise ValueError("시그널 데이터가 없습니다")

                # 5. 모델 가중치 로딩 (weighted_vote용)
                model_weights = {}
                if aggregation_method == "weighted_vote":
                    model_weights = self._load_model_weights(ml_repo, market, model_ids)

                # 6. 엔진 실행
                engine = BacktestEngine(
                    initial_capital=initial_capital,
                    transaction_fee=transaction_fee,
                    tax_rate=tax_rate,
                    max_position_pct=max_position_pct,
                    aggregation_method=aggregation_method,
                    model_weights=model_weights,
                )
                result = engine.run(prices, signals)

                # 7. 거래 기록 저장
                trade_records = [
                    {**t, "run_id": run_id, "market": market}
                    for t in result.trades
                ]
                bt_repo.bulk_insert_trades(trade_records)

                # 8. 일별 스냅샷 저장
                daily_records = [
                    {**d, "run_id": run_id}
                    for d in result.daily_snapshots
                ]
                bt_repo.bulk_insert_daily(daily_records)

                # 9. 지표 업데이트
                bt_repo.update_run(run_id, {
                    "status": "success",
                    "finished_at": datetime.now(),
                    **result.metrics,
                })

                logger.info(
                    f"백테스트 완료: {name} (종목 {len(codes)}개, "
                    f"거래 {result.metrics.get('total_trades', 0)}건, "
                    f"수익률 {result.metrics.get('total_return', 0) * 100:.2f}%)"
                )

                return self._run_to_dict(bt_repo.get_run(run_id))

            except Exception as e:
                bt_repo.update_run(run_id, {
                    "status": "failed",
                    "error_message": str(e)[:1000],
                    "finished_at": datetime.now(),
                })
                logger.error(f"백테스트 실패: {name} — {e}")
                raise

    # ============================================================
    # 조회
    # ============================================================

    def get_runs(self, market: str = None, limit: int = 50) -> list[dict]:
        with database.session() as session:
            repo = BacktestRepository(session)
            runs = repo.get_runs(market, limit)
            return [self._run_to_dict(r) for r in runs]

    def get_run_detail(self, run_id: int) -> dict | None:
        with database.session() as session:
            repo = BacktestRepository(session)
            run = repo.get_run(run_id)
            if not run:
                return None
            return self._run_to_dict(run)

    def get_trades(self, run_id: int, code: str = None, limit: int = 500) -> list[dict]:
        with database.session() as session:
            repo = BacktestRepository(session)
            trades = repo.get_trades(run_id, code, limit)
            return [self._trade_to_dict(t) for t in trades]

    def get_equity_curve(self, run_id: int) -> list[dict]:
        with database.session() as session:
            repo = BacktestRepository(session)
            daily = repo.get_daily(run_id)
            return [self._daily_to_dict(d) for d in daily]

    def delete_run(self, run_id: int) -> bool:
        with database.session() as session:
            repo = BacktestRepository(session)
            return repo.delete_run(run_id)

    def compare_runs(self, run_ids: list[int]) -> list[dict]:
        with database.session() as session:
            repo = BacktestRepository(session)
            results = []
            for rid in run_ids:
                run = repo.get_run(rid)
                if run:
                    results.append(self._run_to_dict(run))
            return results

    # ============================================================
    # 모델 레이스
    # ============================================================

    def run_model_race(
        self,
        market: str,
        start_date: str | None = None,
        end_date: str | None = None,
        codes: list[str] | None = None,
        model_ids: list[int] | None = None,
        initial_capital: float = 10_000_000,
        transaction_fee: float = 0.00015,
        tax_rate: float = 0.0023,
        max_position_pct: float = 0.2,
    ) -> dict:
        """모델 레이스: 각 모델을 개별 백테스트하여 수익률 경주 비교"""
        race_group = str(uuid.uuid4())

        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        # 활성 모델 목록 조회
        with database.session() as session:
            ml_repo = MLRepository(session)
            if model_ids:
                models = [ml_repo.get_model(mid) for mid in model_ids]
                models = [m for m in models if m]
            else:
                models = ml_repo.get_active_models(market=market)

            if not models:
                raise ValueError("레이스할 활성 모델이 없습니다")

            model_meta = {
                m.id: {"model_name": m.model_name, "algorithm": m.algorithm}
                for m in models
            }

        # 각 모델별 개별 백테스트
        participants = []
        for model_id, meta in model_meta.items():
            participant = {
                "model_id": model_id,
                "model_name": meta["model_name"],
                "algorithm": meta["algorithm"],
                "run_id": None,
                "status": "failed",
                "metrics": {},
                "equity_curve": [],
                "error_message": None,
            }
            try:
                result = self.run_backtest(
                    market=market, codes=codes,
                    start_date=start_date, end_date=end_date,
                    model_ids=[model_id], aggregation_method="majority_vote",
                    initial_capital=initial_capital,
                    transaction_fee=transaction_fee, tax_rate=tax_rate,
                    max_position_pct=max_position_pct,
                    name=f"race_{meta['algorithm']}_{model_id}",
                    race_group=race_group,
                )
                participant["run_id"] = result["id"]
                participant["status"] = result["status"]
                participant["metrics"] = result.get("metrics", {})
                equity = self.get_equity_curve(result["id"])
                participant["equity_curve"] = [
                    {"date": e.get("date"), "portfolio_value": e.get("portfolio_value"),
                     "cumulative_return": e.get("cumulative_return"), "drawdown": e.get("drawdown")}
                    for e in equity
                ]
            except Exception as e:
                participant["error_message"] = str(e)[:500]
                logger.warning(f"레이스 모델 실패: {meta['model_name']} — {e}")
            participants.append(participant)

        summary = self._build_race_summary(participants)
        return {
            "race_group": race_group, "race_type": "model",
            "market": market, "start_date": start_date, "end_date": end_date,
            "initial_capital": initial_capital,
            "summary": summary, "participants": participants,
        }

    # ============================================================
    # 종목 레이스
    # ============================================================

    def run_stock_race(
        self,
        market: str,
        codes: list[str],
        period_days: int = 30,
    ) -> dict:
        """종목 레이스: 종목별 종가 수익률 비교 (백테스트 엔진 미사용)"""
        race_group = str(uuid.uuid4())

        with database.session() as session:
            stock_repo = StockRepository(session)

            # 마지막 영업일 기준 end_date 결정
            end_date = None
            for code in codes:
                latest = stock_repo.get_latest_price(code, market)
                if latest:
                    d = str(latest.date)
                    if not end_date or d > end_date:
                        end_date = d
            if not end_date:
                raise ValueError("가격 데이터가 없습니다")

            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=int(period_days * 1.5))  # 영업일 확보 여유
            start_date = start_dt.strftime("%Y-%m-%d")

            participants = []
            for code in codes:
                rows = stock_repo.get_prices(code, market, start_date, end_date)
                if not rows:
                    participants.append({
                        "code": code, "name": code,
                        "equity_curve": [], "total_return": None,
                        "error_message": "가격 데이터 없음",
                    })
                    continue

                # 날짜순 정렬
                sorted_rows = sorted(rows, key=lambda r: r.date)

                # period_days에 맞게 뒤에서 자르기
                if len(sorted_rows) > period_days:
                    sorted_rows = sorted_rows[-period_days:]

                base_price = float(sorted_rows[0].close) if sorted_rows[0].close else None
                if not base_price:
                    participants.append({
                        "code": code, "name": code,
                        "equity_curve": [], "total_return": None,
                        "error_message": "시작일 종가 없음",
                    })
                    continue

                equity_curve = []
                for r in sorted_rows:
                    close = float(r.close) if r.close else base_price
                    ret = (close - base_price) / base_price
                    equity_curve.append({
                        "date": str(r.date),
                        "close": close,
                        "cumulative_return": round(ret, 6),
                    })

                # 종목명 조회
                info = stock_repo.get_info(code, market)
                name = info.name if info else code

                participants.append({
                    "code": code,
                    "name": name,
                    "equity_curve": equity_curve,
                    "total_return": equity_curve[-1]["cumulative_return"] if equity_curve else None,
                    "error_message": None,
                })

        # 서머리
        valid = [p for p in participants if p["total_return"] is not None]
        summary = {
            "total_stocks": len(participants),
            "success_count": len(valid),
            "failed_count": len(participants) - len(valid),
            "best_stock": None, "best_return": None,
            "worst_stock": None, "worst_return": None,
        }
        if valid:
            best = max(valid, key=lambda p: p["total_return"])
            worst = min(valid, key=lambda p: p["total_return"])
            summary["best_stock"] = best["name"]
            summary["best_return"] = best["total_return"]
            summary["worst_stock"] = worst["name"]
            summary["worst_return"] = worst["total_return"]

        return {
            "race_group": race_group, "race_type": "stock",
            "market": market,
            "start_date": participants[0]["equity_curve"][0]["date"] if valid and participants[0]["equity_curve"] else None,
            "end_date": end_date,
            "period_days": period_days,
            "summary": summary, "participants": participants,
        }

    # ============================================================
    # 레이스 결과 재조회
    # ============================================================

    def get_race_results(self, race_group: str) -> dict | None:
        """모델 레이스 결과 재조회"""
        with database.session() as session:
            bt_repo = BacktestRepository(session)
            ml_repo = MLRepository(session)
            runs = bt_repo.get_runs_by_race_group(race_group)
            if not runs:
                return None

            participants = []
            for run in runs:
                config = json.loads(run.config_json) if run.config_json else {}
                mids = config.get("model_ids", [])
                model_id = mids[0] if mids else None
                model_name, algorithm = None, None
                if model_id:
                    model = ml_repo.get_model(model_id)
                    if model:
                        model_name, algorithm = model.model_name, model.algorithm

                daily = bt_repo.get_daily(run.id)
                equity_curve = [
                    {"date": str(d.date),
                     "portfolio_value": float(d.portfolio_value) if d.portfolio_value else None,
                     "cumulative_return": float(d.cumulative_return) if d.cumulative_return else None,
                     "drawdown": float(d.drawdown) if d.drawdown else None}
                    for d in daily
                ]
                run_dict = self._run_to_dict(run)
                participants.append({
                    "model_id": model_id, "model_name": model_name,
                    "algorithm": algorithm, "run_id": run.id,
                    "status": run.status, "metrics": run_dict.get("metrics", {}),
                    "equity_curve": equity_curve, "error_message": run.error_message,
                })

            summary = self._build_race_summary(participants)
            first = runs[0]
            return {
                "race_group": race_group, "race_type": "model",
                "market": first.market,
                "start_date": str(first.start_date), "end_date": str(first.end_date),
                "initial_capital": float(first.initial_capital) if first.initial_capital else None,
                "summary": summary, "participants": participants,
            }

    @staticmethod
    def _build_race_summary(participants: list[dict]) -> dict:
        success_list = [p for p in participants if p.get("status") == "success"]
        failed_list = [p for p in participants if p.get("status") != "success"]
        summary = {
            "total_models": len(participants), "success_count": len(success_list),
            "failed_count": len(failed_list),
            "best_model": None, "best_return": None,
            "worst_model": None, "worst_return": None,
        }
        if success_list:
            returns = [
                (p.get("algorithm", ""), p["metrics"].get("total_return"))
                for p in success_list if p.get("metrics", {}).get("total_return") is not None
            ]
            if returns:
                best = max(returns, key=lambda x: x[1])
                worst = min(returns, key=lambda x: x[1])
                summary.update(best_model=best[0], best_return=best[1],
                               worst_model=worst[0], worst_return=worst[1])
        return summary

    # ============================================================
    # 데이터 로딩 헬퍼
    # ============================================================

    @staticmethod
    def _load_prices(
        stock_repo: StockRepository,
        market: str,
        codes: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, pd.DataFrame]:
        """종목별 가격 DataFrame 로딩"""
        prices = {}
        for code in codes:
            rows = stock_repo.get_prices(code, market, start_date, end_date)
            if rows:
                df = pd.DataFrame([{
                    "date": r.date,
                    "close": float(r.close) if r.close else 0.0,
                } for r in rows])
                if not df.empty:
                    prices[code] = df
        return prices

    @staticmethod
    def _load_signals(
        ml_repo: MLRepository,
        market: str,
        codes: list[str],
        start_date: str,
        end_date: str,
        model_ids: list[int] | None,
    ) -> dict[str, pd.DataFrame]:
        """종목별 시그널 DataFrame 로딩"""
        signals = {}
        for code in codes:
            rows = ml_repo.get_predictions(
                market=market, code=code, limit=10000,
            )
            # (MLPrediction, model_name, algorithm) 튜플
            records = []
            for pred, model_name, algorithm in rows:
                # 기간 필터
                pred_date = pred.prediction_date
                if hasattr(pred_date, "isoformat"):
                    date_str = pred_date.isoformat()
                else:
                    date_str = str(pred_date)

                if date_str < start_date or date_str > end_date:
                    continue

                # 모델 ID 필터
                if model_ids and pred.model_id not in model_ids:
                    continue

                if pred.signal:
                    records.append({
                        "date": pred.prediction_date,
                        "model_id": pred.model_id,
                        "signal": pred.signal,
                        "confidence": float(pred.confidence) if pred.confidence else 0.5,
                        "probability_up": float(pred.probability_up) if pred.probability_up else 0.5,
                    })

            if records:
                signals[code] = pd.DataFrame(records)

        return signals

    @staticmethod
    def _get_predicted_codes(
        ml_repo: MLRepository,
        market: str,
        start_date: str,
        end_date: str,
        model_ids: list[int] | None,
    ) -> list[str]:
        """예측 데이터가 있는 종목 코드 조회"""
        rows = ml_repo.get_predictions(market=market, limit=10000)
        codes = set()
        for pred, _, _ in rows:
            pred_date = pred.prediction_date
            if hasattr(pred_date, "isoformat"):
                date_str = pred_date.isoformat()
            else:
                date_str = str(pred_date)

            if date_str < start_date or date_str > end_date:
                continue
            if model_ids and pred.model_id not in model_ids:
                continue
            codes.add(pred.code)
        return sorted(codes)

    @staticmethod
    def _load_model_weights(
        ml_repo: MLRepository,
        market: str,
        model_ids: list[int] | None,
    ) -> dict[int, float]:
        """모델별 F1 점수 가중치 로딩"""
        models = ml_repo.get_active_models(market=market, model_type="classification")
        weights = {}
        for m in models:
            if model_ids and m.id not in model_ids:
                continue
            weights[m.id] = float(m.f1_score) if m.f1_score else 0.5
        return weights

    # ============================================================
    # 변환 헬퍼
    # ============================================================

    @staticmethod
    def _run_to_dict(r) -> dict:
        return {
            "id": r.id,
            "name": r.name,
            "market": r.market,
            "strategy": r.strategy,
            "start_date": str(r.start_date),
            "end_date": str(r.end_date),
            "initial_capital": float(r.initial_capital) if r.initial_capital else None,
            "transaction_fee": float(r.transaction_fee) if r.transaction_fee else None,
            "tax_rate": float(r.tax_rate) if r.tax_rate else None,
            "codes": json.loads(r.codes_json) if r.codes_json else [],
            "config": json.loads(r.config_json) if r.config_json else {},
            "race_group": r.race_group,
            "status": r.status,
            "error_message": r.error_message,
            "metrics": {
                "total_return": float(r.total_return) if r.total_return else None,
                "annualized_return": float(r.annualized_return) if r.annualized_return else None,
                "sharpe_ratio": float(r.sharpe_ratio) if r.sharpe_ratio else None,
                "sortino_ratio": float(r.sortino_ratio) if r.sortino_ratio else None,
                "max_drawdown": float(r.max_drawdown) if r.max_drawdown else None,
                "calmar_ratio": float(r.calmar_ratio) if r.calmar_ratio else None,
                "win_rate": float(r.win_rate) if r.win_rate else None,
                "profit_factor": float(r.profit_factor) if r.profit_factor else None,
                "total_trades": r.total_trades,
                "benchmark_return": float(r.benchmark_return) if r.benchmark_return else None,
                "alpha": float(r.alpha) if r.alpha else None,
            },
            "started_at": str(r.started_at) if r.started_at else None,
            "finished_at": str(r.finished_at) if r.finished_at else None,
            "created_at": str(r.created_at) if r.created_at else None,
        }

    @staticmethod
    def _trade_to_dict(t) -> dict:
        return {
            "id": t.id,
            "run_id": t.run_id,
            "market": t.market,
            "code": t.code,
            "trade_date": str(t.trade_date),
            "action": t.action,
            "price": float(t.price) if t.price else None,
            "shares": t.shares,
            "amount": float(t.amount) if t.amount else None,
            "fee": float(t.fee) if t.fee else None,
            "tax": float(t.tax) if t.tax else None,
            "signal_source": t.signal_source,
            "signal_confidence": float(t.signal_confidence) if t.signal_confidence else None,
            "probability_up": float(t.probability_up) if t.probability_up else None,
            "cash_after": float(t.cash_after) if t.cash_after else None,
            "portfolio_value_after": float(t.portfolio_value_after) if t.portfolio_value_after else None,
        }

    @staticmethod
    def _daily_to_dict(d) -> dict:
        return {
            "date": str(d.date),
            "portfolio_value": float(d.portfolio_value) if d.portfolio_value else None,
            "cash": float(d.cash) if d.cash else None,
            "positions_value": float(d.positions_value) if d.positions_value else None,
            "daily_return": float(d.daily_return) if d.daily_return else None,
            "cumulative_return": float(d.cumulative_return) if d.cumulative_return else None,
            "drawdown": float(d.drawdown) if d.drawdown else None,
            "benchmark_value": float(d.benchmark_value) if d.benchmark_value else None,
            "benchmark_return": float(d.benchmark_return) if d.benchmark_return else None,
        }

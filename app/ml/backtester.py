"""
백테스팅 엔진
==============

ML/DL/RL 시그널을 과거 가격 데이터에 시뮬레이션하여 전략 성과를 검증.
DB에 의존하지 않는 순수 연산 클래스.
"""

import json
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from core import get_logger

logger = get_logger("backtester")


@dataclass
class BacktestResult:

    metrics: dict = field(default_factory=dict)
    trades: list[dict] = field(default_factory=list)
    daily_snapshots: list[dict] = field(default_factory=list)
    benchmark_daily: list[dict] = field(default_factory=list)
    config: dict = field(default_factory=dict)


class BacktestEngine:
    """이벤트 기반 백테스팅 엔진.

    일별 루프: 시그널 집계 → 매매 실행 → 포트폴리오 갱신 → 일별 스냅샷.

    Args:
        initial_capital: 초기 자본금 (원)
        transaction_fee: 매매 수수료율 (매수/매도 공통, 기본 0.015%)
        tax_rate: 매도 세금율 (기본 0.23%)
        max_position_pct: 종목당 최대 비중 (기본 20%)
        aggregation_method: 시그널 앙상블 방식
        model_weights: 모델별 가중치 (weighted_vote용, {model_id: f1_score})
    """

    VALID_METHODS = {"majority_vote", "weighted_vote", "probability_avg", "unanimous"}

    def __init__(
        self,
        initial_capital: float = 10_000_000,
        transaction_fee: float = 0.00015,
        tax_rate: float = 0.0023,
        max_position_pct: float = 0.2,
        aggregation_method: str = "majority_vote",
        model_weights: dict[int, float] | None = None,
    ):
        self.initial_capital = initial_capital
        self.transaction_fee = transaction_fee
        self.tax_rate = tax_rate
        self.max_position_pct = max_position_pct
        self.aggregation_method = aggregation_method
        self.model_weights = model_weights or {}

        # 포트폴리오 상태
        self.cash = initial_capital
        self.positions: dict[str, dict] = {}  # {code: {"shares": int, "avg_cost": float}}
        self.peak_value = initial_capital

    def run(
        self,
        prices: dict[str, pd.DataFrame],
        signals: dict[str, pd.DataFrame],
    ) -> BacktestResult:
        """백테스트 실행.

        Args:
            prices: {code: DataFrame(date, close)} — 종목별 가격
            signals: {code: DataFrame(date, model_id, signal, confidence, probability_up)} — 종목별 시그널

        Returns:
            BacktestResult
        """
        # 초기화
        self.cash = self.initial_capital
        self.positions = {}
        self.peak_value = self.initial_capital

        trades = []
        daily_snapshots = []

        # 전체 거래일 수집 (모든 종목의 날짜 합집합)
        all_dates = set()
        for code, df in prices.items():
            all_dates.update(df["date"].tolist())
        trading_dates = sorted(all_dates)

        if not trading_dates:
            logger.warning("거래일이 없습니다")
            return BacktestResult(config=self._get_config())

        # 종목별 가격 인덱스 생성
        price_map: dict[str, dict] = {}  # {code: {date: close}}
        for code, df in prices.items():
            price_map[code] = dict(zip(df["date"].tolist(), df["close"].tolist()))

        # 종목별 시그널 인덱스 생성
        signal_map: dict[str, dict[object, list[dict]]] = {}  # {code: {date: [signals]}}
        for code, df in signals.items():
            signal_map[code] = {}
            for _, row in df.iterrows():
                d = row["date"]
                if d not in signal_map[code]:
                    signal_map[code][d] = []
                signal_map[code][d].append({
                    "model_id": row.get("model_id"),
                    "signal": row["signal"],
                    "confidence": float(row.get("confidence", 0.5)),
                    "probability_up": float(row.get("probability_up", 0.5)),
                })

        # 일별 루프
        for date in trading_dates:
            # 매매 실행: 시그널이 있는 종목 처리
            for code in prices:
                close = price_map.get(code, {}).get(date)
                if close is None or close <= 0:
                    continue

                # 해당 날짜의 시그널 조회
                day_signals = signal_map.get(code, {}).get(date, [])
                if not day_signals:
                    continue

                # 시그널 앙상블
                agg_signal, agg_confidence, agg_prob_up = self._aggregate_signals(day_signals)

                # 매매 실행
                trade = self._execute_trade(date, code, agg_signal, agg_confidence, agg_prob_up, float(close))
                if trade:
                    trades.append(trade)

            # 포트폴리오 가치 계산
            portfolio_value = self._compute_portfolio_value(date, price_map)
            self.peak_value = max(self.peak_value, portfolio_value)

            # 당일 거래 기록에 portfolio_value_after 갱신
            for t in trades:
                if t["trade_date"] == date and t["portfolio_value_after"] is None:
                    t["portfolio_value_after"] = round(portfolio_value, 2)

            # 일별 스냅샷
            positions_value = portfolio_value - self.cash
            prev_value = daily_snapshots[-1]["portfolio_value"] if daily_snapshots else self.initial_capital
            daily_ret = (portfolio_value / prev_value - 1.0) if prev_value > 0 else 0.0
            cum_ret = (portfolio_value / self.initial_capital - 1.0)
            dd = (self.peak_value - portfolio_value) / self.peak_value if self.peak_value > 0 else 0.0

            daily_snapshots.append({
                "date": date,
                "portfolio_value": round(portfolio_value, 2),
                "cash": round(self.cash, 2),
                "positions_value": round(positions_value, 2),
                "daily_return": round(daily_ret, 6),
                "cumulative_return": round(cum_ret, 6),
                "drawdown": round(dd, 6),
                "positions_json": json.dumps(
                    {c: p for c, p in self.positions.items()},
                    ensure_ascii=False,
                ),
            })

        # 벤치마크 계산
        benchmark_daily = self._compute_benchmark(prices, trading_dates)

        # 성과 지표 계산
        metrics = self._compute_metrics(daily_snapshots, trades)

        # 벤치마크 수익률
        if benchmark_daily:
            bm_return = (benchmark_daily[-1]["benchmark_value"] / self.initial_capital - 1.0)
            metrics["benchmark_return"] = round(bm_return, 6)
            metrics["alpha"] = round(metrics["total_return"] - bm_return, 6)

            # 일별 스냅샷에 벤치마크 합산
            bm_map = {b["date"]: b for b in benchmark_daily}
            for snap in daily_snapshots:
                bm = bm_map.get(snap["date"])
                if bm:
                    snap["benchmark_value"] = bm["benchmark_value"]
                    snap["benchmark_return"] = bm["benchmark_return"]

        return BacktestResult(
            metrics=metrics,
            trades=trades,
            daily_snapshots=daily_snapshots,
            benchmark_daily=benchmark_daily,
            config=self._get_config(),
        )

    # ============================================================
    # 시그널 앙상블
    # ============================================================

    def _aggregate_signals(self, day_signals: list[dict]) -> tuple[str, float, float]:
        """다중 모델 시그널 집계.

        Returns:
            (signal, confidence, probability_up)
        """
        method = self.aggregation_method
        if method == "majority_vote":
            return self._majority_vote(day_signals)
        elif method == "weighted_vote":
            return self._weighted_vote(day_signals)
        elif method == "probability_avg":
            return self._probability_avg(day_signals)
        elif method == "unanimous":
            return self._unanimous(day_signals)
        else:
            return self._majority_vote(day_signals)

    def _majority_vote(self, sigs: list[dict]) -> tuple[str, float, float]:
        counts = {"BUY": 0, "SELL": 0, "HOLD": 0}
        prob_sum = 0.0
        for s in sigs:
            counts[s["signal"]] = counts.get(s["signal"], 0) + 1
            prob_sum += s["probability_up"]

        total = len(sigs)
        avg_prob = prob_sum / total if total > 0 else 0.5

        if counts["BUY"] > total / 2:
            return "BUY", counts["BUY"] / total, avg_prob
        elif counts["SELL"] > total / 2:
            return "SELL", counts["SELL"] / total, avg_prob
        return "HOLD", max(counts.values()) / total, avg_prob

    def _weighted_vote(self, sigs: list[dict]) -> tuple[str, float, float]:
        weights = {"BUY": 0.0, "SELL": 0.0, "HOLD": 0.0}
        prob_sum = 0.0
        total_weight = 0.0

        for s in sigs:
            w = self.model_weights.get(s.get("model_id"), 1.0)
            weights[s["signal"]] = weights.get(s["signal"], 0.0) + w
            prob_sum += s["probability_up"] * w
            total_weight += w

        avg_prob = prob_sum / total_weight if total_weight > 0 else 0.5
        winner = max(weights, key=weights.get)
        confidence = weights[winner] / total_weight if total_weight > 0 else 0.5

        return winner, confidence, avg_prob

    def _probability_avg(self, sigs: list[dict]) -> tuple[str, float, float]:
        avg_prob = np.mean([s["probability_up"] for s in sigs])
        confidence = max(avg_prob, 1.0 - avg_prob)

        if avg_prob >= 0.6:
            return "BUY", float(confidence), float(avg_prob)
        elif avg_prob <= 0.4:
            return "SELL", float(confidence), float(avg_prob)
        return "HOLD", float(confidence), float(avg_prob)

    def _unanimous(self, sigs: list[dict]) -> tuple[str, float, float]:
        signals_set = set(s["signal"] for s in sigs)
        avg_prob = np.mean([s["probability_up"] for s in sigs])

        if len(signals_set) == 1:
            sig = signals_set.pop()
            min_conf = min(s["confidence"] for s in sigs)
            return sig, float(min_conf), float(avg_prob)
        return "HOLD", 0.5, float(avg_prob)

    # ============================================================
    # 매매 실행
    # ============================================================

    def _execute_trade(
        self,
        date,
        code: str,
        signal: str,
        confidence: float,
        probability_up: float,
        price: float,
    ) -> dict | None:
        """매매 실행 — environment.py 수수료 공식 재사용."""
        if signal == "BUY":
            return self._buy(date, code, price, confidence, probability_up)
        elif signal == "SELL":
            return self._sell(date, code, price, confidence, probability_up)
        return None

    def _buy(self, date, code, price, confidence, probability_up) -> dict | None:
        """매수: 종목당 최대 비중 제한"""
        if self.cash <= 0:
            return None

        # 현재 포트폴리오 가치 근사
        portfolio_value = self.cash
        for c, pos in self.positions.items():
            portfolio_value += pos["shares"] * pos.get("last_price", pos["avg_cost"])

        # 이미 보유 중이면 스킵
        if code in self.positions and self.positions[code]["shares"] > 0:
            return None

        # 종목당 최대 투자 금액
        max_amount = portfolio_value * self.max_position_pct
        invest_amount = min(self.cash, max_amount)

        # 매수 수량 (수수료 포함)
        max_shares = int(invest_amount / (price * (1 + self.transaction_fee)))
        if max_shares <= 0:
            return None

        cost = max_shares * price
        fee = cost * self.transaction_fee
        self.cash -= (cost + fee)

        self.positions[code] = {
            "shares": max_shares,
            "avg_cost": price,
            "last_price": price,
        }

        return {
            "code": code,
            "trade_date": date,
            "action": "BUY",
            "price": round(price, 2),
            "shares": max_shares,
            "amount": round(cost, 2),
            "fee": round(fee, 4),
            "tax": 0.0,
            "signal_source": self.aggregation_method,
            "signal_confidence": round(confidence, 4),
            "probability_up": round(probability_up, 4),
            "cash_after": round(self.cash, 2),
            "portfolio_value_after": None,
        }

    def _sell(self, date, code, price, confidence, probability_up) -> dict | None:
        """매도: 전량 매도"""
        pos = self.positions.get(code)
        if not pos or pos["shares"] <= 0:
            return None

        shares = pos["shares"]
        revenue = shares * price
        fee = revenue * self.transaction_fee
        tax = revenue * self.tax_rate
        self.cash += (revenue - fee - tax)

        del self.positions[code]

        return {
            "code": code,
            "trade_date": date,
            "action": "SELL",
            "price": round(price, 2),
            "shares": shares,
            "amount": round(revenue, 2),
            "fee": round(fee, 4),
            "tax": round(tax, 4),
            "signal_source": self.aggregation_method,
            "signal_confidence": round(confidence, 4),
            "probability_up": round(probability_up, 4),
            "cash_after": round(self.cash, 2),
            "portfolio_value_after": None,
        }

    # ============================================================
    # 포트폴리오 가치
    # ============================================================

    def _compute_portfolio_value(self, date, price_map: dict) -> float:
        """포트폴리오 시가총액 계산"""
        value = self.cash
        for code, pos in self.positions.items():
            close = price_map.get(code, {}).get(date, pos.get("last_price", pos["avg_cost"]))
            pos["last_price"] = close
            value += pos["shares"] * close
        return value

    # ============================================================
    # 성과 지표
    # ============================================================

    def _compute_metrics(self, daily_snapshots: list[dict], trades: list[dict]) -> dict:
        """성과 지표 일괄 계산"""
        if len(daily_snapshots) < 2:
            return {"total_return": 0.0, "total_trades": len(trades)}

        values = [s["portfolio_value"] for s in daily_snapshots]
        daily_returns = np.diff(values) / np.array(values[:-1])
        daily_returns = np.nan_to_num(daily_returns, nan=0.0)

        # 총 수익률
        total_return = values[-1] / values[0] - 1.0

        # 연환산 수익률 (252 거래일)
        n_days = len(daily_returns)
        annualized = (1 + total_return) ** (252 / max(n_days, 1)) - 1

        # 샤프 비율 (무위험이자율 0)
        dr_std = np.std(daily_returns)
        sharpe = float(np.mean(daily_returns) / (dr_std + 1e-8) * np.sqrt(252))

        # 소르티노 비율 (하방 편차만)
        downside = daily_returns[daily_returns < 0]
        downside_std = float(np.std(downside)) if len(downside) > 0 else 1e-8
        sortino = float(np.mean(daily_returns) / (downside_std + 1e-8) * np.sqrt(252))

        # 최대 낙폭
        cumulative = np.cumprod(1 + daily_returns)
        peak = np.maximum.accumulate(cumulative)
        drawdowns = (peak - cumulative) / peak
        max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

        # 칼마 비율
        calmar = annualized / (max_dd + 1e-8)

        # 승률 & 수익 팩터 (매수-매도 페어 기반)
        win_rate, profit_factor = self._compute_trade_metrics(trades)

        # DB Numeric 컬럼 오버플로우 방지 — clamp
        _RATIO_MAX = 9999.0       # Numeric(10, 6) → max ±9999.999999
        _PF_MAX = 999999.0        # Numeric(10, 4) → max ±999999.9999

        return {
            "total_return": round(float(total_return), 6),
            "annualized_return": round(float(annualized), 6),
            "sharpe_ratio": round(max(min(sharpe, _RATIO_MAX), -_RATIO_MAX), 6),
            "sortino_ratio": round(max(min(sortino, _RATIO_MAX), -_RATIO_MAX), 6),
            "max_drawdown": round(max_dd, 6),
            "calmar_ratio": round(max(min(float(calmar), _RATIO_MAX), -_RATIO_MAX), 6),
            "win_rate": round(win_rate, 6),
            "profit_factor": round(min(profit_factor, _PF_MAX), 4),
            "total_trades": len(trades),
        }

    def _compute_trade_metrics(self, trades: list[dict]) -> tuple[float, float]:
        """매수-매도 페어 기반 승률/수익팩터"""
        buy_map: dict[str, list[dict]] = {}
        gross_profit = 0.0
        gross_loss = 0.0
        wins = 0
        total_pairs = 0

        for t in trades:
            code = t["code"]
            if t["action"] == "BUY":
                buy_map.setdefault(code, []).append(t)
            elif t["action"] == "SELL":
                buys = buy_map.get(code, [])
                if buys:
                    buy = buys.pop(0)
                    pnl = (t["price"] - buy["price"]) * t["shares"] - t["fee"] - t["tax"] - buy["fee"]
                    total_pairs += 1
                    if pnl > 0:
                        wins += 1
                        gross_profit += pnl
                    else:
                        gross_loss += abs(pnl)

        win_rate = wins / total_pairs if total_pairs > 0 else 0.0
        profit_factor = gross_profit / (gross_loss + 1e-8)

        return win_rate, profit_factor

    # ============================================================
    # 벤치마크
    # ============================================================

    def _compute_benchmark(
        self,
        prices: dict[str, pd.DataFrame],
        trading_dates: list,
    ) -> list[dict]:
        """Buy & Hold 벤치마크: 첫날 균등 매수, 끝까지 보유"""
        if not prices or not trading_dates:
            return []

        codes = list(prices.keys())
        first_date = trading_dates[0]

        # 종목별 첫날 가격
        bm_positions: dict[str, dict] = {}
        capital_per_code = self.initial_capital / len(codes)
        remaining_cash = self.initial_capital

        price_maps = {}
        for code, df in prices.items():
            price_maps[code] = dict(zip(df["date"].tolist(), df["close"].tolist()))

        for code in codes:
            first_price = price_maps.get(code, {}).get(first_date)
            if first_price and first_price > 0:
                shares = int(capital_per_code / first_price)
                if shares > 0:
                    cost = shares * first_price
                    remaining_cash -= cost
                    bm_positions[code] = {"shares": shares, "price": first_price}

        # 일별 벤치마크 가치
        benchmark_daily = []
        for date in trading_dates:
            bm_value = remaining_cash
            for code, pos in bm_positions.items():
                close = price_maps.get(code, {}).get(date, pos["price"])
                bm_value += pos["shares"] * close

            bm_return = (bm_value / self.initial_capital - 1.0)
            benchmark_daily.append({
                "date": date,
                "benchmark_value": round(bm_value, 2),
                "benchmark_return": round(bm_return, 6),
            })

        return benchmark_daily

    # ============================================================
    # 유틸
    # ============================================================

    def _get_config(self) -> dict:
        return {
            "initial_capital": self.initial_capital,
            "transaction_fee": self.transaction_fee,
            "tax_rate": self.tax_rate,
            "max_position_pct": self.max_position_pct,
            "aggregation_method": self.aggregation_method,
        }

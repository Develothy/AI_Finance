"""
주식 트레이딩 환경 (Gymnasium)
==============================

종목별 시계열 데이터로 에피소드를 구성하여 RL 에이전트를 학습.

관찰 공간: feature_store 피처 + 포지션 정보 (cash_ratio, stock_ratio)
액션 공간: Discrete(3) — 0=HOLD, 1=BUY, 2=SELL
보상: 포트폴리오 수익률 - 거래비용 - 최대낙폭 페널티
"""

import gymnasium as gym
import numpy as np
from gymnasium import spaces


class StockTradingEnv(gym.Env):
    """주식 트레이딩 Gymnasium 환경.

    Args:
        df: 단일 종목의 시계열 DataFrame (date ASC 정렬, close + 피처 컬럼 포함)
        feature_columns: 피처 컬럼명 리스트
        initial_balance: 초기 자금 (원)
        transaction_fee: 매매 수수료율 (매수/매도 공통)
        tax_rate: 매도 세금율
    """

    metadata = {"render_modes": []}

    # 액션 상수
    HOLD = 0
    BUY = 1
    SELL = 2

    def __init__(
        self,
        df,
        feature_columns: list[str],
        initial_balance: float = 10_000_000,
        transaction_fee: float = 0.00015,
        tax_rate: float = 0.0023,
    ):
        super().__init__()

        self.df = df.reset_index(drop=True)
        self.feature_columns = feature_columns
        self.initial_balance = initial_balance
        self.transaction_fee = transaction_fee
        self.tax_rate = tax_rate

        self.n_features = len(feature_columns)
        self.n_steps = len(df)

        # 가격 배열 캐시
        self.close_prices = self.df["close"].values.astype(np.float64)

        # 피처 배열 캐시 (NaN은 사전에 impute/scale 완료된 상태)
        self.feature_array = self.df[feature_columns].values.astype(np.float32)

        # 관찰 공간: 피처 + cash_ratio + stock_ratio
        obs_dim = self.n_features + 2
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(obs_dim,), dtype=np.float32,
        )

        # 액션 공간: HOLD(0), BUY(1), SELL(2)
        self.action_space = spaces.Discrete(3)

        # 상태 변수 (reset에서 초기화)
        self.current_step = 0
        self.cash = initial_balance
        self.shares = 0
        self.portfolio_value = initial_balance
        self.peak_value = initial_balance
        self.trades = []

    def reset(self, seed=None, options=None):
        """에피소드 초기화."""
        super().reset(seed=seed)

        self.current_step = 0
        self.cash = self.initial_balance
        self.shares = 0
        self.portfolio_value = self.initial_balance
        self.peak_value = self.initial_balance
        self.trades = []

        obs = self._get_observation()
        info = self._get_info()
        return obs, info

    def step(self, action: int):
        """한 스텝 진행.

        Args:
            action: 0=HOLD, 1=BUY, 2=SELL

        Returns:
            (observation, reward, terminated, truncated, info)
        """
        prev_value = self.portfolio_value
        current_price = self.close_prices[self.current_step]
        fee_paid = 0.0

        # 매매 실행
        if action == self.BUY and self.cash > 0 and current_price > 0:
            # 전액 매수
            max_shares = int(self.cash / (current_price * (1 + self.transaction_fee)))
            if max_shares > 0:
                cost = max_shares * current_price
                fee = cost * self.transaction_fee
                self.cash -= (cost + fee)
                self.shares += max_shares
                fee_paid = fee
                self.trades.append(("BUY", self.current_step, current_price, max_shares))

        elif action == self.SELL and self.shares > 0 and current_price > 0:
            # 전량 매도
            revenue = self.shares * current_price
            fee = revenue * self.transaction_fee
            tax = revenue * self.tax_rate
            self.cash += (revenue - fee - tax)
            fee_paid = fee + tax
            self.trades.append(("SELL", self.current_step, current_price, self.shares))
            self.shares = 0

        # 다음 스텝으로 이동
        self.current_step += 1
        terminated = self.current_step >= self.n_steps - 1

        # 포트폴리오 가치 업데이트
        if not terminated:
            next_price = self.close_prices[self.current_step]
        else:
            next_price = current_price

        self.portfolio_value = self.cash + self.shares * next_price
        self.peak_value = max(self.peak_value, self.portfolio_value)

        # 보상 계산
        reward = self._compute_reward(prev_value, fee_paid)

        # 파산 체크 (-50% 이하)
        if self.portfolio_value <= self.initial_balance * 0.5:
            terminated = True
            reward -= 1.0  # 파산 페널티

        obs = self._get_observation() if not terminated else np.zeros(
            self.observation_space.shape, dtype=np.float32,
        )
        info = self._get_info()

        return obs, reward, terminated, False, info

    def _get_observation(self) -> np.ndarray:
        """현재 관찰 벡터 구성."""
        features = self.feature_array[self.current_step]

        # 포지션 정보
        total = self.portfolio_value if self.portfolio_value > 0 else 1.0
        cash_ratio = np.float32(self.cash / total)
        stock_ratio = np.float32(1.0 - cash_ratio)

        return np.concatenate([features, [cash_ratio, stock_ratio]])

    def _compute_reward(self, prev_value: float, fee_paid: float) -> float:
        """보상 계산: 포트폴리오 수익률 - 거래비용 - 낙폭 페널티."""
        # 일간 수익률
        portfolio_return = (self.portfolio_value / prev_value - 1.0) if prev_value > 0 else 0.0

        # 거래비용 페널티
        fee_penalty = fee_paid / prev_value if prev_value > 0 else 0.0

        # 최대낙폭 페널티 (20% 초과 시)
        drawdown = (self.peak_value - self.portfolio_value) / self.peak_value if self.peak_value > 0 else 0.0
        drawdown_penalty = max(0.0, drawdown - 0.2) * 0.5

        reward = portfolio_return - fee_penalty - drawdown_penalty

        return float(reward)

    def _get_info(self) -> dict:
        """에피소드 정보."""
        return {
            "portfolio_value": self.portfolio_value,
            "cash": self.cash,
            "shares": self.shares,
            "total_return": (self.portfolio_value / self.initial_balance - 1.0),
            "peak_value": self.peak_value,
            "drawdown": (self.peak_value - self.portfolio_value) / self.peak_value if self.peak_value > 0 else 0.0,
            "n_trades": len(self.trades),
            "step": self.current_step,
        }

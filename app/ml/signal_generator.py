"""
시그널 생성기
=============

모델 예측 결과를 BUY / SELL / HOLD 시그널로 변환
"""


def generate_signal(
    probability_up: float,
    predicted_return: float = None,
    buy_threshold: float = 0.6,
    sell_threshold: float = 0.4,
) -> tuple[str, float]:
    """
    예측 결과를 트레이딩 시그널로 변환

    Args:
        probability_up: 상승 확률 (0.0 ~ 1.0)
        predicted_return: 예측 수익률 (회귀 모델, optional)
        buy_threshold: BUY 기준 확률 (default 0.6)
        sell_threshold: SELL 기준 확률 (default 0.4)

    Returns:
        (signal, confidence) — signal: "BUY"/"SELL"/"HOLD", confidence: 0.0~1.0
    """
    if probability_up >= buy_threshold:
        signal = "BUY"
    elif probability_up <= sell_threshold:
        signal = "SELL"
    else:
        signal = "HOLD"

    confidence = round(max(probability_up, 1 - probability_up), 4)

    return signal, confidence

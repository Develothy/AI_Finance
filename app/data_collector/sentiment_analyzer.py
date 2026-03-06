"""
한국어 뉴스 센티먼트 분석기 (Phase 4)
====================================

snunlp/KR-FinBert-SC (Korean Financial Sentiment Classification)
- 3-class: positive / negative / neutral
- transformers pipeline, lazy loading, CPU inference
"""

from typing import Optional

from core import get_logger, ModelLoadError, InferenceError

logger = get_logger("sentiment_analyzer")

DEFAULT_MODEL = "snunlp/KR-FinBert-SC"

# label → normalized score 매핑
_LABEL_SCORE = {
    "positive": 1.0,
    "negative": -1.0,
    "neutral": 0.0,
}


class SentimentAnalyzer:
    # 한국어 금융 뉴스 센티먼트 분석기 (lazy loading singleton)

    _instance: Optional["SentimentAnalyzer"] = None

    @classmethod
    def get_instance(cls) -> "SentimentAnalyzer":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name
        self._pipeline = None

    def _load_model(self):
        # 첫 호출 시 모델 로드
        if self._pipeline is not None:
            return

        try:
            from transformers import pipeline as hf_pipeline

            logger.info(f"센티먼트 모델 로딩: {self.model_name}", "_load_model")
            self._pipeline = hf_pipeline(
                "sentiment-analysis",
                model=self.model_name,
                tokenizer=self.model_name,
                max_length=512,
                truncation=True,
            )
            logger.info("센티먼트 모델 로드 완료", "_load_model")
        except Exception as e:
            raise ModelLoadError(f"센티먼트 모델 로드 실패: {e}")

    def analyze(self, texts: list[str], batch_size: int = 32) -> list[dict]:
        """
        텍스트 배치 분석

        Returns:
            [{"label": "positive", "confidence": 0.95, "sentiment_score": 0.95}, ...]
        """
        self._load_model()

        if not texts:
            return []

        try:
            outputs = self._pipeline(texts, batch_size=batch_size)

            results = []
            for output in outputs:
                label = output["label"].lower()
                confidence = output["score"]
                sentiment_score = _LABEL_SCORE.get(label, 0.0) * confidence
                results.append({
                    "label": label,
                    "confidence": round(confidence, 4),
                    "sentiment_score": round(sentiment_score, 4),
                })
            return results
        except Exception as e:
            raise InferenceError(f"센티먼트 추론 실패: {e}")

    def analyze_single(self, text: str) -> dict:
        # 단일 텍스트 분석
        results = self.analyze([text])
        return results[0] if results else {
            "label": "neutral",
            "confidence": 0.0,
            "sentiment_score": 0.0,
        }
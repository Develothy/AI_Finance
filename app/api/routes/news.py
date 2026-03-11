"""
뉴스 센티먼트 API 엔드포인트 (Phase 4)
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    NewsCollectRequest,
    NewsCollectResponse,
    NewsArticleResponse,
    NewsSentimentSummaryResponse,
)
from services import news_service

router = APIRouter(prefix="/news", tags=["News"])


@router.post("/collect", response_model=NewsCollectResponse)
def collect_news(req: NewsCollectRequest):
    result = news_service.collect(
        market=req.market,
        codes=req.codes,
        include_market_news=req.include_market_news,
        max_items_per_code=req.max_items_per_code,
    )
    return NewsCollectResponse(**result)


@router.get("/articles", response_model=list[NewsArticleResponse])
def get_articles(
    code: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
):
    rows = news_service.get_articles(code, start_date, end_date, limit)
    return [NewsArticleResponse(**r) for r in rows]


@router.get("/sentiment/{code}", response_model=NewsSentimentSummaryResponse)
def get_sentiment(code: str):
    # 종목 최신 센티먼트 요약
    result = news_service.get_sentiment_summary(code)
    if not result:
        raise HTTPException(status_code=404, detail=f"센티먼트 데이터 없음: {code}")
    return NewsSentimentSummaryResponse(**result)
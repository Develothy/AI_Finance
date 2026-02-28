"""
퀀트 플랫폼 API 서버
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import stock_router, indicator_router, admin_router, ml_router, fundamental_router
from db import database
import models  # noqa: F401 — ModelBase에 모든 모델 등록

logger = logging.getLogger("api")

# 앱 시작 시 테이블 자동 생성
database.create_tables()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 스케줄러 관리"""
    from data_collector import DataScheduler, SCHEDULER_AVAILABLE

    if SCHEDULER_AVAILABLE:
        scheduler = DataScheduler.get_instance()
        scheduler.load_jobs_from_db()
        scheduler.start()
        logger.info("스케줄러 시작 (API 서버 내장)")
    yield
    if SCHEDULER_AVAILABLE:
        scheduler = DataScheduler.get_instance()
        scheduler.stop()
        logger.info("스케줄러 종료")


app = FastAPI(
    title="퀀트 플랫폼 API",
    description="주식 데이터 수집 및 조회 API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 전역 예외 핸들러
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


# 라우터 등록
app.include_router(stock_router)
app.include_router(indicator_router)
app.include_router(admin_router)
app.include_router(ml_router)
app.include_router(fundamental_router)


@app.get("/")
def root():
    """헬스 체크"""
    return {"status": "ok", "message": "퀀트 플랫폼 API"}


# ============================================================
# 실행
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

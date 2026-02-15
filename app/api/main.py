"""
퀀트 플랫폼 API 서버
"""

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import stock_router, indicator_router

logger = logging.getLogger("api")

app = FastAPI(
    title="퀀트 플랫폼 API",
    description="주식 데이터 수집 및 조회 API",
    version="1.0.0"
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
